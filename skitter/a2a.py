"""A2A-over-MQTT protocol: message types, topic scheme, validation, and requester helper.

Everything A2A-protocol-specific lives here. The mqtt module handles pure
MQTT v5 transport (connection, properties); this module handles the A2A
layer on top.
"""

import asyncio
import json
import logging
import os
import random
import uuid
from dataclasses import dataclass, field
from typing import TYPE_CHECKING

from skitter.mqtt import (
    get_correlation_data,
    get_response_topic,
    make_properties,
    mqtt_client_kwargs,
)

if TYPE_CHECKING:
    import aiomqtt

# --- A2A namespace parameters ---

A2A_ORG = os.environ.get("SKITTER_A2A_ORG", "skitter")
A2A_UNIT = os.environ.get("SKITTER_A2A_UNIT", "default")

# --- Topic builders ---

_PREFIX = "$a2a/v1"


def topic_discovery(agent_id: str) -> str:
    """Retained Agent Card: $a2a/v1/discovery/{org}/{unit}/{agent_id}"""
    return f"{_PREFIX}/discovery/{A2A_ORG}/{A2A_UNIT}/{agent_id}"


def topic_discovery_wildcard(org: str = "", unit: str = "") -> str:
    """Wildcard for all discovery cards: $a2a/v1/discovery/{org}/{unit}/+"""
    return f"{_PREFIX}/discovery/{org or A2A_ORG}/{unit or A2A_UNIT}/+"


def topic_request(agent_id: str) -> str:
    """Request topic: $a2a/v1/request/{org}/{unit}/{agent_id}"""
    return f"{_PREFIX}/request/{A2A_ORG}/{A2A_UNIT}/{agent_id}"


def topic_reply(agent_id: str, suffix: str) -> str:
    """Reply topic: $a2a/v1/reply/{org}/{unit}/{agent_id}/{suffix}"""
    return f"{_PREFIX}/reply/{A2A_ORG}/{A2A_UNIT}/{agent_id}/{suffix}"


def topic_a2a_event(agent_id: str) -> str:
    """A2A event topic: $a2a/v1/event/{org}/{unit}/{agent_id}"""
    return f"{_PREFIX}/event/{A2A_ORG}/{A2A_UNIT}/{agent_id}"


def topic_coordinator_lock() -> str:
    """Coordinator lock topic: $a2a/v1/lock/{org}/{unit}/coordinator"""
    return f"{_PREFIX}/lock/{A2A_ORG}/{A2A_UNIT}/coordinator"


# --- A2A error codes (section: Mandatory Binding-Specific Error Mapping) ---

A2A_REQUEST_EXPIRED = -32003
A2A_RESPONDER_UNAVAILABLE = -32004
A2A_TRANSPORT_PROTOCOL_ERROR = -32005

_A2A_ERROR_MAP = {
    A2A_REQUEST_EXPIRED: "request_expired",
    A2A_RESPONDER_UNAVAILABLE: "responder_unavailable",
    A2A_TRANSPORT_PROTOCOL_ERROR: "transport_protocol_error",
}


def make_a2a_error(code: int, message: str) -> dict:
    """Build an A2A error dict with data.a2a_error for transport error codes."""
    error: dict = {"code": code, "message": message}
    if code in _A2A_ERROR_MAP:
        error["data"] = {"a2a_error": _A2A_ERROR_MAP[code]}
    return error


# --- A2A response wrapper ---


@dataclass
class A2AResponse:
    """JSON-RPC 2.0 response wrapper for A2A messages."""

    id: str
    result: dict | None = None
    error: dict | None = None

    def to_json(self) -> str:
        d: dict = {"jsonrpc": "2.0", "id": self.id}
        if self.error is not None:
            d["error"] = self.error
        else:
            d["result"] = self.result or {}
        return json.dumps(d)


# --- Status events ---


def make_status_event(
    request_id: str,
    task_id: str,
    state: str,
    message: str = "",
    context_id: str = "",
    *,
    metadata: dict | None = None,
) -> str:
    """Build a TaskStatusUpdateEvent JSON-RPC response.

    Per A2A v1.0.0 proto: TaskStatusUpdateEvent carries task_id, context_id,
    status (TaskStatus), and event-level metadata. TaskStatus contains state,
    an optional Message object, and timestamp.

    message (str) is wrapped into a proper A2A Message object with role=agent.
    metadata (dict) is placed on the event envelope (not inside status).
    """
    status: dict = {"state": state}
    if message:
        status["message"] = {
            "role": "agent",
            "parts": [{"type": "text", "text": message}],
        }
    result: dict = {
        "type": "TaskStatusUpdateEvent",
        "taskId": task_id,
        "contextId": context_id,
        "status": status,
    }
    if metadata:
        result["metadata"] = metadata
    return json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result})


def make_artifact_event(
    request_id: str,
    task_id: str,
    artifact_text: str,
    context_id: str = "",
    *,
    artifact_id: str = "",
    metadata: dict | None = None,
    last_chunk: bool = True,
) -> str:
    """Build a TaskArtifactUpdateEvent JSON-RPC response.

    Per A2A v1.0.0 proto: delivers result content as an Artifact object,
    separate from status updates.
    """
    result: dict = {
        "type": "TaskArtifactUpdateEvent",
        "taskId": task_id,
        "contextId": context_id,
        "artifact": {
            "artifactId": artifact_id or uuid.uuid4().hex[:12],
            "parts": [{"type": "text", "text": artifact_text}],
        },
        "lastChunk": last_chunk,
    }
    if metadata:
        result["metadata"] = metadata
    return json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result})


# --- Reply classification ---

REPLY_SUBMITTED = "submitted"
REPLY_TEXT = "text"
REPLY_TOOL = "tool_use"
REPLY_ARTIFACT = "artifact"
REPLY_TERMINAL = "terminal"
REPLY_INPUT_REQUIRED = "input_required"
REPLY_FAILED = "failed"
REPLY_ERROR = "error"


def _extract_message_text(status: dict) -> str:
    """Extract text from a TaskStatus.message (Message object or plain string)."""
    msg = status.get("message", "")
    if isinstance(msg, dict):
        parts = msg.get("parts", [])
        return parts[0].get("text", "") if parts else ""
    return msg


def classify_reply(data: dict) -> tuple[str, str]:
    """Classify an A2A reply message. Returns (kind, content).

    kind is one of: REPLY_TEXT, REPLY_TOOL, REPLY_ARTIFACT, REPLY_TERMINAL,
    REPLY_INPUT_REQUIRED, REPLY_FAILED, REPLY_ERROR, or "" for unrecognized.

    REPLY_ARTIFACT is returned for TaskArtifactUpdateEvent (content = artifact text).
    REPLY_TERMINAL is returned for completed TaskStatusUpdateEvent (content = message text).
    REPLY_INPUT_REQUIRED is returned for input-required/auth-required (stream-final, task not terminal).
    REPLY_FAILED is returned for failed/canceled/rejected tasks (content = error message).
    """
    if "error" in data:
        err = data["error"]
        return REPLY_ERROR, err.get("message", str(err))

    result = data.get("result", {})
    event_type = result.get("type", "")

    if event_type == "TaskArtifactUpdateEvent":
        parts = result.get("artifact", {}).get("parts", [])
        text = parts[0].get("text", "") if parts else ""
        return REPLY_ARTIFACT, text

    if event_type != "TaskStatusUpdateEvent":
        return "", ""

    status = result.get("status", {})
    state = status.get("state", "")
    # Event-level metadata (A2A v1.0.0); fall back to status.metadata for compat
    meta = result.get("metadata") or status.get("metadata") or {}

    if state == "submitted":
        task_id = result.get("taskId", "")
        return REPLY_SUBMITTED, task_id

    if state == "working":
        message = _extract_message_text(status)
        msg_type = meta.get("type", "text")
        if msg_type == "tool_use":
            return REPLY_TOOL, message
        return REPLY_TEXT, message

    if state == "completed":
        message = _extract_message_text(status)
        return REPLY_TERMINAL, message

    # Interrupted states: stream-final per spec, but task is not terminal
    if state in ("input-required", "auth-required"):
        message = _extract_message_text(status)
        return REPLY_INPUT_REQUIRED, message or state

    if state in ("failed", "canceled", "rejected"):
        message = _extract_message_text(status)
        return REPLY_FAILED, message or f"Task {state}"

    return "", ""


# --- Core messaging types ---


@dataclass
class A2ARequest:
    """JSON-RPC 2.0 request for A2A message/send."""

    text: str
    request_id: str
    task_id: str | None = None
    context_id: str | None = None
    sender: str = ""
    variables: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.task_id is None:
            self.task_id = str(uuid.uuid4())
        if self.context_id is None:
            self.context_id = str(uuid.uuid4())

    def to_json(self) -> str:
        metadata: dict = {}
        if self.sender:
            metadata["sender"] = self.sender
        if self.variables:
            metadata["variables"] = self.variables
        message: dict = {
            "role": "user",
            "parts": [{"type": "text", "text": self.text}],
            "taskId": self.task_id,
            "contextId": self.context_id,
        }
        params: dict = {"message": message}
        if metadata:
            params["metadata"] = metadata
        return json.dumps(
            {
                "jsonrpc": "2.0",
                "id": self.request_id,
                "method": "message/send",
                "params": params,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "A2ARequest":
        import time

        d = json.loads(data)
        params = d.get("params", {})
        message = params.get("message", {})
        metadata = params.get("metadata", {})
        parts = message.get("parts", [])
        text = parts[0].get("text", "") if parts else ""
        task_id = message.get("taskId", "")
        context_id = message.get("contextId") or None
        return cls(
            text=text,
            request_id=d.get("id", f"req-{time.time_ns()}"),
            task_id=task_id,
            context_id=context_id,
            sender=metadata.get("sender", ""),
            variables=metadata.get("variables", {}),
        )


@dataclass
class TaskTarget:
    """A2A agent address for task dispatch."""

    agent: str  # A2A agent address: {org}/{unit}/{agent_id}
    mqtt_host: str = ""  # broker host (empty = same broker as coordinator)
    mqtt_port: int = 8883


# --- Requester retry/timeout profile ---

REPLY_FIRST_TIMEOUT = float(os.environ.get("SKITTER_REPLY_FIRST_TIMEOUT", "15.0"))
STREAM_IDLE_TIMEOUT = float(os.environ.get("SKITTER_STREAM_IDLE_TIMEOUT", "30.0"))
MAX_ATTEMPTS = int(os.environ.get("SKITTER_MAX_ATTEMPTS", "3"))
_BACKOFF_BASE = [1.0, 2.0, 4.0]
_JITTER_FACTOR = 0.2


def _backoff_delay(attempt: int) -> float:
    """Exponential backoff with jitter for retry attempt (0-indexed)."""
    base = _BACKOFF_BASE[min(attempt, len(_BACKOFF_BASE) - 1)]
    jitter = base * _JITTER_FACTOR * random.random()
    return base + jitter


async def send_and_wait(
    request_topic: str,
    payload: str,
    correlation_id: str,
    on_reply: "callable",
    *,
    wait: bool = True,
    label: str = "",
) -> None:
    """Publish a request and stream replies until terminal or timeout.

    Implements the A2A requester retry/timeout profile: retries with new
    Correlation Data on first-reply timeout, validates Correlation Data
    on incoming messages.

    on_reply(kind, content) is called for each classified reply message.
    It should return True to stop listening (e.g. on terminal/error).
    """
    import aiomqtt

    mqtt_session = uuid.uuid4().hex[:12]
    reply_t = topic_reply("cli", mqtt_session)

    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{A2A_ORG}/{A2A_UNIT}/cli-{mqtt_session}",
        ),
    ) as client:
        if wait:
            await client.subscribe(reply_t, qos=1)

        if not wait:
            props = make_properties(
                response_topic=reply_t,
                correlation_data=correlation_id,
            )
            await client.publish(request_topic, payload, qos=1, properties=props)
            return

        got_first_reply = False
        current_correlation = correlation_id
        # Accept replies matching any correlation from prior attempts,
        # since the responder echoes the correlation from the first
        # request it received (dedup drops retries by Task.id).
        valid_correlations: set[str] = {correlation_id}

        for attempt in range(MAX_ATTEMPTS):
            if attempt > 0:
                delay = _backoff_delay(attempt - 1)
                await asyncio.sleep(delay)
                # New Correlation Data per retry, same Task.id in payload
                current_correlation = uuid.uuid4().hex[:16]
                valid_correlations.add(current_correlation)

            props = make_properties(
                response_topic=reply_t,
                correlation_data=current_correlation,
            )
            await client.publish(request_topic, payload, qos=1, properties=props)

            try:
                timeout = REPLY_FIRST_TIMEOUT
                while True:
                    async with asyncio.timeout(timeout):
                        async for mqtt_msg in client.messages:
                            raw = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                            if not raw:
                                continue

                            # Validate Correlation Data against all attempts
                            msg_corr = get_correlation_data(mqtt_msg)
                            if msg_corr not in valid_correlations:
                                continue

                            try:
                                data = json.loads(raw)
                            except Exception:
                                continue

                            kind, content = classify_reply(data)
                            if not kind:
                                continue

                            got_first_reply = True
                            timeout = STREAM_IDLE_TIMEOUT

                            if on_reply(kind, content):
                                return
                            break  # reset timeout for next message
                    # Inner loop broke normally (processed a message), continue
            except TimeoutError:
                if got_first_reply:
                    # Stream stalled after at least one reply
                    on_reply("timeout", "")
                    return
                # No reply at all: retry (next attempt)
                continue

        # All attempts exhausted
        on_reply("timeout", "")


# --- Shared request validation ---


async def validate_a2a_request(
    mqtt_msg: "aiomqtt.Message",
    client: "aiomqtt.Client",
    *,
    log: logging.Logger,
) -> tuple[A2ARequest, str, str] | None:
    """Parse and validate an inbound A2A request (MQTT v5 props + Task.id).

    Returns (request, reply_topic, correlation) on success.
    Returns None on failure (sends error response automatically).
    """
    payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
    if not payload:
        return None

    reply_topic = get_response_topic(mqtt_msg) or ""
    correlation = get_correlation_data(mqtt_msg) or ""

    if not reply_topic or not correlation:
        log.warning("Request missing Response Topic or Correlation Data")
        if reply_topic:
            resp = A2AResponse(
                id=correlation,
                error=make_a2a_error(
                    A2A_TRANSPORT_PROTOCOL_ERROR,
                    "Missing MQTT v5 Response Topic or Correlation Data",
                ),
            )
            props = make_properties(correlation_data=correlation)
            await client.publish(reply_topic, resp.to_json(), qos=1, properties=props)
        return None

    try:
        req = A2ARequest.from_json(payload)
    except Exception as e:
        log.error("Bad request JSON: %s", e)
        resp = A2AResponse(
            id=correlation,
            error=make_a2a_error(-32700, f"Parse error: {e}"),
        )
        props = make_properties(correlation_data=correlation)
        await client.publish(reply_topic, resp.to_json(), qos=1, properties=props)
        return None

    if not req.task_id:
        log.warning("Request missing Task.id")
        resp = A2AResponse(
            id=correlation,
            error=make_a2a_error(
                A2A_TRANSPORT_PROTOCOL_ERROR,
                "Missing required Task.id (message.taskId)",
            ),
        )
        props = make_properties(correlation_data=correlation)
        await client.publish(reply_topic, resp.to_json(), qos=1, properties=props)
        return None

    return req, reply_topic, correlation
