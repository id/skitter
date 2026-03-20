from dataclasses import dataclass, field
import json
import time
import uuid


# --- A2A error codes (§ Mandatory Binding-Specific Error Mapping) ---

A2A_REQUEST_EXPIRED = -32003
A2A_RESPONDER_UNAVAILABLE = -32004
A2A_TRANSPORT_PROTOCOL_ERROR = -32005

_A2A_ERROR_MAP = {
    -32003: "request_expired",
    -32004: "responder_unavailable",
    -32005: "transport_protocol_error",
}


def make_a2a_error(code: int, message: str) -> dict:
    """Build an A2A error dict with data.a2a_error for transport error codes."""
    error: dict = {"code": code, "message": message}
    if code in _A2A_ERROR_MAP:
        error["data"] = {"a2a_error": _A2A_ERROR_MAP[code]}
    return error


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


def make_status_event(
    request_id: str,
    task_id: str,
    state: str,
    message: str = "",
    message_type: str = "",
    artifact_text: str = "",
    task_name: str = "",
) -> str:
    """Build a TaskStatusUpdateEvent JSON-RPC response (A2A streaming reply).

    Used for both streaming updates (state="working") and terminal results
    (state="completed"/"failed"/"canceled").

    task_id is the A2A Task.id (= session_id). task_name is the internal
    workflow task name, carried in metadata for dashboard routing.
    """
    metadata: dict = {}
    if message_type:
        metadata["type"] = message_type
    if task_name:
        metadata["task_name"] = task_name
    status: dict = {"state": state}
    if message:
        status["message"] = message
    if metadata:
        status["metadata"] = metadata
    result: dict = {
        "type": "TaskStatusUpdateEvent",
        "taskId": task_id,
        "status": status,
    }
    if artifact_text:
        result["artifact"] = {
            "parts": [{"type": "text", "text": artifact_text}],
        }
    return json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result})


# --- Reply classification (used by all consumers) ---

REPLY_SUBMITTED = "submitted"
REPLY_TEXT = "text"
REPLY_TOOL = "tool_use"
REPLY_TERMINAL = "terminal"
REPLY_FAILED = "failed"
REPLY_ERROR = "error"


def classify_reply(data: dict) -> tuple[str, str]:
    """Classify an A2A reply message. Returns (kind, content).

    kind is one of: REPLY_TEXT, REPLY_TOOL, REPLY_TERMINAL, REPLY_FAILED,
    REPLY_ERROR, or "" for unrecognized messages.

    REPLY_TERMINAL is returned for completed tasks (content = artifact text).
    REPLY_FAILED is returned for failed/canceled/rejected tasks (content = error message).
    """
    if "error" in data:
        err = data["error"]
        return REPLY_ERROR, err.get("message", str(err))

    result = data.get("result", {})
    if result.get("type") != "TaskStatusUpdateEvent":
        return "", ""

    status = result.get("status", {})
    state = status.get("state", "")

    if state == "submitted":
        task_id = result.get("taskId", "")
        return REPLY_SUBMITTED, task_id

    if state == "working":
        message = status.get("message", "")
        metadata = status.get("metadata", {})
        msg_type = metadata.get("type", "text")
        if msg_type == "tool_use":
            return REPLY_TOOL, message
        return REPLY_TEXT, message

    def _artifact_text() -> str:
        parts = result.get("artifact", {}).get("parts", [])
        return parts[0].get("text", "") if parts else ""

    if state == "completed":
        return REPLY_TERMINAL, _artifact_text()

    if state in ("failed", "canceled", "rejected"):
        message = status.get("message", "") or _artifact_text()
        return REPLY_FAILED, message or f"Task {state}"

    return "", ""


# --- Core messaging types ---


@dataclass
class A2ARequest:
    """JSON-RPC 2.0 request for A2A message/send."""

    text: str
    request_id: str
    task_id: str | None = None
    sender: str = ""
    variables: dict[str, str] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if self.task_id is None:
            self.task_id = str(uuid.uuid4())

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
        d = json.loads(data)
        params = d.get("params", {})
        message = params.get("message", {})
        metadata = params.get("metadata", {})
        parts = message.get("parts", [])
        text = parts[0].get("text", "") if parts else ""
        task_id = message.get("taskId", "")
        return cls(
            text=text,
            request_id=d.get("id", f"req-{time.time_ns()}"),
            task_id=task_id,
            sender=metadata.get("sender", ""),
            variables=metadata.get("variables", {}),
        )


@dataclass
class TaskTarget:
    """A2A agent address for task dispatch."""

    agent: str  # A2A agent address: {org}/{unit}/{agent_id}
    mqtt_host: str = ""  # broker host (empty = same broker as coordinator)
    mqtt_port: int = 8883
