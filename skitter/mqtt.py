"""MQTT topic scheme and v5 helpers.

A2A namespace: $a2a/v1/{method}/{org}/{unit}/{agent_id} — public protocol.
Skitter namespace: skitter/{type}/... — internal coordination (retained).
"""

import asyncio
import json
import os
import ssl
import uuid

import aiomqtt
from dotenv import load_dotenv
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

load_dotenv()

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TLS = os.environ.get("MQTT_TLS", "") == "1"
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ.get("MQTT_PASS", "")

# A2A namespace parameters
A2A_ORG = os.environ.get("SKITTER_A2A_ORG", "skitter")
A2A_UNIT = os.environ.get("SKITTER_A2A_UNIT", "default")


def mqtt_client_kwargs(**overrides) -> dict:
    """Common connection kwargs for aiomqtt.Client.

    Usage: ``async with aiomqtt.Client(**mqtt_client_kwargs(identifier=...))``
    """
    kwargs: dict = {
        "hostname": MQTT_HOST,
        "port": MQTT_PORT,
        "protocol": aiomqtt.ProtocolVersion.V5,
    }
    if MQTT_TLS:
        kwargs["tls_context"] = ssl.create_default_context()
    if MQTT_USER:
        kwargs["username"] = MQTT_USER
        kwargs["password"] = MQTT_PASS
    kwargs.update(overrides)
    return kwargs


# --- Topic builders ---

_PREFIX = "$a2a/v1"


def topic_discovery(agent_id: str) -> str:
    """Retained Agent Card: $a2a/v1/discovery/{org}/{unit}/{agent_id}"""
    return f"{_PREFIX}/discovery/{A2A_ORG}/{A2A_UNIT}/{agent_id}"


def topic_request(agent_id: str) -> str:
    """Request topic: $a2a/v1/request/{org}/{unit}/{agent_id}"""
    return f"{_PREFIX}/request/{A2A_ORG}/{A2A_UNIT}/{agent_id}"


def topic_request_wildcard() -> str:
    """Wildcard for all agent requests: $a2a/v1/request/{org}/{unit}/+"""
    return f"{_PREFIX}/request/{A2A_ORG}/{A2A_UNIT}/+"


def topic_request_cancel(agent_id: str) -> str:
    """Cancel topic: $a2a/v1/request/{org}/{unit}/{agent_id}/cancel"""
    return f"{_PREFIX}/request/{A2A_ORG}/{A2A_UNIT}/{agent_id}/cancel"


def topic_reply(agent_id: str, suffix: str) -> str:
    """Reply topic: $a2a/v1/reply/{org}/{unit}/{agent_id}/{suffix}"""
    return f"{_PREFIX}/reply/{A2A_ORG}/{A2A_UNIT}/{agent_id}/{suffix}"


# --- Skitter internal topics (retained coordination state) ---

_SK = "skitter"


def topic_session(session_id: str) -> str:
    """Retained session: skitter/session/{sid}"""
    return f"{_SK}/session/{session_id}"


def topic_session_wildcard() -> str:
    """Wildcard for sessions: skitter/session/+"""
    return f"{_SK}/session/+"


def topic_result(workflow_id: str, task: str, session_id: str) -> str:
    """Retained task result: skitter/result/{wf}/{task}/{sid}"""
    return f"{_SK}/result/{workflow_id}/{task}/{session_id}"


def topic_result_wildcard() -> str:
    """Wildcard for results: skitter/result/+/+/+"""
    return f"{_SK}/result/+/+/+"


def topic_status(workflow_id: str, task: str, session_id: str) -> str:
    """Retained task status: skitter/status/{wf}/{task}/{sid}"""
    return f"{_SK}/status/{workflow_id}/{task}/{session_id}"


def topic_status_wildcard() -> str:
    """Wildcard for task statuses: skitter/status/+/+/+"""
    return f"{_SK}/status/+/+/+"


def topic_usage(workflow_id: str, task: str, session_id: str) -> str:
    """Usage tracking: skitter/usage/{wf}/{task}/{sid}"""
    return f"{_SK}/usage/{workflow_id}/{task}/{session_id}"


def topic_event(agent_id: str, event_type: str) -> str:
    """Worker lifecycle: skitter/event/{agent}/{type}"""
    return f"{_SK}/event/{agent_id}/{event_type}"


def topic_dead_wildcard() -> str:
    """Wildcard for worker dead events: skitter/event/+/dead"""
    return f"{_SK}/event/+/dead"


def topic_reload() -> str:
    """Reload signal: skitter/control/reload"""
    return f"{_SK}/control/reload"


# --- MQTT v5 property helpers ---


def make_properties(
    response_topic: str | None = None,
    correlation_data: str | None = None,
) -> Properties:
    """Build MQTT v5 PUBLISH properties with Response Topic and/or Correlation Data."""
    props = Properties(PacketTypes.PUBLISH)
    if response_topic:
        props.ResponseTopic = response_topic
    if correlation_data:
        props.CorrelationData = correlation_data.encode("utf-8")
    return props


def get_correlation_data(msg) -> str | None:
    """Extract Correlation Data from an aiomqtt message (v5 properties)."""
    props = getattr(msg, "properties", None)
    if props is None:
        return None
    cd = getattr(props, "CorrelationData", None)
    if cd is None:
        return None
    return cd.decode("utf-8") if isinstance(cd, (bytes, bytearray)) else str(cd)


def get_response_topic(msg) -> str | None:
    """Extract Response Topic from an aiomqtt message (v5 properties)."""
    props = getattr(msg, "properties", None)
    if props is None:
        return None
    return getattr(props, "ResponseTopic", None)


# --- Send-and-wait helper (used by agent + workflow CLIs) ---


async def send_and_wait(
    request_topic: str,
    payload: str,
    correlation_id: str,
    on_reply: "callable",
    *,
    wait: bool = True,
    timeout: float = 600.0,
    label: str = "",
) -> None:
    """Publish a request and stream replies until terminal or timeout.

    on_reply(kind, content) is called for each classified reply message.
    It should return True to stop listening (e.g. on terminal/error).
    """
    from skitter.types import classify_reply

    mqtt_session = uuid.uuid4().hex[:12]
    reply_t = topic_reply("cli", mqtt_session)

    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{A2A_ORG}/{A2A_UNIT}/cli-{mqtt_session}",
        ),
    ) as client:
        if wait:
            await client.subscribe(reply_t, qos=1)

        props = make_properties(
            response_topic=reply_t,
            correlation_data=correlation_id,
        )
        await client.publish(request_topic, payload, qos=1, properties=props)

        if not wait:
            return

        try:
            async with asyncio.timeout(timeout):
                async for mqtt_msg in client.messages:
                    raw = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                    if not raw:
                        continue
                    try:
                        data = json.loads(raw)
                    except Exception:
                        continue
                    kind, content = classify_reply(data)
                    if on_reply(kind, content):
                        return
        except TimeoutError:
            on_reply("timeout", "")
