"""A2A-over-MQTT topic scheme and MQTT v5 helpers.

Topics follow the A2A spec: $a2a/v1/{method}/{org_id}/{unit_id}/{agent_id}
with application-defined suffixes after agent_id for session/task scoping.
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


def topic_event(agent_id: str, event_type: str) -> str:
    """A2A agent event: $a2a/v1/event/{org}/{unit}/{agent_id}/{event_type}"""
    return f"{_PREFIX}/event/{A2A_ORG}/{A2A_UNIT}/{agent_id}/{event_type}"


def topic_event_wildcard() -> str:
    """Wildcard for all agent events: $a2a/v1/event/{org}/{unit}/+/+"""
    return f"{_PREFIX}/event/{A2A_ORG}/{A2A_UNIT}/+/+"


def topic_dead_wildcard() -> str:
    """Wildcard for worker dead events: $a2a/v1/event/{org}/{unit}/+/dead"""
    return f"{_PREFIX}/event/{A2A_ORG}/{A2A_UNIT}/+/dead"


# --- Coordination state (suffixed event topics, retained) ---


def topic_session(session_id: str) -> str:
    """Retained session: $a2a/v1/event/{org}/{unit}/supervisor/session/{sid}"""
    return f"{_PREFIX}/event/{A2A_ORG}/{A2A_UNIT}/supervisor/session/{session_id}"


def topic_session_wildcard() -> str:
    """Wildcard for sessions: $a2a/v1/event/{org}/{unit}/supervisor/session/+"""
    return f"{_PREFIX}/event/{A2A_ORG}/{A2A_UNIT}/supervisor/session/+"


def topic_chain_result(agent_id: str, session_id: str, task_id: str) -> str:
    """Retained chain result: $a2a/v1/event/{org}/{unit}/{agent_id}/chain-result/{sid}/{tid}"""
    return f"{_PREFIX}/event/{A2A_ORG}/{A2A_UNIT}/{agent_id}/chain-result/{session_id}/{task_id}"


def topic_task_status(agent_id: str, session_id: str, task_id: str) -> str:
    """Retained task status: $a2a/v1/event/{org}/{unit}/{agent_id}/task-status/{sid}/{tid}"""
    return f"{_PREFIX}/event/{A2A_ORG}/{A2A_UNIT}/{agent_id}/task-status/{session_id}/{task_id}"


def topic_task_status_wildcard() -> str:
    """Wildcard for task statuses: $a2a/v1/event/{org}/{unit}/+/task-status/+/+"""
    return f"{_PREFIX}/event/{A2A_ORG}/{A2A_UNIT}/+/task-status/+/+"


def topic_usage(agent_id: str, session_id: str, task_id: str) -> str:
    """Usage tracking: $a2a/v1/event/{org}/{unit}/{agent_id}/usage/{sid}/{tid}"""
    return (
        f"{_PREFIX}/event/{A2A_ORG}/{A2A_UNIT}/{agent_id}/usage/{session_id}/{task_id}"
    )


def topic_reload() -> str:
    """Reload signal: $a2a/v1/request/{org}/{unit}/supervisor/reload"""
    return f"{_PREFIX}/request/{A2A_ORG}/{A2A_UNIT}/supervisor/reload"


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
    session_id: str,
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
            correlation_data=session_id,
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
