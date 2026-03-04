"""A2A-over-MQTT topic scheme and MQTT v5 helpers."""

import os

from dotenv import load_dotenv
from paho.mqtt.properties import Properties
from paho.mqtt.packettypes import PacketTypes

load_dotenv()

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))

# A2A namespace parameters
A2A_ORG = os.environ.get("SKITTER_A2A_ORG", "skitter")
A2A_UNIT = os.environ.get("SKITTER_A2A_UNIT", "default")

# --- Topic builders ---

_PREFIX = "$a2a/v1"


def topic_discovery(agent_id: str) -> str:
    """Retained Agent Card: $a2a/v1/discovery/{org}/{unit}/{agent_id}"""
    return f"{_PREFIX}/discovery/{A2A_ORG}/{A2A_UNIT}/{agent_id}"


def topic_request(agent_id: str) -> str:
    """Request topic: $a2a/v1/request/{org}/{unit}/{agent_id}"""
    return f"{_PREFIX}/request/{A2A_ORG}/{A2A_UNIT}/{agent_id}"


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


def topic_chain_result(session_id: str, source_task_id: str) -> str:
    """Retained chain result: $a2a/v1/state/{org}/{unit}/chain/{session_id}/{source_task_id}"""
    return f"{_PREFIX}/state/{A2A_ORG}/{A2A_UNIT}/chain/{session_id}/{source_task_id}"


def topic_chain_wildcard() -> str:
    """Wildcard for chain results: $a2a/v1/state/{org}/{unit}/chain/+/+"""
    return f"{_PREFIX}/state/{A2A_ORG}/{A2A_UNIT}/chain/+/+"


def topic_state_dispatch(task_id: str) -> str:
    """Retained task dispatch: $a2a/v1/state/{org}/{unit}/dispatch/{task_id}"""
    return f"{_PREFIX}/state/{A2A_ORG}/{A2A_UNIT}/dispatch/{task_id}"


def topic_control_reload() -> str:
    """Reload signal: $a2a/v1/control/{org}/{unit}/reload"""
    return f"{_PREFIX}/control/{A2A_ORG}/{A2A_UNIT}/reload"


def topic_state_session(session_id: str) -> str:
    """Retained session: $a2a/v1/state/{org}/{unit}/sessions/{session_id}"""
    return f"{_PREFIX}/state/{A2A_ORG}/{A2A_UNIT}/sessions/{session_id}"


def topic_state_session_wildcard() -> str:
    """Wildcard for sessions: $a2a/v1/state/{org}/{unit}/sessions/+"""
    return f"{_PREFIX}/state/{A2A_ORG}/{A2A_UNIT}/sessions/+"


def topic_state_usage(session_id: str, task_id: str) -> str:
    """Usage tracking: $a2a/v1/state/{org}/{unit}/usage/{session_id}/{task_id}"""
    return f"{_PREFIX}/state/{A2A_ORG}/{A2A_UNIT}/usage/{session_id}/{task_id}"


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
