"""A2A-over-MQTT topic scheme and MQTT v5 helpers.

Topics follow the A2A spec: $a2a/v1/{method}/{org_id}/{unit_id}/{agent_id}
with application-defined suffixes after agent_id for session/task scoping.
"""

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
