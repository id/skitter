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


def topic_discovery_wildcard() -> str:
    """Wildcard for all Agent Cards: $a2a/v1/discovery/{org}/{unit}/+"""
    return f"{_PREFIX}/discovery/{A2A_ORG}/{A2A_UNIT}/+"


def topic_request(agent_id: str) -> str:
    """Request topic: $a2a/v1/request/{org}/{unit}/{agent_id}"""
    return f"{_PREFIX}/request/{A2A_ORG}/{A2A_UNIT}/{agent_id}"


def topic_request_cancel(agent_id: str) -> str:
    """Cancel topic: $a2a/v1/request/{org}/{unit}/{agent_id}/cancel"""
    return f"{_PREFIX}/request/{A2A_ORG}/{A2A_UNIT}/{agent_id}/cancel"


def topic_reply(agent_id: str, suffix: str) -> str:
    """Reply topic: $a2a/v1/reply/{org}/{unit}/{agent_id}/{suffix}"""
    return f"{_PREFIX}/reply/{A2A_ORG}/{A2A_UNIT}/{agent_id}/{suffix}"


def topic_event_worker(task_id: str) -> str:
    """Worker liveness (LWT): $a2a/v1/event/{org}/{unit}/workers/{task_id}"""
    return f"{_PREFIX}/event/{A2A_ORG}/{A2A_UNIT}/workers/{task_id}"


def topic_event_worker_wildcard() -> str:
    """Wildcard for worker events: $a2a/v1/event/{org}/{unit}/workers/+"""
    return f"{_PREFIX}/event/{A2A_ORG}/{A2A_UNIT}/workers/+"


def topic_state_job(chat_id: str) -> str:
    """Retained job spec: $a2a/v1/state/{org}/{unit}/jobs/{chat_id}"""
    return f"{_PREFIX}/state/{A2A_ORG}/{A2A_UNIT}/jobs/{chat_id}"


def topic_state_job_wildcard() -> str:
    """Wildcard for job specs: $a2a/v1/state/{org}/{unit}/jobs/+"""
    return f"{_PREFIX}/state/{A2A_ORG}/{A2A_UNIT}/jobs/+"


def topic_state_usage(chat_id: str, task_id: str) -> str:
    """Usage tracking: $a2a/v1/state/{org}/{unit}/usage/{chat_id}/{task_id}"""
    return f"{_PREFIX}/state/{A2A_ORG}/{A2A_UNIT}/usage/{chat_id}/{task_id}"


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
