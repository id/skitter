"""MQTT topic scheme and v5 helpers.

A2A namespace: $a2a/v1/{method}/{org}/{unit}/{agent_id} — public protocol.
"""

import asyncio
import json
import os
import random
import ssl
import uuid

import aiomqtt
from dotenv import load_dotenv
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

from skitter.types import classify_reply

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

# Spec-mandated retry/timeout profile (configurable via env)
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

            # If we reach here with a first reply, we're streaming; unreachable
            # due to the return inside on_reply, but guard anyway
            if got_first_reply:
                return

        # All attempts exhausted
        on_reply("timeout", "")
