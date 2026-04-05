"""MQTT v5 transport helpers: connection, properties, extraction."""

from __future__ import annotations

from typing import TYPE_CHECKING

from dotenv import load_dotenv
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

import aiomqtt

if TYPE_CHECKING:
    from skitter.config import BrokerConfig

load_dotenv()


def mqtt_client_kwargs(*, broker: BrokerConfig | None = None, **overrides) -> dict:
    """Common connection kwargs for aiomqtt.Client.

    Resolves broker settings from the unified config (env vars override
    ``~/.skitter/config.yaml``), so host-side commands honour the broker
    URL written by ``skitter setup``.

    Pass *broker* explicitly to skip config loading (used by ``skitter setup``
    to validate just-collected settings before writing config).

    Usage: ``async with aiomqtt.Client(**mqtt_client_kwargs(identifier=...))``
    """
    if broker is None:
        from skitter.config import load_config

        broker = load_config().broker
    return broker.client_kwargs(**overrides)


# --- MQTT v5 property helpers ---


def _build_properties(
    packet_type: int,
    *,
    response_topic: str | None = None,
    correlation_data: str | None = None,
    user_properties: list[tuple[str, str]] | None = None,
) -> Properties:
    """Build MQTT v5 properties for any packet type.

    Note: response_topic and correlation_data are only valid for PUBLISH packets.
    """
    props = Properties(packet_type)
    if response_topic:
        props.ResponseTopic = response_topic
    if correlation_data:
        props.CorrelationData = correlation_data.encode("utf-8")
    if user_properties:
        props.UserProperty = user_properties
    return props


def make_properties(
    response_topic: str | None = None,
    correlation_data: str | None = None,
    user_properties: list[tuple[str, str]] | None = None,
) -> Properties:
    """Build MQTT v5 PUBLISH properties."""
    return _build_properties(
        PacketTypes.PUBLISH,
        response_topic=response_topic,
        correlation_data=correlation_data,
        user_properties=user_properties,
    )


def make_will_properties(
    user_properties: list[tuple[str, str]] | None = None,
) -> Properties:
    """Build MQTT v5 WILL properties (WILLMESSAGE packet type)."""
    return _build_properties(PacketTypes.WILLMESSAGE, user_properties=user_properties)


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


async def mqtt_roundtrip(
    timeout: float = 5.0, *, broker: BrokerConfig | None = None
) -> None:
    """Publish-subscribe round-trip test. Raises on failure."""
    import asyncio
    import uuid

    test_topic = f"skitter/healthcheck/{uuid.uuid4().hex[:8]}"
    test_payload = b"healthcheck-ping"

    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            broker=broker,
            identifier=f"skitter-healthcheck-{uuid.uuid4().hex[:6]}",
        ),
    ) as client:
        await client.subscribe(test_topic, qos=1)
        await client.publish(test_topic, test_payload, qos=1)
        async with asyncio.timeout(timeout):
            async for msg in client.messages:
                if msg.payload == test_payload:
                    return
        raise TimeoutError("MQTT round-trip: no response")


def get_user_property(msg, key: str) -> str | None:
    """Extract a single User Property value by key from an aiomqtt message."""
    props = getattr(msg, "properties", None)
    if props is None:
        return None
    user_props = getattr(props, "UserProperty", None)
    if not user_props:
        return None
    for k, v in user_props:
        if k == key:
            return v
    return None
