"""MQTT v5 transport helpers: connection, properties, extraction."""

import os
import ssl

from dotenv import load_dotenv
from paho.mqtt.packettypes import PacketTypes
from paho.mqtt.properties import Properties

import aiomqtt

load_dotenv()

MQTT_HOST = os.environ.get("MQTT_HOST", "localhost")
MQTT_PORT = int(os.environ.get("MQTT_PORT", "1883"))
MQTT_TLS = os.environ.get("MQTT_TLS", "") == "1"
MQTT_USER = os.environ.get("MQTT_USER", "")
MQTT_PASS = os.environ.get("MQTT_PASS", "")


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
