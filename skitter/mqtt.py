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
