"""EMQX REST API client for publishing from serverless contexts.

Used by the HTTP-mode supervisor (CF Worker) which can't maintain
a persistent MQTT connection. Publishes messages via EMQX's
/api/v5/publish endpoint.
"""

import base64
import json
import logging
import os
from urllib.error import HTTPError
from urllib.request import Request, urlopen

log = logging.getLogger("skitter.emqx")

EMQX_API_URL = os.environ.get(
    "EMQX_API_URL", ""
)  # e.g. https://xyz.emqx.cloud:8443/api/v5
EMQX_API_KEY = os.environ.get("EMQX_API_KEY", "")
EMQX_API_SECRET = os.environ.get("EMQX_API_SECRET", "")


def _auth_header() -> str:
    """HTTP Basic auth header from API key + secret."""
    creds = base64.b64encode(f"{EMQX_API_KEY}:{EMQX_API_SECRET}".encode()).decode()
    return f"Basic {creds}"


def publish(
    topic: str,
    payload: str,
    qos: int = 1,
    retain: bool = False,
    properties: dict | None = None,
) -> None:
    """Publish a message via EMQX REST API (synchronous).

    Args:
        topic: MQTT topic.
        payload: Message payload (string).
        qos: QoS level (0, 1, or 2).
        retain: Whether to retain the message.
        properties: Optional MQTT v5 properties dict with keys like
            "response_topic" and "correlation_data".
    """
    if not EMQX_API_URL:
        raise RuntimeError("EMQX_API_URL not configured")

    body: dict = {
        "topic": topic,
        "payload": payload,
        "qos": qos,
        "retain": retain,
        "payload_encoding": "plain",
    }

    if properties:
        # EMQX REST API accepts v5 properties as a nested object
        mqtt_props: dict = {}
        if "response_topic" in properties:
            mqtt_props["Response-Topic"] = properties["response_topic"]
        if "correlation_data" in properties:
            mqtt_props["Correlation-Data"] = properties["correlation_data"]
        if mqtt_props:
            body["properties"] = mqtt_props

    url = f"{EMQX_API_URL.rstrip('/')}/publish"
    req = Request(
        url,
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": _auth_header(),
        },
        method="POST",
    )

    try:
        with urlopen(req, timeout=10):
            pass
    except HTTPError as e:
        log.error("EMQX publish failed: %s %s", e.code, e.read())
        raise
    except Exception as e:
        log.error("EMQX publish error: %s", e)
        raise
