"""Build and parse A2A discovery cards."""

import json

from skitter.config import AgentDef
from skitter.mqtt import MQTT_HOST, MQTT_PORT

APP_EXTENSION_URI = "urn:skitter:app"


def build_card(
    agent: AgentDef,
    *,
    url: str = "",
    metadata: dict | None = None,
) -> dict:
    """Build a single spec-conformant A2A Agent Card.

    If metadata is provided (e.g. {"tasks": [...], "variables": [...]}),
    it is stored as an app extension in capabilities.extensions.
    """
    url = url or f"mqtt://{MQTT_HOST}:{MQTT_PORT}"
    capabilities = dict(agent.capabilities) if agent.capabilities else {}
    capabilities.setdefault("streaming", True)
    capabilities.setdefault("pushNotifications", False)

    tags = agent.tags if agent.tags else [agent.id]

    card: dict = {
        "name": agent.name,
        "description": agent.description,
        "version": "0.1.0",
        "supportedInterfaces": [
            {
                "url": url,
                "protocolBinding": "MQTTv5+JSONRPCv2",
                "protocolVersion": "1.0.0",
            }
        ],
        "capabilities": capabilities,
        "defaultInputModes": list(agent.input_modes)
        if agent.input_modes
        else ["text/plain"],
        "defaultOutputModes": list(agent.output_modes)
        if agent.output_modes
        else ["text/plain"],
        "skills": [
            {
                "id": "default",
                "name": agent.name,
                "description": agent.description,
                "tags": tags,
            }
        ],
    }

    if metadata:
        card["capabilities"].setdefault("extensions", [])
        card["capabilities"]["extensions"].append(
            {
                "uri": APP_EXTENSION_URI,
                "description": "Skitter composed-app definition",
                "required": False,
                "params": metadata,
            }
        )

    return card


def parse_card(payload: bytes) -> dict:
    """Parse a discovery card from MQTT payload."""
    return json.loads(payload)


def is_app_card(card: dict) -> bool:
    """Detect composed app by presence of app extension with tasks."""
    for ext in card.get("capabilities", {}).get("extensions", []):
        if ext.get("uri") == APP_EXTENSION_URI:
            return bool(ext.get("params", {}).get("tasks"))
    return False
