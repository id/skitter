"""Build and parse A2A discovery cards."""

import json

from skitter.config import AgentDef
from skitter.mqtt import MQTT_HOST, MQTT_PORT


def build_card(
    agent: AgentDef,
    *,
    url: str = "",
    metadata: dict | None = None,
) -> dict:
    """Build a single spec-conformant A2A Agent Card.

    If metadata is provided (e.g. {"tasks": [...], "variables": [...]}),
    it is included in the card for composed apps.
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
                "id": agent.id,
                "name": agent.name,
                "description": agent.description,
                "tags": tags,
            }
        ],
    }

    if metadata:
        card["metadata"] = metadata

    return card


def parse_card(payload: bytes) -> dict:
    """Parse a discovery card from MQTT payload."""
    return json.loads(payload)


def is_workflow_card(card: dict) -> bool:
    """Detect composed app by presence of metadata.tasks."""
    metadata = card.get("metadata", {})
    return bool(metadata.get("tasks"))
