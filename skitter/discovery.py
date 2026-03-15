"""Build and publish A2A discovery cards."""

import asyncio
import json
import logging

import aiomqtt

from skitter.config import AgentDef, WorkflowDef
from skitter.mqtt import (
    A2A_ORG,
    A2A_UNIT,
    MQTT_HOST,
    MQTT_PORT,
    mqtt_client_kwargs,
    topic_discovery,
)

log = logging.getLogger("skitter.discovery")


def build_card(
    agent: AgentDef,
    *,
    url: str = "",
    workflow: WorkflowDef | None = None,
) -> dict:
    """Build a single spec-conformant A2A Agent Card.

    If workflow is provided, the card includes metadata.tasks for composed apps.
    """
    url = url or f"mqtt://{MQTT_HOST}:{MQTT_PORT}"
    capabilities = dict(agent.capabilities) if agent.capabilities else {}
    capabilities.setdefault("streaming", True)
    capabilities.setdefault("pushNotifications", False)

    card: dict = {
        "name": agent.name,
        "description": agent.description,
        "version": "0.1.0",
        "url": url,
        "protocolVersion": "0.2.5",
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
            }
        ],
    }

    if workflow:
        card["metadata"] = {
            "variables": workflow.variables,
            "tasks": [
                {"id": t.id, "agent": t.agent, "description": t.description}
                for t in workflow.tasks
            ],
        }

    return card


def parse_card(payload: bytes) -> dict:
    """Parse a discovery card from MQTT payload.

    Returns the parsed card dict. Composed apps have metadata.tasks.
    """
    return json.loads(payload)


def is_workflow_card(card: dict) -> bool:
    """Detect composed app by presence of metadata.tasks."""
    metadata = card.get("metadata", {})
    return bool(metadata.get("tasks"))


class CardPublisher:
    """Holds a long-lived MQTT connection for a single discovery card.

    Client ID is {org}/{unit}/{card_id}, so the broker tracks per-agent
    liveness and annotates cards with a2a-status: online/offline.
    """

    def __init__(
        self,
        card_id: str,
        card_json: str,
        *,
        mqtt_kwargs: dict | None = None,
    ) -> None:
        self._card_id = card_id
        self._card_json = card_json
        self._mqtt_kwargs = mqtt_kwargs
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        """Start the background connection loop."""
        self._task = asyncio.create_task(self._run())

    async def stop(self) -> None:
        """Stop the connection (card stays retained on broker)."""
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None

    async def _run(self) -> None:
        topic = topic_discovery(self._card_id)
        identifier = f"{A2A_ORG}/{A2A_UNIT}/{self._card_id}"
        if self._mqtt_kwargs:
            kwargs = {**self._mqtt_kwargs, "identifier": identifier}
        else:
            kwargs = mqtt_client_kwargs(identifier=identifier)
        while True:
            try:
                async with aiomqtt.Client(**kwargs) as client:
                    await client.publish(topic, self._card_json, qos=1, retain=True)
                    log.info("Card %s published (client connected)", self._card_id)
                    await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning("Card %s connection lost, reconnecting...", self._card_id)
                await asyncio.sleep(2)
