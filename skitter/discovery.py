"""Build and publish discovery cards from agent/workflow definitions."""

import asyncio
import json
import logging

import aiomqtt

from skitter.config import AgentDef, WorkflowDef, load_agents, load_workflows
from skitter.mqtt import A2A_ORG, A2A_UNIT, mqtt_client_kwargs, topic_discovery

log = logging.getLogger("skitter.discovery")


def build_cards(
    agents: dict[str, AgentDef],
    workflows: dict[str, WorkflowDef],
) -> dict[str, str]:
    """Generate discovery card JSON from agent/workflow definitions."""
    workflow_agents: set[str] = set()
    for wf in workflows.values():
        for t in wf.tasks:
            workflow_agents.add(t.agent)

    cards: dict[str, str] = {}
    for agent in agents.values():
        if agent.id in workflow_agents:
            continue
        cards[agent.id] = json.dumps(
            {
                "agent_id": agent.id,
                "name": agent.name,
                "description": agent.description,
            }
        )

    for wf in workflows.values():
        cards[f"workflow-{wf.id}"] = json.dumps(
            {
                "workflow_id": wf.id,
                "name": wf.name,
                "description": wf.description,
                "variables": wf.variables,
                "agents": [t.agent for t in wf.tasks],
            }
        )

    return cards


class CardRegistry:
    """Maintains one long-lived MQTT connection per discovery card.

    Each card gets its own connection with client ID {org}/{unit}/{card_id},
    so the broker can track per-agent liveness and annotate discovery card
    deliveries with a2a-status: online/offline.
    """

    def __init__(self) -> None:
        self._connections: dict[str, tuple[aiomqtt.Client, asyncio.Task]] = {}
        self._payloads: dict[str, str] = {}

    async def sync(self, cards: dict[str, str]) -> None:
        """Diff current connections against desired cards and reconcile."""
        current_ids = set(self._connections)
        desired_ids = set(cards)

        # Remove cards no longer desired
        for card_id in current_ids - desired_ids:
            await self._teardown(card_id, clear=True)

        # Update changed cards (tear down + respawn)
        for card_id in current_ids & desired_ids:
            if self._payloads.get(card_id) != cards[card_id]:
                await self._teardown(card_id, clear=False)
                self._spawn(card_id, cards[card_id])

        # Add new cards
        for card_id in desired_ids - current_ids:
            self._spawn(card_id, cards[card_id])

        log.info("Card registry synced: %d cards active", len(self._connections))

    async def close(self) -> None:
        """Tear down all connections."""
        for card_id in list(self._connections):
            await self._teardown(card_id, clear=True)

    def _spawn(self, card_id: str, card_json: str) -> None:
        self._payloads[card_id] = card_json
        task = asyncio.create_task(self._run(card_id, card_json))
        # Client is set inside _run once connected; store placeholder
        self._connections[card_id] = (None, task)  # type: ignore[arg-type]

    async def _teardown(self, card_id: str, *, clear: bool) -> None:
        entry = self._connections.pop(card_id, None)
        self._payloads.pop(card_id, None)
        if entry is None:
            return
        client, task = entry
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
        if clear:
            # Publish empty retained message to remove the card from broker
            try:
                async with aiomqtt.Client(
                    **mqtt_client_kwargs(
                        identifier=f"{A2A_ORG}/{A2A_UNIT}/{card_id}",
                    ),
                ) as tmp:
                    await tmp.publish(topic_discovery(card_id), b"", qos=1, retain=True)
            except Exception:
                log.warning("Failed to clear card %s from broker", card_id)

    async def _run(self, card_id: str, card_json: str) -> None:
        """Connect with per-agent client ID, publish card, stay alive."""
        while True:
            try:
                async with aiomqtt.Client(
                    **mqtt_client_kwargs(
                        identifier=f"{A2A_ORG}/{A2A_UNIT}/{card_id}",
                    ),
                ) as client:
                    self._connections[card_id] = (
                        client,
                        self._connections[card_id][1],
                    )
                    await client.publish(
                        topic_discovery(card_id),
                        card_json,
                        qos=1,
                        retain=True,
                    )
                    log.info("Card %s published (client connected)", card_id)
                    # Stay alive until cancelled
                    await asyncio.Event().wait()
            except asyncio.CancelledError:
                raise
            except Exception:
                log.warning("Card %s connection lost, reconnecting...", card_id)
                await asyncio.sleep(2)


def main() -> None:
    """CLI entry point: load definitions, build cards, publish and stay alive."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
    )
    agents = load_agents()
    workflows = load_workflows()
    cards = build_cards(agents, workflows)

    print(f"Publishing {len(cards)} discovery cards...")
    for card_id in sorted(cards):
        print(f"  {card_id}")

    async def _run() -> None:
        registry = CardRegistry()
        try:
            await registry.sync(cards)
            log.info("All cards published. Press Ctrl+C to stop.")
            await asyncio.Event().wait()
        finally:
            await registry.close()

    asyncio.run(_run())


if __name__ == "__main__":
    main()
