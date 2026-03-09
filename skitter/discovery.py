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


async def publish_cards(cards: dict[str, str]) -> None:
    """Publish discovery cards as retained MQTT messages."""
    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{A2A_ORG}/{A2A_UNIT}/discovery-publisher",
        ),
    ) as client:
        for card_id, card_json in cards.items():
            await client.publish(
                topic_discovery(card_id),
                card_json,
                qos=1,
                retain=True,
            )
    log.info("Published %d discovery cards", len(cards))


def main() -> None:
    """CLI entry point: load definitions, build cards, publish."""
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
    )
    agents = load_agents()
    workflows = load_workflows()
    cards = build_cards(agents, workflows)

    print(f"Publishing {len(cards)} discovery cards...")
    for card_id in sorted(cards):
        print(f"  {card_id}")

    asyncio.run(publish_cards(cards))
    print("Done.")


if __name__ == "__main__":
    main()
