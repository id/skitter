"""Pull agent cards from broker and generate local stubs.

    skitter pull

Connects to the broker, reads all retained discovery cards, and writes:
- ~/.skitter/agents/{id}.yaml  (agent definition stub)
- ~/.claude/agents/{id}.md     (claude agent prompt stub)
"""

import asyncio
import logging

import aiomqtt
import yaml

from skitter.config import AGENTS_DIR, CLAUDE_AGENTS_DIR
from skitter.discovery import is_workflow_card, parse_card
from skitter.mqtt import mqtt_client_kwargs, topic_discovery_wildcard

log = logging.getLogger("skitter.pull")


async def pull_cards(timeout: float = 5.0) -> list[dict]:
    """Connect to broker, collect all retained discovery cards."""
    cards: list[dict] = []
    topic = topic_discovery_wildcard()

    async with aiomqtt.Client(**mqtt_client_kwargs()) as client:
        await client.subscribe(topic, qos=1)
        try:
            async with asyncio.timeout(timeout):
                async for msg in client.messages:
                    payload = msg.payload
                    if not payload:
                        continue
                    try:
                        card = parse_card(payload)
                        # Extract agent_id from topic
                        parts = str(msg.topic).split("/")
                        card["_agent_id"] = parts[-1] if parts else ""
                        cards.append(card)
                    except Exception:
                        continue
        except TimeoutError:
            pass

    return cards


def _write_agent_stub(agent_id: str, card: dict) -> list[str]:
    """Write stub files for an agent. Returns list of files written."""
    written: list[str] = []

    # Agent definition YAML (use yaml.dump to safely escape values)
    yaml_path = AGENTS_DIR / f"{agent_id}.yaml"
    if not yaml_path.exists():
        stub: dict = {
            "name": card.get("name", agent_id),
            "description": card.get("description", ""),
            "agent_id": agent_id,
            "runtime": "claude",
        }
        capabilities = card.get("capabilities", {})
        if capabilities.get("streaming") is not None:
            stub["capabilities"] = {"streaming": capabilities["streaming"]}
        yaml_path.write_text(yaml.dump(stub, default_flow_style=False, sort_keys=False))
        written.append(str(yaml_path))

    # Claude agent prompt stub (use yaml.dump for frontmatter)
    md_path = CLAUDE_AGENTS_DIR / f"{agent_id}.md"
    if not md_path.exists():
        name = card.get("name", agent_id)
        description = card.get("description", "")
        frontmatter = yaml.dump(
            {"name": name, "description": description},
            default_flow_style=False,
            sort_keys=False,
        ).rstrip()
        md_path.write_text(f"---\n{frontmatter}\n---\n\nYou are {name}.\n")
        written.append(str(md_path))

    return written


def generate_stubs(cards: list[dict]) -> list[str]:
    """Generate local stub files from discovered cards."""
    AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    CLAUDE_AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    all_written: list[str] = []
    for card in cards:
        agent_id = card.get("_agent_id", "")
        if not agent_id or agent_id == "supervisor":
            continue
        if is_workflow_card(card):
            continue
        all_written.extend(_write_agent_stub(agent_id, card))
    return all_written


async def run() -> None:
    print("Pulling discovery cards from broker...")
    cards = await pull_cards()
    print(f"Found {len(cards)} cards")

    written = generate_stubs(cards)
    if written:
        print(f"Created {len(written)} stub files:")
        for f in written:
            print(f"  {f}")
    else:
        print("All stub files already exist. Nothing to do.")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
    )
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
