"""Pull agent cards from broker and generate local agent files.

    skitter pull [target_dir]

Connects to the broker, reads all retained discovery cards, and writes
Claude agent .md files. Default target: current directory.
"""

import asyncio
import logging
import sys
from pathlib import Path

import aiomqtt
import yaml

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
                        parts = str(msg.topic).split("/")
                        card["_agent_id"] = parts[-1] if parts else ""
                        cards.append(card)
                    except Exception:
                        continue
        except TimeoutError:
            pass

    return cards


def _write_agent_file(target_dir: Path, agent_id: str, card: dict) -> str | None:
    """Write a Claude agent .md file if it doesn't exist. Returns path or None."""
    md_path = target_dir / f"{agent_id}.md"
    if md_path.exists():
        return None
    name = card.get("name", agent_id)
    description = card.get("description", "")
    frontmatter = yaml.dump(
        {"name": agent_id, "description": description},
        default_flow_style=False,
        sort_keys=False,
    ).rstrip()
    md_path.write_text(f"---\n{frontmatter}\n---\n\nYou are {name}.\n")
    return str(md_path)


def generate_agent_files(cards: list[dict], target_dir: Path) -> list[str]:
    """Generate Claude agent .md files from discovered cards."""
    target_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    for card in cards:
        agent_id = card.get("_agent_id", "")
        if not agent_id or agent_id == "coordinator":
            continue
        if is_workflow_card(card):
            continue
        path = _write_agent_file(target_dir, agent_id, card)
        if path:
            written.append(path)
    return written


async def run(target_dir: Path) -> None:
    print("Pulling discovery cards from broker...")
    cards = await pull_cards()
    print(f"Found {len(cards)} cards")

    written = generate_agent_files(cards, target_dir)
    if written:
        print(f"Created {len(written)} agent files:")
        for f in written:
            print(f"  {f}")
    else:
        print("All agent files already exist. Nothing to do.")


def main() -> None:
    logging.basicConfig(
        level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
    )
    target = (
        Path(sys.argv[-1])
        if len(sys.argv) > 1 and sys.argv[-1] != "pull"
        else Path.cwd()
    )
    try:
        asyncio.run(run(target))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
