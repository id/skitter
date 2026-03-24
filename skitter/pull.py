"""Pull agent discovery cards from broker and save as JSON.

    skitter pull [target_dir]

Connects to the broker, reads all retained discovery cards, and saves
them as JSON files. These are the raw A2A Agent Cards as published by
agents, not runnable agent definitions.

Default target: current directory.
"""

import asyncio
import json
import logging
import sys
from pathlib import Path

import aiomqtt

from skitter.discovery import parse_card
from skitter.a2a import topic_discovery_wildcard
from skitter.mqtt import mqtt_client_kwargs

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


def _write_card(target_dir: Path, agent_id: str, card: dict) -> str | None:
    """Write a discovery card as JSON. Skips if file exists."""
    json_path = target_dir / f"{agent_id}.json"

    # Strip internal field before saving
    clean = {k: v for k, v in card.items() if not k.startswith("_")}
    content = json.dumps(clean, indent=2) + "\n"

    try:
        with json_path.open("x") as f:
            f.write(content)
    except FileExistsError:
        return None
    return str(json_path)


def save_cards(cards: list[dict], target_dir: Path) -> list[str]:
    """Save discovery cards as JSON files."""
    target_dir.mkdir(parents=True, exist_ok=True)
    written: list[str] = []
    for card in cards:
        agent_id = card.get("_agent_id", "")
        if not agent_id or agent_id == "skitter":
            continue
        path = _write_card(target_dir, agent_id, card)
        if path:
            written.append(path)
    return written


async def run(target_dir: Path) -> None:
    print("Pulling discovery cards from broker...")
    cards = await pull_cards()
    print(f"Found {len(cards)} cards")

    written = save_cards(cards, target_dir)
    if written:
        print(f"Saved {len(written)} cards:")
        for f in written:
            print(f"  {f}")
    else:
        print("All cards already saved. Nothing to do.")


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
