"""Interactive A2A session client.

    skitter chat <agent_id>

Connects to the MQTT broker, fetches the agent's discovery card, and opens
an interactive prompt where each line becomes an A2A request. Replies are
displayed in real time. Works with any A2A-over-MQTT agent on the broker.
"""

import asyncio
import os
import sys
import uuid

import aiomqtt
from rich.console import Console

from skitter.a2a import (
    a2a_org,
    a2a_unit,
    A2ARequest,
    print_reply,
    stream_request,
    topic_discovery,
    topic_reply,
    topic_request,
)
from skitter.discovery import parse_card
from skitter.mqtt import mqtt_client_kwargs


async def _fetch_card(
    client: aiomqtt.Client, agent_id: str, timeout: float = 3.0
) -> dict | None:
    """Fetch an agent's retained discovery card from the broker."""
    topic = topic_discovery(agent_id)
    await client.subscribe(topic, qos=1)
    try:
        async with asyncio.timeout(timeout):
            async for msg in client.messages:
                if msg.payload:
                    try:
                        return parse_card(msg.payload)
                    except Exception:
                        return None
    except TimeoutError:
        return None
    finally:
        await client.unsubscribe(topic)


async def _run_chat(agent_id: str) -> None:
    console = Console()
    session_id = f"chat-{uuid.uuid4().hex[:8]}"
    context_id = str(uuid.uuid4())
    mqtt_session = uuid.uuid4().hex[:12]
    reply_t = topic_reply("cli", mqtt_session)
    req_topic = topic_request(agent_id)

    async with aiomqtt.Client(
        **mqtt_client_kwargs(identifier=f"{a2a_org()}/{a2a_unit()}/cli-{mqtt_session}"),
    ) as client:
        # Fetch discovery card
        card = await _fetch_card(client, agent_id)
        if card:
            name = card.get("name", agent_id)
            desc = card.get("description", "")
            console.print(f"[bold]{name}[/bold]", end="")
            if desc:
                console.print(f" [dim]{desc}[/dim]")
            else:
                console.print()
        else:
            console.print(
                f"[yellow]No discovery card found for '{agent_id}'. "
                f"Continuing without agent info.[/yellow]"
            )

        console.print(f"[dim]Session: {session_id}[/dim]")
        console.print("[dim]Type a message and press Enter. Ctrl+C to exit.[/dim]\n")

        # Subscribe to reply topic for the entire session
        await client.subscribe(reply_t, qos=1)

        loop = asyncio.get_running_loop()

        try:
            while True:
                try:
                    text = await loop.run_in_executor(None, input, "> ")
                except EOFError:
                    break
                text = text.strip()
                if not text:
                    continue

                request_id = f"{session_id}-{uuid.uuid4().hex[:6]}"
                req = A2ARequest(
                    text=text,
                    request_id=request_id,
                    context_id=context_id,
                    sender="cli",
                )

                async for kind, content in stream_request(
                    client, req_topic, reply_t, req.to_json(), request_id
                ):
                    print_reply(console, kind, content)
                console.print()

        except KeyboardInterrupt:
            console.print("\n[dim]Bye![/dim]")


def main() -> None:
    args = sys.argv[2:]  # skip "skitter" and "chat"
    if not args or args[0].startswith("-"):
        print("Usage: skitter chat <agent_id>", file=sys.stderr)
        sys.exit(1)
    agent_id = args[0]
    try:
        asyncio.run(_run_chat(agent_id))
    except KeyboardInterrupt:
        pass
    finally:
        # The executor thread may still be blocked on input(); os._exit
        # avoids the atexit join hang on the thread pool.
        os._exit(0)
