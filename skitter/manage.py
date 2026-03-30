"""CLI subcommands for managing agents, apps, and sessions.

``list-*`` commands print human-readable summaries.
``get-*`` commands print JSON.
"""

import argparse
import asyncio
import json
import sys
import uuid

import aiomqtt
from rich import box
from rich.console import Console
from rich.table import Table

from skitter.a2a import (
    A2ARequest,
    REPLY_ARTIFACT,
    REPLY_ERROR,
    REPLY_FAILED,
    REPLY_TIMEOUT,
    stream_replies,
    topic_discovery,
    topic_discovery_wildcard,
    topic_request,
)
from skitter.discovery import parse_card
from skitter.mqtt import mqtt_client_kwargs

_console = Console()


# --- Broker queries (agents) ---


async def _fetch_cards(
    topic: str, *, first_only: bool = False, timeout: float = 3.0
) -> list[tuple[str, dict]]:
    """Subscribe to a discovery topic and collect retained cards.

    Returns (agent_id, card) tuples.
    """
    cards: list[tuple[str, dict]] = []
    async with aiomqtt.Client(**mqtt_client_kwargs()) as client:
        await client.subscribe(topic, qos=1)
        try:
            async with asyncio.timeout(timeout):
                async for msg in client.messages:
                    if not msg.payload:
                        continue
                    try:
                        card = parse_card(msg.payload)
                        parts = str(msg.topic).split("/")
                        agent_id = parts[-1] if parts else ""
                        cards.append((agent_id, card))
                        if first_only:
                            break
                    except Exception:
                        continue
        except TimeoutError:
            pass
    return cards


# --- Coordinator queries (apps, sessions) ---


async def _query_json(text: str) -> dict:
    """Send a query to the coordinator and return the parsed JSON artifact."""
    request_id = f"req-{uuid.uuid4().hex[:8]}"
    req = A2ARequest(text=text, request_id=request_id, sender="cli")

    artifact = ""
    async for kind, content in stream_replies(
        topic_request("skitter"), req.to_json(), request_id
    ):
        if kind == REPLY_ARTIFACT:
            artifact = content
            break
        elif kind in (REPLY_ERROR, REPLY_FAILED):
            print(f"Error: {content}", file=sys.stderr)
            raise SystemExit(1)
        elif kind == REPLY_TIMEOUT:
            print("Error: coordinator not reachable", file=sys.stderr)
            raise SystemExit(1)

    if not artifact:
        print("Error: empty response from coordinator", file=sys.stderr)
        raise SystemExit(1)
    return json.loads(artifact)


def _query_or_exit(text: str) -> dict:
    """Run a coordinator query; exit on error."""
    data = asyncio.run(_query_json(text))
    if "error" in data:
        print(f"Error: {data['error']}", file=sys.stderr)
        raise SystemExit(1)
    return data


def _table(*columns: str) -> Table:
    t = Table(box=box.MARKDOWN, show_edge=True, pad_edge=True)
    for col in columns:
        t.add_column(col, no_wrap=(col == "ID"))
    return t


# --- CLI commands ---


def list_agents(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter list-agents", description="List agents discovered from broker."
    )
    parser.parse_args(argv)
    agents = asyncio.run(_fetch_cards(topic_discovery_wildcard()))
    if not agents:
        print("No agents.")
        return
    t = _table("ID", "NAME", "DESCRIPTION")
    for agent_id, card in sorted(agents):
        t.add_row(agent_id, card.get("name", ""), card.get("description", ""))
    _console.print(t)


def get_agent(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter get-agent", description="Get agent discovery card (JSON)."
    )
    parser.add_argument("agent_id", help="Agent ID")
    args = parser.parse_args(argv)
    results = asyncio.run(_fetch_cards(topic_discovery(args.agent_id), first_only=True))
    if not results:
        print(f"Agent '{args.agent_id}' not found.", file=sys.stderr)
        raise SystemExit(1)
    print(json.dumps(results[0][1], indent=2))


def create_app(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter create-app",
        description="Create a composed multi-agent app.",
    )
    parser.add_argument("name", help="App name")
    parser.add_argument("instructions", help="What the app should do (plain English)")
    parser.add_argument(
        "--agents", required=True, help="Comma-separated list of agent IDs"
    )
    parser.add_argument("--id", default="", help="App ID (auto-generated if omitted)")
    parser.add_argument("--description", default="", help="App description")
    args = parser.parse_args(argv)

    agent_ids = [a.strip() for a in args.agents.split(",") if a.strip()]
    if not agent_ids:
        parser.error("--agents must list at least one agent ID")

    spec: dict = {
        "name": args.name,
        "instructions": args.instructions,
        "agents": agent_ids,
    }
    if args.id:
        spec["id"] = args.id
    if args.description:
        spec["description"] = args.description

    data = asyncio.run(_query_json(f"create app {json.dumps(spec)}"))
    created = data.get("created_app", {})
    if created:
        print(f"Created app '{created['app_id']}' v{created['version']}")
    else:
        print(json.dumps(data, indent=2))


def list_apps(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter list-apps", description="List all apps."
    )
    parser.parse_args(argv)
    data = asyncio.run(_query_json("list apps"))
    apps = data.get("apps", [])
    if not apps:
        print("No apps.")
        return
    t = _table("ID", "NAME", "VERSION")
    for app in apps:
        ver = app.get("current_version")
        t.add_row(app["id"], app.get("name", ""), str(ver) if ver is not None else "")
    _console.print(t)


def get_app(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter get-app", description="Get app details (JSON)."
    )
    parser.add_argument("app_id", help="App ID")
    args = parser.parse_args(argv)
    data = _query_or_exit(f"get app {args.app_id}")
    print(json.dumps(data, indent=2))


def delete_app(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter delete-app", description="Delete an app and all its data."
    )
    parser.add_argument("app_id", help="App ID")
    args = parser.parse_args(argv)
    data = _query_or_exit(f"delete app {args.app_id}")
    print(f"Deleted app '{data.get('deleted_app', args.app_id)}'")


def list_sessions(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter list-sessions", description="List sessions."
    )
    parser.add_argument("app_id", nargs="?", default="", help="Filter by app ID")
    args = parser.parse_args(argv)
    q = f"list sessions {args.app_id}" if args.app_id else "list sessions"
    data = asyncio.run(_query_json(q))
    sessions = data.get("sessions", [])
    if not sessions:
        print("No sessions.")
        return
    t = _table("ID", "APP", "STATE", "CREATED")
    for s in sessions:
        t.add_row(
            s["id"], s.get("app_version_id", ""), s["state"], s.get("created_at", "")
        )
    _console.print(t)


def get_session(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter get-session", description="Get session details (JSON)."
    )
    parser.add_argument("session_id", help="Session ID")
    args = parser.parse_args(argv)
    data = _query_or_exit(f"get session {args.session_id}")
    print(json.dumps(data, indent=2))


def cancel_session(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(
        prog="skitter cancel-session", description="Cancel a running session."
    )
    parser.add_argument("session_id", help="Session ID")
    args = parser.parse_args(argv)
    data = _query_or_exit(f"cancel session {args.session_id}")
    print(f"Canceled session '{data.get('canceled', args.session_id)}'")
