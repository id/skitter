"""Click-based CLI command tree.

All subcommands are defined here. Global flags (--skitter-home) are
handled once at the group level. Individual command modules supply the
implementation logic; this file handles only argument parsing and dispatch.
"""

import asyncio
import json
import os
import sys
import uuid

import aiomqtt
import click
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
from skitter.config import configure_logging
from skitter.discovery import parse_card
from skitter.mqtt import mqtt_client_kwargs

_console = Console()


# --- Helpers for broker/coordinator queries ---


async def _fetch_cards(
    topic: str, *, first_only: bool = False, timeout: float = 3.0
) -> list[tuple[str, dict]]:
    """Subscribe to a discovery topic and collect retained cards."""
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


# --- CLI group ---


@click.group(invoke_without_command=True)
@click.option(
    "--skitter-home",
    envvar="SKITTER_HOME",
    default=None,
    help="Override config directory (default: ~/.skitter).",
)
@click.pass_context
def cli(ctx, skitter_home):
    """MQTT-based AI agent orchestrator."""
    if skitter_home:
        os.environ["SKITTER_HOME"] = skitter_home
    configure_logging()
    if ctx.invoked_subcommand is None:
        from skitter.coordinator import main

        main()


# --- One-shot request ---


@cli.command()
@click.argument("agent_id")
@click.argument("prompt", nargs=-1, required=True)
@click.option("--context", "context_id", default="", help="Conversation context ID.")
def ask(agent_id, prompt, context_id):
    """Send a one-shot A2A request to an agent."""
    from skitter.request import request_prompt

    request_prompt(agent_id, " ".join(prompt), context_id=context_id)


@cli.command(hidden=True)
@click.argument("agent_id")
@click.argument("prompt", nargs=-1, required=True)
@click.option("--context", "context_id", default="", help="Conversation context ID.")
def request(agent_id, prompt, context_id):
    """Send a one-shot A2A request (alias for ask)."""
    click.echo("Tip: 'skitter ask' is the recommended command.", err=True)
    from skitter.request import request_prompt

    request_prompt(agent_id, " ".join(prompt), context_id=context_id)


# --- Interactive chat ---


@cli.command()
@click.argument("agent_id")
def chat(agent_id):
    """Start an interactive A2A chat session."""
    from skitter.cli import _run_chat

    try:
        asyncio.run(_run_chat(agent_id))
    except KeyboardInterrupt:
        pass
    finally:
        os._exit(0)


# --- Service management ---


@cli.command()
@click.option("--broker-only", is_flag=True, help="Start only the MQTT broker.")
@click.option("--agent", "agent_id", default="", help="Start a single agent by ID.")
def up(broker_only, agent_id):
    """Start broker, coordinator, and agents."""
    from skitter.services import up as _up

    argv = []
    if broker_only:
        argv.append("--broker-only")
    if agent_id:
        argv.extend(["--agent", agent_id])
    _up(argv)


@cli.command()
@click.option("--agent", "agent_id", default="", help="Stop a single agent by ID.")
def down(agent_id):
    """Stop all skitter containers."""
    from skitter.services import down as _down

    argv = []
    if agent_id:
        argv.extend(["--agent", agent_id])
    _down(argv)


@cli.command()
def status():
    """Show service readiness overview."""
    from skitter.services import status as _status

    _status()


@cli.command()
@click.argument("service")
@click.option("-f", "--follow", is_flag=True, help="Follow log output.")
def logs(service, follow):
    """View logs for a service (emqx, coordinator, agent-ID)."""
    from skitter.services import logs as _logs

    argv = [service]
    if follow:
        argv.append("--follow")
    _logs(argv)


# --- Setup and diagnostics ---


@cli.command()
@click.option("--non-interactive", is_flag=True, help="Use defaults without prompting.")
@click.option("--standalone", is_flag=True, help="Skip coordinator/broker setup.")
def setup(non_interactive, standalone):
    """Interactive setup wizard."""
    from skitter.setup import main as _setup

    argv = []
    if non_interactive:
        argv.append("--non-interactive")
    if standalone:
        argv.append("--standalone")
    _setup(argv)


@cli.command()
def doctor():
    """Run diagnostic health checks."""
    from skitter.doctor import main as _doctor

    _doctor()


# --- Agent runner ---


@cli.command("agent-runner")
@click.argument("agent_path")
def agent_runner(agent_path):
    """Run a standalone A2A agent process."""
    from skitter.agent_runner import run

    asyncio.run(run(agent_path))


# --- Agent scaffolding ---


@cli.command("create-agent")
@click.argument("name")
@click.argument("prompt")
@click.option("--runtime", type=click.Choice(["claude", "codex"]), default="claude")
@click.option("--model", default="", help="Model name.")
@click.option(
    "--skill",
    "skills",
    multiple=True,
    help="Skill in name:description format (repeatable).",
)
@click.option("--dry-run", is_flag=True, help="Print without writing files.")
@click.option("--edit", is_flag=True, help="Open editor before saving.")
@click.option("--force", is_flag=True, help="Overwrite existing definition.")
def create_agent(name, prompt, runtime, model, skills, dry_run, edit, force):
    """Generate an agent definition via LLM."""
    from skitter.create_agent import run as _run

    argv = [name, prompt, "--runtime", runtime]
    if model:
        argv.extend(["--model", model])
    for s in skills:
        argv.extend(["--skill", s])
    if dry_run:
        argv.append("--dry-run")
    if edit:
        argv.append("--edit")
    if force:
        argv.append("--force")
    _run(argv)


# --- Manage commands (agents, apps, sessions) ---


@cli.command("list-agents")
def list_agents():
    """List agents discovered from broker."""
    agents = asyncio.run(_fetch_cards(topic_discovery_wildcard()))
    if not agents:
        print("No agents.")
        return
    t = _table("ID", "NAME", "DESCRIPTION")
    for agent_id, card in sorted(agents):
        t.add_row(agent_id, card.get("name", ""), card.get("description", ""))
    _console.print(t)


@cli.command("get-agent")
@click.argument("agent_id")
def get_agent(agent_id):
    """Get agent discovery card (JSON)."""
    results = asyncio.run(_fetch_cards(topic_discovery(agent_id), first_only=True))
    if not results:
        print(f"Agent '{agent_id}' not found.", file=sys.stderr)
        raise SystemExit(1)
    print(json.dumps(results[0][1], indent=2))


@cli.command("create-app")
@click.argument("name")
@click.argument("instructions")
@click.option("--agents", required=True, help="Comma-separated agent IDs.")
@click.option("--id", "app_id", default="", help="App ID (auto-generated if omitted).")
@click.option("--description", default="", help="App description.")
def create_app(name, instructions, agents, app_id, description):
    """Create a composed multi-agent app."""
    agent_ids = [a.strip() for a in agents.split(",") if a.strip()]
    if not agent_ids:
        raise click.BadParameter(
            "must list at least one agent ID", param_hint="--agents"
        )

    spec: dict = {
        "name": name,
        "instructions": instructions,
        "agents": agent_ids,
    }
    if app_id:
        spec["id"] = app_id
    if description:
        spec["description"] = description

    data = asyncio.run(_query_json(f"create app {json.dumps(spec)}"))
    created = data.get("created_app", {})
    if created:
        print(f"Created app '{created['app_id']}' v{created['version']}")
    else:
        print(json.dumps(data, indent=2))


@cli.command("list-apps")
def list_apps():
    """List all apps."""
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


@cli.command("get-app")
@click.argument("app_id")
def get_app(app_id):
    """Get app details (JSON)."""
    data = _query_or_exit(f"get app {app_id}")
    print(json.dumps(data, indent=2))


@cli.command("delete-app")
@click.argument("app_id")
def delete_app(app_id):
    """Delete an app and all its data."""
    data = _query_or_exit(f"delete app {app_id}")
    print(f"Deleted app '{data.get('deleted_app', app_id)}'")


@cli.command("list-sessions")
@click.argument("app_id", required=False, default="")
def list_sessions(app_id):
    """List sessions (optionally filter by app ID)."""
    q = f"list sessions {app_id}" if app_id else "list sessions"
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


@cli.command("get-session")
@click.argument("session_id")
def get_session(session_id):
    """Get session details (JSON)."""
    data = _query_or_exit(f"get session {session_id}")
    print(json.dumps(data, indent=2))


@cli.command("cancel-session")
@click.argument("session_id")
def cancel_session(session_id):
    """Cancel a running session."""
    data = _query_or_exit(f"cancel session {session_id}")
    print(f"Canceled session '{data.get('canceled', session_id)}'")
