"""CLI for managing and running predefined agents."""

import asyncio
import sys
import uuid

import yaml
from rich.console import Console
from rich.table import Table

from skitter.config import load_agents
from skitter.mqtt import send_and_wait, topic_request
from skitter.types import (
    A2ARequest,
    REPLY_ERROR,
    REPLY_SUBMITTED,
    REPLY_TERMINAL,
    REPLY_TEXT,
    REPLY_TOOL,
)

console = Console()


def cmd_list() -> None:
    agents = load_agents()
    if not agents:
        console.print("No agents found in ~/.skitter/agents/")
        console.print("Run 'skitter init' to create example agents.")
        return
    table = Table(title="Predefined Agents")
    table.add_column("ID", style="cyan")
    table.add_column("Name")
    table.add_column("Runtime", style="green")
    for agent_id, agent in agents.items():
        table.add_row(agent_id, agent.name, agent.runtime)
    console.print(table)


def cmd_show(agent_id: str) -> None:
    agents = load_agents()
    agent = agents.get(agent_id)
    if agent is None:
        console.print(f"Agent '{agent_id}' not found.")
        available = ", ".join(agents.keys()) if agents else "(none)"
        console.print(f"Available: {available}")
        sys.exit(1)
    data = {
        "name": agent.name,
        "description": agent.description,
        "runtime": agent.runtime,
        "workspace": agent.workspace,
    }
    data = {k: v for k, v in data.items() if v}
    console.print(yaml.dump(data, default_flow_style=False, sort_keys=False))

    # Show native sub-agent definition
    from pathlib import Path

    runtime = agent.runtime or "claude"
    if runtime == "claude":
        agent_file = Path.home() / ".claude" / "agents" / f"{agent_id}.md"
    elif runtime == "codex":
        agent_file = Path.home() / ".codex" / "agents" / f"{agent_id}.toml"
    else:
        return
    if agent_file.is_file():
        console.print(f"[dim]{agent_file}[/dim]")
        console.print(agent_file.read_text())
    else:
        console.print(f"[yellow]No sub-agent definition at {agent_file}[/yellow]")


def _print_reply(kind: str, content: str) -> bool:
    """Handle a reply message. Returns True to stop listening."""
    if kind == REPLY_SUBMITTED:
        console.print(f"[dim]Session: {content}[/dim]")
    elif kind == REPLY_TEXT:
        console.print(content, end="")
    elif kind == REPLY_TOOL:
        console.print(f"  [dim][tool] {content}[/dim]")
    elif kind == REPLY_TERMINAL:
        console.print(f"\n\n{content}")
        return True
    elif kind == REPLY_ERROR:
        console.print(f"[red]Error: {content}[/red]")
        return True
    elif kind == "timeout":
        console.print("[yellow]Timed out waiting for result[/yellow]")
        return True
    return False


def cmd_run(agent_id: str, description: str) -> None:
    agents = load_agents()
    if agent_id not in agents:
        console.print(f"Agent '{agent_id}' not found.")
        available = ", ".join(agents.keys()) if agents else "(none)"
        console.print(f"Available: {available}")
        sys.exit(1)

    request_id = f"agent-{uuid.uuid4().hex[:8]}"
    req = A2ARequest(text=description, request_id=request_id, sender="cli")

    console.print(f"Agent '{agent_id}' submitted (request {request_id})")
    console.print("Waiting for result... (Ctrl+C to detach)\n")

    asyncio.run(
        send_and_wait(
            topic_request(agent_id),
            req.to_json(),
            request_id,
            _print_reply,
        )
    )


def main() -> None:
    args = sys.argv[2:]  # skip "skitter" and "agents"
    if not args or args[0] == "list":
        cmd_list()
    elif args[0] == "show":
        if len(args) < 2:
            console.print("Usage: skitter agents show <agent_id>")
            sys.exit(1)
        cmd_show(args[1])
    elif args[0] == "run":
        if len(args) < 3:
            console.print("Usage: skitter agents run <agent_id> '<description>'")
            sys.exit(1)
        cmd_run(args[1], " ".join(args[2:]))
    else:
        console.print(f"Unknown subcommand: {args[0]}")
        console.print("Usage: skitter agents [list|show <id>|run <id> '<description>']")
        sys.exit(1)
