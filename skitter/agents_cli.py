"""CLI for managing and running predefined agents."""

import asyncio
import json
import sys
import uuid

import aiomqtt
import yaml
from rich.console import Console
from rich.table import Table

from skitter.config import load_agents
from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    A2A_ORG,
    A2A_UNIT,
    make_properties,
    topic_reply,
    topic_request,
)
from skitter.types import InboundMessage, StreamItem, TaskStatusUpdate

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
    table.add_column("Model", style="green")
    table.add_column("Max Turns", justify="right")
    for agent_id, agent in agents.items():
        table.add_row(
            agent_id,
            agent.name,
            agent.model or "(default)",
            str(agent.max_turns),
        )
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
        "soul": agent.soul,
        "skills": agent.skills,
        "model": agent.model,
        "max_turns": agent.max_turns,
        "runtime": agent.runtime,
        "workspace": agent.workspace,
    }
    # Remove empty fields for cleaner output
    data = {k: v for k, v in data.items() if v}
    console.print(yaml.dump(data, default_flow_style=False, sort_keys=False))


def cmd_run(agent_id: str, description: str) -> None:
    agents = load_agents()
    if agent_id not in agents:
        console.print(f"Agent '{agent_id}' not found.")
        available = ", ".join(agents.keys()) if agents else "(none)"
        console.print(f"Available: {available}")
        sys.exit(1)

    session_id = f"agent-{uuid.uuid4().hex[:8]}"
    msg = InboundMessage(
        text=description,
        sender="cli",
        session_id=session_id,
        agent_id=agent_id,
    )

    mqtt_session = uuid.uuid4().hex[:12]
    reply_t = topic_reply("cli", mqtt_session)
    gateway_request = topic_request("gateway")

    async def run_agent() -> None:
        async with aiomqtt.Client(
            MQTT_HOST,
            MQTT_PORT,
            identifier=f"{A2A_ORG}/{A2A_UNIT}/agent-cli-{mqtt_session}",
            protocol=aiomqtt.ProtocolVersion.V5,
        ) as client:
            await client.subscribe(reply_t, qos=1)

            props = make_properties(
                response_topic=reply_t,
                correlation_data=session_id,
            )
            await client.publish(
                gateway_request,
                msg.to_json(),
                qos=1,
                properties=props,
            )
            console.print(f"Agent '{agent_id}' started as session {session_id}")
            console.print("Waiting for result... (Ctrl+C to detach)\n")

            seen_seqs: set[tuple[str, int]] = set()
            try:
                async with asyncio.timeout(600.0):
                    async for mqtt_msg in client.messages:
                        payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                        if not payload:
                            continue
                        try:
                            data = json.loads(payload)
                        except Exception:
                            continue

                        # Stream item (with dedup for QoS 1 redelivery)
                        if "seq" in data and "type" in data:
                            item = StreamItem.from_json(payload)
                            dedup_key = (item.task_id, item.seq)
                            if dedup_key in seen_seqs:
                                continue
                            seen_seqs.add(dedup_key)
                            if item.type == "text":
                                console.print(item.content, end="")
                            elif item.type == "tool_use":
                                console.print(f"  [dim][tool] {item.content}[/dim]")
                            continue

                        # Terminal status
                        if "state" in data and "task_id" in data:
                            status = TaskStatusUpdate.from_json(payload)
                            console.print(f"\n\n{status.result}")
                            return

                        # Error
                        if "error" in data:
                            console.print(
                                f"[red]Error: {data['error'].get('message', data['error'])}[/red]"
                            )
                            return
            except TimeoutError:
                console.print("[yellow]Timed out waiting for result[/yellow]")

    asyncio.run(run_agent())


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
