"""CLI for managing and running workflows via A2A-over-MQTT."""

import asyncio
import sys
import uuid

import yaml
from rich.console import Console
from rich.table import Table

from skitter.config import load_workflows
from skitter.mqtt import send_and_wait, topic_request
from skitter.types import (
    A2ARequest,
)

console = Console()


def cmd_list() -> None:
    workflows = load_workflows()
    if not workflows:
        console.print("No workflows found in ~/.skitter/workflows/")
        console.print("Run 'skitter init' to create example workflows.")
        return
    table = Table(title="Workflows")
    table.add_column("ID", style="cyan")
    table.add_column("Name")
    table.add_column("Tasks", justify="right")
    table.add_column("Variables")
    for pid, workflow in workflows.items():
        table.add_row(
            pid,
            workflow.name,
            str(len(workflow.tasks)),
            ", ".join(workflow.variables) if workflow.variables else "",
        )
    console.print(table)


def cmd_show(workflow_id: str) -> None:
    workflows = load_workflows()
    workflow = workflows.get(workflow_id)
    if workflow is None:
        console.print(f"Workflow '{workflow_id}' not found.")
        available = ", ".join(workflows.keys()) if workflows else "(none)"
        console.print(f"Available: {available}")
        sys.exit(1)
    data = {
        "name": workflow.name,
        "description": workflow.description,
        "variables": workflow.variables,
        "tasks": [
            {
                "id": t.id,
                "agent": t.agent,
                "description": t.description,
                "next": t.next,
                "needs": t.needs,
            }
            for t in workflow.tasks
        ],
    }
    console.print(yaml.dump(data, default_flow_style=False, sort_keys=False))


def cmd_run(workflow_id: str, variables: dict[str, str], wait: bool = True) -> None:
    workflows = load_workflows()
    workflow = workflows.get(workflow_id)
    if workflow is None:
        console.print(f"Workflow '{workflow_id}' not found.")
        available = ", ".join(workflows.keys()) if workflows else "(none)"
        console.print(f"Available: {available}")
        sys.exit(1)

    missing = [v for v in workflow.variables if v not in variables]
    if missing:
        console.print(f"Missing required variables: {', '.join(missing)}")
        console.print(
            f"Usage: skitter workflow run {workflow_id} "
            + " ".join(f'--var {v}="..."' for v in missing)
        )
        sys.exit(1)

    session_id = f"workflow-{uuid.uuid4().hex[:8]}"
    description = f"Workflow '{workflow.name}'"
    if variables:
        var_str = ", ".join(f"{k}={v}" for k, v in variables.items())
        description += f" with {var_str}"

    req = A2ARequest(
        text=description,
        session_id=session_id,
        sender="cli",
        variables=variables,
    )

    console.print(f"Workflow '{workflow_id}' submitted as session {session_id}")
    if wait:
        console.print("Waiting for result... (Ctrl+C to detach)\n")

    from skitter.agents_cli import _print_reply

    asyncio.run(
        send_and_wait(
            topic_request(f"workflow-{workflow_id}"),
            req.to_json(),
            session_id,
            _print_reply,
            wait=wait,
        )
    )


def parse_vars(args: list[str]) -> dict[str, str]:
    """Parse --var key=value pairs from args."""
    variables: dict[str, str] = {}
    i = 0
    while i < len(args):
        if args[i] == "--var" and i + 1 < len(args):
            kv = args[i + 1]
            if "=" in kv:
                key, value = kv.split("=", 1)
                variables[key.strip()] = value.strip()
            i += 2
        else:
            i += 1
    return variables


def main() -> None:
    args = sys.argv[2:]  # skip "skitter" and "workflow"
    if not args or args[0] == "list":
        cmd_list()
    elif args[0] == "show":
        if len(args) < 2:
            console.print("Usage: skitter workflow show <workflow_id>")
            sys.exit(1)
        cmd_show(args[1])
    elif args[0] == "run":
        if len(args) < 2:
            console.print(
                "Usage: skitter workflow run <workflow_id> [--var key=value ...]"
            )
            sys.exit(1)
        variables = parse_vars(args[2:])
        wait = "--no-wait" not in args
        cmd_run(args[1], variables, wait=wait)
    else:
        console.print(f"Unknown subcommand: {args[0]}")
        console.print(
            "Usage: skitter workflow [list|show <id>|run <id> --var key=value]"
        )
        sys.exit(1)
