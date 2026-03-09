"""CLI for managing Docker-based worker setup."""

import shutil
import subprocess
import sys
from pathlib import Path

from rich.console import Console

from skitter.config import DOCKER_CLAUDE_DIR
from skitter.spawn import DOCKER_NETWORK, DOCKER_USER_HOME, WORKER_IMAGE

console = Console()

HOST_CLAUDE_DIR = Path.home() / ".claude"


def sync_claude_dir() -> None:
    """Copy ~/.claude/agents/ and ~/.claude/agent-memory/ into docker-claude dir."""
    DOCKER_CLAUDE_DIR.mkdir(parents=True, exist_ok=True)

    for subdir in ("agents", "agent-memory"):
        src = HOST_CLAUDE_DIR / subdir
        dst = DOCKER_CLAUDE_DIR / subdir
        if src.is_dir():
            if dst.exists():
                shutil.rmtree(dst)
            shutil.copytree(src, dst)
            console.print(f"  Synced {subdir}/ ({len(list(dst.iterdir()))} items)")


def cmd_login() -> None:
    """Run interactive container for Claude OAuth login."""
    DOCKER_CLAUDE_DIR.mkdir(parents=True, exist_ok=True)

    console.print("Starting interactive container for Claude login...")
    console.print(f"  Mounting {DOCKER_CLAUDE_DIR} → {DOCKER_USER_HOME}/.claude/")
    console.print(
        "  Run [bold]/login[/bold] inside the container, then [bold]exit[/bold].\n"
    )

    result = subprocess.run(
        [
            "docker",
            "run",
            "-it",
            "--rm",
            "--network",
            DOCKER_NETWORK,
            "-v",
            f"{DOCKER_CLAUDE_DIR}:{DOCKER_USER_HOME}/.claude",
            "--entrypoint",
            "claude",
            WORKER_IMAGE,
        ],
    )

    creds = DOCKER_CLAUDE_DIR / ".credentials.json"
    if creds.exists():
        console.print(
            f"\n[green]Login successful.[/green] Credentials saved to {creds}"
        )
        console.print("Run [bold]skitter docker sync[/bold] to copy agent definitions.")
    else:
        console.print(f"\n[yellow]No credentials found at {creds}[/yellow]")
        if result.returncode != 0:
            console.print(f"Container exited with code {result.returncode}")


def cmd_sync() -> None:
    """Sync agent definitions and memory into docker-claude dir."""
    creds = DOCKER_CLAUDE_DIR / ".credentials.json"
    if not creds.exists():
        console.print(
            "[yellow]No credentials found. Run 'skitter docker login' first.[/yellow]"
        )

    sync_claude_dir()
    console.print("[green]Docker claude dir synced.[/green]")


def cmd_build() -> None:
    """Build the worker Docker image."""
    console.print(f"Building {WORKER_IMAGE}...")
    result = subprocess.run(
        ["docker", "build", "-t", WORKER_IMAGE, "."],
    )
    if result.returncode == 0:
        console.print(f"[green]Built {WORKER_IMAGE}[/green]")
    else:
        sys.exit(result.returncode)


def main() -> None:
    args = sys.argv[2:]  # skip "skitter" and "docker"
    if not args:
        console.print("Usage: skitter docker [login|sync|build]")
        console.print(
            "  login  — run interactive container to authenticate with Claude"
        )
        console.print("  sync   — copy agent definitions into docker-claude dir")
        console.print("  build  — build the worker Docker image")
        sys.exit(1)
    elif args[0] == "login":
        cmd_login()
    elif args[0] == "sync":
        cmd_sync()
    elif args[0] == "build":
        cmd_build()
    else:
        console.print(f"Unknown subcommand: {args[0]}")
        console.print("Usage: skitter docker [login|sync|build]")
        sys.exit(1)
