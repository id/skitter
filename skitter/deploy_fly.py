"""Deploy skitter to Fly Machines.

Builds + pushes a single Docker image to a single Fly app. The deploy
creates a persistent supervisor machine (always-on, ~$2/mo). Workers
are ephemeral machines created by the supervisor via Fly Machines API.
"""

import json
import logging
import os
import shutil
import subprocess
import tempfile
from pathlib import Path

from dotenv import load_dotenv
from rich.console import Console

from skitter.config import AGENTS_DIR, CLAUDE_AGENTS_DIR, WORKFLOWS_DIR
from skitter.fly import FLY_APP, FLY_REGION

load_dotenv()

log = logging.getLogger("skitter.deploy_fly")
console = Console()
PROJECT_DIR = Path(__file__).parent.parent


def _fly(
    *args: str, check: bool = True, cwd: str | Path | None = None, stream: bool = False
) -> subprocess.CompletedProcess:
    """Run a fly CLI command."""
    cmd = ["fly", *args]
    if "secrets" in args:
        log.info("$ fly secrets set ... (redacted)")
    else:
        log.info("$ %s", " ".join(cmd))
    kwargs: dict = {"check": check, "text": True, "cwd": cwd}
    if not stream:
        kwargs["capture_output"] = True
    return subprocess.run(cmd, **kwargs)


def _set_secrets(secrets: dict[str, str]) -> None:
    """Set secrets on the Fly app (skips empty values)."""
    pairs = [f"{k}={v}" for k, v in secrets.items() if v]
    if not pairs:
        return
    result = _fly("secrets", "set", "-a", FLY_APP, *pairs, check=False)
    if result.returncode == 0:
        console.print(f"  Set {len(pairs)} secrets on [bold]{FLY_APP}[/bold]")
    else:
        console.print(f"  [yellow]Secrets warning: {result.stderr.strip()}[/yellow]")


def _prepare_build_context() -> Path:
    """Create a temp build context with project files + agent definitions."""
    ctx = Path(tempfile.mkdtemp(prefix="skitter-fly-"))

    # Project files
    shutil.copy2(PROJECT_DIR / "pyproject.toml", ctx / "pyproject.toml")
    shutil.copytree(
        PROJECT_DIR / "skitter",
        ctx / "skitter",
        ignore=shutil.ignore_patterns("__pycache__", "*.pyc", "*.pyo"),
    )

    # Agent definition files for claude --agent
    claude_dst = ctx / "home" / ".claude" / "agents"
    claude_dst.mkdir(parents=True, exist_ok=True)
    if CLAUDE_AGENTS_DIR.is_dir():
        for f in CLAUDE_AGENTS_DIR.glob("*.md"):
            shutil.copy2(f, claude_dst / f.name)

    # Skitter agent YAML stubs (for config loading)
    skitter_dst = ctx / "home" / ".skitter" / "agents"
    skitter_dst.mkdir(parents=True, exist_ok=True)
    if AGENTS_DIR.is_dir():
        for f in AGENTS_DIR.glob("*.yaml"):
            shutil.copy2(f, skitter_dst / f.name)

    # Workflow definitions
    workflows_dst = ctx / "home" / ".skitter" / "workflows"
    workflows_dst.mkdir(parents=True, exist_ok=True)
    if WORKFLOWS_DIR.is_dir():
        for f in WORKFLOWS_DIR.glob("*.yaml"):
            shutil.copy2(f, workflows_dst / f.name)

    # fly.toml — supervisor runs as the app process
    fly_toml = ctx / "fly.toml"
    fly_toml.write_text(
        f'app = "{FLY_APP}"\n'
        f'primary_region = "{FLY_REGION}"\n'
        "\n[build]\n\n"
        "[env]\n"
        '  SKITTER_SPAWN_MODE = "fly"\n'
        "\n[processes]\n"
        '  app = "python -m skitter.supervisor"\n'
    )

    # Write a Dockerfile that extends the worker image with agent files
    dockerfile = ctx / "Dockerfile"
    dockerfile.write_text(
        (PROJECT_DIR / "Dockerfile").read_text()
        + "\n"
        + "# Config files baked in at deploy time\n"
        "COPY --chown=skitter home/.claude/agents/ /home/skitter/.claude/agents/\n"
        "COPY --chown=skitter home/.skitter/agents/ /home/skitter/.skitter/agents/\n"
        "COPY --chown=skitter home/.skitter/workflows/ /home/skitter/.skitter/workflows/\n"
    )

    return ctx


def cmd_deploy_fly() -> None:
    """Deploy skitter to Fly."""
    console.print(f"[bold]Deploying to Fly app: {FLY_APP}[/bold]\n")

    # Prefer OAuth token over API key
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    oauth_token = os.environ.get("CLAUDE_CODE_OAUTH_TOKEN", "")
    if oauth_token:
        api_key = ""

    # 1. Set secrets
    console.print("Setting secrets...")
    _set_secrets(
        {
            "MQTT_HOST": os.environ.get("MQTT_HOST", ""),
            "MQTT_PORT": os.environ.get("MQTT_PORT", "8883"),
            "MQTT_TLS": "1",
            "MQTT_USER": os.environ.get("MQTT_USER", ""),
            "MQTT_PASS": os.environ.get("MQTT_PASS", ""),
            "ANTHROPIC_API_KEY": api_key,
            "CLAUDE_CODE_OAUTH_TOKEN": oauth_token,
            "OPENAI_API_KEY": os.environ.get("OPENAI_API_KEY", ""),
            "FLY_API_TOKEN": os.environ.get("FLY_API_TOKEN", ""),
            "FLY_APP": FLY_APP,
            "FLY_WORKER_IMAGE": f"registry.fly.io/{FLY_APP}:latest",
            "FLY_REGION": FLY_REGION,
        }
    )

    # 2. Build + deploy image (with agent files)
    #    fly deploy creates/updates the supervisor machine (always-on)
    console.print("\nBuilding and deploying...")
    build_ctx = _prepare_build_context()
    try:
        console.print(f"  Build context: {build_ctx}")
        result = _fly(
            "deploy",
            "-a",
            FLY_APP,
            "--ha=false",
            "--strategy",
            "immediate",
            "--image-label",
            "latest",
            check=False,
            cwd=build_ctx,
            stream=True,
        )
        if result.returncode != 0:
            console.print(f"  [red]Deploy failed (exit {result.returncode})[/red]")
            raise SystemExit(1)
        console.print(f"  Supervisor deployed to [bold]{FLY_APP}[/bold]")
    finally:
        shutil.rmtree(build_ctx, ignore_errors=True)

    # 3. Update FLY_WORKER_IMAGE secret with the deployed image ref
    result = _fly("machines", "list", "-a", FLY_APP, "--json", check=False)
    if result.returncode == 0:
        machines = json.loads(result.stdout)
        for m in machines:
            image_ref = m.get("config", {}).get("image", "")
            if image_ref:
                console.print(f"  Image: [bold]{image_ref}[/bold]")
                _set_secrets({"FLY_WORKER_IMAGE": image_ref})
                break

    console.print(f"\n[bold green]Done![/bold green] App: {FLY_APP}")
    console.print("  Supervisor publishes discovery cards on startup")
