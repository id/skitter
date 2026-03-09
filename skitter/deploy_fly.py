"""Deploy skitter to Fly Machines.

Builds + pushes a single Docker image to a single Fly app ('skitter').
Both supervisor and worker machines are created from this image with
different entrypoints and env vars.

Worker image includes agent definition files baked in.
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

from skitter.config import AGENTS_DIR, CLAUDE_AGENTS_DIR
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

    # fly.toml for the app
    fly_toml = ctx / "fly.toml"
    fly_toml.write_text(
        f'app = "{FLY_APP}"\n'
        f'primary_region = "{FLY_REGION}"\n'
        "\n[build]\n\n"
        "# No services — machines are ephemeral, created via API\n"
    )

    # Write a Dockerfile that extends the worker image with agent files
    dockerfile = ctx / "Dockerfile"
    dockerfile.write_text(
        (PROJECT_DIR / "Dockerfile").read_text()
        + "\n"
        + "# Agent files baked in at deploy time\n"
        "COPY --chown=skitter home/.claude/agents/ /home/skitter/.claude/agents/\n"
        "COPY --chown=skitter home/.skitter/agents/ /home/skitter/.skitter/agents/\n"
    )

    return ctx


def cmd_deploy_fly() -> None:
    """Deploy skitter to Fly."""
    console.print(f"[bold]Deploying to Fly app: {FLY_APP}[/bold]\n")

    # 1. Set secrets
    console.print("Setting secrets...")
    _set_secrets(
        {
            "MQTT_HOST": os.environ.get("MQTT_HOST", ""),
            "MQTT_PORT": os.environ.get("MQTT_PORT", "8883"),
            "MQTT_TLS": "1",
            "MQTT_USER": os.environ.get("MQTT_USER", ""),
            "MQTT_PASS": os.environ.get("MQTT_PASS", ""),
            "ANTHROPIC_API_KEY": os.environ.get("ANTHROPIC_API_KEY", ""),
            "OPENAI_API_KEY": os.environ.get("OPENAI_API_KEY", ""),
            "FLY_API_TOKEN": os.environ.get("FLY_API_TOKEN", ""),
            "FLY_APP": FLY_APP,
            "FLY_WORKER_IMAGE": f"registry.fly.io/{FLY_APP}:latest",
            "FLY_REGION": FLY_REGION,
            "SKITTER_SPAWN_MODE": "fly",
        }
    )

    # 2. Build + deploy image (with agent files)
    console.print("\nBuilding image...")
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
        console.print(f"  Image deployed to [bold]{FLY_APP}[/bold]")
    finally:
        shutil.rmtree(build_ctx, ignore_errors=True)

    # 3. Get actual image ref and clean up deploy-created machine
    image_ref = ""
    result = _fly("machines", "list", "-a", FLY_APP, "--json", check=False)
    if result.returncode == 0:
        machines = json.loads(result.stdout)
        for m in machines:
            if not image_ref:
                image_ref = m.get("config", {}).get("image", "")
            mid = m["id"]
            _fly("machine", "destroy", mid, "-a", FLY_APP, "--force", check=False)
            console.print(f"  Cleaned up deploy machine {mid}")

    if image_ref:
        console.print(f"  Image ref: [bold]{image_ref}[/bold]")
        _set_secrets({"FLY_WORKER_IMAGE": image_ref})
    else:
        console.print("  [yellow]Could not determine image ref[/yellow]")

    # 4. Publish discovery cards
    console.print("\nPublishing discovery cards...")
    from skitter.deploy import publish_discovery_via_emqx
    from skitter.emqx import EMQX_API_URL

    if EMQX_API_URL:
        n = publish_discovery_via_emqx()
        console.print(f"  Published {n} discovery cards")
    else:
        console.print("  [yellow]EMQX_API_URL not set — skipping[/yellow]")

    console.print(f"\n[bold green]Done![/bold green] App: {FLY_APP}")
