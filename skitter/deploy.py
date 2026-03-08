"""Deploy agent configs, cards, and workflows to R2 + publish discovery."""

import json
import logging
from pathlib import Path

from rich.console import Console

from skitter.config import AGENTS_DIR, CARDS_DIR, WORKFLOWS_DIR
from skitter.storage import R2_BUCKET, R2_ENDPOINT, _get_r2_client

log = logging.getLogger("skitter.deploy")
console = Console()


def _upload_file(client, local_path: Path, r2_key: str) -> None:
    client.put_object(
        Bucket=R2_BUCKET,
        Key=r2_key,
        Body=local_path.read_bytes(),
        ContentType="application/octet-stream",
    )


def deploy_agents() -> int:
    """Sync agent YAML stubs and Claude sub-agent .md files to R2."""
    client = _get_r2_client()
    count = 0

    # Agent YAML stubs
    if AGENTS_DIR.is_dir():
        for path in sorted(AGENTS_DIR.glob("*.yaml")):
            _upload_file(client, path, f"config/agents/{path.name}")
            count += 1

    # Claude sub-agent definitions
    claude_agents = Path.home() / ".claude" / "agents"
    if claude_agents.is_dir():
        for path in sorted(claude_agents.glob("*.md")):
            _upload_file(client, path, f"claude-agents/{path.name}")
            count += 1

    # Codex agent configs
    codex_agents = Path.home() / ".codex" / "agents"
    if codex_agents.is_dir():
        for path in sorted(codex_agents.glob("*.toml")):
            _upload_file(client, path, f"codex-config/agents/{path.name}")
            count += 1
    # Codex main config
    codex_config = Path.home() / ".codex" / "config.toml"
    if codex_config.is_file():
        _upload_file(client, codex_config, "codex-config/config.toml")
        count += 1

    # Pre-built cards
    if CARDS_DIR.is_dir():
        for path in sorted(CARDS_DIR.glob("*.json")):
            _upload_file(client, path, f"cards/{path.name}")
            count += 1

    return count


def deploy_workflows() -> int:
    """Sync workflow definitions to R2."""
    client = _get_r2_client()
    count = 0
    if WORKFLOWS_DIR.is_dir():
        for path in sorted(WORKFLOWS_DIR.glob("*.yaml")):
            _upload_file(client, path, f"config/workflows/{path.name}")
            count += 1
    return count


def publish_discovery_via_emqx() -> int:
    """Publish discovery cards via EMQX REST API."""
    from skitter import emqx
    from skitter.mqtt import topic_discovery

    count = 0
    if CARDS_DIR.is_dir():
        for path in sorted(CARDS_DIR.glob("*.json")):
            try:
                card_json = path.read_text()
                json.loads(card_json)  # validate
                emqx.publish(topic_discovery(path.stem), card_json, qos=1, retain=True)
                count += 1
            except Exception as e:
                log.warning("Failed to publish card %s: %s", path.name, e)
    return count


def cmd_deploy(what: str = "all") -> None:
    """Deploy to R2 and publish discovery cards."""
    if not R2_ENDPOINT:
        console.print(
            "[red]R2_ENDPOINT not set. Configure R2 credentials in .env.cloud[/red]"
        )
        return

    if what in ("all", "agents"):
        n = deploy_agents()
        console.print(f"Uploaded {n} agent files to R2")

    if what in ("all", "workflows"):
        n = deploy_workflows()
        console.print(f"Uploaded {n} workflow files to R2")

    if what in ("all", "discovery"):
        from skitter.emqx import EMQX_API_URL

        if EMQX_API_URL:
            n = publish_discovery_via_emqx()
            console.print(f"Published {n} discovery cards via EMQX REST API")
        else:
            console.print(
                "[yellow]EMQX_API_URL not set — skipping discovery publish[/yellow]"
            )
