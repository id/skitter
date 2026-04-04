"""Container management via generated docker compose file.

``skitter up`` generates ``~/.skitter/docker-compose.yml`` from the current
config and discovered agents, then delegates to ``docker compose``.
"""

import logging
import os
import subprocess
import sys
from pathlib import Path
from urllib.parse import urlparse

import yaml

from skitter.config import (
    SkitterConfig,
    config_file,
    detect_runtimes,
    load_config,
    skitter_home,
)

log = logging.getLogger("skitter.services")

BROKER_CONTAINER = "skitter-emqx"
BROKER_IMAGE = "emqx/emqx-enterprise:6.2.0"
COORDINATOR_CONTAINER = "skitter-coordinator"

_CA_CERT_CONTAINER_PATH = "/etc/skitter/ca.crt"

_RUNTIME_AUTH_ENVS: dict[str, list[str]] = {
    "claude": ["CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY"],
    "codex": ["OPENAI_API_KEY"],
}

_RUNTIME_SESSION_PATHS: dict[str, str] = {
    "claude": "/home/skitter/.claude",
    "codex": "/home/skitter/.codex",
}


# --- Low-level helpers (also used by doctor.py) ---

_docker_ok: bool | None = None


def _docker_available() -> bool:
    """Check if the docker CLI is installed and responsive (cached)."""
    global _docker_ok
    if _docker_ok is None:
        import shutil

        if not shutil.which("docker"):
            _docker_ok = False
        else:
            try:
                r = subprocess.run(
                    ["docker", "info"],
                    check=False,
                    capture_output=True,
                    text=True,
                    timeout=10,
                )
                _docker_ok = r.returncode == 0
            except Exception:
                _docker_ok = False
    return _docker_ok


def _run(
    args: list[str], *, check: bool = True, capture: bool = False
) -> subprocess.CompletedProcess:
    log.debug("$ %s", " ".join(args))
    return subprocess.run(args, check=check, capture_output=capture, text=True)


def _container_running(name: str) -> bool:
    if not _docker_available():
        return False
    r = _run(
        ["docker", "inspect", "-f", "{{.State.Running}}", name],
        check=False,
        capture=True,
    )
    return r.returncode == 0 and r.stdout.strip() == "true"


def _verify_broker_connectivity(url: str) -> None:
    """Verify that an external broker is reachable. Raises ConnectionError on failure."""
    import socket

    parsed = urlparse(url)
    host = parsed.hostname or "localhost"
    port = parsed.port or (8883 if parsed.scheme == "mqtts" else 1883)
    try:
        with socket.create_connection((host, port), timeout=5):
            log.info("Broker at %s:%d reachable", host, port)
    except OSError as e:
        raise ConnectionError(f"Cannot reach broker at {host}:{port}: {e}") from e


def _agent_container_name(agent_id: str) -> str:
    return f"skitter-agent-{agent_id}"


def _discover_agents() -> list[tuple[str, str, str]]:
    """Scan ~/.skitter/agents/ for agent definitions.

    Returns list of (agent_id, filename, runtime).
    Delegates to :func:`agent_runner.scan_agents` for parsing.
    """
    from skitter.agent_runner import scan_agents

    return scan_agents()


# --- Image tag ---


def _image(name: str) -> str:
    """Container image for a skitter service."""
    try:
        from importlib.metadata import version

        tag = version("skitter")
    except Exception:
        tag = "latest"
    return f"ghcr.io/id/skitter/{name}:{tag}"


# --- Compose file generation ---


def _compose_file() -> Path:
    return skitter_home() / "docker-compose.yml"


def _resolve_env(cfg: SkitterConfig) -> dict[str, str]:
    """Build environment dict for containers from resolved config."""
    broker_host = BROKER_CONTAINER if cfg.broker.tier == "docker" else ""
    env = cfg.to_env(broker_hostname=broker_host)

    # CA cert: remap host path to container path
    if cfg.broker.ca_cert:
        env["MQTT_CA_CERT"] = _CA_CERT_CONTAINER_PATH

    # LLM API key from host env (not in config file)
    api_key = os.environ.get("SKITTER_LLM_API_KEY", "")
    if api_key:
        env["SKITTER_LLM_API_KEY"] = api_key

    return env


def _generate_compose(
    cfg: SkitterConfig,
    agents: list[tuple[str, str, str]],
) -> str:
    """Generate a docker-compose.yml from config and discovered agents."""
    from skitter.config import skills_dir

    services: dict = {}
    volumes: dict = {}
    env = _resolve_env(cfg)

    # Broker (Docker tier only)
    if cfg.broker.tier == "docker":
        services["emqx"] = {
            "image": BROKER_IMAGE,
            "container_name": BROKER_CONTAINER,
            "ports": ["1883:1883", "8083:8083", "18083:18083"],
            "environment": {"EMQX_A2A_REGISTRY__ENABLE": "true"},
            "healthcheck": {
                "test": ["CMD", "emqx", "ctl", "status"],
                "interval": "10s",
                "timeout": "5s",
                "retries": 5,
            },
        }

    depends: dict = {}
    if cfg.broker.tier == "docker":
        depends = {"emqx": {"condition": "service_healthy"}}

    # Coordinator
    coord_volumes = [f"{skitter_home()}:/home/skitter/.skitter"]
    if cfg.broker.ca_cert:
        coord_volumes.append(f"{cfg.broker.ca_cert}:{_CA_CERT_CONTAINER_PATH}:ro")
    coord_svc: dict = {
        "image": _image("coordinator"),
        "container_name": COORDINATOR_CONTAINER,
        "environment": env,
        "volumes": coord_volumes,
        "restart": "on-failure",
    }
    if depends:
        coord_svc["depends_on"] = depends
    services["coordinator"] = coord_svc

    # Agents
    agents_dir = str(skitter_home() / "agents")
    sd = skills_dir()
    for agent_id, filename, runtime in agents:
        cname = _agent_container_name(agent_id)
        agent_env = dict(env)
        agent_env["SKITTER_HOME"] = "/app"

        for var in _RUNTIME_AUTH_ENVS.get(runtime, []):
            val = os.environ.get(var, "")
            if val:
                agent_env[var] = val

        agent_vols = [f"{agents_dir}:/app/agents:ro"]
        if sd.is_dir():
            agent_vols.append(f"{sd}:/app/skills:ro")
        session_path = _RUNTIME_SESSION_PATHS.get(runtime)
        if session_path:
            vol_name = f"{cname}-sessions"
            agent_vols.append(f"{vol_name}:{session_path}")
            volumes[vol_name] = None
        if cfg.broker.ca_cert:
            agent_vols.append(f"{cfg.broker.ca_cert}:{_CA_CERT_CONTAINER_PATH}:ro")

        agent_svc: dict = {
            "image": _image("agent"),
            "container_name": cname,
            "environment": agent_env,
            "volumes": agent_vols,
            "command": [f"agents/{filename}"],
            "restart": "on-failure",
        }
        if depends:
            agent_svc["depends_on"] = depends
        services[f"agent-{agent_id}"] = agent_svc

    compose: dict = {
        "services": services,
        "networks": {"default": {"name": "skitter", "driver": "bridge"}},
    }
    if volumes:
        compose["volumes"] = volumes

    header = "# Auto-generated by 'skitter up'. Do not edit.\n"
    return header + yaml.dump(compose, default_flow_style=False, sort_keys=False)


def _compose(
    *args: str, check: bool = True, capture: bool = False
) -> subprocess.CompletedProcess:
    """Run ``docker compose -f <generated-file> <args>``."""
    cmd = [
        "docker",
        "compose",
        "-f",
        str(_compose_file()),
        "-p",
        "skitter",
        *args,
    ]
    return _run(cmd, check=check, capture=capture)


# --- Public commands ---


def up(argv: list[str] | None = None) -> None:
    """Start broker (Docker tier only), coordinator, and all agents."""
    argv = argv or []
    broker_only = "--broker-only" in argv
    single_agent = ""
    if "--agent" in argv:
        idx = argv.index("--agent")
        if idx + 1 < len(argv):
            single_agent = argv[idx + 1]

    cfg = load_config()

    if cfg.broker.tier != "docker":
        print(f"Verifying external broker ({cfg.broker.url})...")
        try:
            _verify_broker_connectivity(cfg.broker.url)
        except ConnectionError as e:
            print(f"Error: {e}", file=sys.stderr)
            sys.exit(1)

    agents = _discover_agents()
    if single_agent:
        agents = [(aid, f, r) for aid, f, r in agents if aid == single_agent]
        if not agents:
            print(
                f"Agent '{single_agent}' not found in ~/.skitter/agents/",
                file=sys.stderr,
            )
            sys.exit(1)

    # Generate compose file (always regenerate to pick up config/agent changes)
    compose_yaml = _generate_compose(cfg, [] if broker_only else agents)
    _compose_file().parent.mkdir(parents=True, exist_ok=True)
    _compose_file().write_text(compose_yaml)

    # Start
    targets: list[str] = []
    if broker_only:
        targets = ["emqx"]
    elif single_agent:
        targets = [f"agent-{single_agent}"]
    _compose("up", "-d", "--wait", *targets)

    if broker_only:
        print("Broker ready. Use 'uv run skitter' to run coordinator from source.")
    else:
        print("All services started.")


def down(argv: list[str] | None = None) -> None:
    """Stop all skitter containers (or a single agent with --agent)."""
    if not _docker_available():
        print("Docker is not available. Nothing to stop.")
        return
    if not _compose_file().is_file():
        print("No compose file found. Nothing to stop.")
        return

    argv = argv or []
    single_agent = ""
    if "--agent" in argv:
        idx = argv.index("--agent")
        if idx + 1 < len(argv):
            single_agent = argv[idx + 1]

    if single_agent:
        svc = f"agent-{single_agent}"
        _compose("stop", svc, check=False)
        _compose("rm", "-f", svc, check=False)
        print(f"Stopped agent {single_agent}.")
    else:
        _compose("down", check=False)
        print("All services stopped.")


def _compose_container_status() -> list[tuple[str, str, str]]:
    """Return (name, status, image) for compose-managed containers."""
    if not _docker_available() or not _compose_file().is_file():
        return []
    r = _compose("ps", "-a", "--format", "json", check=False, capture=True)
    if r.returncode != 0:
        return []

    import json

    results: list[tuple[str, str, str]] = []
    for line in r.stdout.strip().splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            continue
        name = obj.get("Name", "")
        state = obj.get("State", "")
        status = obj.get("Status", state)
        image = obj.get("Image", "")
        if name:
            results.append((name, status, image))
    return results


def check_running() -> tuple[bool, bool, dict[str, bool]]:
    """Check which services are running.

    Returns ``(docker_ok, coordinator_running, {agent_id: running})``.
    Used by both ``status`` and ``doctor``.
    """
    if not _docker_available():
        return False, False, {}
    containers = _compose_container_status()
    running_names = {n for n, s, _ in containers if "running" in s.lower() or "Up" in s}
    coord_running = COORDINATOR_CONTAINER in running_names
    agents = _discover_agents()
    agent_status = {
        aid: _agent_container_name(aid) in running_names for aid, _, _ in agents
    }
    return True, coord_running, agent_status


def _next_action(
    has_config: bool,
    runtimes: list[str],
    containers: list[tuple[str, str, str]],
    agents: list[tuple[str, str, str]],
) -> str:
    """Return the recommended next action based on current state."""
    if not has_config:
        return "Run 'skitter setup' to configure."
    if not runtimes:
        return "Install claude or codex CLI."
    running = [n for n, s, _ in containers if "running" in s.lower() or "Up" in s]
    if not running:
        return "Run 'skitter up' to start services."
    if not agents:
        return "Run 'skitter create-agent' to create your first agent."
    first_agent = agents[0][0]
    return f"Run 'skitter ask {first_agent} \"hello\"'."


def status(argv: list[str] | None = None) -> None:
    """Show readiness overview: config, runtimes, broker, agents, next action."""
    from rich.console import Console
    from rich.table import Table

    console = Console()
    home = skitter_home()
    cfg_path = config_file()

    console.print()

    # Home and config
    console.print(f"  Home:    {home}")
    console.print(
        f"  Config:  {cfg_path}", style="green" if cfg_path.is_file() else "red"
    )

    # Runtime availability
    runtimes_map = detect_runtimes()
    runtimes: list[str] = []
    for name, path in runtimes_map.items():
        if path:
            runtimes.append(name)
            console.print(f"  Runtime: {name} [green](found)[/green]")
        else:
            console.print(f"  Runtime: {name} [dim](not found)[/dim]")

    # Broker and coordinator from config
    has_config = cfg_path.is_file()
    if has_config:
        cfg = load_config()
        console.print(f"  Broker:  {cfg.broker.tier} ({cfg.broker.url})")
        if cfg.llm.model:
            console.print(f"  LLM:     {cfg.llm.model}")
    else:
        console.print("  Broker:  [dim](not configured)[/dim]")

    # Docker + containers
    docker_ok = _docker_available()
    if not docker_ok:
        console.print("  Docker:  [dim](not available)[/dim]")
    containers = _compose_container_status()
    agents = _discover_agents()

    if containers:
        console.print()
        table = Table(show_header=True, padding=(0, 1))
        table.add_column("Container")
        table.add_column("Status")
        table.add_column("Image")
        for name, st, image in containers:
            style = "green" if "running" in st.lower() or "Up" in st else "red"
            table.add_row(name, f"[{style}]{st}[/{style}]", image)
        console.print(table)

    # Agent summary
    if agents:
        running_names = {n for n, s, _ in containers if s in ("running", "Up")}
        running_agents = {
            aid for aid, _, _ in agents if _agent_container_name(aid) in running_names
        }
        console.print(
            f"\n  Agents: {len(agents)} defined, {len(running_agents)} running"
        )
        for aid, fname, runtime in agents:
            marker = (
                "[green]running[/green]"
                if aid in running_agents
                else "[dim]stopped[/dim]"
            )
            console.print(f"    {aid} ({runtime}) {marker}")
    elif has_config:
        console.print("\n  Agents: none defined")

    # Next action
    action = _next_action(has_config, runtimes, containers, agents)
    console.print(f"\n  [bold]{action}[/bold]")
    console.print()


def logs(argv: list[str] | None = None) -> None:
    """View logs for a skitter service."""
    argv = argv or []
    if not argv:
        print("Usage: skitter logs <emqx|coordinator|agent-ID>", file=sys.stderr)
        sys.exit(1)
    if not _compose_file().is_file():
        print("No compose file found. Run 'skitter up' first.", file=sys.stderr)
        sys.exit(1)

    service = argv[0]
    if service not in ("emqx", "coordinator"):
        agent_id = service.removeprefix("agent-")
        service = f"agent-{agent_id}"

    flags: list[str] = ["--tail", "100"]
    if "--follow" in argv or "-f" in argv:
        flags.append("-f")

    _compose("logs", *flags, service, check=False)
