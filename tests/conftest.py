"""Shared fixtures and helpers for live e2e tests.

Provides: process management (coordinator, agent-runner in Docker),
MQTT helpers (send_and_collect, wait_for_discovery), and agent config.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import subprocess
import tempfile
import time
import uuid
from pathlib import Path

import aiomqtt
import pytest
import yaml

from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    MQTT_TLS,
    A2A_ORG,
    A2A_UNIT,
    make_properties,
    mqtt_client_kwargs,
    topic_discovery,
    topic_reply,
)
from skitter.types import (
    A2ARequest,
    REPLY_ERROR,
    REPLY_TERMINAL,
    REPLY_TEXT,
    classify_reply,
)


AGENT_IMAGE = os.environ.get("SKITTER_AGENT_IMAGE", "skitter-agent:latest")
COORDINATOR_IMAGE = os.environ.get("SKITTER_COORDINATOR_IMAGE", "skitter:latest")
_ENV_TEST = Path(__file__).resolve().parent.parent / ".env.test"


def _load_env_test() -> dict[str, str]:
    """Load key=value pairs from .env.test into a dict."""
    from dotenv import dotenv_values

    return dict(dotenv_values(_ENV_TEST)) if _ENV_TEST.is_file() else {}


# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------


def mqtt_available() -> bool:
    try:
        s = socket.create_connection((MQTT_HOST, MQTT_PORT), timeout=2)
        if MQTT_TLS:
            import ssl

            ctx = ssl.create_default_context()
            s = ctx.wrap_socket(s, server_hostname=MQTT_HOST)
        s.close()
        return True
    except OSError:
        return False


def docker_available() -> bool:
    return bool(shutil.which("docker"))


needs_mqtt = pytest.mark.skipif(not mqtt_available(), reason="No MQTT broker")


def runtime_available(runtime: str) -> bool:
    env = {**_load_env_test(), **os.environ}
    if runtime == "claude":
        return bool(env.get("CLAUDE_CODE_OAUTH_TOKEN"))
    if runtime == "codex":
        return (
            bool(env.get("OPENAI_API_KEY"))
            or Path.home().joinpath(".codex/auth.json").is_file()
        )
    return False


def pytest_addoption(parser):
    parser.addoption(
        "--runtime",
        action="store",
        default=None,
        choices=["claude", "codex"],
        help="Runtime to test (claude or codex). Default: auto-detect.",
    )


# ---------------------------------------------------------------------------
# Agent file scaffolding (written to temp dirs, mounted into Docker)
# ---------------------------------------------------------------------------


def write_agent_file(agent_id: str, runtime: str, model: str = "") -> Path:
    """Write a native agent file to a temp dir. Returns the path."""
    tmp = Path(tempfile.mkdtemp(prefix=f"skitter-test-{agent_id}-"))
    if runtime == "codex":
        path = tmp / f"{agent_id}.toml"
        lines = []
        if model:
            lines.append(f'model = "{model}"')
        lines.append(f'developer_instructions = "Minimal {runtime} test agent"')
        path.write_text("\n".join(lines) + "\n")
    else:
        path = tmp / f"{agent_id}.md"
        frontmatter = {
            "name": agent_id,
            "description": f"Minimal {runtime} test agent",
        }
        if model:
            frontmatter["model"] = model
        fm = yaml.dump(frontmatter, default_flow_style=False, sort_keys=False)
        path.write_text(f"---\n{fm}---\nYou are a test agent. Be brief.\n")
    return path


# ---------------------------------------------------------------------------
# Process management
# ---------------------------------------------------------------------------


def _wait_for_container(
    container_id: str,
    *,
    marker: str,
    timeout: float,
    label: str,
) -> None:
    """Poll docker logs until *marker* appears. Fails the test on timeout or early exit."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        logs = subprocess.run(
            ["docker", "logs", container_id],
            capture_output=True,
            text=True,
        )
        if marker in logs.stdout or marker in logs.stderr:
            return
        inspect = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", container_id],
            capture_output=True,
            text=True,
        )
        if inspect.stdout.strip() != "true":
            pytest.fail(f"{label} exited early:\n{logs.stdout}{logs.stderr}")
        time.sleep(0.3)

    all_logs = subprocess.run(
        ["docker", "logs", container_id],
        capture_output=True,
        text=True,
    )
    stop_container(container_id)
    pytest.fail(
        f"{label} not ready within {timeout:.0f}s:\n{all_logs.stdout}\n{all_logs.stderr}"
    )


_COORDINATOR_ENV_KEYS = {
    "ANTHROPIC_API_KEY",
    "OPENAI_API_KEY",
    "SKITTER_LLM_MODEL",
    "SKITTER_A2A_ORG",
    "SKITTER_A2A_UNIT",
}


def start_coordinator(*, env_vars: dict[str, str] | None = None) -> str:
    """Start the coordinator in Docker. Returns the container ID.

    Uses an in-memory SQLite DB (no host state) and connects to the
    broker via the skitter Docker network.  Only coordinator-relevant
    env vars from .env.test are passed through.
    """
    all_env = {**_load_env_test(), **(env_vars or {})}
    merged = {k: v for k, v in all_env.items() if k in _COORDINATOR_ENV_KEYS}
    cmd = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--network=skitter",
        "-e",
        "MQTT_HOST=emqx",
    ]
    for key, val in merged.items():
        cmd.extend(["-e", f"{key}={val}"])

    cmd.append(COORDINATOR_IMAGE)

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        pytest.fail(f"Failed to start coordinator container: {result.stderr}")
    container_id = result.stdout.strip()
    _wait_for_container(container_id, marker="ready", timeout=15, label="Coordinator")
    return container_id


_image_checked = False


def _ensure_agent_image() -> None:
    """Build the agent Docker image if it doesn't exist."""
    global _image_checked
    if _image_checked:
        return
    result = subprocess.run(
        ["docker", "image", "inspect", AGENT_IMAGE],
        capture_output=True,
    )
    if result.returncode != 0:
        print(f"Building {AGENT_IMAGE}...")
        subprocess.run(
            ["docker", "build", "-f", "Dockerfile.agent", "-t", AGENT_IMAGE, "."],
            check=True,
        )
    _image_checked = True


def start_agent_runner(agent_path: str | Path, runtime: str) -> str:
    """Start an agent-runner in Docker. Returns the container ID.

    Credentials come from .env.test (CLAUDE_CODE_OAUTH_TOKEN or OPENAI_API_KEY).
    For codex, also mounts ~/.codex/auth.json if present.
    """
    _ensure_agent_image()
    agent_path = Path(agent_path)
    container_agent_path = f"/tmp/agents/{agent_path.name}"
    env = _load_env_test()

    cmd = [
        "docker",
        "run",
        "-d",
        "--rm",
        "--network=skitter",
        "-e",
        "MQTT_HOST=emqx",
        # Mount the agent file
        "-v",
        f"{agent_path}:{container_agent_path}:ro",
    ]

    if runtime == "claude":
        token = env.get("CLAUDE_CODE_OAUTH_TOKEN") or os.environ.get(
            "CLAUDE_CODE_OAUTH_TOKEN"
        )
        if not token:
            pytest.fail("CLAUDE_CODE_OAUTH_TOKEN required for claude runtime")
        cmd.extend(["-e", f"CLAUDE_CODE_OAUTH_TOKEN={token}"])
    elif runtime == "codex":
        codex_auth = Path.home() / ".codex" / "auth.json"
        if codex_auth.is_file():
            cmd.extend(["-v", f"{codex_auth}:/home/skitter/.codex/auth.json:ro"])
        if env.get("OPENAI_API_KEY"):
            cmd.extend(["-e", f"OPENAI_API_KEY={env['OPENAI_API_KEY']}"])

    cmd.extend([AGENT_IMAGE, container_agent_path])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        pytest.fail(f"Failed to start agent container: {result.stderr}")
    container_id = result.stdout.strip()
    _wait_for_container(
        container_id,
        marker="Listening on",
        timeout=30,
        label="Agent",
    )
    return container_id


def stop_container(container_id: str) -> None:
    """Stop a Docker container."""
    subprocess.run(["docker", "stop", "-t", "5", container_id], capture_output=True)


# ---------------------------------------------------------------------------
# MQTT helpers
# ---------------------------------------------------------------------------


async def wait_for_discovery(agent_id: str, timeout: float = 30.0) -> dict:
    """Wait for an agent's discovery card to appear on the broker."""
    topic = topic_discovery(agent_id)
    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{A2A_ORG}/{A2A_UNIT}/test-disco-{uuid.uuid4().hex[:6]}",
        ),
    ) as client:
        await client.subscribe(topic, qos=1)
        try:
            async with asyncio.timeout(timeout):
                async for msg in client.messages:
                    if msg.payload:
                        return json.loads(msg.payload)
        except TimeoutError:
            pytest.fail(
                f"Discovery card for '{agent_id}' did not appear within {timeout}s"
            )
    return {}


async def send_and_collect(
    request_topic: str,
    msg: A2ARequest,
    timeout: float = 60.0,
) -> str:
    """Publish A2A request, stream replies, return terminal result."""
    test_id = uuid.uuid4().hex[:8]
    reply_t = topic_reply("test", test_id)

    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{A2A_ORG}/{A2A_UNIT}/test-client-{test_id}",
        ),
    ) as client:
        await client.subscribe(reply_t, qos=1)

        props = make_properties(
            response_topic=reply_t,
            correlation_data=msg.request_id,
        )
        await client.publish(
            request_topic,
            msg.to_json(),
            qos=1,
            properties=props,
        )

        try:
            async with asyncio.timeout(timeout):
                async for mqtt_msg in client.messages:
                    payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                    if not payload:
                        continue
                    try:
                        data = json.loads(payload)
                    except Exception:
                        continue

                    kind, content = classify_reply(data)
                    if kind == REPLY_TEXT:
                        print(content, end="", flush=True)
                    elif kind == REPLY_TERMINAL:
                        print()
                        return content
                    elif kind == REPLY_ERROR:
                        return f"Error: {content}"
        except TimeoutError:
            pytest.fail(f"Timed out after {timeout}s waiting for result")

    return ""
