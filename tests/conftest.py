"""Shared fixtures and helpers for live e2e tests.

Provides: process management (coordinator, agent-runner in Docker),
MQTT helpers (send_and_collect, wait_for_discovery), and agent config.
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import signal
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
    topic_result_wildcard,
)
from skitter.types import (
    A2ARequest,
    REPLY_ERROR,
    REPLY_TERMINAL,
    REPLY_TEXT,
    classify_reply,
)


AGENT_IMAGE = os.environ.get("SKITTER_AGENT_IMAGE", "skitter-agent:latest")

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
    if runtime == "claude":
        return bool(os.environ.get("CLAUDE_CODE_OAUTH_TOKEN"))
    if runtime == "codex":
        return Path.home().joinpath(".codex/auth.json").is_file()
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


def _start_process(
    cmd: list[str],
    marker: str,
    label: str = "process",
    timeout_s: float = 15.0,
) -> tuple[subprocess.Popen, Path]:
    """Start a subprocess, wait for `marker` in its output."""
    log_path = Path(tempfile.mktemp(prefix=f"skitter-test-{label}-", suffix=".log"))
    log_file = log_path.open("w")
    env = os.environ.copy()
    env.pop("CLAUDECODE", None)
    proc = subprocess.Popen(cmd, env=env, stdout=log_file, stderr=subprocess.STDOUT)
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        if proc.poll() is not None:
            log_file.close()
            out = log_path.read_text()
            pytest.fail(f"{label} exited early (rc={proc.returncode}): {out}")
        if marker in log_path.read_text():
            return proc, log_path
        time.sleep(0.1)
    proc.kill()
    log_file.close()
    out = log_path.read_text()
    pytest.fail(f"{label} did not become ready within {timeout_s}s:\n{out}")
    return proc, log_path  # unreachable


def start_coordinator() -> subprocess.Popen:
    """Start the coordinator with a fresh DB, wait for it to be ready."""
    db_path = Path.home() / ".skitter" / "skitter.db"
    for suffix in ("", "-wal", "-shm"):
        (db_path.parent / f"{db_path.name}{suffix}").unlink(missing_ok=True)

    proc, _ = _start_process(
        ["uv", "run", "python", "-m", "skitter"],
        marker="ready",
        label="coordinator",
    )
    return proc


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

    Claude: pass CLAUDE_CODE_OAUTH_TOKEN as env var.
    Codex: mount ~/.codex/auth.json into the container.
    """
    _ensure_agent_image()
    agent_path = Path(agent_path)
    container_agent_path = f"/tmp/agents/{agent_path.name}"

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
        if not os.environ.get("CLAUDE_CODE_OAUTH_TOKEN"):
            pytest.fail("CLAUDE_CODE_OAUTH_TOKEN required for claude runtime")
        cmd.extend(["-e", "CLAUDE_CODE_OAUTH_TOKEN"])
    elif runtime == "codex":
        codex_auth = Path.home() / ".codex" / "auth.json"
        if not codex_auth.is_file():
            pytest.fail(f"Codex auth not found: {codex_auth}")
        cmd.extend(["-v", f"{codex_auth}:/home/skitter/.codex/auth.json:ro"])

    cmd.extend([AGENT_IMAGE, container_agent_path])

    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode != 0:
        pytest.fail(f"Failed to start agent container: {result.stderr}")
    container_id = result.stdout.strip()

    # Wait for agent to be ready (check logs for marker)
    deadline = time.monotonic() + 30.0
    while time.monotonic() < deadline:
        logs = subprocess.run(
            ["docker", "logs", container_id], capture_output=True, text=True
        )
        if "Listening on" in logs.stdout or "Listening on" in logs.stderr:
            return container_id
        # Check if container died
        inspect = subprocess.run(
            ["docker", "inspect", "-f", "{{.State.Running}}", container_id],
            capture_output=True,
            text=True,
        )
        if inspect.stdout.strip() != "true":
            all_logs = logs.stdout + logs.stderr
            pytest.fail(f"Agent container exited early:\n{all_logs}")
        time.sleep(0.5)

    all_logs = subprocess.run(
        ["docker", "logs", container_id], capture_output=True, text=True
    )
    stop_container(container_id)
    pytest.fail(
        f"Agent container not ready within 30s:\n{all_logs.stdout}\n{all_logs.stderr}"
    )
    return ""  # unreachable


def stop_process(proc: subprocess.Popen) -> None:
    """Gracefully stop a subprocess."""
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


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


async def clean_retained():
    """Clear leftover retained messages from previous test runs."""
    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{A2A_ORG}/{A2A_UNIT}/test-cleaner-{uuid.uuid4().hex[:6]}",
        ),
    ) as client:
        await client.subscribe(topic_result_wildcard(), qos=1)
        try:
            async with asyncio.timeout(0.5):
                async for msg in client.messages:
                    if msg.retain and msg.payload:
                        await client.publish(str(msg.topic), b"", qos=1, retain=True)
        except TimeoutError:
            pass


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
