"""Shared fixtures and helpers for live e2e tests.

Provides: process management (supervisor, agent-runner), MQTT helpers
(send_and_collect, wait_for_discovery), and agent config scaffolding.
"""

from __future__ import annotations

import asyncio
import json
import os
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
from skitter.config import AGENTS_DIR
from skitter.types import (
    A2ARequest,
    REPLY_ERROR,
    REPLY_TERMINAL,
    REPLY_TEXT,
    classify_reply,
)


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


needs_mqtt = pytest.mark.skipif(not mqtt_available(), reason="No MQTT broker")


def runtime_available(runtime: str) -> bool:
    import shutil

    if runtime == "claude":
        return bool(shutil.which("claude") and "CLAUDECODE" not in os.environ)
    if runtime == "codex":
        return bool(shutil.which("codex"))
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
# Agent config scaffolding
# ---------------------------------------------------------------------------


def write_agent_yaml(agent_id: str, runtime: str, model: str = "") -> Path:
    """Write a minimal agent YAML to ~/.skitter/agents/. Returns the path."""
    AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    path = AGENTS_DIR / f"{agent_id}.yaml"
    data: dict = {
        "name": f"Test {runtime.title()} Agent",
        "description": f"Minimal {runtime} test agent",
        "runtime": runtime,
    }
    if model:
        data["model"] = model
    path.write_text(yaml.dump(data, default_flow_style=False, sort_keys=False))
    return path


# ---------------------------------------------------------------------------
# Process management — stdout goes to log files to avoid pipe buffer deadlock
# ---------------------------------------------------------------------------


def _start_process(
    cmd: list[str],
    marker: str,
    label: str = "process",
    timeout_s: float = 15.0,
) -> tuple[subprocess.Popen, Path]:
    """Start a subprocess, wait for `marker` in its output. Returns (proc, log_path)."""
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
    return proc, log_path  # unreachable, keeps type checker happy


def start_supervisor() -> subprocess.Popen:
    """Start the supervisor with a fresh DB, wait for it to be ready."""
    # Remove leftover DB from previous runs so recovery doesn't pollute tests
    db_path = Path.home() / ".skitter" / "skitter.db"
    for suffix in ("", "-wal", "-shm"):
        (db_path.parent / f"{db_path.name}{suffix}").unlink(missing_ok=True)

    proc, _ = _start_process(
        ["uv", "run", "python", "-m", "skitter"],
        marker="listening on",
        label="supervisor",
    )
    return proc


def start_agent_runner(agent_id: str) -> subprocess.Popen:
    """Start an agent-runner subprocess, wait for it to be ready."""
    proc, log_path = _start_process(
        ["uv", "run", "python", "-m", "skitter", "agent-runner", agent_id],
        marker="Listening on",
        label=f"agent-{agent_id}",
    )
    return proc


def stop_process(proc: subprocess.Popen) -> None:
    """Gracefully stop a subprocess (SIGINT, then kill after timeout)."""
    if proc.poll() is not None:
        return
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


# ---------------------------------------------------------------------------
# MQTT helpers
# ---------------------------------------------------------------------------


async def wait_for_discovery(agent_id: str, timeout: float = 10.0) -> dict:
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
