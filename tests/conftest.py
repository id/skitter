"""Shared fixtures and helpers for live e2e tests.

Provides: supervisor subprocess management, MQTT retained message cleanup,
and send_and_collect() for publishing requests and collecting replies.
"""

from __future__ import annotations

import asyncio
import json
import os
import select
import signal
import socket
import subprocess
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
    topic_reply,
    topic_result_wildcard,
    topic_session_wildcard,
)
from skitter.config import AGENTS_DIR, WORKFLOWS_DIR
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


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


def write_test_configs(
    agent_id: str,
    runtime: str,
    workflow_id: str,
    model: str = "",
) -> list[Path]:
    """Create temporary agent + workflow YAML in ~/.skitter/. Returns created paths."""
    agent_yaml = AGENTS_DIR / f"{agent_id}.yaml"
    workflow_yaml = WORKFLOWS_DIR / f"{workflow_id}.yaml"
    created = []

    AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    WORKFLOWS_DIR.mkdir(parents=True, exist_ok=True)

    if not agent_yaml.exists():
        agent_yaml.write_text(
            yaml.dump(
                {
                    "name": f"Test {runtime.title()} Agent",
                    "description": f"Minimal {runtime} test agent",
                    "runtime": runtime,
                }
            )
        )
        created.append(agent_yaml)

    if not workflow_yaml.exists():
        task_model = {"model": model} if model else {}
        workflow_yaml.write_text(
            yaml.dump(
                {
                    "name": f"Test {runtime.title()} Workflow",
                    "description": "Fan-out + join test",
                    "variables": ["topic"],
                    "tasks": [
                        {
                            "id": "research_a",
                            "agent": agent_id,
                            "description": "In one sentence, name one fact about '{topic}'.",
                            "next": "synthesize",
                            "needs": [],
                            **task_model,
                        },
                        {
                            "id": "research_b",
                            "agent": agent_id,
                            "description": "In one sentence, name a different fact about '{topic}'.",
                            "next": "synthesize",
                            "needs": [],
                            **task_model,
                        },
                        {
                            "id": "synthesize",
                            "agent": agent_id,
                            "description": "Combine the facts about '{topic}' into a single sentence.",
                            "next": "output",
                            "needs": ["research_a", "research_b"],
                            **task_model,
                        },
                    ],
                }
            )
        )
        created.append(workflow_yaml)

    return created


def start_supervisor() -> subprocess.Popen:
    """Start the supervisor as a subprocess, wait for it to be ready."""
    env = os.environ.copy()
    env.pop("CLAUDECODE", None)
    proc = subprocess.Popen(
        ["uv", "run", "python", "-m", "skitter"],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.STDOUT,
        text=True,
    )
    ready = False
    for _ in range(100):  # up to 10 seconds
        if proc.poll() is not None:
            out = proc.stdout.read() if proc.stdout else ""
            pytest.fail(f"Supervisor exited early: {out}")
        if select.select([proc.stdout], [], [], 0.1)[0]:
            line = proc.stdout.readline()
            if "Supervisor ready" in line or "listening on" in line:
                ready = True
                break
    if not ready:
        proc.kill()
        pytest.fail("Supervisor did not become ready within 10 seconds")
    return proc


def stop_supervisor(proc: subprocess.Popen) -> None:
    proc.send_signal(signal.SIGINT)
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()


# ---------------------------------------------------------------------------
# MQTT helpers
# ---------------------------------------------------------------------------


async def clean_retained():
    """Clear leftover retained messages from previous test runs."""
    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{A2A_ORG}/{A2A_UNIT}/test-cleaner-{uuid.uuid4().hex[:6]}",
        ),
    ) as client:
        for pattern in [
            topic_session_wildcard(),
            topic_result_wildcard(),
        ]:
            await client.subscribe(pattern, qos=1)
            try:
                async with asyncio.timeout(0.5):
                    async for msg in client.messages:
                        if msg.retain and msg.payload:
                            await client.publish(
                                str(msg.topic), b"", qos=1, retain=True
                            )
            except TimeoutError:
                pass
            await client.unsubscribe(pattern)


async def send_and_collect(
    request_topic: str,
    msg: A2ARequest,
    timeout: float = 60.0,
) -> str:
    """Publish request via MQTT with v5 properties, collect reply."""
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
            correlation_data=msg.session_id,
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
