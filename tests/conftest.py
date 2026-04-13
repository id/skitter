"""Shared fixtures and helpers for tests.

Provides: MQTT helpers (send_and_collect, wait_for_discovery, create_test_app),
subprocess runner, and skip conditions.
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import uuid
from pathlib import Path

# Isolate tests from the developer's ~/.skitter/ config and from MQTT_*/SKITTER_*
# values uv and skitter.mqtt pull out of the project .env. Point SKITTER_HOME at
# a throwaway dir and blank the relevant env vars so load_config() falls back to
# built-in defaults (localhost:1883, no auth, org=skitter unit=default), matching
# what CI sees on a fresh runner. Set to empty string rather than popping so
# skitter.mqtt's load_dotenv() call at import time leaves them alone (dotenv
# respects already-set vars). Must happen before importing anything from skitter.*.
os.environ["SKITTER_HOME"] = tempfile.mkdtemp(prefix="skitter-test-")
for _var in (
    "MQTT_BROKER_URL",
    "MQTT_USERNAME",
    "MQTT_PASSWORD",
    "MQTT_CA_CERT",
    "SKITTER_A2A_ORG",
    "SKITTER_A2A_UNIT",
):
    os.environ[_var] = ""

import aiomqtt
import pytest

from skitter.a2a import (
    a2a_org,
    a2a_unit,
    A2ARequest,
    REPLY_ARTIFACT,
    REPLY_ERROR,
    REPLY_FAILED,
    REPLY_TEXT,
    stream_request,
    topic_discovery,
    topic_reply,
)
from skitter.config import load_config as _load_config
from skitter.mqtt import mqtt_client_kwargs

PROJECT_ROOT = Path(__file__).parent.parent


# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------


def mqtt_available() -> bool:
    from urllib.parse import urlparse

    broker = _load_config().broker
    parsed = urlparse(broker.url)
    host = parsed.hostname or "localhost"
    tls = parsed.scheme == "mqtts"
    port = parsed.port or (8883 if tls else 1883)
    try:
        s = socket.create_connection((host, port), timeout=2)
        if tls:
            import ssl

            ctx = ssl.create_default_context()
            s = ctx.wrap_socket(s, server_hostname=host)
        s.close()
        return True
    except OSError:
        return False


def broker_reachable(host: str = "localhost", port: int = 1883) -> bool:
    """Check if a broker is reachable via TCP."""
    try:
        with socket.create_connection((host, port), timeout=2):
            return True
    except OSError:
        return False


needs_mqtt = pytest.mark.skipif(not mqtt_available(), reason="No MQTT broker")


# ---------------------------------------------------------------------------
# Subprocess helper
# ---------------------------------------------------------------------------


def run_skitter(
    args: list[str],
    env: dict[str, str],
    *,
    timeout: int = 60,
    check: bool = False,
    cwd: str | Path | None = None,
) -> subprocess.CompletedProcess:
    """Run ``python -m skitter <args>`` as a subprocess."""
    cmd = [sys.executable, "-m", "skitter"] + args
    return subprocess.run(
        cmd,
        env=env,
        capture_output=True,
        text=True,
        timeout=timeout,
        check=check,
        cwd=str(cwd or PROJECT_ROOT),
    )


# ---------------------------------------------------------------------------
# MQTT helpers
# ---------------------------------------------------------------------------


async def wait_for_discovery(agent_id: str, timeout: float = 60.0) -> dict:
    """Wait for an agent's discovery card to appear on the broker."""
    topic = topic_discovery(agent_id)
    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{a2a_org()}/{a2a_unit()}/test-disco-{uuid.uuid4().hex[:6]}",
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
    timeout: float = 120.0,
) -> str:
    """Publish A2A request via stream_request, return terminal result."""
    test_id = uuid.uuid4().hex[:8]
    reply_t = topic_reply("test", test_id)

    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{a2a_org()}/{a2a_unit()}/test-client-{test_id}",
        ),
    ) as client:
        await client.subscribe(reply_t, qos=1)

        artifact_text = ""
        try:
            async with asyncio.timeout(timeout):
                async for kind, content in stream_request(
                    client,
                    request_topic,
                    reply_t,
                    msg.to_json(),
                    msg.request_id,
                ):
                    if kind == REPLY_TEXT:
                        print(content, end="", flush=True)
                    elif kind == REPLY_ARTIFACT:
                        artifact_text = content
                    elif kind == REPLY_FAILED:
                        return f"Failed: {content}"
                    elif kind == REPLY_ERROR:
                        return f"Error: {content}"
                print()
                return artifact_text
        except TimeoutError:
            pytest.fail(f"Timed out after {timeout}s waiting for result")

    return ""


async def create_test_app(
    agent_ids: list[str],
    instructions: str,
    timeout: float = 30.0,
) -> str:
    """Create a composed app via the runtime API, return the app_id."""
    from skitter.a2a import topic_request

    spec = json.dumps(
        {
            "name": f"Test-{uuid.uuid4().hex[:6]}",
            "instructions": instructions,
            "agents": agent_ids,
        }
    )
    create_req = A2ARequest(
        text=f"create app {spec}",
        request_id=f"create-{uuid.uuid4().hex[:8]}",
        sender="test",
    )
    result = await send_and_collect(
        topic_request("skitter"), create_req, timeout=timeout
    )
    assert result, "Create app returned empty"
    data = json.loads(result)
    assert "created_app" in data, f"Unexpected: {result}"
    return data["created_app"]["app_id"]
