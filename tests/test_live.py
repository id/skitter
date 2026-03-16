"""Live e2e tests — agent-runner (Docker) + coordinator + composed apps.

Agent runners run in Docker containers for full isolation. The coordinator
runs as a local subprocess.

Requires:
  - Docker with the skitter network (docker compose up -d)
  - MQTT broker on the skitter network
  - Claude: CLAUDE_CODE_OAUTH_TOKEN env var
  - Codex: ~/.codex/auth.json

Usage:
  unset CLAUDECODE
  uv run pytest tests/test_live.py -v -s                    # auto-detect runtime
  uv run pytest tests/test_live.py -v -s --runtime claude   # claude only
  uv run pytest tests/test_live.py -v -s --runtime codex    # codex only
"""

from __future__ import annotations

import asyncio
import json
import os
import uuid

import pytest

from skitter.mqtt import topic_request
from skitter.types import A2ARequest

from .conftest import (
    docker_available,
    needs_mqtt,
    runtime_available,
    send_and_collect,
    start_agent_runner,
    start_coordinator,
    stop_container,
    stop_process,
    wait_for_discovery,
    write_agent_file,
)

MODELS = {
    "claude": os.environ.get("SKITTER_TEST_CLAUDE_MODEL", "haiku"),
    "codex": os.environ.get("SKITTER_TEST_CODEX_MODEL", ""),
}

# Default LLM model for graph generation, per runtime
LLM_MODELS = {
    "claude": "claude-haiku-4-6-20260514",
    "codex": "gpt-5-mini",
}

needs_docker = pytest.mark.skipif(not docker_available(), reason="Docker not available")


def _pick_runtime(config) -> str:
    selected = config.getoption("--runtime", default=None)
    if selected:
        if not runtime_available(selected):
            pytest.skip(f"{selected} credentials not available")
        return selected
    for rt in ("claude", "codex"):
        if runtime_available(rt):
            return rt
    pytest.skip(
        "No runtime credentials (need CLAUDE_CODE_OAUTH_TOKEN or ~/.codex/auth.json)"
    )


@pytest.fixture(scope="module")
def runtime(request):
    rt = _pick_runtime(request.config)
    # Set LLM model for coordinator graph generation (inherits env)
    os.environ.setdefault("SKITTER_LLM_MODEL", LLM_MODELS.get(rt, "claude-haiku-4-6-20260514"))
    return rt


@pytest.fixture(scope="module")
def coordinator():
    proc = start_coordinator()
    yield proc
    stop_process(proc)


@pytest.fixture(scope="module")
def agent(runtime, coordinator):
    """Start an agent-runner in Docker, wait for discovery."""
    agent_id = f"test-{runtime}"
    model = MODELS.get(runtime, "")
    agent_path = write_agent_file(agent_id, runtime, model)
    container_id = start_agent_runner(agent_path, runtime)
    yield agent_id
    stop_container(container_id)
    agent_path.unlink(missing_ok=True)


@pytest.fixture(scope="module")
def two_agents(runtime, coordinator):
    """Start two agent-runners in Docker for composed app testing."""
    agent_a = f"test-{runtime}-a"
    agent_b = f"test-{runtime}-b"
    model = MODELS.get(runtime, "")
    path_a = write_agent_file(agent_a, runtime, model)
    path_b = write_agent_file(agent_b, runtime, model)
    cid_a = start_agent_runner(path_a, runtime)
    cid_b = start_agent_runner(path_b, runtime)
    yield agent_a, agent_b
    stop_container(cid_a)
    stop_container(cid_b)
    path_a.unlink(missing_ok=True)
    path_b.unlink(missing_ok=True)


@needs_mqtt
@needs_docker
class TestLive:
    @pytest.mark.asyncio
    async def test_agent_discovery(self, agent):
        """Agent-runner publishes a spec-conformant discovery card."""
        card = await wait_for_discovery(agent)
        assert card["protocolVersion"] == "0.2.5"
        assert card["capabilities"]["streaming"] is True

    @pytest.mark.asyncio
    async def test_agent_query(self, agent):
        """Send a query to the agent, get a response."""

        req = A2ARequest(
            text="What is 2+2? Reply with just the number.",
            request_id=f"query-{uuid.uuid4().hex[:8]}",
            sender="test",
        )
        result = await send_and_collect(topic_request(agent), req, timeout=30.0)
        assert result, "Empty result"
        assert "4" in result
        print(f"\nResult: {result}")


@needs_mqtt
@needs_docker
class TestComposedApp:
    @pytest.mark.asyncio
    async def test_create_and_run_composed_app(self, two_agents):
        """Full flow: create app via runtime API, send request, verify orchestrated result."""
        agent_a, agent_b = two_agents

        # Wait for both agents to be discovered by broker
        await wait_for_discovery(agent_a)
        await wait_for_discovery(agent_b)

        # Give coordinator time to process discovery messages
        await asyncio.sleep(2)

        # Step 1: Create composed app via skitter runtime API
        spec = json.dumps(
            {
                "name": "Test Composed App",
                "description": "E2E test app",
                "instructions": (
                    f"First, {agent_a} should say a short greeting. "
                    f"Then, {agent_b} should summarize what {agent_a} said."
                ),
                "agents": [agent_a, agent_b],
            }
        )

        create_req = A2ARequest(
            text=f"create app {spec}",
            request_id=f"create-{uuid.uuid4().hex[:8]}",
            sender="test",
        )
        print("\nCreating composed app...")
        create_result = await send_and_collect(
            topic_request("skitter"), create_req, timeout=30.0
        )
        assert create_result, "Create app returned empty result"

        result_data = json.loads(create_result)
        assert "created_app" in result_data, f"Unexpected response: {create_result}"
        app_id = result_data["created_app"]["app_id"]
        version = result_data["created_app"]["version"]
        print(f"Created app '{app_id}' v{version}")

        # Give coordinator time to subscribe to the new app topic
        await asyncio.sleep(1)

        # Step 2: Send a request to the composed app

        app_req = A2ARequest(
            text="Please greet me warmly.",
            request_id=f"app-{uuid.uuid4().hex[:8]}",
            sender="test",
        )
        print(f"Sending request to composed app '{app_id}'...")
        result = await send_and_collect(topic_request(app_id), app_req, timeout=120.0)
        assert result, "Composed app returned empty result"
        print(f"\nComposed app result: {result}")
