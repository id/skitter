"""Live e2e tests — agent-runner + coordinator + composed apps.

Tests the full A2A flow: agent-runner publishes discovery card, coordinator
discovers it, client sends requests, coordinator orchestrates responses.

Requires:
  - MQTT broker (localhost:1883 or configured via MQTT_HOST/MQTT_PORT)
  - CLI for the selected runtime (`claude` or `codex`) on PATH

Usage:
  uv run pytest tests/test_live.py -v -s                    # auto-detect runtime
  uv run pytest tests/test_live.py -v -s --runtime claude   # claude only
  uv run pytest tests/test_live.py -v -s --runtime codex    # codex only

Note: claude tests require CLAUDECODE to be unset (can't run claude inside Claude Code).
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
    clean_retained,
    needs_mqtt,
    runtime_available,
    send_and_collect,
    start_agent_runner,
    start_coordinator,
    stop_process,
    wait_for_discovery,
    write_agent_yaml,
)

MODELS = {
    "claude": os.environ.get("SKITTER_TEST_CLAUDE_MODEL", "haiku"),
    "codex": os.environ.get("SKITTER_TEST_CODEX_MODEL", ""),
}

# Set LLM model for graph generation (coordinator subprocess inherits env)
os.environ.setdefault("SKITTER_LLM_MODEL", "claude-haiku-4-5-20251001")


def _pick_runtime(config) -> str:
    selected = config.getoption("--runtime", default=None)
    if selected:
        if not runtime_available(selected):
            pytest.skip(f"{selected} CLI not available")
        return selected
    for rt in ("claude", "codex"):
        if runtime_available(rt):
            return rt
    pytest.skip("No runtime available (need claude or codex CLI)")


@pytest.fixture(scope="module")
def runtime(request):
    return _pick_runtime(request.config)


@pytest.fixture(scope="module")
def coordinator():
    proc = start_coordinator()
    yield proc
    stop_process(proc)


@pytest.fixture(scope="module")
def agent(runtime, coordinator):
    """Start an agent-runner for the selected runtime, wait for discovery."""
    agent_id = f"test-{runtime}"
    model = MODELS.get(runtime, "")
    yaml_path = write_agent_yaml(agent_id, runtime, model)
    proc = start_agent_runner(agent_id)
    yield agent_id
    stop_process(proc)
    yaml_path.unlink(missing_ok=True)


@pytest.fixture(scope="module")
def two_agents(runtime, coordinator):
    """Start two agent-runners for composed app testing."""
    agent_a = f"test-{runtime}-a"
    agent_b = f"test-{runtime}-b"
    model = MODELS.get(runtime, "")
    yaml_a = write_agent_yaml(agent_a, runtime, model)
    yaml_b = write_agent_yaml(agent_b, runtime, model)
    proc_a = start_agent_runner(agent_a)
    proc_b = start_agent_runner(agent_b)
    yield agent_a, agent_b
    stop_process(proc_a)
    stop_process(proc_b)
    yaml_a.unlink(missing_ok=True)
    yaml_b.unlink(missing_ok=True)


@needs_mqtt
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
        await clean_retained()
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
        await clean_retained()
        app_req = A2ARequest(
            text="Please greet me warmly.",
            request_id=f"app-{uuid.uuid4().hex[:8]}",
            sender="test",
        )
        print(f"Sending request to composed app '{app_id}'...")
        result = await send_and_collect(topic_request(app_id), app_req, timeout=120.0)
        assert result, "Composed app returned empty result"
        print(f"\nComposed app result: {result}")
