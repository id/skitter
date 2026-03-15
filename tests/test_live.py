"""Live e2e tests — agent-runner + supervisor + composed apps.

Tests the full A2A flow: agent-runner publishes discovery card, supervisor
discovers it, client sends requests, supervisor orchestrates responses.

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
    start_supervisor,
    stop_process,
    wait_for_discovery,
    write_agent_yaml,
)

MODELS = {
    "claude": os.environ.get("SKITTER_TEST_CLAUDE_MODEL", "haiku"),
    "codex": os.environ.get("SKITTER_TEST_CODEX_MODEL", ""),
}


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
def supervisor():
    proc = start_supervisor()
    yield proc
    stop_process(proc)


@pytest.fixture(scope="module")
def agent(runtime, supervisor):
    """Start an agent-runner for the selected runtime, wait for discovery."""
    agent_id = f"test-{runtime}"
    model = MODELS.get(runtime, "")
    yaml_path = write_agent_yaml(agent_id, runtime, model)
    proc = start_agent_runner(agent_id)
    yield agent_id
    stop_process(proc)
    yaml_path.unlink(missing_ok=True)


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
        """Send a query through the supervisor, get a response from the agent."""
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

    @pytest.mark.asyncio
    async def test_unknown_agent_rejected(self, agent):
        """Request for a nonexistent agent returns an error."""
        req = A2ARequest(
            text="Hello",
            request_id=f"unknown-{uuid.uuid4().hex[:8]}",
            sender="test",
        )
        result = await send_and_collect(
            topic_request("nonexistent-agent-xyz"), req, timeout=10.0
        )
        assert "Unknown agent" in result
