"""Live e2e tests — claude CLI with haiku model, max_turns=0.

Requires:
  - MQTT broker on localhost:1883
  - `claude` CLI on PATH with valid auth

Run with: unset CLAUDECODE && uv run python -m pytest tests/test_live_claude.py -v -s
"""

from __future__ import annotations

import os
import shutil
import uuid

import pytest

from skitter.mqtt import topic_request
from skitter.types import A2ARequest

from .conftest import (
    clean_retained,
    needs_mqtt,
    send_and_collect,
    start_supervisor,
    stop_supervisor,
    write_test_configs,
)

CLAUDE_MODEL = os.environ.get("SKITTER_TEST_CLAUDE_MODEL", "haiku")

needs_claude = pytest.mark.skipif(
    not (shutil.which("claude") and "CLAUDECODE" not in os.environ),
    reason="No claude CLI or running inside Claude Code",
)


@pytest.fixture(scope="module")
def supervisor():
    created = write_test_configs("test_claude", "claude", CLAUDE_MODEL)
    proc = start_supervisor()
    yield proc
    stop_supervisor(proc)
    for f in created:
        f.unlink(missing_ok=True)


@needs_mqtt
@needs_claude
class TestLiveClaude:
    @pytest.mark.asyncio
    async def test_single_agent(self, supervisor):
        await clean_retained()
        req = A2ARequest(
            text="What is 2+2? Reply with just the number.",
            request_id=f"live-claude-{uuid.uuid4().hex[:8]}",
            sender="test",
        )
        result = await send_and_collect(topic_request("test_claude"), req, timeout=30.0)
        assert result and "4" in result
        print(f"\nResult: {result}")

    @pytest.mark.asyncio
    async def test_workflow_fan_out_join(self, supervisor):
        await clean_retained()
        req = A2ARequest(
            text="Workflow 'Test Workflow' with topic=Python",
            request_id=f"live-workflow-{uuid.uuid4().hex[:8]}",
            sender="test",
            variables={"topic": "Python"},
        )
        result = await send_and_collect(
            topic_request("workflow-test_workflow"), req, timeout=120.0
        )
        assert result and len(result) > 0
        assert not result.startswith("("), f"Workflow failed: {result}"
        assert "not logged in" not in result.lower(), f"Auth error: {result}"
        print(f"\nWorkflow result: {result}")

    @pytest.mark.asyncio
    async def test_unknown_agent_rejected(self, supervisor):
        req = A2ARequest(
            text="Hello",
            request_id=f"live-unknown-{uuid.uuid4().hex[:8]}",
            sender="test",
        )
        result = await send_and_collect(
            topic_request("nonexistent_agent"), req, timeout=10.0
        )
        assert "Unknown agent" in result
