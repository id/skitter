"""Live e2e tests — codex CLI.

Requires:
  - MQTT broker on localhost:1883
  - `codex` CLI on PATH with valid OPENAI_API_KEY

Run with: uv run python -m pytest tests/test_live_codex.py -v -s
"""

from __future__ import annotations

import os
import shutil
import uuid

import pytest

from skitter.mqtt import topic_request
from skitter.types import InboundMessage

from .conftest import (
    clean_retained,
    needs_mqtt,
    send_and_collect,
    start_supervisor,
    stop_supervisor,
    write_test_configs,
)

CODEX_MODEL = os.environ.get("SKITTER_TEST_CODEX_MODEL", "")

needs_codex = pytest.mark.skipif(not shutil.which("codex"), reason="No codex CLI")


@pytest.fixture(scope="module")
def supervisor():
    created = write_test_configs(
        "test_codex", "codex", "test_codex_workflow", CODEX_MODEL
    )
    proc = start_supervisor()
    yield proc
    stop_supervisor(proc)
    for f in created:
        f.unlink(missing_ok=True)


@needs_mqtt
@needs_codex
class TestLiveCodex:
    @pytest.mark.asyncio
    async def test_single_agent(self, supervisor):
        await clean_retained()
        msg = InboundMessage(
            text="What is 2+2? Reply with just the number.",
            sender="test",
            session_id=f"live-codex-{uuid.uuid4().hex[:8]}",
            agent_id="test_codex",
        )
        result = await send_and_collect(topic_request("test_codex"), msg, timeout=30.0)
        assert result and len(result) > 0
        assert not result.startswith("("), f"Codex failed: {result}"
        assert "error" not in result.lower() or "4" in result, f"Codex error: {result}"
        print(f"\nCodex result: {result}")

    @pytest.mark.asyncio
    async def test_workflow_fan_out_join(self, supervisor):
        await clean_retained()
        msg = InboundMessage(
            text="Workflow with topic=Python",
            sender="test",
            session_id=f"live-codex-wf-{uuid.uuid4().hex[:8]}",
            workflow_id="test_codex_workflow",
            workflow_vars={"topic": "Python"},
        )
        result = await send_and_collect(
            topic_request("workflow-test_codex_workflow"), msg, timeout=120.0
        )
        assert result and len(result) > 0
        assert not result.startswith("("), f"Workflow failed: {result}"
        print(f"\nCodex workflow result: {result}")
