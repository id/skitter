"""Live e2e tests — codex CLI with gpt-5-nano model, max_turns=0.

Requires:
  - MQTT broker on localhost:1883
  - `codex` CLI on PATH with valid OPENAI_API_KEY

Run with: uv run python -m pytest tests/test_live_codex.py -v -s
"""

from __future__ import annotations

import asyncio
import json
import os
import shutil
import socket
import uuid

import aiomqtt
import pytest

from skitter.config import AgentDef, WorkflowDef, WorkflowTask
from skitter.gateway import handle_request, _publish_discovery
from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    A2A_ORG,
    A2A_UNIT,
    topic_reply,
    topic_state_session_wildcard,
)
from skitter.types import InboundMessage, StreamItem, TaskStatusUpdate


# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------


def _mqtt_available() -> bool:
    try:
        s = socket.create_connection((MQTT_HOST, MQTT_PORT), timeout=1)
        s.close()
        return True
    except OSError:
        return False


def _codex_available() -> bool:
    return shutil.which("codex") is not None


needs_mqtt = pytest.mark.skipif(not _mqtt_available(), reason="No MQTT broker")
needs_codex = pytest.mark.skipif(not _codex_available(), reason="No codex CLI")

CODEX_MODEL = os.environ.get("SKITTER_TEST_CODEX_MODEL", "")


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

AGENTS = {
    "test_codex": AgentDef(
        id="test_codex",
        name="Test Codex Agent",
        description="Minimal codex test agent",
        model=CODEX_MODEL,
        max_turns=0,
        runtime="codex",
    ),
}

WORKFLOW = WorkflowDef(
    id="test_workflow",
    name="Test Codex Workflow",
    description="Fan-out + join test",
    variables=["topic"],
    tasks=[
        WorkflowTask(
            id="research_a",
            agent="test_codex",
            description="In one sentence, name one fact about '{topic}'.",
            next="synthesize",
            needs=[],
        ),
        WorkflowTask(
            id="research_b",
            agent="test_codex",
            description="In one sentence, name a different fact about '{topic}'.",
            next="synthesize",
            needs=[],
        ),
        WorkflowTask(
            id="synthesize",
            agent="test_codex",
            description="Combine the facts about '{topic}' into a single sentence.",
            next="output",
            needs=["research_a", "research_b"],
        ),
    ],
)

WORKFLOWS = {"test_workflow": WORKFLOW}


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


async def clean_retained(client: aiomqtt.Client):
    for pattern in [
        topic_state_session_wildcard(),
        f"$a2a/v1/state/{A2A_ORG}/{A2A_UNIT}/chain/+/+",
    ]:
        await client.subscribe(pattern, qos=1)
        try:
            async with asyncio.timeout(0.5):
                async for msg in client.messages:
                    if msg.retain and msg.payload:
                        await client.publish(str(msg.topic), b"", qos=1, retain=True)
        except TimeoutError:
            pass
        await client.unsubscribe(pattern)


async def run_request(msg: InboundMessage, timeout: float = 60.0) -> str:
    gateway_id = uuid.uuid4().hex[:8]
    reply_t = topic_reply("test", gateway_id)

    async with aiomqtt.Client(
        MQTT_HOST,
        MQTT_PORT,
        identifier=f"{A2A_ORG}/{A2A_UNIT}/test-codex-{gateway_id}",
        protocol=aiomqtt.ProtocolVersion.V5,
    ) as client:
        await clean_retained(client)
        await _publish_discovery(client, AGENTS, WORKFLOWS)
        await handle_request(
            client, msg.to_json(), reply_t, msg.session_id, AGENTS, WORKFLOWS
        )
        await client.subscribe(reply_t, qos=1)

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

                    if "seq" in data and "type" in data:
                        item = StreamItem.from_json(payload)
                        if item.type == "text":
                            print(item.content, end="", flush=True)
                        elif item.type == "tool_use":
                            print(f"\n  [tool] {item.content}", flush=True)
                        continue

                    if "state" in data and "task_id" in data:
                        status = TaskStatusUpdate.from_json(payload)
                        print()
                        return status.result

                    if "error" in data:
                        return f"Error: {data['error'].get('message', data['error'])}"
        except TimeoutError:
            pytest.fail(f"Timed out after {timeout}s waiting for result")

    return ""


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@needs_mqtt
@needs_codex
class TestLiveCodex:
    @pytest.mark.asyncio
    async def test_single_agent(self):
        msg = InboundMessage(
            text="What is 2+2? Reply with just the number.",
            sender="test",
            session_id=f"live-codex-{uuid.uuid4().hex[:8]}",
            agent_id="test_codex",
        )
        result = await run_request(msg, timeout=30.0)
        assert result and len(result) > 0
        assert not result.startswith("("), f"Codex failed: {result}"
        assert "error" not in result.lower() or "4" in result, f"Codex error: {result}"
        print(f"\nCodex result: {result}")

    @pytest.mark.asyncio
    async def test_workflow_fan_out_join(self):
        msg = InboundMessage(
            text="Workflow with topic=Python",
            sender="test",
            session_id=f"live-codex-wf-{uuid.uuid4().hex[:8]}",
            workflow_id="test_workflow",
            workflow_vars={"topic": "Python"},
        )
        result = await run_request(msg, timeout=120.0)
        assert result and len(result) > 0
        assert not result.startswith("("), f"Workflow failed: {result}"
        print(f"\nCodex workflow result: {result}")
