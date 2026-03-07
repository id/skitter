"""Live e2e tests — claude CLI with haiku model, max_turns=0.

Requires:
  - MQTT broker on localhost:1883
  - `claude` CLI on PATH with valid auth

Run with: uv run python -m pytest tests/test_live_claude.py -v -s
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


def _claude_available() -> bool:
    return shutil.which("claude") is not None and "CLAUDECODE" not in os.environ


needs_mqtt = pytest.mark.skipif(not _mqtt_available(), reason="No MQTT broker")
needs_claude = pytest.mark.skipif(
    not _claude_available(),
    reason="No claude CLI or running inside Claude Code",
)

CLAUDE_MODEL = os.environ.get("SKITTER_TEST_CLAUDE_MODEL", "haiku")


# ---------------------------------------------------------------------------
# Test fixtures
# ---------------------------------------------------------------------------

AGENTS = {
    "test_claude": AgentDef(
        id="test_claude",
        name="Test Claude Agent",
        description="Minimal test agent",
        runtime="claude",
    ),
}

WORKFLOW = WorkflowDef(
    id="test_workflow",
    name="Test Workflow",
    description="Fan-out + join test",
    variables=["topic"],
    tasks=[
        WorkflowTask(
            id="research_a",
            agent="test_claude",
            description="In one sentence, name one fact about '{topic}'.",
            next="synthesize",
            needs=[],
            model=CLAUDE_MODEL,
        ),
        WorkflowTask(
            id="research_b",
            agent="test_claude",
            description="In one sentence, name a different fact about '{topic}'.",
            next="synthesize",
            needs=[],
            model=CLAUDE_MODEL,
        ),
        WorkflowTask(
            id="synthesize",
            agent="test_claude",
            description="Combine the facts about '{topic}' into a single sentence.",
            next="output",
            needs=["research_a", "research_b"],
            model=CLAUDE_MODEL,
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
        identifier=f"{A2A_ORG}/{A2A_UNIT}/test-claude-{gateway_id}",
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
@needs_claude
class TestLiveClaude:
    @pytest.mark.asyncio
    async def test_single_agent(self):
        msg = InboundMessage(
            text="What is 2+2? Reply with just the number.",
            sender="test",
            session_id=f"live-claude-{uuid.uuid4().hex[:8]}",
            agent_id="test_claude",
        )
        result = await run_request(msg, timeout=30.0)
        assert result and "4" in result
        print(f"\nResult: {result}")

    @pytest.mark.asyncio
    async def test_workflow_fan_out_join(self):
        msg = InboundMessage(
            text="Workflow 'Test Workflow' with topic=Python",
            sender="test",
            session_id=f"live-workflow-{uuid.uuid4().hex[:8]}",
            workflow_id="test_workflow",
            workflow_vars={"topic": "Python"},
        )
        result = await run_request(msg, timeout=120.0)
        assert result and len(result) > 0
        assert not result.startswith("("), f"Workflow failed: {result}"
        assert "not logged in" not in result.lower(), f"Auth error: {result}"
        print(f"\nWorkflow result: {result}")

    @pytest.mark.asyncio
    async def test_unknown_agent_rejected(self):
        msg = InboundMessage(
            text="Hello",
            sender="test",
            session_id=f"live-unknown-{uuid.uuid4().hex[:8]}",
            agent_id="nonexistent_agent",
        )
        gateway_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", gateway_id)

        async with aiomqtt.Client(
            MQTT_HOST,
            MQTT_PORT,
            identifier=f"{A2A_ORG}/{A2A_UNIT}/test-err-{gateway_id}",
            protocol=aiomqtt.ProtocolVersion.V5,
        ) as client:
            await client.subscribe(reply_t, qos=1)
            await handle_request(
                client, msg.to_json(), reply_t, msg.session_id, AGENTS, WORKFLOWS
            )

            async with asyncio.timeout(5.0):
                async for mqtt_msg in client.messages:
                    payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                    if not payload:
                        continue
                    data = json.loads(payload)
                    if "error" in data:
                        assert "Unknown agent" in data["error"]["message"]
                        return

            pytest.fail("No error response received")
