"""Tests for worker streaming and cancel with A2A-over-MQTT.

Requires a running MQTT broker on localhost:1883 with MQTT v5 support.
Start one with: docker compose up -d

Uses a MockTransport to simulate the Claude agent SDK transport protocol.
"""

from __future__ import annotations

import asyncio
import json
import uuid
from collections.abc import AsyncIterator
from typing import Any

import aiomqtt
import pytest

import claude_agent_sdk
from claude_agent_sdk._internal.transport import Transport

from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    make_properties,
    topic_reply,
    topic_request,
    topic_request_cancel,
    topic_state_usage,
)
from skitter.types import (
    AgentCard,
    A2ARequest,
    A2AResponse,
    CancelSignal,
    JobTask,
    StreamItem,
    TaskMessage,
    TaskStatusUpdate,
)
from skitter.worker import run as worker_run


# ---------------------------------------------------------------------------
# MQTT availability check
# ---------------------------------------------------------------------------


def _check_mqtt() -> bool:
    import socket

    try:
        s = socket.create_connection((MQTT_HOST, MQTT_PORT), timeout=1)
        s.close()
        return True
    except OSError:
        return False


needs_mqtt = pytest.mark.skipif(
    not _check_mqtt(),
    reason="MQTT broker not reachable on localhost:1883",
)


# ---------------------------------------------------------------------------
# MockTransport — simulates the Claude Code CLI transport protocol
# ---------------------------------------------------------------------------


class MockTransport(Transport):
    """A scripted transport that yields pre-defined messages."""

    def __init__(self, script: list[Any]) -> None:
        self._script = script
        self._ready = False
        self._closed = False
        self._written: list[str] = []
        self._outgoing: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self._input_ended = asyncio.Event()
        self._hook_callbacks: dict[str, str] = {}

    async def connect(self) -> None:
        self._ready = True

    async def write(self, data: str) -> None:
        self._written.append(data)
        parsed = json.loads(data.strip())

        if parsed.get("type") == "control_request":
            request = parsed["request"]
            request_id = parsed["request_id"]

            if request["subtype"] == "initialize":
                hooks = request.get("hooks") or {}
                for event, matchers in hooks.items():
                    for matcher in matchers:
                        for cb_id in matcher.get("hookCallbackIds", []):
                            self._hook_callbacks[cb_id] = event

                response = {
                    "type": "control_response",
                    "response": {
                        "subtype": "success",
                        "request_id": request_id,
                        "response": {},
                    },
                }
                await self._outgoing.put(response)

        elif parsed.get("type") == "control_response":
            await self._outgoing.put(parsed)

    async def read_messages(self) -> AsyncIterator[dict[str, Any]]:
        init_resp = await self._outgoing.get()
        yield init_resp

        await self._input_ended.wait()

        for item in self._script:
            if self._closed:
                break

            if isinstance(item, dict):
                yield item

            elif isinstance(item, tuple):
                action = item[0]

                if action == "hook":
                    _, hook_event, hook_input, tool_use_id = item

                    cb_id = None
                    for cid, evt in self._hook_callbacks.items():
                        if evt == hook_event:
                            cb_id = cid
                            break

                    if cb_id is None:
                        continue

                    request = {
                        "type": "control_request",
                        "request_id": f"mock_{uuid.uuid4().hex[:8]}",
                        "request": {
                            "subtype": "hook_callback",
                            "callback_id": cb_id,
                            "input": hook_input,
                            "tool_use_id": tool_use_id,
                        },
                    }
                    yield request

                    await asyncio.wait_for(self._outgoing.get(), timeout=5.0)
                    pass

                elif action == "callback":
                    _, callback_fn = item
                    await callback_fn()

    async def close(self) -> None:
        self._closed = True

    def is_ready(self) -> bool:
        return self._ready

    async def end_input(self) -> None:
        self._input_ended.set()


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def text_message(text: str, model: str = "claude-haiku") -> dict:
    return {
        "type": "assistant",
        "message": {
            "model": model,
            "content": [{"type": "text", "text": text}],
        },
    }


def tool_use_message(
    tool_name: str,
    tool_input: dict,
    tool_id: str = "tool_1",
    model: str = "claude-haiku",
) -> dict:
    return {
        "type": "assistant",
        "message": {
            "model": model,
            "content": [
                {
                    "type": "tool_use",
                    "id": tool_id,
                    "name": tool_name,
                    "input": tool_input,
                },
            ],
        },
    }


def result_message(
    num_turns: int = 1,
    cost_usd: float = 0.001,
    is_error: bool = False,
) -> dict:
    return {
        "type": "result",
        "subtype": "success",
        "duration_ms": 1000,
        "duration_api_ms": 800,
        "is_error": is_error,
        "num_turns": num_turns,
        "session_id": "test-session",
        "total_cost_usd": cost_usd,
        "usage": {"input_tokens": 100, "output_tokens": 50},
    }


class MQTTCollector:
    """Collects MQTT messages on given topic patterns."""

    def __init__(self, topics: list[str]):
        self.topics = topics
        self.messages: list[tuple[str, str]] = []

    async def start(self) -> None:
        self._client = aiomqtt.Client(
            MQTT_HOST,
            MQTT_PORT,
            identifier=f"test-collector-{uuid.uuid4().hex[:8]}",
            protocol=aiomqtt.ProtocolVersion.V5,
        )
        await self._client.__aenter__()
        for topic in self.topics:
            await self._client.subscribe(topic, qos=1)
        self._task = asyncio.create_task(self._listen())

    async def _listen(self) -> None:
        try:
            async for msg in self._client.messages:
                payload = msg.payload.decode() if msg.payload else ""
                if payload:
                    self.messages.append((str(msg.topic), payload))
        except asyncio.CancelledError:
            pass

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        if self._client:
            await self._client.__aexit__(None, None, None)

    def all_payloads(self) -> list[dict]:
        results = []
        for _, payload in self.messages:
            try:
                results.append(json.loads(payload))
            except json.JSONDecodeError:
                pass
        return results


async def dispatch_task_to_worker(
    agent: str,
    chat_id: str,
    task_id: str,
    description: str = "Test task",
    max_turns: int = 10,
    model: str = "haiku",
    reply_topic: str = "",
) -> None:
    """Publish a task to the agent's request topic with v5 properties (simulates coordinator dispatch)."""
    task_msg = TaskMessage(
        task_id=task_id,
        chat_id=chat_id,
        description=description,
        soul="Test soul",
        skills="Test skills",
        max_turns=max_turns,
        model=model,
    )
    props = make_properties(
        response_topic=reply_topic,
        correlation_data=task_id,
    )
    async with aiomqtt.Client(
        MQTT_HOST,
        MQTT_PORT,
        identifier=f"test-dispatch-{uuid.uuid4().hex[:8]}",
        protocol=aiomqtt.ProtocolVersion.V5,
    ) as client:
        await client.publish(
            topic_request(agent),
            task_msg.to_json(),
            qos=1,
            properties=props,
        )


_original_query = claude_agent_sdk.query


def make_mock_query(script: list[Any]):
    async def mock_query(*, prompt, options=None, transport=None):
        mock_t = MockTransport(script)
        async for msg in _original_query(
            prompt=prompt,
            options=options,
            transport=mock_t,
        ):
            yield msg

    return mock_query


# ---------------------------------------------------------------------------
# Unit tests — types
# ---------------------------------------------------------------------------


class TestStreamingTypes:
    def test_stream_item_roundtrip(self):
        item = StreamItem(
            task_id="t1",
            seq=1,
            type="text",
            content="Hello",
        )
        parsed = StreamItem.from_json(item.to_json())
        assert parsed.task_id == "t1"
        assert parsed.seq == 1
        assert parsed.type == "text"
        assert parsed.content == "Hello"

    def test_task_status_update_roundtrip(self):
        status = TaskStatusUpdate(
            task_id="t1",
            state="completed",
            result="All done",
        )
        parsed = TaskStatusUpdate.from_json(status.to_json())
        assert parsed.task_id == "t1"
        assert parsed.state == "completed"
        assert parsed.result == "All done"

    def test_cancel_signal_roundtrip(self):
        sig = CancelSignal(
            task_id="t1",
            chat_id="c1",
            reason="User requested stop",
        )
        parsed = CancelSignal.from_json(sig.to_json())
        assert parsed.reason == "User requested stop"

    def test_agent_card_roundtrip(self):
        card = AgentCard(
            agent_id="researcher",
            name="Research Specialist",
            description="Deep research",
            capabilities=["tool_use"],
            model="sonnet",
            max_turns=15,
        )
        parsed = AgentCard.from_json(card.to_json())
        assert parsed.agent_id == "researcher"
        assert parsed.capabilities == ["tool_use"]

    def test_a2a_request_roundtrip(self):
        req = A2ARequest(
            method="tasks/send",
            params={"task_id": "t1", "description": "Do stuff"},
            id="req-1",
        )
        parsed = A2ARequest.from_json(req.to_json())
        assert parsed.method == "tasks/send"
        assert parsed.params["task_id"] == "t1"
        assert parsed.id == "req-1"

    def test_a2a_response_roundtrip(self):
        resp = A2AResponse(
            id="req-1",
            result={"output": "done"},
        )
        parsed = A2AResponse.from_json(resp.to_json())
        assert parsed.result == {"output": "done"}

    def test_a2a_response_error(self):
        resp = A2AResponse(
            id="req-1",
            error={"code": -32602, "message": "Bad params"},
        )
        parsed = A2AResponse.from_json(resp.to_json())
        assert parsed.error["code"] == -32602

    def test_job_task_roundtrip(self):
        task = JobTask(
            logical_id="research",
            task_id="t1",
            agent="researcher",
            description="Do research",
            soul="",
            skills="",
        )
        d = task.to_dict()
        restored = JobTask.from_dict(d)
        assert restored.logical_id == "research"
        assert restored.agent == "researcher"


# ---------------------------------------------------------------------------
# Worker integration tests with MockTransport
# ---------------------------------------------------------------------------


@needs_mqtt
@pytest.mark.asyncio
class TestWorkerStreaming:
    """Test worker's A2A streaming behavior using MockTransport."""

    async def test_text_streaming(self, monkeypatch):
        """Worker streams text items to response topic with correlation data."""
        agent = "researcher"
        chat_id = f"test-stream-{uuid.uuid4().hex[:8]}"
        task_id = uuid.uuid4().hex[:12]

        # Set up reply topic to collect results
        reply_t = topic_reply("test", uuid.uuid4().hex[:8])

        script = [
            text_message("Hello "),
            text_message("World"),
            result_message(),
        ]
        monkeypatch.setattr("claude_agent_sdk.query", make_mock_query(script))

        collector = MQTTCollector([reply_t])
        await collector.start()
        await asyncio.sleep(0.1)

        # Start worker
        worker_task = asyncio.create_task(worker_run(agent, chat_id, task_id))

        # Wait for worker alive, then dispatch
        await asyncio.sleep(0.5)
        await dispatch_task_to_worker(agent, chat_id, task_id, reply_topic=reply_t)

        await asyncio.wait_for(worker_task, timeout=10.0)
        await asyncio.sleep(0.3)
        await collector.stop()

        payloads = collector.all_payloads()
        # Should have stream items + terminal status
        text_items = [p for p in payloads if p.get("type") == "text"]
        status_items = [p for p in payloads if "state" in p]

        assert len(text_items) >= 1
        assert len(status_items) == 1
        assert status_items[0]["state"] == "completed"
        assert "Hello" in status_items[0]["result"]

    async def test_tool_use_streaming(self, monkeypatch):
        """Worker streams tool_use and tool_result items."""
        agent = "coder"
        chat_id = f"test-tools-{uuid.uuid4().hex[:8]}"
        task_id = uuid.uuid4().hex[:12]
        reply_t = topic_reply("test", uuid.uuid4().hex[:8])

        script = [
            text_message("Let me check..."),
            tool_use_message("Bash", {"command": "echo hello"}, "tool_1"),
            (
                "hook",
                "PostToolUse",
                {
                    "tool_name": "Bash",
                    "tool_response": "hello\n",
                },
                "tool_1",
            ),
            text_message("The command output was hello."),
            result_message(num_turns=2),
        ]
        monkeypatch.setattr("claude_agent_sdk.query", make_mock_query(script))

        collector = MQTTCollector([reply_t])
        await collector.start()
        await asyncio.sleep(0.1)

        worker_task = asyncio.create_task(worker_run(agent, chat_id, task_id))
        await asyncio.sleep(0.5)
        await dispatch_task_to_worker(
            agent, chat_id, task_id, max_turns=5, reply_topic=reply_t
        )

        await asyncio.wait_for(worker_task, timeout=10.0)
        await asyncio.sleep(0.3)
        await collector.stop()

        payloads = collector.all_payloads()
        tool_use = [p for p in payloads if p.get("type") == "tool_use"]
        tool_result = [p for p in payloads if p.get("type") == "tool_result"]
        status = [p for p in payloads if "state" in p]

        assert len(tool_use) >= 1
        assert len(tool_result) >= 1
        assert len(status) == 1
        assert (
            "hello" in status[0]["result"].lower()
            or "Tool Results" in status[0]["result"]
        )

    async def test_cancel(self, monkeypatch):
        """Worker stops on cancel signal."""
        agent = "researcher"
        chat_id = f"test-cancel-{uuid.uuid4().hex[:8]}"
        task_id = uuid.uuid4().hex[:12]
        reply_t = topic_reply("test", uuid.uuid4().hex[:8])

        async def send_cancel():
            await asyncio.sleep(0.2)
            cancel_payload = json.dumps(
                {
                    "jsonrpc": "2.0",
                    "method": "tasks/cancel",
                    "params": {"task_id": task_id},
                    "id": "cancel-1",
                }
            )
            async with aiomqtt.Client(
                MQTT_HOST,
                MQTT_PORT,
                identifier=f"test-cancel-pub-{uuid.uuid4().hex[:8]}",
                protocol=aiomqtt.ProtocolVersion.V5,
            ) as c:
                await c.publish(
                    topic_request_cancel(agent),
                    cancel_payload,
                    qos=1,
                )

        script = [
            text_message("Starting..."),
            tool_use_message("Bash", {"command": "sleep 10"}, "tool_1"),
            (
                "hook",
                "PreToolUse",
                {"tool_name": "Bash", "input": {"command": "sleep 10"}},
                "tool_1",
            ),
            ("callback", send_cancel),
            tool_use_message("Bash", {"command": "echo next"}, "tool_2"),
            (
                "hook",
                "PreToolUse",
                {"tool_name": "Bash", "input": {"command": "echo next"}},
                "tool_2",
            ),
            text_message("Done"),
            result_message(num_turns=2),
        ]
        monkeypatch.setattr("claude_agent_sdk.query", make_mock_query(script))

        collector = MQTTCollector([reply_t])
        await collector.start()
        await asyncio.sleep(0.1)

        worker_task = asyncio.create_task(worker_run(agent, chat_id, task_id))
        await asyncio.sleep(0.5)
        await dispatch_task_to_worker(
            agent, chat_id, task_id, max_turns=5, reply_topic=reply_t
        )

        await asyncio.wait_for(worker_task, timeout=15.0)
        await asyncio.sleep(0.3)
        await collector.stop()

        payloads = collector.all_payloads()
        status = [p for p in payloads if "state" in p]
        assert len(status) == 1

    async def test_usage_published(self, monkeypatch):
        """Worker publishes usage to the A2A state topic."""
        agent = "writer"
        chat_id = f"test-usage-{uuid.uuid4().hex[:8]}"
        task_id = uuid.uuid4().hex[:12]
        reply_t = topic_reply("test", uuid.uuid4().hex[:8])
        usage_t = topic_state_usage(chat_id, task_id)

        script = [
            text_message("Short response"),
            result_message(cost_usd=0.0042),
        ]
        monkeypatch.setattr("claude_agent_sdk.query", make_mock_query(script))

        collector = MQTTCollector([reply_t, usage_t])
        await collector.start()
        await asyncio.sleep(0.1)

        worker_task = asyncio.create_task(worker_run(agent, chat_id, task_id))
        await asyncio.sleep(0.5)
        await dispatch_task_to_worker(
            agent, chat_id, task_id, max_turns=0, reply_topic=reply_t
        )

        await asyncio.wait_for(worker_task, timeout=10.0)
        await asyncio.sleep(0.3)
        await collector.stop()

        usage_msgs = [p for _, p in collector.messages if usage_t in _ or "usage" in _]
        # Usage should be published
        assert len(usage_msgs) >= 1
        usage_data = json.loads(usage_msgs[0])
        assert usage_data["task_id"] == task_id
        assert usage_data["cost_usd"] == 0.0042

    async def test_workspace_created(self, monkeypatch, tmp_path):
        """Worker creates workspace directory."""
        agent = "coder"
        chat_id = f"test-ws-{uuid.uuid4().hex[:8]}"
        task_id = uuid.uuid4().hex[:12]
        reply_t = topic_reply("test", uuid.uuid4().hex[:8])

        script = [
            text_message("Done"),
            result_message(),
        ]
        monkeypatch.setattr("claude_agent_sdk.query", make_mock_query(script))

        # Override WORKSPACES_DIR to use tmp_path
        from skitter import config

        monkeypatch.setattr(config, "WORKSPACES_DIR", tmp_path / "workspaces")

        worker_task = asyncio.create_task(worker_run(agent, chat_id, task_id))
        await asyncio.sleep(0.5)
        await dispatch_task_to_worker(
            agent, chat_id, task_id, max_turns=0, reply_topic=reply_t
        )

        await asyncio.wait_for(worker_task, timeout=10.0)

        workspace = tmp_path / "workspaces" / task_id
        assert workspace.is_dir()
