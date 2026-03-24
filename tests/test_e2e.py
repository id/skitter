"""E2E tests: coordinator + agent-runner in-process, real MQTT, mocked CLI + graph gen.

Requires EMQX on localhost:1883. No Docker, no LLM API.

Usage:
  uv run python -m pytest tests/test_e2e.py -v -s
"""

from __future__ import annotations

import asyncio
import json
import uuid
from unittest.mock import AsyncMock, patch

import aiomqtt
import pytest
import pytest_asyncio

from skitter.config import AgentDef
from skitter.coordinator import Coordinator
from skitter.db import SqliteDB
from skitter.a2a import (
    A2A_ORG,
    A2A_UNIT,
    A2ARequest,
    REPLY_ARTIFACT,
    REPLY_ERROR,
    REPLY_FAILED,
    REPLY_SUBMITTED,
    REPLY_TERMINAL,
    REPLY_TEXT,
    classify_reply,
    topic_coordinator_lock,
    topic_discovery,
    topic_reply,
    topic_request,
)
from skitter.mqtt import (
    make_properties,
    mqtt_client_kwargs,
)

from .conftest import create_test_app, needs_mqtt, send_and_collect, wait_for_discovery

pytestmark = [needs_mqtt, pytest.mark.asyncio]


# ---------------------------------------------------------------------------
# Mock dispatch for _run_cli
# ---------------------------------------------------------------------------

_cli_handlers: dict[str, object] = {}


async def _dispatching_run_cli(agent, prompt, publish_stream, env):
    handler = _cli_handlers.get(agent.id)
    if handler:
        return await handler(agent, prompt, publish_stream, env)
    return f"Response from {agent.id}"


# ---------------------------------------------------------------------------
# MQTT cleanup helper
# ---------------------------------------------------------------------------


async def _clear_retained(topic: str) -> None:
    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{A2A_ORG}/{A2A_UNIT}/clear-{uuid.uuid4().hex[:6]}",
        ),
    ) as client:
        await client.publish(topic, b"", qos=1, retain=True)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(autouse=True)
def _patch_run_cli():
    with patch("skitter.agent_runner._run_cli", new=_dispatching_run_cli):
        yield
    _cli_handlers.clear()


@pytest_asyncio.fixture
async def coordinator():
    """Start coordinator in-process with in-memory DB."""
    db = SqliteDB(":memory:")
    coord = Coordinator(db)
    coord._check_coordinator_lock = AsyncMock()

    # Clear stale retained messages before starting
    await _clear_retained(topic_coordinator_lock())
    await _clear_retained(topic_discovery("skitter"))

    task = asyncio.create_task(coord.run())
    await wait_for_discovery("skitter")
    yield coord
    task.cancel()
    try:
        await task
    except asyncio.CancelledError:
        pass
    db.close()
    await _clear_retained(topic_coordinator_lock())
    await _clear_retained(topic_discovery("skitter"))


@pytest.fixture
def mock_graph():
    """Fixture to set the graph that generate_graph will return."""
    container: dict = {}

    async def _mock(instructions, agent_cards, *, model=""):
        return dict(container["graph"])

    with patch("skitter.runtime_api.generate_graph", new=_mock):

        def setter(graph):
            container["graph"] = graph

        yield setter


@pytest_asyncio.fixture
async def start_agent():
    """Factory: start agent runners in-process with mocked _run_cli."""
    from skitter.agent_runner import run_with_def

    agents: list[tuple[str, asyncio.Task]] = []

    async def _start(agent_id, *, description="test agent", handler=None):
        if handler:
            _cli_handlers[agent_id] = handler
        agent_def = AgentDef(id=agent_id, name=agent_id, description=description)
        t = asyncio.create_task(run_with_def(agent_def))
        agents.append((agent_id, t))
        await wait_for_discovery(agent_id)
        return agent_id

    yield _start

    for agent_id, t in agents:
        t.cancel()
        try:
            await t
        except asyncio.CancelledError:
            pass
        _cli_handlers.pop(agent_id, None)

    if agents:
        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{A2A_ORG}/{A2A_UNIT}/disco-clear-{uuid.uuid4().hex[:6]}",
            ),
        ) as client:
            for agent_id, _ in agents:
                await client.publish(topic_discovery(agent_id), b"", qos=1, retain=True)


# ---------------------------------------------------------------------------
# Agent runner tests (no coordinator)
# ---------------------------------------------------------------------------


class TestAgentRunner:
    async def test_discovery_card(self, start_agent):
        aid = f"test-disco-{uuid.uuid4().hex[:4]}"
        await start_agent(aid, description="Discovery test agent")
        card = await wait_for_discovery(aid)
        assert card["supportedInterfaces"][0]["protocolVersion"] == "1.0.0"
        assert card["capabilities"]["streaming"] is True
        assert card["skills"][0]["id"] == aid
        assert "tags" in card["skills"][0]

    async def test_direct_query(self, start_agent):
        aid = f"test-query-{uuid.uuid4().hex[:4]}"

        async def handler(agent, prompt, publish_stream, env):
            return "42"

        await start_agent(aid, handler=handler)

        req = A2ARequest(
            text="What is 2+2?",
            request_id=f"q-{uuid.uuid4().hex[:8]}",
            sender="test",
        )
        result = await send_and_collect(topic_request(aid), req, timeout=10.0)
        assert "42" in result

    async def test_streaming(self, start_agent):
        aid = f"test-stream-{uuid.uuid4().hex[:4]}"

        async def handler(agent, prompt, publish_stream, env):
            await publish_stream("text", "thinking...")
            await publish_stream("text", "done.")
            return "Final answer"

        await start_agent(aid, handler=handler)

        test_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", test_id)
        req = A2ARequest(
            text="Stream test",
            request_id=f"s-{uuid.uuid4().hex[:8]}",
            sender="test",
        )

        messages: list[tuple[str, str]] = []
        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{A2A_ORG}/{A2A_UNIT}/test-stream-{test_id}",
            ),
        ) as client:
            await client.subscribe(reply_t, qos=1)
            props = make_properties(
                response_topic=reply_t, correlation_data=req.request_id
            )
            await client.publish(
                topic_request(aid), req.to_json(), qos=1, properties=props
            )

            async with asyncio.timeout(10.0):
                async for msg in client.messages:
                    payload = msg.payload.decode() if msg.payload else ""
                    if not payload:
                        continue
                    data = json.loads(payload)
                    kind, content = classify_reply(data)
                    messages.append((kind, content))
                    if kind in (REPLY_TERMINAL, REPLY_FAILED, REPLY_ERROR):
                        break

        stream_msgs = [(k, c) for k, c in messages if k == REPLY_TEXT]
        assert len(stream_msgs) >= 2
        assert any("thinking" in c for _, c in stream_msgs)
        artifacts = [(k, c) for k, c in messages if k == REPLY_ARTIFACT]
        assert artifacts
        assert "Final answer" in artifacts[0][1]
        terminal = [(k, c) for k, c in messages if k == REPLY_TERMINAL]
        assert terminal

    async def test_reply_echoes_requester_task_id(self, start_agent):
        """All reply events must carry the requester's taskId (spec gap 1)."""
        aid = f"test-taskid-{uuid.uuid4().hex[:4]}"

        async def handler(agent, prompt, publish_stream, env):
            await publish_stream("text", "working")
            return "done"

        await start_agent(aid, handler=handler)

        test_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", test_id)
        task_id = str(uuid.uuid4())
        req = A2ARequest(
            text="Echo test",
            request_id=f"e-{uuid.uuid4().hex[:8]}",
            task_id=task_id,
            sender="test",
        )

        task_ids_in_replies: list[str] = []
        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{A2A_ORG}/{A2A_UNIT}/test-echo-{test_id}",
            ),
        ) as client:
            await client.subscribe(reply_t, qos=1)
            props = make_properties(
                response_topic=reply_t, correlation_data=req.request_id
            )
            await client.publish(
                topic_request(aid), req.to_json(), qos=1, properties=props
            )

            async with asyncio.timeout(10.0):
                async for msg in client.messages:
                    payload = msg.payload.decode() if msg.payload else ""
                    if not payload:
                        continue
                    data = json.loads(payload)
                    result = data.get("result", {})
                    if result.get("type") == "TaskStatusUpdateEvent":
                        task_ids_in_replies.append(result.get("taskId", ""))
                    kind, _ = classify_reply(data)
                    if kind in (REPLY_TERMINAL, REPLY_FAILED, REPLY_ERROR):
                        break

        assert len(task_ids_in_replies) >= 2  # at least submitted + completed
        assert all(tid == task_id for tid in task_ids_in_replies)

    async def test_rejects_missing_task_id(self, start_agent):
        """Agent runner must reject requests with no Task.id (spec gap 6)."""
        aid = f"test-notaskid-{uuid.uuid4().hex[:4]}"
        await start_agent(aid)

        test_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", test_id)

        # Hand-craft a payload with empty taskId
        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": "rpc-1",
                "method": "message/send",
                "params": {
                    "message": {
                        "role": "user",
                        "parts": [{"type": "text", "text": "hello"}],
                        "taskId": "",
                    }
                },
            }
        )

        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{A2A_ORG}/{A2A_UNIT}/test-notaskid-{test_id}",
            ),
        ) as client:
            await client.subscribe(reply_t, qos=1)
            props = make_properties(response_topic=reply_t, correlation_data="corr-1")
            await client.publish(topic_request(aid), payload, qos=1, properties=props)

            async with asyncio.timeout(5.0):
                async for msg in client.messages:
                    raw = msg.payload.decode() if msg.payload else ""
                    if not raw:
                        continue
                    data = json.loads(raw)
                    assert "error" in data
                    assert data["error"]["code"] == -32005
                    assert (
                        data["error"]["data"]["a2a_error"] == "transport_protocol_error"
                    )
                    return

            pytest.fail("No error response received for missing Task.id")

    async def test_deduplication_by_task_id(self, start_agent):
        """Duplicate Task.id must return existing task state, not re-execute."""
        aid = f"test-dedup-{uuid.uuid4().hex[:4]}"
        call_count = 0

        async def counting_handler(agent, prompt, publish_stream, env):
            nonlocal call_count
            call_count += 1
            return f"call-{call_count}"

        await start_agent(aid, handler=counting_handler)

        task_id = str(uuid.uuid4())
        test_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", test_id)

        # First request
        req1 = A2ARequest(
            text="first",
            request_id=f"r1-{uuid.uuid4().hex[:8]}",
            task_id=task_id,
            sender="test",
        )

        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{A2A_ORG}/{A2A_UNIT}/test-dedup-{test_id}",
            ),
        ) as client:
            await client.subscribe(reply_t, qos=1)
            props = make_properties(
                response_topic=reply_t, correlation_data=req1.request_id
            )
            await client.publish(
                topic_request(aid), req1.to_json(), qos=1, properties=props
            )

            # Wait for completion
            async with asyncio.timeout(10.0):
                async for msg in client.messages:
                    raw = msg.payload.decode() if msg.payload else ""
                    if not raw:
                        continue
                    data = json.loads(raw)
                    kind, _ = classify_reply(data)
                    if kind in (REPLY_TERMINAL, REPLY_FAILED, REPLY_ERROR):
                        break

            assert call_count == 1

            # Send duplicate with same task_id and context_id (retry)
            req2 = A2ARequest(
                text="duplicate",
                request_id=f"r2-{uuid.uuid4().hex[:8]}",
                task_id=task_id,
                context_id=req1.context_id,
                sender="test",
            )
            props2 = make_properties(
                response_topic=reply_t, correlation_data=req2.request_id
            )
            await client.publish(
                topic_request(aid), req2.to_json(), qos=1, properties=props2
            )

            # Must get a reply with existing completed state
            async with asyncio.timeout(5.0):
                async for msg in client.messages:
                    raw = msg.payload.decode() if msg.payload else ""
                    if not raw:
                        continue
                    data = json.loads(raw)
                    result = data.get("result", {})
                    state = result.get("status", {}).get("state", "")
                    if state == "completed":
                        break

        assert call_count == 1  # handler was NOT called again


# ---------------------------------------------------------------------------
# Composed app tests (coordinator + agent runners)
# ---------------------------------------------------------------------------


class TestComposedApp:
    async def test_linear_pipeline(self, coordinator, start_agent, mock_graph):
        aid_a = f"agent-a-{uuid.uuid4().hex[:4]}"
        aid_b = f"agent-b-{uuid.uuid4().hex[:4]}"

        async def handler_a(agent, prompt, publish_stream, env):
            return "output-from-A"

        async def handler_b(agent, prompt, publish_stream, env):
            return f"B got: {prompt}"

        await start_agent(aid_a, description="First agent", handler=handler_a)
        await start_agent(aid_b, description="Second agent", handler=handler_b)

        mock_graph(
            {
                "tasks": [
                    {
                        "id": "step-a",
                        "agent": aid_a,
                        "description": "Do A",
                        "needs": [],
                        "next": "step-b",
                    },
                    {
                        "id": "step-b",
                        "agent": aid_b,
                        "description": "Do B",
                        "needs": ["step-a"],
                        "next": "output",
                    },
                ]
            }
        )

        app_id = await create_test_app(
            [aid_a, aid_b], f"First {aid_a} does A, then {aid_b} does B."
        )
        await wait_for_discovery(app_id)

        req = A2ARequest(
            text="Go.",
            request_id=f"app-{uuid.uuid4().hex[:8]}",
            sender="test",
        )
        result = await send_and_collect(topic_request(app_id), req, timeout=15.0)
        assert result
        assert "output-from-A" in result

    async def test_pipeline_replies_carry_requester_task_id(
        self, coordinator, start_agent, mock_graph
    ):
        """All coordinator replies must carry the requester's Task.id (spec gap 1)."""
        aid = f"agent-tid-{uuid.uuid4().hex[:4]}"

        async def handler(agent, prompt, publish_stream, env):
            await publish_stream("text", "working")
            return "result"

        await start_agent(aid, description="TaskId test", handler=handler)

        mock_graph(
            {
                "tasks": [
                    {
                        "id": "step",
                        "agent": aid,
                        "description": "Do it",
                        "needs": [],
                        "next": "output",
                    },
                ]
            }
        )

        app_id = await create_test_app([aid], f"Use {aid}")
        await wait_for_discovery(app_id)

        test_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", test_id)
        task_id = str(uuid.uuid4())
        req = A2ARequest(
            text="Go.",
            request_id=f"app-{uuid.uuid4().hex[:8]}",
            task_id=task_id,
            sender="test",
        )

        task_ids_in_replies: list[str] = []
        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{A2A_ORG}/{A2A_UNIT}/test-tid-{test_id}",
            ),
        ) as client:
            await client.subscribe(reply_t, qos=1)
            props = make_properties(
                response_topic=reply_t, correlation_data=req.request_id
            )
            await client.publish(
                topic_request(app_id), req.to_json(), qos=1, properties=props
            )

            async with asyncio.timeout(15.0):
                async for msg in client.messages:
                    payload = msg.payload.decode() if msg.payload else ""
                    if not payload:
                        continue
                    data = json.loads(payload)
                    result = data.get("result", {})
                    if result.get("type") == "TaskStatusUpdateEvent":
                        task_ids_in_replies.append(result.get("taskId", ""))
                    kind, _ = classify_reply(data)
                    if kind in (REPLY_TERMINAL, REPLY_FAILED, REPLY_ERROR):
                        break

        # Coordinator replies carry req.task_id (the original requester's Task.id)
        assert len(task_ids_in_replies) >= 2  # at least submitted + completed
        assert all(tid == task_id for tid in task_ids_in_replies)

    async def test_fan_out_fan_in(self, coordinator, start_agent, mock_graph):
        aid_a = f"fork-a-{uuid.uuid4().hex[:4]}"
        aid_b = f"fork-b-{uuid.uuid4().hex[:4]}"
        aid_c = f"merge-{uuid.uuid4().hex[:4]}"

        num_a, num_b = 42, 99

        async def handler_a(agent, prompt, publish_stream, env):
            return json.dumps({"a": num_a})

        async def handler_b(agent, prompt, publish_stream, env):
            return json.dumps({"b": num_b})

        async def handler_c(agent, prompt, publish_stream, env):
            merged: dict = {}
            for line in prompt.split("\n"):
                line = line.strip()
                if line.startswith("{"):
                    try:
                        merged.update(json.loads(line))
                    except json.JSONDecodeError:
                        pass
            return json.dumps(merged)

        await start_agent(aid_a, description="Produces JSON A", handler=handler_a)
        await start_agent(aid_b, description="Produces JSON B", handler=handler_b)
        await start_agent(aid_c, description="Merges JSON", handler=handler_c)

        mock_graph(
            {
                "tasks": [
                    {
                        "id": "fork-a",
                        "agent": aid_a,
                        "description": "Produce A",
                        "needs": [],
                        "next": "merge",
                    },
                    {
                        "id": "fork-b",
                        "agent": aid_b,
                        "description": "Produce B",
                        "needs": [],
                        "next": "merge",
                    },
                    {
                        "id": "merge",
                        "agent": aid_c,
                        "description": "Merge",
                        "needs": ["fork-a", "fork-b"],
                        "next": "output",
                    },
                ]
            }
        )

        app_id = await create_test_app([aid_a, aid_b, aid_c], "Fork and merge")
        await wait_for_discovery(app_id)

        req = A2ARequest(
            text="Go.",
            request_id=f"app-{uuid.uuid4().hex[:8]}",
            sender="test",
        )
        result = await send_and_collect(topic_request(app_id), req, timeout=15.0)
        assert result
        data = json.loads(result)
        assert data["a"] == num_a
        assert data["b"] == num_b


# ---------------------------------------------------------------------------
# Correlation data validation
# ---------------------------------------------------------------------------


class TestCorrelationData:
    async def test_replies_carry_correlation_data(
        self, coordinator, start_agent, mock_graph
    ):
        """Agent replies must echo MQTT Correlation Data from the coordinator's dispatch."""
        aid = f"corr-echo-{uuid.uuid4().hex[:4]}"
        await start_agent(aid, handler=lambda a, p, s, e: "done")

        mock_graph(
            {
                "tasks": [
                    {
                        "id": "step",
                        "agent": aid,
                        "description": "Do it",
                        "needs": [],
                        "next": "output",
                    },
                ]
            }
        )

        app_id = await create_test_app([aid], f"Use {aid}")
        await wait_for_discovery(app_id)

        # Subscribe to the coordinator's internal reply topic to inspect correlation
        # The coordinator subscribes to $a2a/v1/reply/{org}/{unit}/skitter/{sid}/{nid}
        # but we don't know the session_id yet. Subscribe with wildcard.
        spy_topic = "$a2a/v1/reply/skitter/default/skitter/#"

        test_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", test_id)
        req = A2ARequest(
            text="Go.",
            request_id=f"app-{uuid.uuid4().hex[:8]}",
            sender="test",
        )

        correlation_on_replies: list[str] = []
        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{A2A_ORG}/{A2A_UNIT}/test-corr-spy-{test_id}",
            ),
        ) as client:
            await client.subscribe(spy_topic, qos=1)
            await client.subscribe(reply_t, qos=1)
            props = make_properties(
                response_topic=reply_t, correlation_data=req.request_id
            )
            await client.publish(
                topic_request(app_id), req.to_json(), qos=1, properties=props
            )

            async with asyncio.timeout(15.0):
                async for msg in client.messages:
                    topic = str(msg.topic)
                    payload = msg.payload.decode() if msg.payload else ""
                    if not payload:
                        continue

                    # Capture correlation from agent replies to coordinator
                    if "/reply/skitter/default/skitter/" in topic:
                        corr_bytes = getattr(msg.properties, "CorrelationData", None)
                        if corr_bytes:
                            correlation_on_replies.append(corr_bytes.decode())

                    # Stop when the caller gets the terminal reply
                    if topic == reply_t:
                        data = json.loads(payload)
                        kind, _ = classify_reply(data)
                        if kind in (REPLY_TERMINAL, REPLY_FAILED, REPLY_ERROR):
                            break

        # Agent replies must carry correlation data
        assert len(correlation_on_replies) >= 1
        # All replies for the same dispatch must use the same correlation value
        assert len(set(correlation_on_replies)) == 1

    async def test_injected_reply_with_wrong_correlation_is_ignored(
        self, coordinator, start_agent, mock_graph
    ):
        """A spoofed reply with wrong correlation must not complete the task."""
        aid = f"corr-spoof-{uuid.uuid4().hex[:4]}"

        got_prompt = asyncio.Event()

        async def handler_slow(agent, prompt, publish_stream, env):
            got_prompt.set()
            await asyncio.sleep(5)
            return "real-result"

        await start_agent(aid, handler=handler_slow)

        mock_graph(
            {
                "tasks": [
                    {
                        "id": "step",
                        "agent": aid,
                        "description": "Do it",
                        "needs": [],
                        "next": "output",
                    },
                ]
            }
        )

        app_id = await create_test_app([aid], f"Use {aid}")
        await wait_for_discovery(app_id)

        test_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", test_id)
        req = A2ARequest(
            text="Go.",
            request_id=f"app-{uuid.uuid4().hex[:8]}",
            sender="test",
        )

        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{A2A_ORG}/{A2A_UNIT}/test-spoof-{test_id}",
            ),
        ) as client:
            await client.subscribe(reply_t, qos=1)
            props = make_properties(
                response_topic=reply_t, correlation_data=req.request_id
            )
            await client.publish(
                topic_request(app_id), req.to_json(), qos=1, properties=props
            )

            # Wait for the agent to receive the prompt (task is dispatched)
            async with asyncio.timeout(10.0):
                await got_prompt.wait()

            # Find the session to build the correct reply topic
            sessions = list(coordinator._sessions.values())
            assert len(sessions) == 1
            state = sessions[0]
            sid = state.session_id

            # Inject a fake completed reply with wrong correlation
            spoof_topic = f"$a2a/v1/reply/skitter/default/skitter/{sid}/step"
            spoof_payload = json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": "bad-corr",
                    "result": {
                        "type": "TaskStatusUpdateEvent",
                        "taskId": "fake",
                        "contextId": "",
                        "status": {"state": "completed"},
                    },
                }
            )
            spoof_props = make_properties(correlation_data="bad-corr")
            await client.publish(
                spoof_topic, spoof_payload, qos=1, properties=spoof_props
            )

            # The real agent will complete after ~5s; wait for the real result
            result_text = ""
            async with asyncio.timeout(15.0):
                async for msg in client.messages:
                    payload = msg.payload.decode() if msg.payload else ""
                    if not payload:
                        continue
                    data = json.loads(payload)
                    kind, content = classify_reply(data)
                    if kind == REPLY_ARTIFACT:
                        result_text = content
                    elif kind in (REPLY_TERMINAL, REPLY_FAILED, REPLY_ERROR):
                        break

        # The real agent's result must come through, not the spoofed one
        assert "real-result" in result_text


# ---------------------------------------------------------------------------
# Cancellation
# ---------------------------------------------------------------------------


class TestCancellation:
    async def test_cancel_running_session(self, coordinator, start_agent, mock_graph):
        aid = f"slow-{uuid.uuid4().hex[:4]}"

        async def handler_slow(agent, prompt, publish_stream, env):
            await asyncio.sleep(60)
            return "should not reach"

        await start_agent(aid, description="Slow agent", handler=handler_slow)

        mock_graph(
            {
                "tasks": [
                    {
                        "id": "slow-task",
                        "agent": aid,
                        "description": "Slow",
                        "needs": [],
                        "next": "output",
                    },
                ]
            }
        )

        app_id = await create_test_app([aid], f"Use {aid}")
        await wait_for_discovery(app_id)

        test_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", test_id)

        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{A2A_ORG}/{A2A_UNIT}/test-cancel-{test_id}",
            ),
        ) as client:
            await client.subscribe(reply_t, qos=1)

            app_req = A2ARequest(
                text="Go.",
                request_id=f"app-{uuid.uuid4().hex[:8]}",
                sender="test",
            )
            props = make_properties(
                response_topic=reply_t, correlation_data=app_req.request_id
            )
            await client.publish(
                topic_request(app_id), app_req.to_json(), qos=1, properties=props
            )

            # Wait for submitted ack
            session_id = None
            async with asyncio.timeout(10.0):
                async for msg in client.messages:
                    payload = msg.payload.decode() if msg.payload else ""
                    if not payload:
                        continue
                    data = json.loads(payload)
                    kind, content = classify_reply(data)
                    if kind == REPLY_SUBMITTED:
                        session_id = content
                        break

            assert session_id

            # Cancel the session
            cancel_req = A2ARequest(
                text=f"cancel session {session_id}",
                request_id=f"cancel-{uuid.uuid4().hex[:8]}",
                sender="test",
            )
            cancel_reply_t = topic_reply("test", f"cancel-{test_id}")
            cancel_props = make_properties(
                response_topic=cancel_reply_t,
                correlation_data=cancel_req.request_id,
            )
            await client.subscribe(cancel_reply_t, qos=1)
            await client.publish(
                topic_request("skitter"),
                cancel_req.to_json(),
                qos=1,
                properties=cancel_props,
            )

            # Wait for cancelled reply on original request
            async with asyncio.timeout(10.0):
                async for msg in client.messages:
                    payload = msg.payload.decode() if msg.payload else ""
                    if not payload:
                        continue
                    data = json.loads(payload)
                    kind, content = classify_reply(data)
                    if kind == REPLY_FAILED:
                        assert "cancel" in content.lower()
                        return
                    if kind in (REPLY_TERMINAL, REPLY_ERROR):
                        return  # race: completed before cancel

            pytest.fail("No cancellation reply received")


# ---------------------------------------------------------------------------
# Agent failure
# ---------------------------------------------------------------------------


class TestAgentFailure:
    async def test_failure_propagates(self, coordinator, start_agent, mock_graph):
        aid = f"fail-{uuid.uuid4().hex[:4]}"

        async def handler_fail(agent, prompt, publish_stream, env):
            raise RuntimeError("Intentional failure")

        await start_agent(aid, description="Failing agent", handler=handler_fail)

        mock_graph(
            {
                "tasks": [
                    {
                        "id": "fail-task",
                        "agent": aid,
                        "description": "Fail",
                        "needs": [],
                        "next": "output",
                    },
                ]
            }
        )

        app_id = await create_test_app([aid], f"Use {aid}")
        await wait_for_discovery(app_id)

        req = A2ARequest(
            text="Go.",
            request_id=f"app-{uuid.uuid4().hex[:8]}",
            sender="test",
        )
        result = await send_and_collect(topic_request(app_id), req, timeout=15.0)
        assert result
        assert "failed" in result.lower()

    async def test_failure_cascades_in_dag(self, coordinator, start_agent, mock_graph):
        aid_a = f"fail-a-{uuid.uuid4().hex[:4]}"
        aid_b = f"ok-b-{uuid.uuid4().hex[:4]}"

        async def handler_fail(agent, prompt, publish_stream, env):
            raise RuntimeError("A crashed")

        async def handler_ok(agent, prompt, publish_stream, env):
            return "B ran successfully"

        await start_agent(aid_a, description="Failing A", handler=handler_fail)
        await start_agent(aid_b, description="OK B", handler=handler_ok)

        mock_graph(
            {
                "tasks": [
                    {
                        "id": "step-a",
                        "agent": aid_a,
                        "description": "Fail",
                        "needs": [],
                        "next": "step-b",
                    },
                    {
                        "id": "step-b",
                        "agent": aid_b,
                        "description": "OK",
                        "needs": ["step-a"],
                        "next": "output",
                    },
                ]
            }
        )

        app_id = await create_test_app([aid_a, aid_b], "A then B")
        await wait_for_discovery(app_id)

        req = A2ARequest(
            text="Go.",
            request_id=f"app-{uuid.uuid4().hex[:8]}",
            sender="test",
        )
        result = await send_and_collect(topic_request(app_id), req, timeout=15.0)
        assert result
        assert "failed" in result.lower()
