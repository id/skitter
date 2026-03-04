"""E2E tests for skitter supervisor with A2A-over-MQTT.

Requires a running MQTT broker on localhost:1883 with MQTT v5 support.
Start one with: docker compose up -d
"""

from __future__ import annotations

import asyncio
import json
import uuid

import aiomqtt
import pytest
import pytest_asyncio

from skitter.config import AgentDef, PipelineDef, PipelineTask
from skitter.coordinator import (
    create_session,
    get_entry_tasks,
    run,
)
from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    make_properties,
    topic_reply,
    topic_request,
    topic_state_dispatch,
    topic_state_session_wildcard,
)
from skitter.types import (
    InboundMessage,
    Session,
    SessionTask,
    TaskStatusUpdate,
)


# ---------------------------------------------------------------------------
# Helpers
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
# Unit tests — pure functions, no MQTT
# ---------------------------------------------------------------------------


class TestGetEntryTasks:
    def test_pending_no_needs_is_entry(self):
        session = Session(session_id="c1", label="test")
        session.tasks["a"] = SessionTask(
            id="a",
            task_id="t1",
            agent="w",
            description="A",
            status="pending",
        )
        entry = get_entry_tasks(session)
        assert len(entry) == 1

    def test_pending_with_needs_not_entry(self):
        session = Session(session_id="c1", label="test")
        session.tasks["a"] = SessionTask(
            id="a",
            task_id="t1",
            agent="w",
            description="A",
            status="done",
        )
        session.tasks["b"] = SessionTask(
            id="b",
            task_id="t2",
            agent="w",
            description="B",
            needs=["a"],
            status="pending",
        )
        entry = get_entry_tasks(session)
        assert len(entry) == 0

    def test_running_not_entry(self):
        session = Session(session_id="c1", label="test")
        session.tasks["a"] = SessionTask(
            id="a",
            task_id="t1",
            agent="w",
            description="A",
            status="running",
        )
        entry = get_entry_tasks(session)
        assert len(entry) == 0


class TestCreateSessionFromPipeline:
    """Verify pipeline building with variable interpolation and agent defaults."""

    MODELS = {"haiku": "fast", "sonnet": "balanced"}
    AGENTS = {
        "researcher": AgentDef(
            id="researcher",
            name="Research Specialist",
            soul="Be thorough.",
            skills="Cite sources.",
            model="sonnet",
            max_turns=15,
        ),
    }

    def test_variable_interpolation(self):
        pipeline = PipelineDef(
            id="test",
            name="Test",
            variables=["topic"],
            tasks=[
                PipelineTask(
                    id="r1",
                    agent="researcher",
                    description="Research '{topic}' in depth.",
                    next="output",
                ),
            ],
        )
        session = create_session(
            "c1",
            "test",
            pipeline=pipeline,
            variables={"topic": "MQTT"},
            models=self.MODELS,
            agents=self.AGENTS,
        )
        assert session.tasks["r1"].description == "Research 'MQTT' in depth."

    def test_agent_defaults_applied(self):
        pipeline = PipelineDef(
            id="test",
            name="Test",
            tasks=[
                PipelineTask(
                    id="r1", agent="researcher", description="Go", next="output"
                ),
            ],
        )
        session = create_session(
            "c1",
            "test",
            pipeline=pipeline,
            models=self.MODELS,
            agents=self.AGENTS,
        )
        t = session.tasks["r1"]
        assert t.model == "sonnet"

    def test_pipeline_task_override_beats_agent(self):
        pipeline = PipelineDef(
            id="test",
            name="Test",
            tasks=[
                PipelineTask(
                    id="r1",
                    agent="researcher",
                    description="Quick check",
                    model="haiku",
                    max_turns=3,
                    next="output",
                ),
            ],
        )
        session = create_session(
            "c1",
            "test",
            pipeline=pipeline,
            models=self.MODELS,
            agents=self.AGENTS,
        )
        t = session.tasks["r1"]
        assert t.model == "haiku"

    def test_unknown_vars_left_intact(self):
        """Variables not in the vars dict are left as {placeholder}."""
        pipeline = PipelineDef(
            id="test",
            name="Test",
            variables=["topic"],
            tasks=[
                PipelineTask(
                    id="r1",
                    agent="researcher",
                    description="Research {topic}, output as {format}.",
                    next="output",
                ),
            ],
        )
        session = create_session(
            "c1",
            "test",
            pipeline=pipeline,
            variables={"topic": "AI"},
            models=self.MODELS,
        )
        assert session.tasks["r1"].description == "Research AI, output as {format}."


class TestCreateSessionFromAgent:
    """Verify direct agent session building."""

    MODELS = {"haiku": "fast", "sonnet": "balanced"}
    AGENTS = {
        "researcher": AgentDef(
            id="researcher",
            name="Research Specialist",
            soul="Be thorough.",
            skills="Cite sources.",
            model="sonnet",
            max_turns=15,
        ),
    }

    def test_single_task_created(self):
        session = create_session(
            "c1",
            "What is MQTT?",
            agent_id="researcher",
            text="What is MQTT?",
            agents=self.AGENTS,
            models=self.MODELS,
        )
        assert len(session.tasks) == 1
        assert "researcher" in session.tasks

    def test_agent_defaults_applied(self):
        session = create_session(
            "c1",
            "What is MQTT?",
            agent_id="researcher",
            text="What is MQTT?",
            agents=self.AGENTS,
            models=self.MODELS,
        )
        t = session.tasks["researcher"]
        assert t.agent == "researcher"
        assert t.description == "What is MQTT?"
        assert t.model == "sonnet"
        assert t.next == "output"

    def test_unknown_agent_uses_defaults(self):
        session = create_session(
            "c1",
            "Do something",
            agent_id="unknown_agent",
            text="Do something",
            agents={},
            models=self.MODELS,
        )
        t = session.tasks["unknown_agent"]
        assert t.model == "haiku"  # first model


# ---------------------------------------------------------------------------
# E2E fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def mqtt_client():
    """Async MQTT v5 client for the test driver."""
    async with aiomqtt.Client(
        MQTT_HOST,
        MQTT_PORT,
        identifier=f"skitter-test-{uuid.uuid4().hex[:8]}",
        protocol=aiomqtt.ProtocolVersion.V5,
    ) as client:
        yield client


@pytest.fixture
def mock_worker_factory():
    """Return a factory that creates a mock spawn_worker function.

    The mock reads retained dispatch from the state/dispatch topic,
    then publishes a TaskStatusUpdate result.
    """

    def _factory(responses: dict[str, str], delay: float = 0.05):
        spawned: list[tuple[str, str, str]] = []

        async def _simulate_worker(
            agent: str, session_id: str, task_id: str, result_text: str
        ):
            """Simulate reading retained dispatch and publishing result."""
            await asyncio.sleep(delay)
            dispatch_t = topic_state_dispatch(task_id)

            async with aiomqtt.Client(
                MQTT_HOST,
                MQTT_PORT,
                identifier=f"mock-worker-{task_id[:8]}",
                protocol=aiomqtt.ProtocolVersion.V5,
            ) as c:
                # Subscribe to retained dispatch topic
                await c.subscribe(dispatch_t, qos=1)

                # Read retained dispatch
                response_topic = None
                correlation_data = None
                caller_reply_topic = ""
                caller_correlation = ""
                try:
                    async with asyncio.timeout(5.0):
                        async for msg in c.messages:
                            payload = msg.payload.decode() if msg.payload else ""
                            if not payload:
                                continue
                            data = json.loads(payload)
                            response_topic = data["reply_topic"]
                            correlation_data = data["correlation"]
                            task_data = data["task"]
                            caller_reply_topic = task_data.get("caller_reply_topic", "")
                            caller_correlation = task_data.get("caller_correlation", "")
                            break
                except TimeoutError:
                    return

                if not response_topic:
                    return

                # Clear retained dispatch
                await c.publish(dispatch_t, b"", qos=1, retain=True)

                status = TaskStatusUpdate(
                    task_id=task_id,
                    state="completed",
                    result=result_text,
                )

                # Send to caller directly (terminal tasks)
                if caller_reply_topic:
                    caller_props = make_properties(correlation_data=caller_correlation)
                    await c.publish(
                        caller_reply_topic,
                        status.to_json(),
                        qos=1,
                        properties=caller_props,
                    )

                # Bookkeeping to coordinator
                props = make_properties(correlation_data=correlation_data)
                await c.publish(
                    response_topic,
                    status.to_json(),
                    qos=1,
                    properties=props,
                )

        def mock_spawn(agent: str, session_id: str, task_id: str):
            spawned.append((agent, session_id, task_id))
            if agent in responses:
                asyncio.get_running_loop().create_task(
                    _simulate_worker(agent, session_id, task_id, responses[agent])
                )

        mock_spawn.spawned = spawned
        return mock_spawn

    return _factory


async def _drain_retained(client: aiomqtt.Client):
    """Subscribe to retained topics and clear them."""
    for pattern in [topic_state_session_wildcard()]:
        await client.subscribe(pattern, qos=1)
        try:
            async with asyncio.timeout(0.5):
                async for msg in client.messages:
                    if msg.retain and msg.payload:
                        await client.publish(str(msg.topic), b"", qos=1, retain=True)
        except TimeoutError:
            pass
        await client.unsubscribe(pattern)


@pytest_asyncio.fixture
async def clear_retained(mqtt_client):
    """Clear retained session messages between tests."""
    await _drain_retained(mqtt_client)
    yield
    await _drain_retained(mqtt_client)


async def wait_for_result(reply_topic: str, timeout: float = 10.0) -> str:
    """Subscribe to a reply topic and wait for a TaskStatusUpdate."""
    async with aiomqtt.Client(
        MQTT_HOST,
        MQTT_PORT,
        identifier=f"test-result-{uuid.uuid4().hex[:8]}",
        protocol=aiomqtt.ProtocolVersion.V5,
    ) as client:
        await client.subscribe(reply_topic, qos=1)
        try:
            async with asyncio.timeout(timeout):
                async for msg in client.messages:
                    payload = msg.payload.decode() if msg.payload else ""
                    if not payload:
                        continue
                    data = json.loads(payload)
                    if "state" in data and "task_id" in data:
                        status = TaskStatusUpdate.from_json(payload)
                        return status.result
                    if "error" in data:
                        return (
                            f"Error: {data['error'].get('message', str(data['error']))}"
                        )
        except TimeoutError:
            pytest.fail(f"Timed out waiting for result on {reply_topic}")


async def send_inbound(
    session_id: str,
    text: str,
    pipeline_id: str = "",
    pipeline_vars: dict | None = None,
    agent_id: str = "",
    reply_topic: str = "",
) -> None:
    """Publish an inbound request to the supervisor's A2A request topic."""
    msg = InboundMessage(
        text=text,
        sender="test-user",
        session_id=session_id,
        pipeline_id=pipeline_id,
        pipeline_vars=pipeline_vars or {},
        agent_id=agent_id,
    )
    async with aiomqtt.Client(
        MQTT_HOST,
        MQTT_PORT,
        identifier=f"test-inbound-{uuid.uuid4().hex[:8]}",
        protocol=aiomqtt.ProtocolVersion.V5,
    ) as client:
        props = make_properties(
            response_topic=reply_topic,
            correlation_data=session_id,
        )
        await client.publish(
            topic_request("coordinator"),
            msg.to_json(),
            qos=1,
            properties=props,
        )


# ---------------------------------------------------------------------------
# E2E tests
# ---------------------------------------------------------------------------


@needs_mqtt
@pytest.mark.asyncio
class TestE2E:
    """E2E tests that run the real supervisor loop with mocked workers."""

    async def _run_with_supervisor(
        self,
        monkeypatch,
        mock_spawn,
        session_id: str,
        pipeline_id: str = "",
        pipeline_vars: dict | None = None,
        agent_id: str = "",
        text: str = "Pipeline request",
        agents: dict | None = None,
        timeout: float = 10.0,
    ) -> str:
        """Start supervisor, send inbound request, wait for result."""
        monkeypatch.setattr("skitter.coordinator.spawn_worker", mock_spawn)

        async def _fast_recover(client):
            return {}

        monkeypatch.setattr("skitter.coordinator.recover_sessions", _fast_recover)

        async def _no_chain_results(client):
            return {}

        monkeypatch.setattr(
            "skitter.coordinator.recover_chain_results", _no_chain_results
        )
        monkeypatch.setattr("skitter.coordinator.load_agents", lambda: agents or {})

        # Set up a reply topic for the test
        mqtt_session = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", mqtt_session)

        # Start result listener BEFORE supervisor
        result_future = asyncio.ensure_future(wait_for_result(reply_t, timeout))
        await asyncio.sleep(0.1)

        # Start supervisor
        coord_task = asyncio.create_task(run())
        await asyncio.sleep(0.3)

        # Send inbound
        await send_inbound(
            session_id,
            text,
            pipeline_id=pipeline_id,
            pipeline_vars=pipeline_vars or {},
            agent_id=agent_id,
            reply_topic=reply_t,
        )

        try:
            result = await result_future
        finally:
            coord_task.cancel()
            try:
                await coord_task
            except asyncio.CancelledError:
                pass

        return result

    async def test_direct_agent_call(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Direct agent call creates single-task session and returns result."""
        session_id = f"test-agent-{uuid.uuid4().hex[:8]}"
        test_agents = {
            "researcher": AgentDef(
                id="researcher",
                name="Research Specialist",
                soul="Be thorough.",
                skills="Cite sources.",
                model="sonnet",
                max_turns=15,
            ),
        }
        monkeypatch.setattr("skitter.coordinator.load_pipelines", lambda: {})
        mock_spawn = mock_worker_factory(
            {"researcher": "MQTT v5 adds shared subscriptions and flow control."}
        )

        result = await self._run_with_supervisor(
            monkeypatch,
            mock_spawn,
            session_id,
            agent_id="researcher",
            text="What is MQTT v5?",
            agents=test_agents,
        )
        assert result == "MQTT v5 adds shared subscriptions and flow control."

    async def test_no_agent_or_pipeline_rejected(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Request without agent_id or pipeline_id is rejected."""
        session_id = f"test-nopipe-{uuid.uuid4().hex[:8]}"
        monkeypatch.setattr("skitter.coordinator.spawn_worker", mock_worker_factory({}))

        async def _fast_recover(client):
            return {}

        monkeypatch.setattr("skitter.coordinator.recover_sessions", _fast_recover)

        async def _no_chain_results(client):
            return {}

        monkeypatch.setattr(
            "skitter.coordinator.recover_chain_results", _no_chain_results
        )
        monkeypatch.setattr("skitter.coordinator.load_agents", lambda: {})
        monkeypatch.setattr("skitter.coordinator.load_pipelines", lambda: {})

        mqtt_session = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", mqtt_session)

        result_future = asyncio.ensure_future(wait_for_result(reply_t, 5.0))
        await asyncio.sleep(0.1)

        coord_task = asyncio.create_task(run())
        await asyncio.sleep(0.3)

        await send_inbound(session_id, "hello", reply_topic=reply_t)

        try:
            result = await result_future
            assert "agent_id or pipeline_id" in result
        finally:
            coord_task.cancel()
            try:
                await coord_task
            except asyncio.CancelledError:
                pass

    async def test_unknown_pipeline_rejected(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Request with unknown pipeline_id returns error."""
        session_id = f"test-unknown-{uuid.uuid4().hex[:8]}"
        monkeypatch.setattr("skitter.coordinator.spawn_worker", mock_worker_factory({}))

        async def _fast_recover(client):
            return {}

        monkeypatch.setattr("skitter.coordinator.recover_sessions", _fast_recover)

        async def _no_chain_results(client):
            return {}

        monkeypatch.setattr(
            "skitter.coordinator.recover_chain_results", _no_chain_results
        )
        monkeypatch.setattr("skitter.coordinator.load_agents", lambda: {})
        monkeypatch.setattr("skitter.coordinator.load_pipelines", lambda: {})

        mqtt_session = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", mqtt_session)

        result_future = asyncio.ensure_future(wait_for_result(reply_t, 5.0))
        await asyncio.sleep(0.1)

        coord_task = asyncio.create_task(run())
        await asyncio.sleep(0.3)

        await send_inbound(
            session_id, "go", pipeline_id="nonexistent", reply_topic=reply_t
        )

        try:
            result = await result_future
            assert "Unknown pipeline" in result
        finally:
            coord_task.cancel()
            try:
                await coord_task
            except asyncio.CancelledError:
                pass
