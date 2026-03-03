"""E2E tests for skitter coordinator with A2A-over-MQTT.

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
    build_context,
    build_job_from_agent,
    build_job_from_pipeline,
    extract_json,
    get_ready_tasks,
    run,
)
from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    make_properties,
    topic_reply,
    topic_request,
    topic_state_job_wildcard,
)
from skitter.types import (
    InboundMessage,
    JobSpec,
    JobTask,
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


class TestExtractJson:
    def test_bare_json(self):
        raw = '{"action":"respond","text":"hi"}'
        assert extract_json(raw) == {"action": "respond", "text": "hi"}

    def test_code_fenced(self):
        raw = '```json\n{"action":"respond","text":"hi"}\n```'
        assert extract_json(raw) == {"action": "respond", "text": "hi"}

    def test_trailing_text(self):
        raw = 'Here is your plan: {"action":"respond","text":"hi"} hope that helps!'
        assert extract_json(raw) == {"action": "respond", "text": "hi"}

    def test_prose_only(self):
        with pytest.raises(ValueError, match="No JSON"):
            extract_json("I think we should do research first.")

    def test_nested_braces(self):
        raw = '{"action":"respond","data":{"nested":true}}'
        result = extract_json(raw)
        assert result["action"] == "respond"
        assert result["data"]["nested"] is True

    def test_escaped_quotes(self):
        raw = r'{"action":"respond","text":"she said \"hello\""}'
        result = extract_json(raw)
        assert result["action"] == "respond"
        assert "hello" in result["text"]


class TestGetReadyTasks:
    def test_pending_with_deps_done(self):
        job = JobSpec(chat_id="c1", original_text="test")
        job.tasks["a"] = JobTask(
            logical_id="a",
            task_id="t1",
            agent="w",
            description="A",
            soul="",
            skills="",
            status="done",
        )
        job.tasks["b"] = JobTask(
            logical_id="b",
            task_id="t2",
            agent="w",
            description="B",
            soul="",
            skills="",
            depends_on=["a"],
            status="pending",
        )
        ready = get_ready_tasks(job)
        assert len(ready) == 1
        assert ready[0].logical_id == "b"

    def test_pending_with_deps_not_done(self):
        job = JobSpec(chat_id="c1", original_text="test")
        job.tasks["a"] = JobTask(
            logical_id="a",
            task_id="t1",
            agent="w",
            description="A",
            soul="",
            skills="",
            status="running",
        )
        job.tasks["b"] = JobTask(
            logical_id="b",
            task_id="t2",
            agent="w",
            description="B",
            soul="",
            skills="",
            depends_on=["a"],
            status="pending",
        )
        ready = get_ready_tasks(job)
        assert len(ready) == 0

    def test_no_deps_is_ready(self):
        job = JobSpec(chat_id="c1", original_text="test")
        job.tasks["a"] = JobTask(
            logical_id="a",
            task_id="t1",
            agent="w",
            description="A",
            soul="",
            skills="",
            status="pending",
        )
        ready = get_ready_tasks(job)
        assert len(ready) == 1


class TestBuildContext:
    def test_upstream_results_concatenated(self):
        job = JobSpec(chat_id="c1", original_text="test")
        job.tasks["a"] = JobTask(
            logical_id="a",
            task_id="t1",
            agent="w",
            description="A",
            soul="",
            skills="",
            status="done",
        )
        job.tasks["b"] = JobTask(
            logical_id="b",
            task_id="t2",
            agent="w",
            description="B",
            soul="",
            skills="",
            status="done",
        )
        job.tasks["c"] = JobTask(
            logical_id="c",
            task_id="t3",
            agent="w",
            description="C",
            soul="",
            skills="",
            depends_on=["a", "b"],
            status="pending",
        )
        job.results["a"] = "Result A"
        job.results["b"] = "Result B"
        ctx = build_context(job, job.tasks["c"])
        assert "Result A" in ctx
        assert "Result B" in ctx

    def test_no_deps_empty_context(self):
        job = JobSpec(chat_id="c1", original_text="test")
        job.tasks["a"] = JobTask(
            logical_id="a",
            task_id="t1",
            agent="w",
            description="A",
            soul="",
            skills="",
            status="pending",
        )
        assert build_context(job, job.tasks["a"]) == ""


class TestBuildJobFromPipeline:
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
                    logical_id="r1",
                    agent="researcher",
                    description="Research '{topic}' in depth.",
                ),
            ],
        )
        job = build_job_from_pipeline(
            "c1", "test", pipeline, {"topic": "MQTT"}, self.MODELS, self.AGENTS
        )
        assert job.tasks["r1"].description == "Research 'MQTT' in depth."

    def test_agent_defaults_applied(self):
        pipeline = PipelineDef(
            id="test",
            name="Test",
            tasks=[
                PipelineTask(logical_id="r1", agent="researcher", description="Go"),
            ],
        )
        job = build_job_from_pipeline(
            "c1", "test", pipeline, {}, self.MODELS, self.AGENTS
        )
        t = job.tasks["r1"]
        assert t.soul == "Be thorough."
        assert t.model == "sonnet"
        assert t.max_turns == 15

    def test_pipeline_task_override_beats_agent(self):
        pipeline = PipelineDef(
            id="test",
            name="Test",
            tasks=[
                PipelineTask(
                    logical_id="r1",
                    agent="researcher",
                    description="Quick check",
                    model="haiku",
                    max_turns=3,
                ),
            ],
        )
        job = build_job_from_pipeline(
            "c1", "test", pipeline, {}, self.MODELS, self.AGENTS
        )
        t = job.tasks["r1"]
        assert t.model == "haiku"
        assert t.max_turns == 3
        assert t.soul == "Be thorough."  # not overridden, falls to agent

    def test_unknown_vars_left_intact(self):
        """Variables not in the vars dict are left as {placeholder}."""
        pipeline = PipelineDef(
            id="test",
            name="Test",
            variables=["topic"],
            tasks=[
                PipelineTask(
                    logical_id="r1",
                    agent="researcher",
                    description="Research {topic}, output as {format}.",
                ),
            ],
        )
        job = build_job_from_pipeline(
            "c1", "test", pipeline, {"topic": "AI"}, self.MODELS
        )
        assert job.tasks["r1"].description == "Research AI, output as {format}."


class TestBuildJobFromAgent:
    """Verify direct agent job building."""

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
        job = build_job_from_agent(
            "c1", "What is MQTT?", "researcher", self.AGENTS, self.MODELS
        )
        assert len(job.tasks) == 1
        assert "researcher" in job.tasks
        assert "synthesize" not in job.tasks

    def test_agent_defaults_applied(self):
        job = build_job_from_agent(
            "c1", "What is MQTT?", "researcher", self.AGENTS, self.MODELS
        )
        t = job.tasks["researcher"]
        assert t.agent == "researcher"
        assert t.description == "What is MQTT?"
        assert t.soul == "Be thorough."
        assert t.skills == "Cite sources."
        assert t.model == "sonnet"
        assert t.max_turns == 15

    def test_unknown_agent_uses_defaults(self):
        job = build_job_from_agent(
            "c1", "Do something", "unknown_agent", {}, self.MODELS
        )
        t = job.tasks["unknown_agent"]
        assert t.model == "haiku"  # first model
        assert t.max_turns == 10


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

    The mock uses alive-triggered dispatch: publishes alive event on the
    worker liveness topic, then waits for the task dispatch on the reply
    topic, and publishes a TaskStatusUpdate result.
    """

    def _factory(responses: dict[str, str], delay: float = 0.05):
        spawned: list[tuple[str, str, str]] = []

        async def _simulate_worker(
            agent: str, chat_id: str, task_id: str, result_text: str
        ):
            """Simulate the alive → dispatch → result cycle."""
            await asyncio.sleep(delay)
            from skitter.mqtt import topic_event_worker, topic_request as tr

            async with aiomqtt.Client(
                MQTT_HOST,
                MQTT_PORT,
                identifier=f"mock-worker-{task_id[:8]}",
                protocol=aiomqtt.ProtocolVersion.V5,
            ) as c:
                # Subscribe to agent's request topic to receive the dispatched task
                await c.subscribe(tr(agent), qos=1)

                # Publish alive
                await c.publish(
                    topic_event_worker(task_id),
                    json.dumps({"status": "alive", "task_id": task_id}),
                    qos=1,
                )

                # Wait for task dispatch with v5 properties
                response_topic = None
                correlation_data = None
                try:
                    async with asyncio.timeout(5.0):
                        async for msg in c.messages:
                            from skitter.mqtt import (
                                get_correlation_data as gcd,
                                get_response_topic as grt,
                            )

                            response_topic = grt(msg)
                            correlation_data = gcd(msg)
                            break
                except TimeoutError:
                    return

                if not response_topic:
                    return

                # Publish result as TaskStatusUpdate with correlation data
                status = TaskStatusUpdate(
                    task_id=task_id,
                    state="completed",
                    result=result_text,
                )
                props = make_properties(correlation_data=correlation_data)
                await c.publish(
                    response_topic,
                    status.to_json(),
                    qos=1,
                    properties=props,
                )

        def mock_spawn(agent: str, chat_id: str, task_id: str):
            spawned.append((agent, chat_id, task_id))
            if agent in responses:
                asyncio.get_running_loop().create_task(
                    _simulate_worker(agent, chat_id, task_id, responses[agent])
                )

        mock_spawn.spawned = spawned
        return mock_spawn

    return _factory


async def _drain_retained(client: aiomqtt.Client):
    """Subscribe to retained topics and clear them."""
    for pattern in [topic_state_job_wildcard()]:
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
    """Clear retained job messages between tests."""
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
    chat_id: str,
    text: str,
    pipeline_id: str = "",
    pipeline_vars: dict | None = None,
    agent_id: str = "",
    reply_topic: str = "",
) -> None:
    """Publish an inbound request to the coordinator's A2A request topic."""
    msg = InboundMessage(
        text=text,
        sender="test-user",
        chat_id=chat_id,
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
            correlation_data=chat_id,
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
    """E2E tests that run the real coordinator loop with mocked workers."""

    async def _run_with_coordinator(
        self,
        monkeypatch,
        mock_spawn,
        chat_id: str,
        pipeline_id: str = "",
        pipeline_vars: dict | None = None,
        agent_id: str = "",
        text: str = "Pipeline request",
        agents: dict | None = None,
        timeout: float = 10.0,
    ) -> str:
        """Start coordinator, send inbound request, wait for result."""
        monkeypatch.setattr("skitter.coordinator.spawn_worker", mock_spawn)

        async def _fast_recover(client):
            return {}

        monkeypatch.setattr("skitter.coordinator.recover_jobs", _fast_recover)
        monkeypatch.setattr("skitter.coordinator.load_agents", lambda: agents or {})

        # Set up a reply topic for the test
        session_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", session_id)

        # Start result listener BEFORE coordinator
        result_future = asyncio.ensure_future(wait_for_result(reply_t, timeout))
        await asyncio.sleep(0.1)

        # Start coordinator
        coord_task = asyncio.create_task(run())
        await asyncio.sleep(0.3)

        # Send inbound
        await send_inbound(
            chat_id,
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

    async def test_pipeline_parallel(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Pipeline with 2 independent tasks + explicit synthesize task."""
        chat_id = f"test-parallel-{uuid.uuid4().hex[:8]}"
        pipeline = PipelineDef(
            id="test-par",
            name="Test Parallel",
            tasks=[
                PipelineTask(
                    logical_id="research",
                    agent="researcher",
                    description="Research topic",
                ),
                PipelineTask(
                    logical_id="analyze",
                    agent="analyst",
                    description="Analyze data",
                ),
                PipelineTask(
                    logical_id="synthesize",
                    agent="writer",
                    description="Combine results",
                    depends_on=["research", "analyze"],
                ),
            ],
        )
        monkeypatch.setattr(
            "skitter.coordinator.load_pipelines",
            lambda: {"test-par": pipeline},
        )
        mock_spawn = mock_worker_factory(
            {
                "researcher": "Research findings: X is true",
                "analyst": "Analysis: Y correlates with Z",
                "writer": "Combined: X is true and Y correlates with Z",
            }
        )

        result = await self._run_with_coordinator(
            monkeypatch, mock_spawn, chat_id, "test-par"
        )
        assert result == "Combined: X is true and Y correlates with Z"

    async def test_pipeline_sequential(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Pipeline with sequential dependency chain."""
        chat_id = f"test-seq-{uuid.uuid4().hex[:8]}"
        pipeline = PipelineDef(
            id="test-seq",
            name="Test Sequential",
            tasks=[
                PipelineTask(
                    logical_id="step_a",
                    agent="researcher",
                    description="Step A",
                ),
                PipelineTask(
                    logical_id="step_b",
                    agent="analyst",
                    description="Step B",
                    depends_on=["step_a"],
                ),
            ],
        )
        monkeypatch.setattr(
            "skitter.coordinator.load_pipelines",
            lambda: {"test-seq": pipeline},
        )
        mock_spawn = mock_worker_factory(
            {
                "researcher": "Step A result",
                "analyst": "Step B used A's result",
            }
        )

        result = await self._run_with_coordinator(
            monkeypatch, mock_spawn, chat_id, "test-seq"
        )
        assert "Step B used A's result" in result

    async def test_direct_agent_call(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Direct agent call creates single-task job and returns result."""
        chat_id = f"test-agent-{uuid.uuid4().hex[:8]}"
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

        result = await self._run_with_coordinator(
            monkeypatch,
            mock_spawn,
            chat_id,
            agent_id="researcher",
            text="What is MQTT v5?",
            agents=test_agents,
        )
        assert result == "MQTT v5 adds shared subscriptions and flow control."

    async def test_no_agent_or_pipeline_rejected(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Request without agent_id or pipeline_id is rejected."""
        chat_id = f"test-nopipe-{uuid.uuid4().hex[:8]}"
        monkeypatch.setattr("skitter.coordinator.spawn_worker", mock_worker_factory({}))

        async def _fast_recover(client):
            return {}

        monkeypatch.setattr("skitter.coordinator.recover_jobs", _fast_recover)
        monkeypatch.setattr("skitter.coordinator.load_agents", lambda: {})
        monkeypatch.setattr("skitter.coordinator.load_pipelines", lambda: {})

        session_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", session_id)

        result_future = asyncio.ensure_future(wait_for_result(reply_t, 5.0))
        await asyncio.sleep(0.1)

        coord_task = asyncio.create_task(run())
        await asyncio.sleep(0.3)

        await send_inbound(chat_id, "hello", reply_topic=reply_t)

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
        chat_id = f"test-unknown-{uuid.uuid4().hex[:8]}"
        monkeypatch.setattr("skitter.coordinator.spawn_worker", mock_worker_factory({}))

        async def _fast_recover(client):
            return {}

        monkeypatch.setattr("skitter.coordinator.recover_jobs", _fast_recover)
        monkeypatch.setattr("skitter.coordinator.load_agents", lambda: {})
        monkeypatch.setattr("skitter.coordinator.load_pipelines", lambda: {})

        session_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", session_id)

        result_future = asyncio.ensure_future(wait_for_result(reply_t, 5.0))
        await asyncio.sleep(0.1)

        coord_task = asyncio.create_task(run())
        await asyncio.sleep(0.3)

        await send_inbound(
            chat_id, "go", pipeline_id="nonexistent", reply_topic=reply_t
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

    async def test_worker_crash_lwt(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Worker LWT triggers respawn."""
        chat_id = f"test-lwt-{uuid.uuid4().hex[:8]}"
        pipeline = PipelineDef(
            id="test-lwt",
            name="Test LWT",
            tasks=[
                PipelineTask(
                    logical_id="work",
                    agent="researcher",
                    description="Do work",
                ),
            ],
        )
        monkeypatch.setattr(
            "skitter.coordinator.load_pipelines",
            lambda: {"test-lwt": pipeline},
        )

        spawn_count = {"researcher": 0}
        spawned_tasks: list[tuple[str, str, str]] = []

        async def _simulate_worker(
            agent: str, chat_id_: str, task_id_: str, result_text: str
        ):
            await asyncio.sleep(0.05)
            from skitter.mqtt import topic_event_worker, topic_request as tr

            async with aiomqtt.Client(
                MQTT_HOST,
                MQTT_PORT,
                identifier=f"mock-{task_id_[:8]}",
                protocol=aiomqtt.ProtocolVersion.V5,
            ) as c:
                await c.subscribe(tr(agent), qos=1)
                await c.publish(
                    topic_event_worker(task_id_),
                    json.dumps({"status": "alive", "task_id": task_id_}),
                    qos=1,
                )
                response_topic = None
                correlation_data = None
                try:
                    async with asyncio.timeout(5.0):
                        async for msg in c.messages:
                            from skitter.mqtt import (
                                get_correlation_data as gcd,
                                get_response_topic as grt,
                            )

                            response_topic = grt(msg)
                            correlation_data = gcd(msg)
                            break
                except TimeoutError:
                    return
                if not response_topic:
                    return
                status = TaskStatusUpdate(
                    task_id=task_id_, state="completed", result=result_text
                )
                props = make_properties(correlation_data=correlation_data)
                await c.publish(
                    response_topic, status.to_json(), qos=1, properties=props
                )

        async def _send_lwt(task_id_: str):
            await asyncio.sleep(0.15)
            from skitter.mqtt import topic_event_worker

            async with aiomqtt.Client(
                MQTT_HOST,
                MQTT_PORT,
                identifier=f"lwt-{task_id_[:8]}",
                protocol=aiomqtt.ProtocolVersion.V5,
            ) as c:
                await c.publish(
                    topic_event_worker(task_id_),
                    json.dumps({"status": "dead", "task_id": task_id_}),
                    qos=1,
                )

        def mock_spawn(agent: str, chat_id_: str, task_id_: str):
            spawned_tasks.append((agent, chat_id_, task_id_))
            if agent == "researcher":
                spawn_count["researcher"] += 1
                if spawn_count["researcher"] == 1:
                    # First spawn: publish alive (to trigger dispatch) then send LWT
                    async def alive_then_lwt():
                        await asyncio.sleep(0.05)
                        from skitter.mqtt import (
                            topic_event_worker,
                        )

                        async with aiomqtt.Client(
                            MQTT_HOST,
                            MQTT_PORT,
                            identifier=f"mock-alive-{task_id_[:8]}",
                            protocol=aiomqtt.ProtocolVersion.V5,
                        ) as c:
                            # Publish alive to trigger dispatch
                            await c.publish(
                                topic_event_worker(task_id_),
                                json.dumps({"status": "alive", "task_id": task_id_}),
                                qos=1,
                            )
                            # Then simulate crash
                            await asyncio.sleep(0.15)
                            await c.publish(
                                topic_event_worker(task_id_),
                                json.dumps({"status": "dead", "task_id": task_id_}),
                                qos=1,
                            )

                    asyncio.get_running_loop().create_task(alive_then_lwt())
                else:
                    asyncio.get_running_loop().create_task(
                        _simulate_worker(
                            agent,
                            chat_id_,
                            task_id_,
                            "Research done after respawn",
                        )
                    )

        result = await self._run_with_coordinator(
            monkeypatch, mock_spawn, chat_id, "test-lwt"
        )
        assert result == "Research done after respawn"
        researcher_spawns = [s for s in spawned_tasks if s[0] == "researcher"]
        assert len(researcher_spawns) == 2
