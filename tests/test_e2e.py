"""E2E tests for skitter coordinator with mocked workers.

Requires a running MQTT broker on localhost:1883.
Start one with: docker compose up -d
"""

from __future__ import annotations

import asyncio
import json
import uuid

import aiomqtt
import pytest
import pytest_asyncio

from skitter.coordinator import (
    build_context,
    build_job_from_plan,
    extract_json,
    get_ready_tasks,
    run,
)
from skitter.mqtt import MQTT_HOST, MQTT_PORT
from skitter.types import (
    InboundMessage,
    JobSpec,
    JobTask,
    OutboundMessage,
    TaskResultMessage,
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


class TestBuildJobFromPlan:
    def test_synthesize_added(self):
        models = {"haiku": "fast", "sonnet": "balanced"}
        plan = {
            "tasks": [
                {
                    "logical_id": "research",
                    "agent": "researcher",
                    "model": "haiku",
                    "description": "Do research",
                    "depends_on": [],
                },
            ]
        }
        job = build_job_from_plan("c1", "original", plan, models)
        assert "synthesize" in job.tasks
        assert "research" in job.tasks["synthesize"].depends_on

    def test_unknown_model_falls_back(self):
        models = {"haiku": "fast", "sonnet": "balanced"}
        plan = {
            "tasks": [
                {
                    "logical_id": "t1",
                    "agent": "worker",
                    "model": "gpt-5",
                    "description": "Do stuff",
                    "depends_on": [],
                },
            ]
        }
        job = build_job_from_plan("c1", "original", plan, models)
        assert job.tasks["t1"].model == "haiku"  # first key = default


# ---------------------------------------------------------------------------
# E2E fixtures
# ---------------------------------------------------------------------------


@pytest_asyncio.fixture
async def mqtt_client():
    """Async MQTT client for the test driver."""
    async with aiomqtt.Client(
        MQTT_HOST, MQTT_PORT, identifier=f"skitter-test-{uuid.uuid4().hex[:8]}"
    ) as client:
        yield client


@pytest.fixture
def mock_worker_factory():
    """Return a factory that creates a mock spawn_worker function.

    The mock publishes canned results to MQTT when the coordinator calls spawn_worker.
    """

    def _factory(responses: dict[str, str], delay: float = 0.05):
        """
        responses: mapping of agent name (or logical_id) -> result text.
        When spawn_worker is called for a matching agent, publishes the result.
        """
        spawned: list[tuple[str, str, str]] = []

        async def _publish_result(chat_id: str, task_id: str, result_text: str):
            await asyncio.sleep(delay)
            result_msg = TaskResultMessage(
                task_id=task_id, chat_id=chat_id, result=result_text
            )
            async with aiomqtt.Client(
                MQTT_HOST, MQTT_PORT, identifier=f"mock-worker-{task_id[:8]}"
            ) as c:
                await c.publish(
                    f"skitter/results/{chat_id}/{task_id}",
                    result_msg.to_json(),
                    qos=1,
                )

        def mock_spawn(agent: str, chat_id: str, task_id: str):
            spawned.append((agent, chat_id, task_id))
            # Look up response by agent name
            if agent in responses:
                asyncio.get_running_loop().create_task(
                    _publish_result(chat_id, task_id, responses[agent])
                )

        mock_spawn.spawned = spawned
        return mock_spawn

    return _factory


async def _drain_retained(client: aiomqtt.Client):
    """Subscribe to retained topics and clear them."""
    for pattern in ["skitter/jobs/+", "skitter/tasks/+/+/+"]:
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


async def wait_for_outbound(chat_id: str, timeout: float = 10.0) -> str:
    """Subscribe to outbound topic and wait for a message."""
    async with aiomqtt.Client(
        MQTT_HOST, MQTT_PORT, identifier=f"test-outbound-{uuid.uuid4().hex[:8]}"
    ) as client:
        await client.subscribe(f"skitter/outbound/{chat_id}", qos=1)
        try:
            async with asyncio.timeout(timeout):
                async for msg in client.messages:
                    payload = msg.payload.decode() if msg.payload else ""
                    if not payload:
                        continue
                    out = OutboundMessage.from_json(payload)
                    return out.text
        except TimeoutError:
            pytest.fail(f"Timed out waiting for outbound message on chat {chat_id}")


async def send_inbound(chat_id: str, text: str):
    """Publish an inbound user message."""
    msg = InboundMessage(text=text, sender="test-user", chat_id=chat_id)
    async with aiomqtt.Client(
        MQTT_HOST, MQTT_PORT, identifier=f"test-inbound-{uuid.uuid4().hex[:8]}"
    ) as client:
        await client.publish(f"skitter/inbound/{chat_id}", msg.to_json(), qos=1)


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
        inbound_text: str,
        timeout: float = 10.0,
    ) -> str:
        """Start coordinator, send inbound, wait for outbound, return result text."""
        monkeypatch.setattr("skitter.coordinator.spawn_worker", mock_spawn)

        # Patch recover_jobs to return immediately (no stale jobs in test)
        async def _fast_recover(client):
            return {}

        monkeypatch.setattr("skitter.coordinator.recover_jobs", _fast_recover)

        # Start outbound listener BEFORE coordinator so we don't miss messages
        outbound_future = asyncio.ensure_future(wait_for_outbound(chat_id, timeout))

        # Small delay to let the subscriber connect
        await asyncio.sleep(0.1)

        # Start coordinator as background task
        coord_task = asyncio.create_task(run())

        # Let coordinator subscribe to topics
        await asyncio.sleep(0.3)

        # Send inbound message
        await send_inbound(chat_id, inbound_text)

        try:
            result = await outbound_future
        finally:
            coord_task.cancel()
            try:
                await coord_task
            except asyncio.CancelledError:
                pass

        return result

    async def test_direct_response(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Planner returns direct respond action — coordinator publishes outbound."""
        chat_id = f"test-direct-{uuid.uuid4().hex[:8]}"
        planner_response = json.dumps(
            {"action": "respond", "text": "Hello! How can I help?"}
        )
        mock_spawn = mock_worker_factory({"planner": planner_response})

        result = await self._run_with_coordinator(
            monkeypatch, mock_spawn, chat_id, "Hi there"
        )
        assert result == "Hello! How can I help?"

    async def test_delegate_parallel(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Planner delegates 2 independent tasks, then synthesize combines them."""
        chat_id = f"test-parallel-{uuid.uuid4().hex[:8]}"
        planner_response = json.dumps(
            {
                "action": "delegate",
                "tasks": [
                    {
                        "logical_id": "research",
                        "agent": "researcher",
                        "model": "haiku",
                        "description": "Research topic",
                        "depends_on": [],
                    },
                    {
                        "logical_id": "analyze",
                        "agent": "analyst",
                        "model": "haiku",
                        "description": "Analyze data",
                        "depends_on": [],
                    },
                ],
            }
        )
        mock_spawn = mock_worker_factory(
            {
                "planner": planner_response,
                "researcher": "Research findings: X is true",
                "analyst": "Analysis: Y correlates with Z",
                "writer": "Combined: X is true and Y correlates with Z",
            }
        )

        result = await self._run_with_coordinator(
            monkeypatch, mock_spawn, chat_id, "Research and analyze topic X"
        )
        assert result == "Combined: X is true and Y correlates with Z"

    async def test_delegate_sequential(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Planner delegates tasks where B depends on A, then synthesize."""
        chat_id = f"test-seq-{uuid.uuid4().hex[:8]}"
        planner_response = json.dumps(
            {
                "action": "delegate",
                "tasks": [
                    {
                        "logical_id": "step_a",
                        "agent": "researcher",
                        "model": "haiku",
                        "description": "Step A",
                        "depends_on": [],
                    },
                    {
                        "logical_id": "step_b",
                        "agent": "analyst",
                        "model": "haiku",
                        "description": "Step B",
                        "depends_on": ["step_a"],
                    },
                ],
            }
        )
        mock_spawn = mock_worker_factory(
            {
                "planner": planner_response,
                "researcher": "Step A result",
                "analyst": "Step B used A's result",
                "writer": "Final: A then B",
            }
        )

        result = await self._run_with_coordinator(
            monkeypatch, mock_spawn, chat_id, "Do A then B"
        )
        assert result == "Final: A then B"

    async def test_planner_returns_prose(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Planner returns plain text — coordinator publishes planning error."""
        chat_id = f"test-prose-{uuid.uuid4().hex[:8]}"
        mock_spawn = mock_worker_factory(
            {
                "planner": "I think we should research this topic first and then analyze it."
            }
        )

        result = await self._run_with_coordinator(
            monkeypatch, mock_spawn, chat_id, "Do something complex"
        )
        assert "Planning error" in result

    async def test_planner_returns_json_in_code_fence(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Planner wraps JSON in code fences — coordinator handles via extract_json."""
        chat_id = f"test-fence-{uuid.uuid4().hex[:8]}"
        planner_response = '```json\n{"action":"respond","text":"Fenced response"}\n```'
        mock_spawn = mock_worker_factory({"planner": planner_response})

        result = await self._run_with_coordinator(
            monkeypatch, mock_spawn, chat_id, "Hello"
        )
        assert result == "Fenced response"

    async def test_planner_unknown_action(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Planner returns unknown action — coordinator publishes error."""
        chat_id = f"test-unknown-{uuid.uuid4().hex[:8]}"
        planner_response = json.dumps({"action": "frobnicate", "data": "stuff"})
        mock_spawn = mock_worker_factory({"planner": planner_response})

        result = await self._run_with_coordinator(
            monkeypatch, mock_spawn, chat_id, "Do something weird"
        )
        assert "Unknown planner action" in result

    async def test_planner_delegate_empty_tasks(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Planner delegates with empty tasks list — coordinator publishes error."""
        chat_id = f"test-empty-{uuid.uuid4().hex[:8]}"
        planner_response = json.dumps({"action": "delegate", "tasks": []})
        mock_spawn = mock_worker_factory({"planner": planner_response})

        result = await self._run_with_coordinator(
            monkeypatch, mock_spawn, chat_id, "Do nothing"
        )
        assert "No tasks generated" in result

    async def test_worker_crash_lwt(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """After delegation, a worker LWT triggers respawn."""
        chat_id = f"test-lwt-{uuid.uuid4().hex[:8]}"
        planner_response = json.dumps(
            {
                "action": "delegate",
                "tasks": [
                    {
                        "logical_id": "work",
                        "agent": "researcher",
                        "model": "haiku",
                        "description": "Do work",
                        "depends_on": [],
                    },
                ],
            }
        )

        # First spawn of researcher: don't auto-respond (simulate crash)
        # Second spawn: respond normally
        spawn_count = {"researcher": 0}
        real_responses = {
            "planner": planner_response,
            "writer": "Synthesized result",
        }
        spawned_tasks: list[tuple[str, str, str]] = []

        async def _delayed_result(chat_id_: str, task_id_: str, text: str):
            await asyncio.sleep(0.05)
            result_msg = TaskResultMessage(
                task_id=task_id_, chat_id=chat_id_, result=text
            )
            async with aiomqtt.Client(
                MQTT_HOST, MQTT_PORT, identifier=f"mock-{task_id_[:8]}"
            ) as c:
                await c.publish(
                    f"skitter/results/{chat_id_}/{task_id_}",
                    result_msg.to_json(),
                    qos=1,
                )

        async def _send_lwt(chat_id_: str, task_id_: str):
            await asyncio.sleep(0.15)
            lwt = json.dumps({"status": "dead", "task_id": task_id_})
            async with aiomqtt.Client(
                MQTT_HOST, MQTT_PORT, identifier=f"lwt-{task_id_[:8]}"
            ) as c:
                await c.publish(
                    f"skitter/workers/{chat_id_}/{task_id_}/status", lwt, qos=1
                )

        def mock_spawn(agent: str, chat_id_: str, task_id_: str):
            spawned_tasks.append((agent, chat_id_, task_id_))
            if agent in real_responses:
                asyncio.get_running_loop().create_task(
                    _delayed_result(chat_id_, task_id_, real_responses[agent])
                )
            elif agent == "researcher":
                spawn_count["researcher"] += 1
                if spawn_count["researcher"] == 1:
                    # First spawn: simulate crash via LWT
                    asyncio.get_running_loop().create_task(
                        _send_lwt(chat_id_, task_id_)
                    )
                else:
                    # Second spawn (after respawn): succeed
                    asyncio.get_running_loop().create_task(
                        _delayed_result(
                            chat_id_, task_id_, "Research done after respawn"
                        )
                    )

        result = await self._run_with_coordinator(
            monkeypatch, mock_spawn, chat_id, "Research something"
        )
        assert result == "Synthesized result"
        # Researcher should have been spawned twice (initial + respawn)
        researcher_spawns = [s for s in spawned_tasks if s[0] == "researcher"]
        assert len(researcher_spawns) == 2

    async def test_unknown_model_falls_back(
        self, monkeypatch, mock_worker_factory, clear_retained
    ):
        """Planner assigns unknown model — build_job_from_plan uses default."""
        chat_id = f"test-model-{uuid.uuid4().hex[:8]}"
        planner_response = json.dumps(
            {
                "action": "delegate",
                "tasks": [
                    {
                        "logical_id": "work",
                        "agent": "researcher",
                        "model": "gpt-5",
                        "description": "Do work",
                        "depends_on": [],
                    },
                ],
            }
        )
        mock_spawn = mock_worker_factory(
            {
                "planner": planner_response,
                "researcher": "Research result",
                "writer": "Final answer",
            }
        )

        result = await self._run_with_coordinator(
            monkeypatch, mock_spawn, chat_id, "Research something"
        )
        assert result == "Final answer"
