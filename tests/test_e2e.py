"""E2E tests for skitter supervisor with self-coordinating workers.

Requires a running MQTT broker on localhost:1883 with MQTT v5 support.
Start one with: docker compose up -d

NOTE: These tests need updating for the coordinatorless architecture.
The supervisor spawns all workers upfront, and workers self-coordinate
via MQTT retained messages. Mock workers need to simulate this flow.
"""

from __future__ import annotations

import asyncio
import json

import aiomqtt
import pytest

from skitter.config import AgentDef, WorkflowDef, WorkflowTask
from skitter.mqtt import (
    mqtt_client_kwargs,
    topic_result,
    topic_session,
    topic_status,
)
from skitter.supervisor import create_session
from skitter.types import (
    Session,
    SessionTask,
)


# ---------------------------------------------------------------------------
# Unit tests — pure functions, no MQTT
# ---------------------------------------------------------------------------


class TestGetEntryTasks:
    def test_pending_no_needs_is_entry(self):
        session = Session(session_id="c1", label="test")
        session.tasks["a"] = SessionTask(
            id="a",
            agent="w",
            description="A",
            status="pending",
        )
        entry = [
            t for t in session.tasks.values() if not t.needs and t.status == "pending"
        ]
        assert len(entry) == 1

    def test_pending_with_needs_not_entry(self):
        session = Session(session_id="c1", label="test")
        session.tasks["a"] = SessionTask(
            id="a",
            agent="w",
            description="A",
            status="done",
        )
        session.tasks["b"] = SessionTask(
            id="b",
            agent="w",
            description="B",
            needs=["a"],
            status="pending",
        )
        entry = [
            t for t in session.tasks.values() if not t.needs and t.status == "pending"
        ]
        assert len(entry) == 0

    def test_running_not_entry(self):
        session = Session(session_id="c1", label="test")
        session.tasks["a"] = SessionTask(
            id="a",
            agent="w",
            description="A",
            status="running",
        )
        entry = [
            t for t in session.tasks.values() if not t.needs and t.status == "pending"
        ]
        assert len(entry) == 0


class TestCreateSessionFromWorkflow:
    """Verify workflow building with variable interpolation and agent defaults."""

    AGENTS = {
        "researcher": AgentDef(
            id="researcher",
            name="Research Specialist",
            runtime="claude",
        ),
    }

    def test_variable_interpolation(self):
        workflow = WorkflowDef(
            id="test",
            name="Test",
            variables=["topic"],
            tasks=[
                WorkflowTask(
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
            workflow=workflow,
            variables={"topic": "MQTT"},
            agents=self.AGENTS,
        )
        assert session.tasks["r1"].description == "Research 'MQTT' in depth."

    def test_workflow_task_model_override(self):
        workflow = WorkflowDef(
            id="test",
            name="Test",
            tasks=[
                WorkflowTask(
                    id="r1",
                    agent="researcher",
                    description="Quick check",
                    model="haiku",
                    next="output",
                ),
            ],
        )
        session = create_session(
            "c1",
            "test",
            workflow=workflow,
            agents=self.AGENTS,
        )
        t = session.tasks["r1"]
        assert t.model == "haiku"

    def test_unknown_vars_left_intact(self):
        """Variables not in the vars dict are left as {placeholder}."""
        workflow = WorkflowDef(
            id="test",
            name="Test",
            variables=["topic"],
            tasks=[
                WorkflowTask(
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
            workflow=workflow,
            variables={"topic": "AI"},
        )
        assert session.tasks["r1"].description == "Research AI, output as {format}."


class TestCreateSessionFromAgent:
    """Verify direct agent session building."""

    AGENTS = {
        "researcher": AgentDef(
            id="researcher",
            name="Research Specialist",
            runtime="claude",
        ),
    }

    def test_single_task_created(self):
        session = create_session(
            "c1",
            "What is MQTT?",
            agent_id="researcher",
            text="What is MQTT?",
            agents=self.AGENTS,
        )
        assert len(session.tasks) == 1
        assert "researcher" in session.tasks

    def test_agent_session_fields(self):
        session = create_session(
            "c1",
            "What is MQTT?",
            agent_id="researcher",
            text="What is MQTT?",
            agents=self.AGENTS,
        )
        t = session.tasks["researcher"]
        assert t.agent == "researcher"
        assert t.description == "What is MQTT?"
        assert t.next == "output"


# ---------------------------------------------------------------------------
# E2E tests — require running MQTT broker on localhost:1883
# ---------------------------------------------------------------------------


async def _publish_retained(topic: str, payload: str) -> None:
    async with aiomqtt.Client(**mqtt_client_kwargs(identifier="test-setup")) as c:
        await c.publish(topic, payload, qos=1, retain=True)


async def _clear_retained(topic: str) -> None:
    async with aiomqtt.Client(**mqtt_client_kwargs(identifier="test-cleanup")) as c:
        await c.publish(topic, "", qos=1, retain=True)


class TestProbeTaskState:
    """E2E test for _probe_task_state — reads retained messages from broker."""

    @pytest.mark.asyncio
    async def test_finds_session_and_result(self):
        from skitter.supervisor import _probe_task_state

        session_topic = topic_session("probe-s1")
        result_topic = topic_result("probe-wf", "t1", "probe-s1")

        session = Session(session_id="probe-s1", workflow_id="probe-wf")
        session.tasks["t1"] = SessionTask(
            id="t1", agent="r", description="d", next="output"
        )

        try:
            await _publish_retained(session_topic, session.to_json())
            await _publish_retained(
                result_topic,
                json.dumps({"task": "t1", "session_id": "probe-s1", "result": "done"}),
            )
            await asyncio.sleep(0.1)

            found, has_result = await _probe_task_state("probe-s1", "t1")
            assert found is not None
            assert found.session_id == "probe-s1"
            assert has_result is True
        finally:
            await _clear_retained(session_topic)
            await _clear_retained(result_topic)

    @pytest.mark.asyncio
    async def test_finds_session_no_result(self):
        from skitter.supervisor import _probe_task_state

        session_topic = topic_session("probe-s1")
        result_topic = topic_result("probe-wf", "t1", "probe-s1")

        session = Session(session_id="probe-s1", workflow_id="probe-wf")
        session.tasks["t1"] = SessionTask(
            id="t1", agent="r", description="d", next="output"
        )

        try:
            await _publish_retained(session_topic, session.to_json())
            await asyncio.sleep(0.1)

            found, has_result = await _probe_task_state("probe-s1", "t1", timeout=1.0)
            assert found is not None
            assert has_result is False
        finally:
            await _clear_retained(session_topic)
            await _clear_retained(result_topic)

    @pytest.mark.asyncio
    async def test_no_session_no_result(self):
        from skitter.supervisor import _probe_task_state

        found, has_result = await _probe_task_state(
            "probe-nonexistent", "t1", timeout=1.0
        )
        assert found is None
        assert has_result is False


class TestFailTaskE2E:
    """E2E test for _fail_task — verifies published messages on broker."""

    @pytest.mark.asyncio
    async def test_fail_publishes_retained_result_and_status(self):
        from skitter.supervisor import _fail_task

        session = Session(
            session_id="fail-s1",
            workflow_id="fail-wf",
            caller_reply_topic="$a2a/v1/reply/skitter/default/test-caller",
            caller_correlation="corr-1",
        )
        session.tasks["a"] = SessionTask(id="a", agent="r", description="d", next="b")
        session.tasks["b"] = SessionTask(
            id="b", agent="w", description="d", next="output"
        )
        result_topic = topic_result("fail-wf", "a", "fail-s1")
        status_topic = topic_status("fail-wf", "a", "fail-s1")

        try:
            async with aiomqtt.Client(
                **mqtt_client_kwargs(identifier="test-fail-task"),
            ) as client:
                await _fail_task(client, session, "a", "worker crashed")

            # Read back the retained messages
            result = status = None
            async with aiomqtt.Client(
                **mqtt_client_kwargs(identifier="test-fail-reader"),
            ) as reader:
                await reader.subscribe(result_topic, qos=1)
                await reader.subscribe(status_topic, qos=1)
                try:
                    async with asyncio.timeout(2.0):
                        async for msg in reader.messages:
                            payload = json.loads(msg.payload.decode())
                            if str(msg.topic) == result_topic:
                                result = payload
                            elif str(msg.topic) == status_topic:
                                status = payload
                            if result and status:
                                break
                except TimeoutError:
                    pass

            assert result is not None, "Failed result not found on broker"
            assert result["result"] == "worker crashed"
            assert result["failed"] is True
            assert status is not None, "Failed status not found on broker"
            assert status["status"] == "failed"
            assert "last_active" in status
        finally:
            await _clear_retained(result_topic)
            await _clear_retained(status_topic)
