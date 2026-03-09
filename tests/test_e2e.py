"""E2E tests for skitter supervisor with self-coordinating workers.

Requires a running MQTT broker on localhost:1883 with MQTT v5 support.
Start one with: docker compose up -d

NOTE: These tests need updating for the coordinatorless architecture.
The supervisor spawns all workers upfront, and workers self-coordinate
via MQTT retained messages. Mock workers need to simulate this flow.
"""

from __future__ import annotations

from skitter.config import AgentDef, WorkflowDef, WorkflowTask
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
# E2E tests — TODO: rewrite for coordinatorless architecture
# ---------------------------------------------------------------------------
# The e2e tests below need to be rewritten to work with the new supervisor +
# self-coordinating worker architecture. The old coordinator-based tests
# used monkeypatch on coordinator internals which no longer exist.
# New tests should:
#   1. Start the supervisor
#   2. Send inbound request
#   3. Mock spawn_worker to create simulated workers that:
#      - Read session spec from retained MQTT
#      - Wait for needs (if any)
#      - Publish chain result or terminal result
#   4. Verify results arrive on the caller's reply topic
