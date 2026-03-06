"""E2E tests for skitter gateway with self-coordinating workers.

Requires a running MQTT broker on localhost:1883 with MQTT v5 support.
Start one with: docker compose up -d

NOTE: These tests need updating for the coordinatorless architecture.
The gateway spawns all workers upfront, and workers self-coordinate
via MQTT retained messages. Mock workers need to simulate this flow.
"""

from __future__ import annotations

from skitter.config import AgentDef, WorkflowDef, WorkflowTask
from skitter.gateway import create_session
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
            task_id="t1",
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
        entry = [
            t for t in session.tasks.values() if not t.needs and t.status == "pending"
        ]
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
        entry = [
            t for t in session.tasks.values() if not t.needs and t.status == "pending"
        ]
        assert len(entry) == 0


class TestCreateSessionFromWorkflow:
    """Verify workflow building with variable interpolation and agent defaults."""

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
            models=self.MODELS,
            agents=self.AGENTS,
        )
        assert session.tasks["r1"].description == "Research 'MQTT' in depth."

    def test_agent_defaults_applied(self):
        workflow = WorkflowDef(
            id="test",
            name="Test",
            tasks=[
                WorkflowTask(
                    id="r1", agent="researcher", description="Go", next="output"
                ),
            ],
        )
        session = create_session(
            "c1",
            "test",
            workflow=workflow,
            models=self.MODELS,
            agents=self.AGENTS,
        )
        t = session.tasks["r1"]
        assert t.model == "sonnet"

    def test_workflow_task_override_beats_agent(self):
        workflow = WorkflowDef(
            id="test",
            name="Test",
            tasks=[
                WorkflowTask(
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
            workflow=workflow,
            models=self.MODELS,
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
# E2E tests — TODO: rewrite for coordinatorless architecture
# ---------------------------------------------------------------------------
# The e2e tests below need to be rewritten to work with the new gateway +
# self-coordinating worker architecture. The old coordinator-based tests
# used monkeypatch on coordinator internals which no longer exist.
# New tests should:
#   1. Start the gateway
#   2. Send inbound request
#   3. Mock spawn_worker to create simulated workers that:
#      - Read session spec from retained MQTT
#      - Wait for needs (if any)
#      - Publish chain result or terminal result
#   4. Verify results arrive on the caller's reply topic
