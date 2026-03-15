"""E2E tests for skitter supervisor.

Unit tests (no broker) test the new DB-backed supervisor.
E2E tests require a running MQTT broker on localhost:1883 with v5 support.
"""

from __future__ import annotations

from skitter.config import WorkflowDef, WorkflowTask, safe_format
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


class TestWorkflowToGraph:
    """Verify workflow→graph conversion preserves model/workspace."""

    def test_variable_interpolation(self):
        wf = WorkflowDef(
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
        from skitter.db import SqliteDB
        from skitter.supervisor import Supervisor

        db = SqliteDB(":memory:")
        sup = Supervisor(db)
        graph = sup._workflow_to_graph(wf, {"topic": "MQTT"})
        assert graph["tasks"][0]["description"] == "Research 'MQTT' in depth."
        db.close()

    def test_model_preserved(self):
        wf = WorkflowDef(
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
        from skitter.db import SqliteDB
        from skitter.supervisor import Supervisor

        db = SqliteDB(":memory:")
        sup = Supervisor(db)
        graph = sup._workflow_to_graph(wf)
        assert graph["tasks"][0]["model"] == "haiku"
        db.close()

    def test_workspace_preserved(self):
        wf = WorkflowDef(
            id="test",
            name="Test",
            workspace="my-workspace",
            tasks=[
                WorkflowTask(
                    id="r1",
                    agent="researcher",
                    description="Do work",
                    next="output",
                ),
            ],
        )
        from skitter.db import SqliteDB
        from skitter.supervisor import Supervisor

        db = SqliteDB(":memory:")
        sup = Supervisor(db)
        graph = sup._workflow_to_graph(wf)
        assert graph["workspace"] == "my-workspace"
        db.close()

    def test_unknown_vars_left_intact(self):
        desc = safe_format("Research {topic}, output as {format}.", {"topic": "AI"})
        assert desc == "Research AI, output as {format}."
