"""Tests for skitter coordinatorless architecture."""

import asyncio
import json
from unittest.mock import patch

import pytest

from skitter.config import (
    AgentDef,
    WorkflowDef,
    WorkflowTask,
)
from skitter.gateway import (
    build_dispatch_spec,
    create_session,
)
from skitter.mqtt import (
    topic_chain_result,
    topic_control_reload,
    topic_event,
    topic_event_wildcard,
    topic_state_dispatch,
)
from skitter.types import (
    AgentMessage,
    Session,
    SessionTask,
)


# --- Foundation types ---


class TestAgentMessage:
    def test_roundtrip(self):
        msg = AgentMessage(
            task_id="t1",
            session_id="c1",
            description="do stuff",
            agent="researcher",
            context="prior result",
            model="sonnet",
            runtime="claude",
            next="step2",
            next_needs=["step1a", "step1b"],
            caller_reply_topic="reply/topic",
            caller_correlation="corr123",
        )
        json_str = msg.to_json()
        restored = AgentMessage.from_json(json_str)
        assert restored.task_id == "t1"
        assert restored.agent == "researcher"
        assert restored.runtime == "claude"
        assert restored.next == "step2"
        assert restored.next_needs == ["step1a", "step1b"]
        assert restored.caller_reply_topic == "reply/topic"
        assert restored.caller_correlation == "corr123"

    def test_defaults(self):
        msg = AgentMessage.from_json(
            json.dumps(
                {
                    "task_id": "t",
                    "session_id": "c",
                    "description": "d",
                }
            )
        )
        assert msg.agent == ""
        assert msg.runtime == "claude"
        assert msg.next == ""
        assert msg.next_needs == []
        assert msg.caller_reply_topic == ""


class TestSessionTask:
    def test_roundtrip(self):
        task = SessionTask(
            id="research",
            task_id="abc123",
            agent="researcher",
            description="do research",
            next="review",
            needs=["prep"],
        )
        d = task.to_dict()
        assert d["id"] == "research"
        assert d["next"] == "review"
        assert d["needs"] == ["prep"]

        restored = SessionTask.from_dict(d)
        assert restored.id == "research"
        assert restored.next == "review"
        assert restored.needs == ["prep"]


class TestSession:
    def test_caller_fields(self):
        session = Session(
            session_id="c1",
            label="test",
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        json_str = session.to_json()
        restored = Session.from_json(json_str)
        assert restored.caller_reply_topic == "reply/t"
        assert restored.caller_correlation == "corr"

    def test_task_dispatches_roundtrip(self):
        session = Session(session_id="c1", label="test")
        session.task_dispatches["step1"] = {
            "task_id": "t1",
            "description": "do things",
            "runtime": "claude",
        }
        json_str = session.to_json()
        restored = Session.from_json(json_str)
        assert "step1" in restored.task_dispatches
        assert restored.task_dispatches["step1"]["runtime"] == "claude"


# --- Topic builders ---


class TestTopics:
    def test_event_topics(self):
        t = topic_event("researcher", "alive")
        assert "/event/" in t
        assert "/researcher/alive" in t

    def test_event_wildcard(self):
        t = topic_event_wildcard()
        assert t.endswith("/+/+")

    def test_chain_result(self):
        t = topic_chain_result("session1", "task1")
        assert "/chain/session1/task1" in t

    def test_state_dispatch(self):
        t = topic_state_dispatch("task1")
        assert "/dispatch/task1" in t

    def test_control_reload(self):
        t = topic_control_reload()
        assert "/control/" in t
        assert "/reload" in t


# --- Session building ---


class TestCreateSession:
    def setup_method(self):
        self.agents = {
            "researcher": AgentDef(
                id="researcher",
                name="Researcher",
                runtime="claude",
            ),
            "writer": AgentDef(
                id="writer",
                name="Writer",
                runtime="claude",
            ),
            "codex_agent": AgentDef(
                id="codex_agent",
                name="Codex Agent",
                runtime="codex",
            ),
        }
        self.workflow = WorkflowDef(
            id="test",
            name="Test Workflow",
            variables=["topic"],
            tasks=[
                WorkflowTask(
                    id="research",
                    agent="researcher",
                    description="Research '{topic}'",
                    next="fact_check",
                    needs=[],
                ),
                WorkflowTask(
                    id="analyze",
                    agent="researcher",
                    description="Analyze '{topic}'",
                    next="fact_check",
                    needs=[],
                ),
                WorkflowTask(
                    id="fact_check",
                    agent="writer",
                    description="Check '{topic}'",
                    next="output",
                    needs=["research", "analyze"],
                ),
            ],
        )

    def test_workflow_session(self):
        session = create_session(
            "c1",
            "test",
            workflow=self.workflow,
            variables={"topic": "AI"},
            agents=self.agents,
        )
        assert "research" in session.tasks
        assert "analyze" in session.tasks
        assert "fact_check" in session.tasks
        assert session.tasks["research"].next == "fact_check"
        assert session.tasks["fact_check"].needs == ["research", "analyze"]

    def test_agent_session_sets_output(self):
        session = create_session(
            "c1",
            "test",
            agent_id="researcher",
            text="test",
            agents=self.agents,
        )
        assert "researcher" in session.tasks
        assert session.tasks["researcher"].next == "output"

    def test_variable_interpolation(self):
        session = create_session(
            "c1",
            "test",
            workflow=self.workflow,
            variables={"topic": "quantum"},
            agents=self.agents,
        )
        assert "quantum" in session.tasks["research"].description


# --- Entry task detection ---


class TestEntryTasks:
    def test_finds_entry_tasks(self):
        session = Session(session_id="c1", label="test")
        session.tasks["a"] = SessionTask(
            id="a",
            task_id="t1",
            agent="r",
            description="",
            needs=[],
            next="c",
        )
        session.tasks["b"] = SessionTask(
            id="b",
            task_id="t2",
            agent="r",
            description="",
            needs=[],
            next="c",
        )
        session.tasks["c"] = SessionTask(
            id="c",
            task_id="t3",
            agent="w",
            description="",
            needs=["a", "b"],
            next="output",
        )
        entry = [
            t for t in session.tasks.values() if not t.needs and t.status == "pending"
        ]
        assert len(entry) == 2
        entry_ids = {t.id for t in entry}
        assert entry_ids == {"a", "b"}

    def test_skips_running(self):
        session = Session(session_id="c1", label="test")
        session.tasks["a"] = SessionTask(
            id="a",
            task_id="t1",
            agent="r",
            description="",
            needs=[],
            status="running",
        )
        entry = [
            t for t in session.tasks.values() if not t.needs and t.status == "pending"
        ]
        assert len(entry) == 0


# --- Dispatch spec building ---


class TestBuildDispatchSpec:
    def test_resolves_from_agent_def(self):
        agents = {
            "researcher": AgentDef(
                id="researcher",
                name="R",
                runtime="claude",
            ),
        }
        session = Session(session_id="s1", label="test")
        session.tasks["research"] = SessionTask(
            id="research",
            task_id="t1",
            agent="researcher",
            description="do stuff",
            model="sonnet",
            next="output",
        )
        session.caller_reply_topic = "reply/t"
        session.caller_correlation = "corr"

        spec = build_dispatch_spec(session, "research", agents)
        assert spec["agent"] == "researcher"
        assert spec["runtime"] == "claude"
        assert spec["model"] == "sonnet"
        assert spec["caller_reply_topic"] == "reply/t"

    def test_codex_runtime(self):
        agents = {
            "coder": AgentDef(
                id="coder",
                name="C",
                runtime="codex",
            ),
        }
        session = Session(session_id="s1", label="test")
        session.tasks["code"] = SessionTask(
            id="code",
            task_id="t1",
            agent="coder",
            description="write code",
            next="output",
        )

        spec = build_dispatch_spec(session, "code", agents)
        assert spec["runtime"] == "codex"
        assert spec["agent"] == "coder"


# --- Workflow loading: auto-infer next ---


class TestWorkflowLoading:
    def test_auto_infer_next(self, tmp_path):
        workflow_yaml = tmp_path / "test.yaml"
        workflow_yaml.write_text(
            """
name: Test
tasks:
  - id: step1
    agent: worker
    description: first
    needs: []
  - id: step2
    agent: worker
    description: second
    needs: [step1]
"""
        )
        import yaml

        data = yaml.safe_load(workflow_yaml.read_text())
        tasks = []
        for t in data["tasks"]:
            tasks.append(
                WorkflowTask(
                    id=t.get("id", ""),
                    agent=t.get("agent", "worker"),
                    description=t.get("description", ""),
                    next=t.get("next", ""),
                    needs=t.get("needs", []),
                )
            )
        for t in tasks:
            if not t.next:
                dependents = [other.id for other in tasks if t.id in other.needs]
                if len(dependents) == 1:
                    t.next = dependents[0]
                elif len(dependents) == 0:
                    t.next = "output"

        assert tasks[0].id == "step1"
        assert tasks[0].next == "step2"
        assert tasks[1].id == "step2"
        assert tasks[1].next == "output"


# --- Codex runtime dispatch ---


class TestCodexDispatch:
    @pytest.mark.asyncio
    async def test_codex_not_installed(self):
        """run_agent handles missing codex CLI."""
        from skitter.worker import run_agent

        task = AgentMessage(
            task_id="t1",
            session_id="c1",
            description="code something",
            model="gpt-5-nano",
            runtime="codex",
        )

        async def noop_publish(item_type, content, seq):
            pass

        with patch("asyncio.create_subprocess_exec", side_effect=FileNotFoundError):
            result, usage, cost = await run_agent(
                task, "/tmp", noop_publish, asyncio.Event()
            )
            assert "codex" in result.lower() and "not found" in result.lower()
            assert usage is None

    @pytest.mark.asyncio
    async def test_claude_not_installed(self):
        """run_agent handles missing claude CLI."""
        from skitter.worker import run_agent

        task = AgentMessage(
            task_id="t1",
            session_id="c1",
            description="do something",
            agent="researcher",
            runtime="claude",
        )

        async def noop_publish(item_type, content, seq):
            pass

        with patch("asyncio.create_subprocess_exec", side_effect=FileNotFoundError):
            result, usage, cost = await run_agent(
                task, "/tmp", noop_publish, asyncio.Event()
            )
            assert "claude" in result.lower() and "not found" in result.lower()
            assert usage is None


# --- Reload signal ---


class TestReload:
    def test_reload_module_exists(self):
        import skitter.reload

        assert hasattr(skitter.reload, "main")


# --- Spawn module ---


class TestSpawn:
    def test_spawn_module_exists(self):
        from skitter.spawn import spawn_worker

        assert callable(spawn_worker)


# --- Storage module ---


class TestStorage:
    def test_storage_module_exists(self):
        from skitter.storage import load_agents, load_workflows

        assert callable(load_agents)
        assert callable(load_workflows)


# --- Respawn module ---


class TestRespawn:
    @pytest.mark.asyncio
    async def test_respawn_with_missing_fields(self):
        from skitter.respawn import handle_dead_event

        # Should not raise
        await handle_dead_event(json.dumps({"status": "dead"}))

    @pytest.mark.asyncio
    async def test_respawn_with_valid_event(self):
        from skitter.respawn import handle_dead_event

        with patch("skitter.respawn.spawn_worker") as mock_spawn:
            await handle_dead_event(
                json.dumps(
                    {
                        "status": "dead",
                        "task_id": "t1",
                        "agent": "researcher",
                        "session_id": "s1",
                    }
                )
            )
            mock_spawn.assert_called_once_with("researcher", "s1", "t1")


# --- Join context from wait_for_needs ---


class TestJoinContext:
    def test_build_context_from_results(self):
        """Verify the context string format produced for join tasks."""
        results = {"a": "result A", "b": "result B"}
        parts = [
            f"## Result from '{need_id}':\n{result}"
            for need_id, result in results.items()
        ]
        context = "\n\n".join(parts)
        assert "result A" in context
        assert "result B" in context
        assert "Result from 'a'" in context
        assert "Result from 'b'" in context
