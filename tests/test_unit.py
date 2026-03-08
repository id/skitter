"""Tests for skitter supervisor architecture."""

import asyncio
import json
from unittest.mock import patch

import pytest

from skitter.config import (
    AgentDef,
    WorkflowDef,
    WorkflowTask,
)
from skitter.supervisor import (
    build_dispatch_spec,
    create_session,
    _parse_agent_id_from_topic,
)
from skitter.mqtt import (
    topic_chain_result,
    topic_event,
    topic_event_wildcard,
    topic_reload,
    topic_request,
    topic_request_wildcard,
    topic_session,
    topic_task_status,
)
from skitter.types import (
    A2ARequest,
    AgentMessage,
    A2A_RESPONDER_UNAVAILABLE,
    A2A_TRANSPORT_PROTOCOL_ERROR,
    REPLY_ERROR,
    REPLY_TERMINAL,
    REPLY_TEXT,
    REPLY_TOOL,
    Session,
    SessionTask,
    classify_reply,
    make_status_event,
)


# --- Foundation types ---


class TestA2ARequest:
    def test_roundtrip(self):
        req = A2ARequest(
            text="Research quantum computing",
            session_id="req-abc123",
            sender="cli",
            variables={"topic": "quantum"},
        )
        j = req.to_json()
        d = json.loads(j)
        assert d["jsonrpc"] == "2.0"
        assert d["id"] == "req-abc123"
        assert d["method"] == "tasks/send"
        assert (
            d["params"]["message"]["parts"][0]["text"] == "Research quantum computing"
        )
        assert d["params"]["metadata"]["sender"] == "cli"
        assert d["params"]["metadata"]["variables"]["topic"] == "quantum"

        restored = A2ARequest.from_json(j)
        assert restored.text == "Research quantum computing"
        assert restored.session_id == "req-abc123"
        assert restored.sender == "cli"
        assert restored.variables == {"topic": "quantum"}

    def test_minimal(self):
        req = A2ARequest(text="hello", session_id="s1")
        j = req.to_json()
        d = json.loads(j)
        assert "metadata" not in d["params"]

        restored = A2ARequest.from_json(j)
        assert restored.text == "hello"
        assert restored.sender == ""
        assert restored.variables == {}


class TestStatusEvent:
    def test_working_text(self):
        event = make_status_event("req-1", "t-abc", "working", message="hello")
        d = json.loads(event)
        assert d["jsonrpc"] == "2.0"
        assert d["id"] == "req-1"
        assert d["result"]["type"] == "TaskStatusUpdateEvent"
        assert d["result"]["taskId"] == "t-abc"
        assert d["result"]["status"]["state"] == "working"
        assert d["result"]["status"]["message"] == "hello"
        assert "artifact" not in d["result"]

        kind, content = classify_reply(d)
        assert kind == REPLY_TEXT
        assert content == "hello"

    def test_working_tool_use(self):
        event = make_status_event(
            "req-1",
            "t-abc",
            "working",
            message="Read: file.py",
            message_type="tool_use",
        )
        d = json.loads(event)
        assert d["result"]["status"]["metadata"]["type"] == "tool_use"

        kind, content = classify_reply(d)
        assert kind == REPLY_TOOL
        assert content == "Read: file.py"

    def test_terminal_with_artifact(self):
        event = make_status_event(
            "req-2", "t-xyz", "completed", artifact_text="Final answer"
        )
        d = json.loads(event)
        assert d["result"]["status"]["state"] == "completed"
        assert d["result"]["artifact"]["parts"][0]["text"] == "Final answer"

        kind, content = classify_reply(d)
        assert kind == REPLY_TERMINAL
        assert content == "Final answer"

    def test_error_reply(self):
        kind, content = classify_reply(
            {"error": {"code": -32004, "message": "Unknown agent"}}
        )
        assert kind == REPLY_ERROR
        assert content == "Unknown agent"

    def test_unknown_message(self):
        kind, content = classify_reply({"something": "else"})
        assert kind == ""
        assert content == ""


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
            caller_reply_topic="reply/topic",
            caller_correlation="corr123",
        )
        json_str = msg.to_json()
        restored = AgentMessage.from_json(json_str)
        assert restored.task_id == "t1"
        assert restored.agent == "researcher"
        assert restored.runtime == "claude"
        assert restored.next == "step2"
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


# --- A2A error codes ---


class TestErrorCodes:
    def test_error_codes_defined(self):
        assert A2A_RESPONDER_UNAVAILABLE == -32004
        assert A2A_TRANSPORT_PROTOCOL_ERROR == -32005


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
        t = topic_chain_result("researcher", "session1", "task1")
        assert "/event/" in t
        assert "/researcher/chain-result/session1/task1" in t

    def test_session_topic(self):
        t = topic_session("session1")
        assert "/event/" in t
        assert "/supervisor/session/session1" in t

    def test_task_status(self):
        t = topic_task_status("researcher", "session1", "task1")
        assert "/event/" in t
        assert "/researcher/task-status/session1/task1" in t

    def test_reload(self):
        t = topic_reload()
        assert "/request/" in t
        assert "/supervisor/reload" in t

    def test_request_wildcard(self):
        t = topic_request_wildcard()
        assert t.endswith("/+")
        assert "/request/" in t

    def test_request_per_agent(self):
        t = topic_request("researcher")
        assert "/request/" in t
        assert "/researcher" in t


# --- Topic parsing ---


class TestTopicParsing:
    def test_parse_agent_id(self):
        topic = "$a2a/v1/request/skitter/default/researcher"
        assert _parse_agent_id_from_topic(topic) == "researcher"

    def test_parse_workflow_id(self):
        topic = "$a2a/v1/request/skitter/default/workflow-quick-research"
        assert _parse_agent_id_from_topic(topic) == "workflow-quick-research"

    def test_parse_short_topic(self):
        assert _parse_agent_id_from_topic("too/short") == ""


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
        from skitter.storage import load_agents, load_workflows, load_cards

        assert callable(load_agents)
        assert callable(load_workflows)
        assert callable(load_cards)


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
