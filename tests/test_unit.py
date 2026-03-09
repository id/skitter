"""Tests for skitter supervisor architecture."""

import asyncio
import json
from unittest.mock import patch

import pytest

from skitter.config import (
    AgentDef,
    WorkflowDef,
    WorkflowTask,
    WorkspaceConfig,
)
from skitter.supervisor import (
    create_session,
    _parse_agent_id_from_topic,
)
from skitter.mqtt import (
    topic_event,
    topic_dead_wildcard,
    topic_reload,
    topic_request,
    topic_request_wildcard,
    topic_result,
    topic_session,
    topic_status,
)
from skitter.types import (
    A2ARequest,
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
            request_id="req-abc123",
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
        assert restored.request_id == "req-abc123"
        assert restored.sender == "cli"
        assert restored.variables == {"topic": "quantum"}

    def test_minimal(self):
        req = A2ARequest(text="hello", request_id="s1")
        j = req.to_json()
        d = json.loads(j)
        assert "metadata" not in d["params"]

        restored = A2ARequest.from_json(j)
        assert restored.text == "hello"
        assert restored.sender == ""
        assert restored.variables == {}


class TestStatusEvent:
    def test_working_text(self):
        event = make_status_event(
            "req-1", "sess-abc123", "working", message="hello", task_name="research"
        )
        d = json.loads(event)
        assert d["jsonrpc"] == "2.0"
        assert d["id"] == "req-1"
        assert d["result"]["type"] == "TaskStatusUpdateEvent"
        assert d["result"]["taskId"] == "sess-abc123"
        assert d["result"]["status"]["state"] == "working"
        assert d["result"]["status"]["message"] == "hello"
        assert d["result"]["status"]["metadata"]["task_name"] == "research"
        assert "artifact" not in d["result"]

        kind, content = classify_reply(d)
        assert kind == REPLY_TEXT
        assert content == "hello"

    def test_working_tool_use(self):
        event = make_status_event(
            "req-1",
            "sess-abc123",
            "working",
            message="Read: file.py",
            message_type="tool_use",
            task_name="research",
        )
        d = json.loads(event)
        assert d["result"]["status"]["metadata"]["type"] == "tool_use"
        assert d["result"]["status"]["metadata"]["task_name"] == "research"

        kind, content = classify_reply(d)
        assert kind == REPLY_TOOL
        assert content == "Read: file.py"

    def test_terminal_with_artifact(self):
        event = make_status_event(
            "req-2",
            "sess-def456",
            "completed",
            artifact_text="Final answer",
            task_name="summarize",
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


class TestSessionTask:
    def test_roundtrip(self):
        from dataclasses import asdict

        task = SessionTask(
            id="research",
            agent="researcher",
            description="do research",
            runtime="claude",
            next="review",
            needs=["prep"],
        )
        d = asdict(task)
        assert d["id"] == "research"
        assert d["runtime"] == "claude"
        assert d["next"] == "review"
        assert d["needs"] == ["prep"]

        restored = SessionTask(**d)
        assert restored.id == "research"
        assert restored.runtime == "claude"
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

    def test_roundtrip_with_tasks(self):
        session = Session(session_id="c1", workflow_id="test-wf", label="test")
        session.tasks["research"] = SessionTask(
            id="research",
            agent="researcher",
            description="do stuff",
            runtime="claude",
            model="sonnet",
        )
        json_str = session.to_json()
        restored = Session.from_json(json_str)
        assert "research" in restored.tasks
        assert restored.tasks["research"].runtime == "claude"
        assert restored.workflow_id == "test-wf"


# --- A2A error codes ---


class TestErrorCodes:
    def test_error_codes_defined(self):
        assert A2A_RESPONDER_UNAVAILABLE == -32004
        assert A2A_TRANSPORT_PROTOCOL_ERROR == -32005


# --- Topic builders ---


class TestTopics:
    def test_event_topics(self):
        t = topic_event("researcher", "alive")
        assert t == "skitter/event/researcher/alive"

    def test_dead_wildcard(self):
        t = topic_dead_wildcard()
        assert t == "skitter/event/+/dead"

    def test_result(self):
        t = topic_result("my-wf", "research", "session1")
        assert t == "skitter/result/my-wf/research/session1"

    def test_session_topic(self):
        t = topic_session("session1")
        assert t == "skitter/session/session1"

    def test_task_status(self):
        t = topic_status("my-wf", "research", "session1")
        assert t == "skitter/status/my-wf/research/session1"

    def test_reload(self):
        t = topic_reload()
        assert t == "skitter/control/reload"

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
        assert session.workflow_id == "test"

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
        assert session.workflow_id == "researcher"

    def test_variable_interpolation(self):
        session = create_session(
            "c1",
            "test",
            workflow=self.workflow,
            variables={"topic": "quantum"},
            agents=self.agents,
        )
        assert "quantum" in session.tasks["research"].description

    def test_runtime_from_agent_def(self):
        session = create_session(
            "c1",
            "test",
            agent_id="codex_agent",
            text="code something",
            agents=self.agents,
        )
        assert session.tasks["codex_agent"].runtime == "codex"

    def test_workflow_runtime_from_agent_def(self):
        session = create_session(
            "c1",
            "test",
            workflow=self.workflow,
            variables={"topic": "AI"},
            agents=self.agents,
        )
        assert session.tasks["research"].runtime == "claude"


# --- Entry task detection ---


class TestEntryTasks:
    def test_finds_entry_tasks(self):
        session = Session(session_id="c1", label="test")
        session.tasks["a"] = SessionTask(
            id="a",
            agent="r",
            description="",
            needs=[],
            next="c",
        )
        session.tasks["b"] = SessionTask(
            id="b",
            agent="r",
            description="",
            needs=[],
            next="c",
        )
        session.tasks["c"] = SessionTask(
            id="c",
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
            agent="r",
            description="",
            needs=[],
            status="running",
        )
        entry = [
            t for t in session.tasks.values() if not t.needs and t.status == "pending"
        ]
        assert len(entry) == 0


# --- Codex runtime dispatch ---


class TestCodexDispatch:
    @pytest.mark.asyncio
    async def test_codex_not_installed(self):
        """run_agent handles missing codex CLI."""
        from skitter.worker import run_agent

        task = SessionTask(
            id="code",
            agent="coder",
            description="code something",
            model="gpt-5-nano",
            runtime="codex",
        )

        async def noop_publish(item_type, content):
            pass

        with patch("asyncio.create_subprocess_exec", side_effect=FileNotFoundError):
            result, usage, cost = await run_agent(
                task, "", "/tmp", noop_publish, asyncio.Event()
            )
            assert "codex" in result.lower() and "not found" in result.lower()
            assert usage is None

    @pytest.mark.asyncio
    async def test_claude_not_installed(self):
        """run_agent handles missing claude CLI."""
        from skitter.worker import run_agent

        task = SessionTask(
            id="research",
            agent="researcher",
            description="do something",
            runtime="claude",
        )

        async def noop_publish(item_type, content):
            pass

        with patch("asyncio.create_subprocess_exec", side_effect=FileNotFoundError):
            result, usage, cost = await run_agent(
                task, "", "/tmp", noop_publish, asyncio.Event()
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
    def test_config_loaders_exist(self):
        from skitter.config import load_agents, load_cards, load_workflows

        assert callable(load_agents)
        assert callable(load_workflows)
        assert callable(load_cards)


# --- Respawn module ---


class TestRespawn:
    @pytest.mark.asyncio
    async def test_respawn_with_missing_fields(self):
        from skitter.supervisor import handle_dead_event

        await handle_dead_event(json.dumps({"status": "dead"}))

    @pytest.mark.asyncio
    async def test_respawn_with_valid_event(self):
        from skitter.supervisor import handle_dead_event

        with patch("skitter.supervisor.spawn_worker") as mock_spawn:
            await handle_dead_event(
                json.dumps(
                    {
                        "status": "dead",
                        "task": "research",
                        "agent": "researcher",
                        "session_id": "s1",
                    }
                )
            )
            mock_spawn.assert_called_once_with("researcher", "s1", "research")


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


# --- Persistent workspaces ---


class TestWorkspaceConfig:
    def test_load_workspace_config_missing(self, tmp_path):
        """No workspace key in config returns defaults."""
        from skitter.config import load_workspace_config

        config_file = tmp_path / "config.yaml"
        config_file.write_text("default_runtime: claude\n")
        with patch("skitter.config.CONFIG_FILE", config_file):
            cfg = load_workspace_config()
        assert cfg.remote == ""
        assert cfg.local_mount == ""
        assert cfg.base_path == "skitter/workspaces"

    def test_load_workspace_config_present(self, tmp_path):
        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "workspace:\n"
            "  remote: drive\n"
            "  local_mount: /mnt/gdrive\n"
            "  base_path: my/workspaces\n"
        )
        with patch("skitter.config.CONFIG_FILE", config_file):
            from skitter.config import load_workspace_config

            cfg = load_workspace_config()
        assert cfg.remote == "drive"
        assert cfg.local_mount == "/mnt/gdrive"
        assert cfg.base_path == "my/workspaces"


class TestWorkspaceSessionCreation:
    def setup_method(self):
        self.agents = {
            "researcher": AgentDef(id="researcher", name="R", runtime="claude"),
            "writer": AgentDef(id="writer", name="W", runtime="claude"),
        }

    def test_workflow_with_workspace(self):
        wf = WorkflowDef(
            id="test",
            name="Test",
            workspace="my-workspace",
            tasks=[
                WorkflowTask(id="a", agent="researcher", description="do A", next="c"),
                WorkflowTask(id="b", agent="researcher", description="do B", next="c"),
                WorkflowTask(
                    id="c",
                    agent="writer",
                    description="join",
                    next="output",
                    needs=["a", "b"],
                ),
            ],
        )
        session = create_session("s1", "test", workflow=wf, agents=self.agents)
        # All tasks share the workspace slug; worker creates task subdirs
        assert session.tasks["a"].workspace == "my-workspace"
        assert session.tasks["b"].workspace == "my-workspace"
        assert session.tasks["c"].workspace == "my-workspace"

    def test_workflow_without_workspace(self):
        wf = WorkflowDef(
            id="test",
            name="Test",
            tasks=[
                WorkflowTask(
                    id="a", agent="researcher", description="do A", next="output"
                ),
            ],
        )
        session = create_session("s1", "test", workflow=wf, agents=self.agents)
        assert session.tasks["a"].workspace == ""

    def test_agent_session_no_workspace(self):
        session = create_session(
            "s1", "test", agent_id="researcher", text="hi", agents=self.agents
        )
        assert session.tasks["researcher"].workspace == ""


class TestResolveWorkspace:
    def test_subprocess_with_local_mount(self, tmp_path):
        from skitter.workspace import resolve_workspace

        cfg = WorkspaceConfig(remote="drive", local_mount=str(tmp_path), base_path="ws")
        with patch("skitter.workspace.load_workspace_config", return_value=cfg):
            local, remote = resolve_workspace("my-ws", "subprocess")
        assert local == tmp_path / "ws" / "my-ws"
        assert local.is_dir()
        assert remote == ""  # no rclone needed

    def test_fly_mode_uses_rclone(self, tmp_path):
        from skitter.workspace import resolve_workspace

        cfg = WorkspaceConfig(remote="drive", base_path="ws")
        with (
            patch("skitter.workspace.load_workspace_config", return_value=cfg),
            patch("skitter.workspace.WORKSPACES_DIR", tmp_path),
        ):
            local, remote = resolve_workspace("my-ws", "fly")
        assert local == tmp_path / "my-ws"
        assert local.is_dir()
        assert remote == "drive:ws/my-ws"

    def test_no_remote_configured_raises(self, tmp_path):
        from skitter.workspace import resolve_workspace

        cfg = WorkspaceConfig()  # no remote, no local_mount
        with (
            patch("skitter.workspace.load_workspace_config", return_value=cfg),
            patch("skitter.workspace.WORKSPACES_DIR", tmp_path),
            pytest.raises(RuntimeError, match="no rclone remote"),
        ):
            resolve_workspace("my-ws", "fly")


class TestSessionWorkspaceRoundtrip:
    def test_workspace_survives_json_roundtrip(self):
        session = Session(session_id="s1")
        session.tasks["a"] = SessionTask(
            id="a", agent="r", description="d", workspace="my-ws"
        )
        restored = Session.from_json(session.to_json())
        assert restored.tasks["a"].workspace == "my-ws"

    def test_old_session_without_workspace(self):
        """Sessions from before workspace support default to empty string."""
        raw = json.dumps(
            {
                "session_id": "s1",
                "tasks": {"a": {"id": "a", "agent": "r", "description": "d"}},
            }
        )
        session = Session.from_json(raw)
        assert session.tasks["a"].workspace == ""
