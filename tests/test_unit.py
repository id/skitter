"""Tests for skitter supervisor architecture."""

import json
from unittest.mock import MagicMock, patch

import pytest

from skitter.config import AgentDef
from skitter.supervisor import _parse_agent_id_from_topic
from skitter.mqtt import (
    topic_a2a_event,
    topic_discovery_wildcard,
    topic_event,
    topic_request,
    topic_result,
)
from skitter.types import (
    A2ARequest,
    A2A_RESPONDER_UNAVAILABLE,
    A2A_TRANSPORT_PROTOCOL_ERROR,
    REPLY_ERROR,
    REPLY_TERMINAL,
    REPLY_TEXT,
    REPLY_TOOL,
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

    def test_result(self):
        t = topic_result("my-wf", "research", "session1")
        assert t == "skitter/result/my-wf/research/session1"

    def test_request_per_agent(self):
        t = topic_request("researcher")
        assert "/request/" in t
        assert "/researcher" in t

    def test_a2a_event_topic(self):
        t = topic_a2a_event("skitter-runtime")
        assert t == "$a2a/v1/event/skitter/default/skitter-runtime"


# --- Topic parsing ---


class TestTopicParsing:
    def test_parse_agent_id(self):
        topic = "$a2a/v1/request/skitter/default/researcher"
        assert _parse_agent_id_from_topic(topic) == "researcher"

    def test_parse_workflow_id(self):
        topic = "$a2a/v1/request/skitter/default/quick-research"
        assert _parse_agent_id_from_topic(topic) == "quick-research"

    def test_parse_short_topic(self):
        assert _parse_agent_id_from_topic("too/short") == ""


# --- Session building (DB-backed supervisor) ---


class TestSupervisorSession:
    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    def _make_supervisor(self):
        from skitter.supervisor import Supervisor

        return Supervisor(self.db)

    def test_create_session_from_graph(self):
        from skitter.db import App, AppVersion

        sup = self._make_supervisor()
        self.db.create_app(App(id="test-app", name="Test"))
        self.db.create_app_version(
            AppVersion(
                id="v1",
                app_id="test-app",
                version=1,
                graph_json=json.dumps(
                    {
                        "tasks": [
                            {
                                "id": "research",
                                "agent": "researcher",
                                "description": "Research AI",
                                "needs": [],
                                "next": "review",
                            },
                            {
                                "id": "review",
                                "agent": "writer",
                                "description": "Review results",
                                "needs": ["research"],
                                "next": "output",
                            },
                        ]
                    }
                ),
            )
        )
        req = A2ARequest(text="test", request_id="r1")
        state = sup.create_session_from_graph(
            graph_json=self.db.get_app_version("v1").graph_json,
            app_version_id="v1",
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        assert "research" in state.graph
        assert "review" in state.graph
        assert state.graph["review"].needs == ["research"]
        assert "research" in state.pending
        assert "review" in state.pending

    def test_variable_interpolation(self):
        from skitter.db import App, AppVersion

        sup = self._make_supervisor()
        self.db.create_app(App(id="test-app", name="Test"))
        self.db.create_app_version(
            AppVersion(
                id="v1",
                app_id="test-app",
                version=1,
                graph_json=json.dumps(
                    {
                        "tasks": [
                            {
                                "id": "research",
                                "agent": "researcher",
                                "description": "Research '{topic}'",
                                "needs": [],
                                "next": "output",
                            }
                        ]
                    }
                ),
            )
        )
        req = A2ARequest(text="test", request_id="r1")
        state = sup.create_session_from_graph(
            graph_json=self.db.get_app_version("v1").graph_json,
            app_version_id="v1",
            request=req,
            caller_reply_topic="",
            caller_correlation="",
            variables={"topic": "quantum"},
        )
        assert "quantum" in state.graph["research"].description


# --- Entry task detection ---


# --- App creation ---


class TestAppCreation:
    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    def test_create_app(self):
        from skitter.apps import create_app

        app, version, card_json = create_app(
            self.db,
            name="Test App",
            description="A test",
            graph={
                "tasks": [
                    {
                        "id": "t1",
                        "agent": "researcher",
                        "description": "do stuff",
                        "needs": [],
                        "next": "output",
                    }
                ]
            },
        )
        assert app is not None
        assert version.version == 1
        assert app.card_json != ""
        card = json.loads(card_json)
        assert card["name"] == "Test App"
        assert len(card["metadata"]["tasks"]) == 1

    def test_version_increment(self):
        from skitter.apps import create_app

        app1, v1, _ = create_app(
            self.db, app_id="my-app", name="App", graph={"tasks": []}
        )
        _, v2, _ = create_app(self.db, app_id="my-app", name="App", graph={"tasks": []})
        assert v1.version == 1
        assert v2.version == 2


# --- Discovery cards ---


class TestBuildCard:
    def test_agent_card_schema(self):
        from skitter.discovery import build_card

        agent = AgentDef(
            id="researcher",
            name="Researcher",
            description="Deep research with citations",
        )
        card = build_card(agent)
        assert card["name"] == "Researcher"
        assert card["description"] == "Deep research with citations"
        assert card["version"] == "0.1.0"
        assert card["protocolVersion"] == "0.2.5"
        assert card["capabilities"]["streaming"] is True
        assert card["capabilities"]["pushNotifications"] is False
        assert card["defaultInputModes"] == ["text/plain"]
        assert card["defaultOutputModes"] == ["text/plain"]
        assert card["skills"][0]["id"] == "researcher"
        assert "metadata" not in card

    def test_agent_card_custom_capabilities(self):
        from skitter.discovery import build_card

        agent = AgentDef(
            id="coder",
            name="Coder",
            description="Writes code",
            capabilities={"streaming": False},
            input_modes=["text/plain", "application/json"],
        )
        card = build_card(agent)
        assert card["capabilities"]["streaming"] is False
        assert card["capabilities"]["pushNotifications"] is False
        assert card["defaultInputModes"] == ["text/plain", "application/json"]

    def test_composed_app_card_has_metadata_tasks(self):
        from skitter.discovery import build_card

        agent = AgentDef(id="my-app", name="My App", description="A composed app")
        metadata = {
            "variables": ["topic"],
            "tasks": [
                {
                    "id": "step1",
                    "agent": "researcher",
                    "description": "Research {topic}",
                },
            ],
        }
        card = build_card(agent, metadata=metadata)
        assert "metadata" in card
        assert card["metadata"]["variables"] == ["topic"]
        assert len(card["metadata"]["tasks"]) == 1
        assert card["metadata"]["tasks"][0]["id"] == "step1"

    def test_card_has_url(self):
        from skitter.discovery import build_card

        agent = AgentDef(id="test", name="Test")
        card = build_card(agent, url="mqtt://custom:1883")
        assert card["url"] == "mqtt://custom:1883"


class TestParseCard:
    def test_parse_card(self):
        from skitter.discovery import parse_card

        raw = json.dumps({"name": "Test", "version": "0.1.0"}).encode()
        card = parse_card(raw)
        assert card["name"] == "Test"

    def test_is_workflow_card(self):
        from skitter.discovery import is_workflow_card

        assert not is_workflow_card({"name": "Agent"})
        assert not is_workflow_card({"name": "Agent", "metadata": {}})
        assert is_workflow_card({"metadata": {"tasks": [{"id": "step1"}]}})


class TestDiscoveryWildcard:
    def test_default_org_unit(self):
        t = topic_discovery_wildcard()
        assert "/discovery/" in t
        assert t.endswith("/+")

    def test_custom_org_unit(self):
        t = topic_discovery_wildcard("myorg", "myunit")
        assert "myorg/myunit/+" in t


# --- Agent runner ---


class TestAgentRunnerCli:
    def test_build_claude_cmd(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(
            id="researcher",
            name="Researcher",
            runtime="claude",
            model="sonnet",
            agent_file="researcher.md",
        )
        cmd = _build_cli_cmd(agent, "test prompt")
        assert cmd[0] == "claude"
        assert "-p" in cmd
        assert "test prompt" in cmd
        assert "--agent" in cmd
        assert "researcher" in cmd  # agent file without .md
        assert "--model" in cmd
        assert "sonnet" in cmd

    def test_build_codex_cmd(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(
            id="coder",
            name="Coder",
            runtime="codex",
            model="gpt-5-nano",
        )
        cmd = _build_cli_cmd(agent, "code something")
        assert cmd[0] == "codex"
        assert "code something" in cmd
        assert "--model" in cmd
        assert "gpt-5-nano" in cmd

    def test_build_claude_cmd_default_agent_file(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(id="researcher", name="Researcher", runtime="claude")
        cmd = _build_cli_cmd(agent, "test")
        # When agent_file is empty, uses agent.id
        idx = cmd.index("--agent")
        assert cmd[idx + 1] == "researcher"

    @pytest.mark.asyncio
    async def test_run_cli_missing_binary(self):
        from skitter.agent_runner import _run_cli

        agent = AgentDef(id="test", name="Test", runtime="claude")

        async def noop(t, c):
            pass

        with patch("asyncio.create_subprocess_exec", side_effect=FileNotFoundError):
            result = await _run_cli(agent, "test", noop, {})
            assert "claude" in result.lower()
            assert "not found" in result.lower()


class TestLoadAgent:
    def test_load_agent_with_all_fields(self, tmp_path):
        (tmp_path / "test-agent.yaml").write_text(
            "name: Test Agent\n"
            "description: A test agent\n"
            "agent_id: custom-id\n"
            "runtime: claude\n"
            "model: sonnet\n"
            "agent_file: test.md\n"
            "broker:\n"
            "  host: broker.example.com\n"
            "  port: 8883\n"
            "capabilities:\n"
            "  streaming: true\n"
            "input_modes: ['text/plain', 'application/json']\n"
            "output_modes: ['text/plain']\n"
        )
        from skitter.agent_runner import load_agent

        agent = load_agent(str(tmp_path / "test-agent.yaml"))
        assert agent.id == "custom-id"
        assert agent.name == "Test Agent"
        assert agent.model == "sonnet"
        assert agent.agent_file == "test.md"
        assert agent.broker is not None
        assert agent.broker.host == "broker.example.com"
        assert agent.broker.port == 8883
        assert agent.capabilities == {"streaming": True}
        assert agent.input_modes == ["text/plain", "application/json"]

    def test_load_agent_defaults(self, tmp_path):
        (tmp_path / "simple.yaml").write_text(
            "name: Simple\ndescription: A simple agent\n"
        )
        from skitter.agent_runner import load_agent

        agent = load_agent(str(tmp_path / "simple.yaml"))
        assert agent.id == "simple"
        assert agent.model == ""
        assert agent.agent_file == ""
        assert agent.broker is None
        assert agent.runtime == "claude"
        assert agent.input_modes == ["text/plain"]


# --- Safe format ---


class TestSafeFormat:
    def test_unknown_vars_left_intact(self):
        from skitter.config import safe_format

        desc = safe_format("Research {topic}, output as {format}.", {"topic": "AI"})
        assert desc == "Research AI, output as {format}."


# --- Dependency resolution ---


class TestDependencyResolution:
    def test_compute_ready_no_needs(self):
        from skitter.supervisor import SessionState, SessionTask as ST, _compute_ready

        state = SessionState(session_id="s1", app_version_id="v1")
        state.graph["a"] = ST(task_id="a", agent="r", description="d", needs=[])
        state.graph["b"] = ST(task_id="b", agent="r", description="d", needs=["a"])
        state.pending = {"a", "b"}
        ready = _compute_ready(state)
        assert ready == ["a"]

    def test_compute_ready_after_completion(self):
        from skitter.supervisor import SessionState, SessionTask as ST, _compute_ready

        state = SessionState(session_id="s1", app_version_id="v1")
        state.graph["a"] = ST(task_id="a", agent="r", description="d", needs=[])
        state.graph["b"] = ST(task_id="b", agent="r", description="d", needs=["a"])
        state.results["a"] = "done"
        state.pending = {"b"}
        ready = _compute_ready(state)
        assert ready == ["b"]

    def test_propagate_failure(self):
        from skitter.supervisor import (
            SessionState,
            SessionTask as ST,
            _propagate_failure,
        )

        state = SessionState(session_id="s1", app_version_id="v1")
        state.graph["a"] = ST(task_id="a", agent="r", description="d", needs=[])
        state.graph["b"] = ST(task_id="b", agent="r", description="d", needs=["a"])
        state.graph["c"] = ST(task_id="c", agent="r", description="d", needs=["b"])
        state.failed.add("a")
        state.pending = {"b", "c"}
        newly_failed = _propagate_failure(state, "a")
        assert "b" in newly_failed
        assert "c" in newly_failed
        assert "b" in state.failed
        assert "c" in state.failed

    def test_find_terminal_tasks(self):
        from skitter.supervisor import (
            SessionState,
            SessionTask as ST,
            _find_terminal_tasks,
        )

        state = SessionState(session_id="s1", app_version_id="v1")
        state.graph["a"] = ST(task_id="a", agent="r", description="d", next="b")
        state.graph["b"] = ST(task_id="b", agent="r", description="d", next="output")
        state.graph["c"] = ST(task_id="c", agent="r", description="d", next="")
        terminals = _find_terminal_tasks(state)
        assert set(terminals) == {"b", "c"}

    def test_build_context(self):
        from skitter.supervisor import (
            SessionState,
            SessionTask as ST,
            _build_context,
        )

        state = SessionState(session_id="s1", app_version_id="v1")
        state.results["a"] = "result A"
        state.results["b"] = "result B"
        task = ST(task_id="c", agent="w", description="d", needs=["a", "b"])
        ctx = _build_context(state, task)
        assert "result A" in ctx
        assert "result B" in ctx
        assert "Result from 'a'" in ctx


# --- Discovery registry ---


class TestDiscoveryRegistry:
    def test_update_and_get(self):
        from skitter.supervisor import DiscoveryRegistry

        reg = DiscoveryRegistry()
        reg.update("researcher", {"name": "Researcher"})
        assert reg.get("researcher") == {"name": "Researcher"}
        assert reg.get("unknown") is None

    def test_remove(self):
        from skitter.supervisor import DiscoveryRegistry

        reg = DiscoveryRegistry()
        reg.update("researcher", {"name": "Researcher"})
        reg.remove("researcher")
        assert reg.get("researcher") is None

    def test_list_agents_vs_apps(self):
        from skitter.supervisor import DiscoveryRegistry

        reg = DiscoveryRegistry()
        reg.update("agent1", {"name": "Agent1"})
        reg.update("app1", {"name": "App1", "metadata": {"tasks": [{"id": "t1"}]}})
        assert "agent1" in reg.list_agents()
        assert "app1" not in reg.list_agents()
        assert "app1" in reg.list_apps()
        assert "agent1" not in reg.list_apps()


# --- Persistent workspaces ---


# --- DB module ---


class TestSqliteDB:
    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    def test_app_crud(self):
        from skitter.db import App

        self.db.create_app(App(id="a1", name="Test App", description="desc"))
        app = self.db.get_app("a1")
        assert app is not None
        assert app.name == "Test App"

        self.db.update_app_card("a1", '{"name":"Test"}')
        app = self.db.get_app("a1")
        assert app.card_json == '{"name":"Test"}'

        apps = self.db.list_apps()
        assert len(apps) == 1

        self.db.delete_app("a1")
        assert self.db.get_app("a1") is None

    def test_app_version(self):
        from skitter.db import App, AppVersion

        self.db.create_app(App(id="a1", name="Test"))
        self.db.create_app_version(
            AppVersion(id="v1", app_id="a1", version=1, graph_json='{"tasks":[]}')
        )
        self.db.create_app_version(
            AppVersion(id="v2", app_id="a1", version=2, graph_json='{"tasks":[]}')
        )
        current = self.db.get_current_version("a1")
        assert current is not None
        assert current.version == 2

        versions = self.db.list_app_versions("a1")
        assert len(versions) == 2

    def test_session_and_tasks(self):
        from skitter.db import App, AppVersion, DBSession, DBTask

        self.db.create_app(App(id="a1", name="Test"))
        self.db.create_app_version(AppVersion(id="v1", app_id="a1", version=1))
        self.db.create_session(DBSession(id="s1", app_version_id="v1", state="running"))
        self.db.create_task(
            DBTask(
                id="s1/t1",
                session_id="s1",
                task_id="t1",
                agent="researcher",
                state="pending",
            )
        )

        session = self.db.get_session("s1")
        assert session is not None
        assert session.state == "running"

        tasks = self.db.list_tasks("s1")
        assert len(tasks) == 1
        assert tasks[0].agent == "researcher"

        self.db.update_task("s1/t1", state="completed", result="done")
        task = self.db.get_task("s1/t1")
        assert task.state == "completed"
        assert task.result == "done"

        self.db.update_session_state("s1", "completed")
        session = self.db.get_session("s1")
        assert session.state == "completed"
        assert session.completed_at != ""

    def test_cascade_delete(self):
        from skitter.db import App, AppVersion, DBSession, DBTask

        self.db.create_app(App(id="a1", name="Test"))
        self.db.create_app_version(AppVersion(id="v1", app_id="a1", version=1))
        self.db.create_session(DBSession(id="s1", app_version_id="v1"))
        self.db.create_task(
            DBTask(id="s1/t1", session_id="s1", task_id="t1", agent="r")
        )
        self.db.delete_app("a1")
        assert self.db.get_app_version("v1") is None
        assert self.db.get_session("s1") is None
        assert self.db.get_task("s1/t1") is None

    def test_list_sessions_by_app(self):
        from skitter.db import App, AppVersion, DBSession

        self.db.create_app(App(id="a1", name="App1"))
        self.db.create_app(App(id="a2", name="App2"))
        self.db.create_app_version(AppVersion(id="v1", app_id="a1", version=1))
        self.db.create_app_version(AppVersion(id="v2", app_id="a2", version=1))
        self.db.create_session(DBSession(id="s1", app_version_id="v1"))
        self.db.create_session(DBSession(id="s2", app_version_id="v2"))

        all_sessions = self.db.list_sessions()
        assert len(all_sessions) == 2

        a1_sessions = self.db.list_sessions(app_id="a1")
        assert len(a1_sessions) == 1
        assert a1_sessions[0].id == "s1"


class TestTaskTarget:
    def test_defaults(self):
        from skitter.types import TaskTarget

        t = TaskTarget(agent="researcher")
        assert t.mqtt_host == ""
        assert t.mqtt_port == 8883
        assert t.http_url == ""


class TestDBConfig:
    def test_load_db_config_default(self, tmp_path):
        from skitter.config import load_db_config

        config_file = tmp_path / "config.yaml"
        config_file.write_text("default_runtime: claude\n")
        with patch("skitter.config.CONFIG_FILE", config_file):
            cfg = load_db_config()
        assert cfg.backend == "sqlite"
        assert "skitter.db" in cfg.sqlite_path

    def test_load_db_config_custom(self, tmp_path):
        from skitter.config import load_db_config

        config_file = tmp_path / "config.yaml"
        config_file.write_text(
            "db:\n  backend: postgres\n  postgres_dsn: postgresql://localhost/skitter\n"
        )
        with patch("skitter.config.CONFIG_FILE", config_file):
            cfg = load_db_config()
        assert cfg.backend == "postgres"
        assert cfg.postgres_dsn == "postgresql://localhost/skitter"

    def test_load_llm_config_default(self, tmp_path):
        from skitter.config import load_llm_config

        config_file = tmp_path / "config.yaml"
        config_file.write_text("")
        with patch("skitter.config.CONFIG_FILE", config_file):
            cfg = load_llm_config()
        assert cfg.model == ""

    def test_load_llm_config_custom(self, tmp_path):
        from skitter.config import load_llm_config

        config_file = tmp_path / "config.yaml"
        config_file.write_text("llm:\n  model: claude-haiku-4-5-20251001\n")
        with patch("skitter.config.CONFIG_FILE", config_file):
            cfg = load_llm_config()
        assert cfg.model == "claude-haiku-4-5-20251001"


class TestLLMComplete:
    def _mock_response(self, content="test response"):
        from unittest.mock import AsyncMock

        mock_resp = MagicMock()
        mock_resp.choices = [MagicMock()]
        mock_resp.choices[0].message.content = content
        mock_ac = AsyncMock(return_value=mock_resp)
        return mock_ac

    @pytest.mark.asyncio
    async def test_complete_calls_litellm(self):
        from skitter.llm import complete

        mock_ac = self._mock_response("test response")
        with patch("litellm.acompletion", mock_ac):
            result = await complete("hello", model="test-model")

        assert result == "test response"
        mock_ac.assert_called_once()
        assert mock_ac.call_args.kwargs["model"] == "test-model"
        msgs = mock_ac.call_args.kwargs["messages"]
        assert len(msgs) == 1
        assert msgs[0]["role"] == "user"

    @pytest.mark.asyncio
    async def test_complete_with_system(self):
        from skitter.llm import complete

        mock_ac = self._mock_response("ok")
        with patch("litellm.acompletion", mock_ac):
            await complete("hello", system="be helpful", model="test-model")

        msgs = mock_ac.call_args.kwargs["messages"]
        assert len(msgs) == 2
        assert msgs[0]["role"] == "system"
        assert msgs[1]["role"] == "user"

    @pytest.mark.asyncio
    async def test_complete_no_model_raises(self):
        from skitter.llm import complete

        with (
            patch("skitter.llm.load_llm_config", return_value=MagicMock(model="")),
            patch.dict("os.environ", {"SKITTER_LLM_MODEL": ""}),
        ):
            with pytest.raises(ValueError, match="No LLM model configured"):
                await complete("hello")

    @pytest.mark.asyncio
    async def test_complete_none_content_raises(self):
        from skitter.llm import complete

        mock_ac = self._mock_response(None)
        with patch("litellm.acompletion", mock_ac):
            with pytest.raises(ValueError, match="no text content"):
                await complete("hello", model="test-model")


# --- Graph generation and validation ---


class TestGraphValidation:
    def test_valid_graph(self):
        from skitter.graph_gen import validate_graph

        graph = {
            "tasks": [
                {
                    "id": "read",
                    "agent": "reader",
                    "description": "Read data",
                    "needs": [],
                    "next": "analyze",
                },
                {
                    "id": "analyze",
                    "agent": "analyzer",
                    "description": "Analyze data",
                    "needs": ["read"],
                    "next": "output",
                },
            ]
        }
        validate_graph(graph, {"reader", "analyzer"})  # should not raise

    def test_empty_tasks(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        with pytest.raises(GraphValidationError, match="non-empty"):
            validate_graph({"tasks": []}, {"a"})

    def test_unknown_agent(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [{"id": "t1", "agent": "unknown", "needs": [], "next": "output"}]
        }
        with pytest.raises(GraphValidationError, match="unknown agent"):
            validate_graph(graph, {"reader"})

    def test_duplicate_task_id(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "t1", "agent": "a", "needs": [], "next": "output"},
                {"id": "t1", "agent": "a", "needs": [], "next": "output"},
            ]
        }
        with pytest.raises(GraphValidationError, match="Duplicate"):
            validate_graph(graph, {"a"})

    def test_cycle_detected(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "a", "agent": "x", "needs": ["b"], "next": "b"},
                {"id": "b", "agent": "y", "needs": ["a"], "next": "output"},
            ]
        }
        with pytest.raises(GraphValidationError, match="Cycle"):
            validate_graph(graph, {"x", "y"})

    def test_no_terminal(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "t1", "agent": "a", "needs": [], "next": "t1"},
            ]
        }
        with pytest.raises(GraphValidationError, match="terminal"):
            validate_graph(graph, {"a"})

    def test_unknown_need(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "t1", "agent": "a", "needs": ["nonexistent"], "next": "output"},
            ]
        }
        with pytest.raises(GraphValidationError, match="unknown task"):
            validate_graph(graph, {"a"})

    def test_invalid_next_reference(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "t1", "agent": "a", "needs": [], "next": "nonexistent"},
            ]
        }
        with pytest.raises(GraphValidationError, match="not a valid task ID"):
            validate_graph(graph, {"a"})


class TestGraphGeneration:
    def _make_cards(self):
        return [
            {
                "name": "Reader",
                "description": "Reads sensor data",
                "skills": [{"id": "reader", "name": "Reader"}],
            },
            {
                "name": "Analyzer",
                "description": "Analyzes data",
                "skills": [{"id": "analyzer", "name": "Analyzer"}],
            },
        ]

    @pytest.mark.asyncio
    async def test_generate_valid_graph(self):
        from unittest.mock import AsyncMock

        from skitter.graph_gen import generate_graph

        valid_graph = json.dumps(
            {
                "tasks": [
                    {
                        "id": "read",
                        "agent": "reader",
                        "description": "Read sensor data",
                        "needs": [],
                        "next": "analyze",
                    },
                    {
                        "id": "analyze",
                        "agent": "analyzer",
                        "description": "Analyze the data",
                        "needs": ["read"],
                        "next": "output",
                    },
                ]
            }
        )

        with patch("skitter.graph_gen.complete", new_callable=AsyncMock) as mock_llm:
            mock_llm.return_value = valid_graph
            graph = await generate_graph(
                "Read and analyze sensors", self._make_cards(), model="test"
            )

        assert len(graph["tasks"]) == 2
        assert graph["tasks"][0]["agent"] == "reader"

    @pytest.mark.asyncio
    async def test_generate_strips_markdown_fences(self):
        from unittest.mock import AsyncMock

        from skitter.graph_gen import generate_graph

        fenced = '```json\n{"tasks": [{"id": "t1", "agent": "reader", "description": "do it", "needs": [], "next": "output"}]}\n```'

        with patch("skitter.graph_gen.complete", new_callable=AsyncMock) as mock_llm:
            mock_llm.return_value = fenced
            graph = await generate_graph("Do it", self._make_cards(), model="test")

        assert len(graph["tasks"]) == 1

    @pytest.mark.asyncio
    async def test_generate_retries_on_validation_error(self):
        from unittest.mock import AsyncMock

        from skitter.graph_gen import generate_graph

        bad_graph = json.dumps(
            {
                "tasks": [
                    {
                        "id": "t1",
                        "agent": "nonexistent",
                        "needs": [],
                        "next": "output",
                    }
                ]
            }
        )
        good_graph = json.dumps(
            {
                "tasks": [
                    {
                        "id": "t1",
                        "agent": "reader",
                        "description": "Read",
                        "needs": [],
                        "next": "output",
                    }
                ]
            }
        )

        with patch("skitter.graph_gen.complete", new_callable=AsyncMock) as mock_llm:
            mock_llm.side_effect = [bad_graph, good_graph]
            graph = await generate_graph("Read", self._make_cards(), model="test")

        assert mock_llm.call_count == 2
        assert graph["tasks"][0]["agent"] == "reader"

    @pytest.mark.asyncio
    async def test_generate_fails_after_retries(self):
        from unittest.mock import AsyncMock

        from skitter.graph_gen import GraphValidationError, generate_graph

        bad_graph = json.dumps(
            {
                "tasks": [
                    {
                        "id": "t1",
                        "agent": "nonexistent",
                        "needs": [],
                        "next": "output",
                    }
                ]
            }
        )

        with patch("skitter.graph_gen.complete", new_callable=AsyncMock) as mock_llm:
            mock_llm.return_value = bad_graph
            with pytest.raises(GraphValidationError, match="unknown agent"):
                await generate_graph("Read", self._make_cards(), model="test")

        assert mock_llm.call_count == 2


# --- Runtime API ---


class TestRuntimeApi:
    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    def _populate(self):
        from skitter.db import App, AppVersion, DBSession, DBTask

        self.db.create_app(App(id="app1", name="App One", description="First app"))
        self.db.create_app_version(
            AppVersion(
                id="app1-v1", app_id="app1", version=1, graph_json='{"tasks":[]}'
            )
        )
        self.db.create_app_version(
            AppVersion(
                id="app1-v2", app_id="app1", version=2, graph_json='{"tasks":[]}'
            )
        )
        self.db.create_session(
            DBSession(id="s1", app_version_id="app1-v2", state="running")
        )
        self.db.create_task(
            DBTask(
                id="s1/research",
                session_id="s1",
                task_id="research",
                agent="researcher",
                state="pending",
            )
        )
        self.db.update_task("s1/research", state="completed", result="found stuff")
        self.db.create_task(
            DBTask(
                id="s1/review",
                session_id="s1",
                task_id="review",
                agent="writer",
                state="running",
            )
        )

    def test_list_apps(self):
        from skitter.runtime_api import handle_query

        self._populate()
        result = json.loads(handle_query(self.db, "list apps"))
        assert len(result["apps"]) == 1
        assert result["apps"][0]["id"] == "app1"
        assert result["apps"][0]["current_version"] == 2

    def test_list_apps_empty(self):
        from skitter.runtime_api import handle_query

        result = json.loads(handle_query(self.db, "list apps"))
        assert result["apps"] == []

    def test_get_app(self):
        from skitter.runtime_api import handle_query

        self._populate()
        result = json.loads(handle_query(self.db, "get app app1"))
        assert result["id"] == "app1"
        assert result["name"] == "App One"
        assert len(result["versions"]) == 2
        assert result["versions"][0]["version"] == 1
        assert result["versions"][1]["version"] == 2

    def test_get_app_not_found(self):
        from skitter.runtime_api import handle_query

        result = json.loads(handle_query(self.db, "get app nonexistent"))
        assert "error" in result

    def test_list_sessions(self):
        from skitter.runtime_api import handle_query

        self._populate()
        result = json.loads(handle_query(self.db, "list sessions"))
        assert len(result["sessions"]) == 1
        assert result["sessions"][0]["id"] == "s1"
        assert result["sessions"][0]["state"] == "running"

    def test_list_sessions_by_app(self):
        from skitter.runtime_api import handle_query

        self._populate()
        result = json.loads(handle_query(self.db, "list sessions app1"))
        assert len(result["sessions"]) == 1

        result = json.loads(handle_query(self.db, "list sessions nonexistent"))
        assert result["sessions"] == []

    def test_get_session(self):
        from skitter.runtime_api import handle_query

        self._populate()
        result = json.loads(handle_query(self.db, "get session s1"))
        assert result["id"] == "s1"
        assert result["state"] == "running"
        assert len(result["tasks"]) == 2
        task_ids = {t["task_id"] for t in result["tasks"]}
        assert task_ids == {"research", "review"}
        research = next(t for t in result["tasks"] if t["task_id"] == "research")
        assert research["state"] == "completed"
        assert research["result"] == "found stuff"

    def test_get_session_not_found(self):
        from skitter.runtime_api import handle_query

        result = json.loads(handle_query(self.db, "get session nonexistent"))
        assert "error" in result

    def test_cancel_session(self):
        from skitter.runtime_api import handle_query

        self._populate()
        result = json.loads(handle_query(self.db, "cancel session s1"))
        assert result["cancelled"] == "s1"

        session = self.db.get_session("s1")
        assert session.state == "cancelled"

    def test_cancel_session_not_running(self):
        from skitter.runtime_api import handle_query

        self._populate()
        self.db.update_session_state("s1", "completed")
        result = json.loads(handle_query(self.db, "cancel session s1"))
        assert "error" in result
        assert "not running" in result["error"].lower()

    def test_cancel_session_not_found(self):
        from skitter.runtime_api import handle_query

        result = json.loads(handle_query(self.db, "cancel session nonexistent"))
        assert "error" in result

    def test_unknown_query(self):
        from skitter.runtime_api import handle_query

        result = json.loads(handle_query(self.db, "do something"))
        assert "error" in result

    def test_empty_query(self):
        from skitter.runtime_api import handle_query

        result = json.loads(handle_query(self.db, ""))
        assert "error" in result

    def test_runtime_card(self):
        from skitter.runtime_api import AGENT_ID, runtime_card

        card = runtime_card()
        assert card["name"] == "Skitter Runtime"
        assert card["skills"][0]["id"] == AGENT_ID


class TestSupervisorRuntimeRouting:
    """Test that the supervisor routes runtime queries correctly."""

    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    def test_handle_discovery_skips_runtime(self):
        from skitter.runtime_api import AGENT_ID
        from skitter.supervisor import Supervisor

        sup = Supervisor(self.db)
        # Should not add to registry
        sup.handle_discovery(
            f"$a2a/v1/discovery/skitter/default/{AGENT_ID}",
            b'{"name":"Skitter Runtime"}',
        )
        assert sup.registry.get(AGENT_ID) is None

    @pytest.mark.asyncio
    async def test_publish_event_structure(self):
        """Verify _publish_event builds correct payload."""
        from unittest.mock import AsyncMock, MagicMock

        from skitter.supervisor import Supervisor

        sup = Supervisor(self.db)
        mock_client = MagicMock()
        mock_client.publish = AsyncMock()
        sup._client = mock_client

        await sup._publish_event("task_completed", "sess1", task_id="research")
        mock_client.publish.assert_called_once()
        topic, payload_str = mock_client.publish.call_args.args[:2]
        assert "/event/" in topic
        assert "skitter-runtime" in topic
        payload = json.loads(payload_str)
        assert payload["event"] == "task_completed"
        assert payload["session_id"] == "sess1"
        assert payload["task_id"] == "research"
        assert "timestamp" in payload

    @pytest.mark.asyncio
    async def test_publish_event_no_client(self):
        """No crash when client is None."""
        from skitter.supervisor import Supervisor

        sup = Supervisor(self.db)
        # _client is None by default — should not raise
        await sup._publish_event("session_created", "sess1")


# --- Phase 3.3 Verification ---


class TestRuntimeApiIntegration:
    """Integration tests: create app, run session lifecycle, verify events + queries."""

    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    def _make_supervisor(self):
        from unittest.mock import AsyncMock, MagicMock

        from skitter.supervisor import Supervisor

        sup = Supervisor(self.db)
        mock_client = MagicMock()
        mock_client.publish = AsyncMock()
        mock_client.subscribe = AsyncMock()
        sup._client = mock_client
        return sup, mock_client

    def _create_test_app(self):
        from skitter.apps import create_app

        return create_app(
            self.db,
            app_id="test-app",
            name="Test App",
            description="Integration test app",
            graph={
                "tasks": [
                    {
                        "id": "research",
                        "agent": "researcher",
                        "description": "Do research",
                        "needs": [],
                        "next": "review",
                    },
                    {
                        "id": "review",
                        "agent": "writer",
                        "description": "Review results",
                        "needs": ["research"],
                        "next": "output",
                    },
                ]
            },
        )

    @pytest.mark.asyncio
    async def test_session_lifecycle_events(self):
        """Create app, dispatch session, complete tasks — verify all events."""
        sup, mock_client = self._make_supervisor()
        self._create_test_app()

        # Create session
        req = A2ARequest(text="test request", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        sid = state.session_id

        # Simulate what handle_request does: publish session_created, then dispatch
        await sup._publish_event("session_created", sid)
        await sup.dispatch_ready(state)

        # Collect event payloads from mock
        event_calls = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if "/event/" in str(call.args[0])
        ]
        event_types = [e["event"] for e in event_calls]
        assert "session_created" in event_types
        assert "task_started" in event_types
        # session_created must come before task_started
        assert event_types.index("session_created") < event_types.index("task_started")

        # Simulate research task completion
        mock_client.publish.reset_mock()
        await sup._complete_task(state, "research", "Research findings")

        event_calls = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if "/event/" in str(call.args[0])
        ]
        event_types = [e["event"] for e in event_calls]
        assert "task_completed" in event_types
        # review should now be dispatched
        assert "task_started" in event_types

        # Simulate review task completion (terminal task)
        mock_client.publish.reset_mock()
        await sup._complete_task(state, "review", "Final review")

        event_calls = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if "/event/" in str(call.args[0])
        ]
        event_types = [e["event"] for e in event_calls]
        assert "task_completed" in event_types
        assert "session_completed" in event_types

        # Verify session is cleaned up
        assert sid not in sup._sessions

        # Verify DB state
        db_session = self.db.get_session(sid)
        assert db_session.state == "completed"

    @pytest.mark.asyncio
    async def test_session_failure_events(self):
        """Verify task_failed and session_failed events."""
        sup, mock_client = self._make_supervisor()
        self._create_test_app()

        req = A2ARequest(text="test", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        sid = state.session_id
        await sup.dispatch_ready(state)

        # Fail the research task
        mock_client.publish.reset_mock()
        await sup._fail_task(state, "research", "Agent crashed")

        event_calls = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if "/event/" in str(call.args[0])
        ]
        event_types = [e["event"] for e in event_calls]
        assert "task_failed" in event_types
        assert "session_failed" in event_types

        failed_event = next(e for e in event_calls if e["event"] == "task_failed")
        assert failed_event["task_id"] == "research"
        assert "Agent crashed" in failed_event["data"]["error"]

        # Verify cascade: review should be failed in DB
        review_task = self.db.get_task(f"{sid}/review")
        assert review_task.state == "failed"

    @pytest.mark.asyncio
    async def test_query_via_runtime_handler(self):
        """Verify queries through _handle_runtime_query produce correct A2A replies."""
        sup, mock_client = self._make_supervisor()
        self._create_test_app()

        # Query: list apps
        req = A2ARequest(text="list apps", request_id="q1")
        await sup._handle_runtime_query(req.to_json(), "reply/q", "corr-q1")

        # Find the reply (non-event publish)
        reply_calls = [
            call
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/q"
        ]
        assert len(reply_calls) == 1
        reply_data = json.loads(reply_calls[0].args[1])
        # Should be a TaskStatusUpdateEvent with completed state
        assert reply_data["result"]["status"]["state"] == "completed"
        artifact = reply_data["result"]["artifact"]["parts"][0]["text"]
        result = json.loads(artifact)
        assert len(result["apps"]) == 1
        assert result["apps"][0]["id"] == "test-app"

    @pytest.mark.asyncio
    async def test_query_get_session_via_handler(self):
        """Verify get session query returns task details."""
        sup, mock_client = self._make_supervisor()
        self._create_test_app()

        # Create a session
        req = A2ARequest(text="test", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        sid = state.session_id

        # Query: get session
        mock_client.publish.reset_mock()
        query_req = A2ARequest(text=f"get session {sid}", request_id="q2")
        await sup._handle_runtime_query(query_req.to_json(), "reply/q", "corr-q2")

        reply_calls = [
            call
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/q"
        ]
        assert len(reply_calls) == 1
        reply_data = json.loads(reply_calls[0].args[1])
        artifact = reply_data["result"]["artifact"]["parts"][0]["text"]
        result = json.loads(artifact)
        assert result["id"] == sid
        assert result["state"] == "running"
        assert len(result["tasks"]) == 2

    @pytest.mark.asyncio
    async def test_cancel_via_handler_cleans_up(self):
        """Verify cancel session cleans up DB + in-memory state."""
        sup, mock_client = self._make_supervisor()
        self._create_test_app()

        req = A2ARequest(text="test", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/caller",
            caller_correlation="corr-caller",
        )
        sid = state.session_id
        await sup.dispatch_ready(state)
        assert sid in sup._sessions

        # Cancel via runtime query
        mock_client.publish.reset_mock()
        cancel_req = A2ARequest(text=f"cancel session {sid}", request_id="q3")
        await sup._handle_runtime_query(cancel_req.to_json(), "reply/q", "corr-q3")

        # Session removed from memory
        assert sid not in sup._sessions

        # DB state is cancelled
        db_session = self.db.get_session(sid)
        assert db_session.state == "cancelled"

        # Tasks are cancelled in DB
        tasks = self.db.list_tasks(sid)
        for t in tasks:
            if t.state not in ("completed",):
                assert t.state == "cancelled"

        # Original caller was notified
        caller_notified = any(
            str(call.args[0]) == "reply/caller"
            for call in mock_client.publish.call_args_list
        )
        assert caller_notified

        # Query caller got a reply
        query_reply = [
            call
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/q"
        ]
        assert len(query_reply) == 1
