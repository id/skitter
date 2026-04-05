"""Database unit tests."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from skitter.a2a import A2ARequest


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
        self.db.create_session(
            DBSession(
                id="s1",
                app_version_id="v1",
                request_task_id="rtid-s1",
                state="running",
            )
        )
        self.db.create_task(
            DBTask(
                id="s1/t1",
                session_id="s1",
                node_id="t1",
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
        self.db.create_session(
            DBSession(id="s1", app_version_id="v1", request_task_id="rtid-s1")
        )
        self.db.create_task(
            DBTask(id="s1/t1", session_id="s1", node_id="t1", agent="r")
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
        self.db.create_session(
            DBSession(id="s1", app_version_id="v1", request_task_id="rtid-s1")
        )
        self.db.create_session(
            DBSession(id="s2", app_version_id="v2", request_task_id="rtid-s2")
        )

        all_sessions = self.db.list_sessions()
        assert len(all_sessions) == 2

        a1_sessions = self.db.list_sessions(app_id="a1")
        assert len(a1_sessions) == 1
        assert a1_sessions[0].id == "s1"


class TestContextSessions:
    """Test list_context_sessions DB query for app conversation continuity."""

    def setup_method(self):
        from skitter.db import App, AppVersion, SqliteDB

        self.db = SqliteDB(":memory:")
        self.db.create_app(App(id="app1", name="App1"))
        self.db.create_app(App(id="app2", name="App2"))
        self.db.create_app_version(AppVersion(id="v1", app_id="app1", version=1))
        self.db.create_app_version(AppVersion(id="v2", app_id="app2", version=1))

    def teardown_method(self):
        self.db.close()

    def _add_session(self, sid, app_version_id, context_id, state="completed"):
        from skitter.db import DBSession

        self.db.create_session(
            DBSession(
                id=sid,
                app_version_id=app_version_id,
                request_task_id=f"rtid-{sid}",
                context_id=context_id,
                request_json='{"params":{"message":{"parts":[{"text":"hello"}]}}}',
            )
        )
        if state != "running":
            self.db.update_session_state(sid, state)

    def test_returns_completed_sessions_for_app_context(self):
        self._add_session("s1", "v1", "ctx-1", state="completed")
        self._add_session("s2", "v1", "ctx-1", state="completed")
        self._add_session("s3", "v1", "ctx-1", state="running")  # excluded

        result = self.db.list_context_sessions("app1", "ctx-1")
        assert len(result) == 2
        assert [s.id for s in result] == ["s1", "s2"]

    def test_isolates_by_context_id(self):
        self._add_session("s1", "v1", "ctx-1")
        self._add_session("s2", "v1", "ctx-2")

        result = self.db.list_context_sessions("app1", "ctx-1")
        assert len(result) == 1
        assert result[0].id == "s1"

    def test_isolates_by_app_id(self):
        self._add_session("s1", "v1", "ctx-1")
        self._add_session("s2", "v2", "ctx-1")  # different app

        result = self.db.list_context_sessions("app1", "ctx-1")
        assert len(result) == 1
        assert result[0].id == "s1"

    def test_respects_limit(self):
        for i in range(5):
            self._add_session(f"s{i}", "v1", "ctx-1")

        result = self.db.list_context_sessions("app1", "ctx-1", limit=3)
        assert len(result) == 3

    def test_limit_returns_newest_sessions_in_order(self):
        """Regression: limit must return the newest N sessions, not oldest."""
        for i in range(12):
            self._add_session(f"s{i:02d}", "v1", "ctx-1")

        result = self.db.list_context_sessions("app1", "ctx-1", limit=3)
        ids = [s.id for s in result]
        # Newest 3, ascending order
        assert ids == ["s09", "s10", "s11"]

    def test_empty_for_no_matches(self):
        result = self.db.list_context_sessions("app1", "ctx-none")
        assert result == []


class TestConversationHistory:
    """Test coordinator conversation history injection."""

    def setup_method(self):
        from skitter.db import App, AppVersion, DBSession, DBTask, SqliteDB

        self.db = SqliteDB(":memory:")
        self.db.create_app(App(id="app1", name="App1"))
        self.db.create_app_version(
            AppVersion(
                id="v1",
                app_id="app1",
                version=1,
                graph_json=json.dumps(
                    {
                        "tasks": [
                            {
                                "id": "step",
                                "agent": "researcher",
                                "description": "Do it",
                                "needs": [],
                                "terminal": True,
                            }
                        ]
                    }
                ),
            )
        )
        # Create a prior completed session with result
        self.db.create_session(
            DBSession(
                id="prior-1",
                app_version_id="v1",
                request_task_id="rtid-prior-1",
                context_id="ctx-1",
                request_json=A2ARequest(
                    text="what is skitter?", request_id="r0"
                ).to_json(),
            )
        )
        self.db.create_task(
            DBTask(
                id="prior-1/step",
                session_id="prior-1",
                node_id="step",
                agent="researcher",
                terminal="1",
            )
        )
        self.db.update_task(
            "prior-1/step", state="completed", result="Skitter is an MQTT assistant."
        )
        self.db.update_session_state(
            "prior-1", "completed", result="Skitter is an MQTT assistant."
        )

    def teardown_method(self):
        self.db.close()

    def _make_coordinator(self):
        from skitter.coordinator import Coordinator

        sup = Coordinator(self.db)
        mock_client = MagicMock()
        mock_client.publish = AsyncMock()
        mock_client.subscribe = AsyncMock()
        sup._client = mock_client
        return sup, mock_client

    @pytest.mark.asyncio
    async def test_build_conversation_history(self):
        sup, _ = self._make_coordinator()
        history = await sup._build_conversation_history("app1", "ctx-1")
        assert "## Conversation history" in history
        assert "what is skitter?" in history
        assert "Skitter is an MQTT assistant." in history

    @pytest.mark.asyncio
    async def test_empty_history_for_no_context(self):
        sup, _ = self._make_coordinator()
        assert await sup._build_conversation_history("app1", "") == ""

    @pytest.mark.asyncio
    async def test_empty_history_for_unknown_context(self):
        sup, _ = self._make_coordinator()
        assert await sup._build_conversation_history("app1", "ctx-unknown") == ""

    @pytest.mark.asyncio
    async def test_history_injected_in_dispatched_prompt(self):
        sup, mock_client = self._make_coordinator()

        req = A2ARequest(text="tell me more", request_id="r1", context_id="ctx-1")
        version = self.db.get_current_version("app1")

        state = await sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
            app_id="app1",
        )
        state.conversation_history = await sup._build_conversation_history(
            "app1", "ctx-1"
        )
        await sup.dispatch_ready(state)

        dispatched = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if "/request/" in str(call.args[0]) and "researcher" in str(call.args[0])
        ]
        assert len(dispatched) == 1
        prompt = dispatched[0]["params"]["message"]["parts"][0]["text"]
        assert "Conversation history" in prompt
        assert "what is skitter?" in prompt
        assert "Skitter is an MQTT assistant." in prompt
        assert "tell me more" in prompt

    @pytest.mark.asyncio
    async def test_no_history_without_context_id(self):
        sup, mock_client = self._make_coordinator()

        req = A2ARequest(text="fresh start", request_id="r2", context_id="ctx-new")
        version = self.db.get_current_version("app1")

        state = await sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
            app_id="app1",
        )
        await sup.dispatch_ready(state)

        dispatched = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if "/request/" in str(call.args[0]) and "researcher" in str(call.args[0])
        ]
        assert len(dispatched) == 1
        prompt = dispatched[0]["params"]["message"]["parts"][0]["text"]
        assert "Conversation history" not in prompt

    @pytest.mark.asyncio
    async def test_app_id_stored_on_session_state(self):
        sup, _ = self._make_coordinator()
        req = A2ARequest(text="go", request_id="r3")
        version = self.db.get_current_version("app1")
        state = await sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="",
            caller_correlation="",
            app_id="app1",
        )
        assert state.app_id == "app1"


class TestTaskTarget:
    def test_defaults(self):
        from skitter.a2a import TaskTarget

        t = TaskTarget(agent="researcher")
        assert t.mqtt_host == ""
        assert t.mqtt_port == 8883


class TestDBConfig:
    def test_load_config_db_default(self, tmp_path):
        from skitter.config import load_config

        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("default_runtime: claude\n")
        with patch.dict("os.environ", {"SKITTER_HOME": str(tmp_path)}, clear=False):
            cfg = load_config().db
        assert cfg.backend == "sqlite"
        assert "skitter.db" in cfg.sqlite_path

    def test_load_config_db_custom(self, tmp_path):
        from skitter.config import load_config

        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text(
            "db:\n  backend: postgres\n  postgres_dsn: postgresql://localhost/skitter\n"
        )
        with patch.dict("os.environ", {"SKITTER_HOME": str(tmp_path)}, clear=False):
            cfg = load_config().db
        assert cfg.backend == "postgres"
        assert cfg.postgres_dsn == "postgresql://localhost/skitter"

    def test_load_config_llm_default(self, tmp_path):
        from skitter.config import load_config

        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("")
        with patch.dict(
            "os.environ",
            {"SKITTER_HOME": str(tmp_path), "SKITTER_LLM_MODEL": ""},
            clear=False,
        ):
            cfg = load_config().llm
        assert cfg.model == ""

    def test_load_config_llm_custom(self, tmp_path):
        from skitter.config import load_config

        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("llm:\n  model: claude-haiku-4-5-20251001\n")
        with patch.dict(
            "os.environ",
            {"SKITTER_HOME": str(tmp_path), "SKITTER_LLM_MODEL": ""},
            clear=False,
        ):
            cfg = load_config().llm
        assert cfg.model == "claude-haiku-4-5-20251001"
