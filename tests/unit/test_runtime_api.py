"""Runtime API unit tests."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from skitter.a2a import A2ARequest


# --- App creation ---


class TestAppCreation:
    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    def test_create_app(self):
        from skitter.runtime_api import create_app

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
                        "terminal": True,
                    }
                ]
            },
        )
        assert app is not None
        assert version.version == 1
        assert app.card_json != ""
        card = json.loads(card_json)
        assert card["name"] == "Test App"
        wf = next(
            e
            for e in card["capabilities"]["extensions"]
            if e["uri"] == "urn:skitter:app"
        )
        assert len(wf["params"]["tasks"]) == 1

    def test_provided_app_id(self):
        from skitter.runtime_api import create_app

        app_id = "predefined_id"
        app, version, card_json = create_app(
            self.db,
            app_id=app_id,
            name="Test App",
            description="A test",
            graph={
                "tasks": [
                    {
                        "id": "t1",
                        "agent": "researcher",
                        "description": "do stuff",
                        "needs": [],
                        "terminal": True,
                    }
                ]
            },
        )
        assert app is not None
        assert app.id == app_id

    def test_version_increment(self):
        from skitter.runtime_api import create_app

        app1, v1, _ = create_app(
            self.db, app_id="my-app", name="App", graph={"tasks": []}
        )
        _, v2, _ = create_app(self.db, app_id="my-app", name="App", graph={"tasks": []})
        assert v1.version == 1
        assert v2.version == 2


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
            DBSession(
                id="s1",
                app_version_id="app1-v2",
                request_task_id="rtid-s1",
                state="running",
            )
        )
        self.db.create_task(
            DBTask(
                id="s1/research",
                session_id="s1",
                node_id="research",
                agent="researcher",
                state="pending",
            )
        )
        self.db.update_task("s1/research", state="completed", result="found stuff")
        self.db.create_task(
            DBTask(
                id="s1/review",
                session_id="s1",
                node_id="review",
                agent="writer",
                state="running",
            )
        )

    @pytest.mark.asyncio
    async def test_list_apps(self):
        from skitter.runtime_api import handle_query

        self._populate()
        result = (await handle_query(self.db, "list apps")).to_dict()
        assert len(result["apps"]) == 1
        assert result["apps"][0]["id"] == "app1"
        assert result["apps"][0]["current_version"] == 2

    @pytest.mark.asyncio
    async def test_list_apps_empty(self):
        from skitter.runtime_api import handle_query

        result = (await handle_query(self.db, "list apps")).to_dict()
        assert result["apps"] == []

    @pytest.mark.asyncio
    async def test_get_app(self):
        from skitter.runtime_api import handle_query

        self._populate()
        result = (await handle_query(self.db, "get app app1")).to_dict()
        assert result["id"] == "app1"
        assert result["name"] == "App One"
        assert len(result["versions"]) == 2
        assert result["versions"][0]["version"] == 1
        assert result["versions"][1]["version"] == 2

    @pytest.mark.asyncio
    async def test_get_app_not_found(self):
        from skitter.runtime_api import ErrorResult, handle_query

        result = await handle_query(self.db, "get app nonexistent")
        assert isinstance(result, ErrorResult)

    @pytest.mark.asyncio
    async def test_list_sessions(self):
        from skitter.runtime_api import handle_query

        self._populate()
        result = (await handle_query(self.db, "list sessions")).to_dict()
        assert len(result["sessions"]) == 1
        assert result["sessions"][0]["id"] == "s1"
        assert result["sessions"][0]["state"] == "running"

    @pytest.mark.asyncio
    async def test_list_sessions_by_app(self):
        from skitter.runtime_api import handle_query

        self._populate()
        result = (await handle_query(self.db, "list sessions app1")).to_dict()
        assert len(result["sessions"]) == 1

        result = (await handle_query(self.db, "list sessions nonexistent")).to_dict()
        assert result["sessions"] == []

    @pytest.mark.asyncio
    async def test_get_session(self):
        from skitter.runtime_api import handle_query

        self._populate()
        result = (await handle_query(self.db, "get session s1")).to_dict()
        assert result["id"] == "s1"
        assert result["state"] == "running"
        assert len(result["tasks"]) == 2
        node_ids = {t["node_id"] for t in result["tasks"]}
        assert node_ids == {"research", "review"}
        research = next(t for t in result["tasks"] if t["node_id"] == "research")
        assert research["state"] == "completed"
        assert research["result"] == "found stuff"

    @pytest.mark.asyncio
    async def test_get_session_by_request_task_id(self):
        """get session must resolve by request_task_id (what callers know)."""
        from skitter.runtime_api import handle_query

        self._populate()
        result = (await handle_query(self.db, "get session rtid-s1")).to_dict()
        assert result["id"] == "s1"
        assert result["state"] == "running"

    @pytest.mark.asyncio
    async def test_get_session_not_found(self):
        from skitter.runtime_api import ErrorResult, handle_query

        result = await handle_query(self.db, "get session nonexistent")
        assert isinstance(result, ErrorResult)

    @pytest.mark.asyncio
    async def test_cancel_session(self):
        from skitter.runtime_api import CancelSessionResult, handle_query

        self._populate()
        result = await handle_query(self.db, "cancel session s1")
        assert isinstance(result, CancelSessionResult)
        assert result.session_id == "s1"

        session = self.db.get_session("s1")
        assert session.state == "canceled"

    @pytest.mark.asyncio
    async def test_cancel_session_by_request_task_id(self):
        """cancel session must resolve by request_task_id."""
        from skitter.runtime_api import CancelSessionResult, handle_query

        self._populate()
        result = await handle_query(self.db, "cancel session rtid-s1")
        assert isinstance(result, CancelSessionResult)
        assert result.session_id == "s1"

        session = self.db.get_session("s1")
        assert session.state == "canceled"

    @pytest.mark.asyncio
    async def test_cancel_session_not_running(self):
        from skitter.runtime_api import ErrorResult, handle_query

        self._populate()
        self.db.update_session_state("s1", "completed")
        result = await handle_query(self.db, "cancel session s1")
        assert isinstance(result, ErrorResult)
        assert "not running" in result.message.lower()

    @pytest.mark.asyncio
    async def test_cancel_session_not_found(self):
        from skitter.runtime_api import ErrorResult, handle_query

        result = await handle_query(self.db, "cancel session nonexistent")
        assert isinstance(result, ErrorResult)

    @pytest.mark.asyncio
    async def test_unknown_query(self):
        from skitter.runtime_api import ErrorResult, handle_query

        result = await handle_query(self.db, "do something")
        assert isinstance(result, ErrorResult)

    @pytest.mark.asyncio
    async def test_empty_query(self):
        from skitter.runtime_api import ErrorResult, handle_query

        result = await handle_query(self.db, "")
        assert isinstance(result, ErrorResult)

    def test_coordinator_card(self):
        from skitter.runtime_api import coordinator_card

        card = coordinator_card()
        assert card["name"] == "Skitter"
        assert card["skills"][0]["id"] == "default"

    @pytest.mark.asyncio
    async def test_create_app(self):

        from skitter.runtime_api import CreateAppResult, handle_query
        from skitter.coordinator import DiscoveryRegistry

        registry = DiscoveryRegistry()
        registry.update(
            "reader",
            {
                "name": "Reader",
                "description": "Reads data",
                "skills": [{"id": "reader", "name": "Reader"}],
            },
        )
        registry.update(
            "analyzer",
            {
                "name": "Analyzer",
                "description": "Analyzes data",
                "skills": [{"id": "analyzer", "name": "Analyzer"}],
            },
        )

        graph = {
            "tasks": [
                {
                    "id": "read",
                    "agent": "reader",
                    "description": "Read",
                    "needs": [],
                },
                {
                    "id": "analyze",
                    "agent": "analyzer",
                    "description": "Analyze",
                    "needs": ["read"],
                    "terminal": True,
                },
            ]
        }

        spec = json.dumps(
            {
                "name": "Test App",
                "description": "A test",
                "instructions": "Read then analyze",
                "agents": ["reader", "analyzer"],
            }
        )

        with patch(
            "skitter.runtime_api.generate_graph", new_callable=AsyncMock
        ) as mock_gen:
            mock_gen.return_value = graph
            result = await handle_query(self.db, f"create app {spec}", registry)

        assert isinstance(result, CreateAppResult)
        assert result.version == 1
        assert result.card_json

        # Verify DB state
        app = self.db.get_app(result.app_id)
        assert app is not None
        assert app.name == "Test App"

    @pytest.mark.asyncio
    async def test_create_app_missing_agent(self):
        from skitter.runtime_api import ErrorResult, handle_query
        from skitter.coordinator import DiscoveryRegistry

        registry = DiscoveryRegistry()
        registry.update(
            "reader",
            {"name": "Reader", "skills": [{"id": "reader"}]},
        )

        spec = json.dumps(
            {
                "name": "Test",
                "instructions": "Do stuff",
                "agents": ["reader", "missing-agent"],
            }
        )
        result = await handle_query(self.db, f"create app {spec}", registry)
        assert isinstance(result, ErrorResult)
        assert "missing-agent" in result.message

    @pytest.mark.asyncio
    async def test_create_app_no_registry(self):
        from skitter.runtime_api import ErrorResult, handle_query

        spec = json.dumps({"name": "Test", "instructions": "Do stuff", "agents": ["a"]})
        result = await handle_query(self.db, f"create app {spec}")
        assert isinstance(result, ErrorResult)
        assert "registry" in result.message.lower()


class TestCoordinatorRuntimeRouting:
    """Test that the coordinator routes runtime queries correctly."""

    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    def test_handle_discovery_skips_runtime(self):
        from skitter.runtime_api import AGENT_ID
        from skitter.coordinator import Coordinator

        sup = Coordinator(self.db)
        # Should not add to registry
        sup.handle_discovery(
            f"$a2a/v1/discovery/skitter/default/{AGENT_ID}",
            b'{"name":"Skitter Runtime"}',
        )
        assert sup.registry.get(AGENT_ID) is None

    @pytest.mark.asyncio
    async def test_publish_event_structure(self):
        """Verify _publish_event builds correct payload."""

        from skitter.coordinator import Coordinator

        sup = Coordinator(self.db)
        mock_client = MagicMock()
        mock_client.publish = AsyncMock()
        sup._client = mock_client

        await sup._publish_event("task_completed", "sess1", task_id="research")
        mock_client.publish.assert_called_once()
        topic, payload_str = mock_client.publish.call_args.args[:2]
        assert "/event/" in topic
        assert "/skitter" in topic
        payload = json.loads(payload_str)
        assert payload["event"] == "task_completed"
        assert payload["session_id"] == "sess1"
        assert payload["task_id"] == "research"
        assert "timestamp" in payload

    @pytest.mark.asyncio
    async def test_publish_event_no_client(self):
        """No crash when client is None."""
        from skitter.coordinator import Coordinator

        sup = Coordinator(self.db)
        # _client is None by default — should not raise
        await sup._publish_event("session_created", "sess1")


class TestRuntimeApiIntegration:
    """Integration tests: create app, run session lifecycle, verify events + queries."""

    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

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

    def _create_test_app(self):
        from skitter.runtime_api import create_app

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
                    },
                    {
                        "id": "review",
                        "agent": "writer",
                        "description": "Review results",
                        "needs": ["research"],
                        "terminal": True,
                    },
                ]
            },
        )

    @pytest.mark.asyncio
    async def test_session_lifecycle_events(self):
        """Create app, dispatch session, complete tasks — verify all events."""
        sup, mock_client = self._make_coordinator()
        self._create_test_app()

        # Create session
        req = A2ARequest(text="test request", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = await sup.create_session_from_graph(
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
        sup, mock_client = self._make_coordinator()
        self._create_test_app()

        req = A2ARequest(text="test", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = await sup.create_session_from_graph(
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
        sup, mock_client = self._make_coordinator()
        self._create_test_app()

        # Query: list apps
        req = A2ARequest(text="list apps", request_id="q1")
        await sup._handle_runtime_query(req, "reply/q", "corr-q1")

        # Find the replies (artifact event + status event)
        reply_calls = [
            call
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/q"
        ]
        assert len(reply_calls) == 2
        parsed = [json.loads(c.args[1]) for c in reply_calls]
        artifact_reply = next(r for r in parsed if "artifactUpdate" in r["result"])
        status_reply = next(r for r in parsed if "statusUpdate" in r["result"])
        su = status_reply["result"]["statusUpdate"]
        assert su["status"]["state"] == "TASK_STATE_COMPLETED"
        au = artifact_reply["result"]["artifactUpdate"]
        artifact = au["artifact"]["parts"][0]["text"]
        result = json.loads(artifact)
        assert len(result["apps"]) == 1
        assert result["apps"][0]["id"] == "test-app"

    @pytest.mark.asyncio
    async def test_query_get_session_via_handler(self):
        """Verify get session query returns task details."""
        sup, mock_client = self._make_coordinator()
        self._create_test_app()

        # Create a session
        req = A2ARequest(text="test", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = await sup.create_session_from_graph(
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
        await sup._handle_runtime_query(query_req, "reply/q", "corr-q2")

        reply_calls = [
            call
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/q"
        ]
        assert len(reply_calls) == 2
        parsed = [json.loads(c.args[1]) for c in reply_calls]
        artifact_reply = next(r for r in parsed if "artifactUpdate" in r["result"])
        au = artifact_reply["result"]["artifactUpdate"]
        artifact = au["artifact"]["parts"][0]["text"]
        result = json.loads(artifact)
        assert result["id"] == sid
        assert result["state"] == "running"
        assert len(result["tasks"]) == 2

    @pytest.mark.asyncio
    async def test_cancel_via_handler_cleans_up(self):
        """Verify cancel session cleans up DB + in-memory state."""
        sup, mock_client = self._make_coordinator()
        self._create_test_app()

        req = A2ARequest(text="test", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = await sup.create_session_from_graph(
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
        await sup._handle_runtime_query(cancel_req, "reply/q", "corr-q3")

        # Session removed from memory
        assert sid not in sup._sessions

        # DB state is canceled
        db_session = self.db.get_session(sid)
        assert db_session.state == "canceled"

        # Tasks are canceled in DB
        tasks = self.db.list_tasks(sid)
        for t in tasks:
            if t.state not in ("completed",):
                assert t.state == "canceled"

        # Original caller was notified
        caller_notified = any(
            str(call.args[0]) == "reply/caller"
            for call in mock_client.publish.call_args_list
        )
        assert caller_notified

        # Query caller got a reply (artifact + status)
        query_reply = [
            call
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/q"
        ]
        assert len(query_reply) == 2

    @pytest.mark.asyncio
    async def test_create_app_subscribes_and_publishes(self):
        """Verify that creating an app opens a dedicated connection, subscribes, and publishes card."""
        from unittest.mock import patch

        sup, mock_client = self._make_coordinator()

        # Register agents in discovery
        sup.registry.update(
            "reader",
            {
                "name": "Reader",
                "description": "Reads data",
                "skills": [{"id": "reader", "name": "Reader"}],
            },
        )
        sup.registry.update(
            "analyzer",
            {
                "name": "Analyzer",
                "description": "Analyzes data",
                "skills": [{"id": "analyzer", "name": "Analyzer"}],
            },
        )

        graph = {
            "tasks": [
                {
                    "id": "read",
                    "agent": "reader",
                    "description": "Read",
                    "needs": [],
                },
                {
                    "id": "analyze",
                    "agent": "analyzer",
                    "description": "Analyze",
                    "needs": ["read"],
                    "terminal": True,
                },
            ]
        }

        spec = json.dumps(
            {
                "name": "Test App",
                "description": "A test",
                "instructions": "Read then analyze",
                "agents": ["reader", "analyzer"],
            }
        )

        req = A2ARequest(text=f"create app {spec}", request_id="q1")

        # Mock aiomqtt.Client so _start_app_connection doesn't open a real connection
        mock_app_client = MagicMock()
        mock_app_client.publish = AsyncMock()
        mock_app_client.subscribe = AsyncMock()
        mock_app_client.__aenter__ = AsyncMock(return_value=mock_app_client)
        mock_app_client.__aexit__ = AsyncMock(return_value=False)
        mock_app_client.messages = AsyncMock()

        with (
            patch(
                "skitter.runtime_api.generate_graph", new_callable=AsyncMock
            ) as mock_gen,
            patch(
                "skitter.coordinator.service.aiomqtt.Client",
                return_value=mock_app_client,
            ),
        ):
            mock_gen.return_value = graph
            await sup._handle_runtime_query(req, "reply/q", "corr-q1")

        # Dedicated client should have subscribed to the app's request topic
        subscribe_calls = [
            str(call.args[0]) for call in mock_app_client.subscribe.call_args_list
        ]
        app_request_topics = [t for t in subscribe_calls if "/request/" in t]
        assert len(app_request_topics) == 1

        # Dedicated client should have published the discovery card (retained)
        publish_calls = mock_app_client.publish.call_args_list
        discovery_publishes = [
            c for c in publish_calls if "/discovery/" in str(c.args[0])
        ]
        assert len(discovery_publishes) >= 1

        # Clean up the background task
        for task in sup._app_tasks.values():
            task.cancel()

    @pytest.mark.asyncio
    async def test_delete_app(self):
        """Verify delete app removes from DB and replies with deleted_app."""
        sup, mock_client = self._make_coordinator()
        self._create_test_app()
        assert self.db.get_app("test-app") is not None

        req = A2ARequest(text="delete app test-app", request_id="q1")
        await sup._handle_runtime_query(req, "reply/q", "corr-q1")

        # App deleted from DB
        assert self.db.get_app("test-app") is None

        # Reply contains deleted_app (artifact + status)
        reply_calls = [
            call
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/q"
        ]
        assert len(reply_calls) == 2
        artifact_call = next(
            c for c in reply_calls if "artifactUpdate" in str(c.args[1])
        )
        reply_data = json.loads(artifact_call.args[1])
        au = reply_data["result"]["artifactUpdate"]
        artifact = au["artifact"]["parts"][0]["text"]
        result = json.loads(artifact)
        assert result["deleted_app"] == "test-app"

    @pytest.mark.asyncio
    async def test_delete_app_with_running_sessions(self):
        """Verify delete app fails when sessions are running."""
        sup, mock_client = self._make_coordinator()
        self._create_test_app()

        # Create a running session
        req = A2ARequest(text="test", request_id="r1")
        version = self.db.get_current_version("test-app")
        await sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )

        mock_client.publish.reset_mock()
        del_req = A2ARequest(text="delete app test-app", request_id="q1")
        await sup._handle_runtime_query(del_req, "reply/q", "corr-q1")

        # App still exists
        assert self.db.get_app("test-app") is not None

        # Reply contains error
        reply_calls = [
            call
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/q"
        ]
        reply_data = json.loads(reply_calls[0].args[1])
        au = reply_data["result"]["artifactUpdate"]
        artifact = au["artifact"]["parts"][0]["text"]
        result = json.loads(artifact)
        assert "running session" in result["error"]

    @pytest.mark.asyncio
    async def test_delete_nonexistent_app(self):
        """Verify delete app returns error for unknown app."""
        sup, mock_client = self._make_coordinator()

        req = A2ARequest(text="delete app no-such-app", request_id="q1")
        await sup._handle_runtime_query(req, "reply/q", "corr-q1")

        reply_calls = [
            call
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/q"
        ]
        reply_data = json.loads(reply_calls[0].args[1])
        au = reply_data["result"]["artifactUpdate"]
        artifact = au["artifact"]["parts"][0]["text"]
        result = json.loads(artifact)
        assert "not found" in result["error"].lower()
