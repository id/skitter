"""Tests for skitter coordinator architecture."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from skitter.config import AgentDef
from skitter.coordinator import _parse_agent_id_from_topic
from skitter.a2a import (
    A2A_UNIT,
    A2ARequest,
    A2A_REQUEST_EXPIRED,
    A2A_RESPONDER_UNAVAILABLE,
    A2A_TRANSPORT_PROTOCOL_ERROR,
    REPLY_ARTIFACT,
    REPLY_ERROR,
    REPLY_FAILED,
    REPLY_TERMINAL,
    REPLY_TEXT,
    REPLY_TOOL,
    classify_reply,
    make_a2a_error,
    make_artifact_event,
    make_status_event,
    topic_a2a_event,
    topic_discovery_wildcard,
    topic_request,
)


# --- Foundation types ---


class TestA2ARequest:
    def test_roundtrip(self):
        req = A2ARequest(
            text="Research quantum computing",
            request_id="req-abc123",
            task_id="550e8400-e29b-41d4-a716-446655440000",
            sender="cli",
            variables={"topic": "quantum"},
        )
        j = req.to_json()
        d = json.loads(j)
        assert d["jsonrpc"] == "2.0"
        assert d["id"] == "req-abc123"
        assert d["method"] == "message/send"
        msg = d["params"]["message"]
        assert msg["parts"][0]["text"] == "Research quantum computing"
        assert msg["taskId"] == "550e8400-e29b-41d4-a716-446655440000"
        assert "messageId" in msg  # REQUIRED per A2A v1.0.0 proto
        assert d["params"]["metadata"]["sender"] == "cli"
        assert d["params"]["metadata"]["variables"]["topic"] == "quantum"

        restored = A2ARequest.from_json(j)
        assert restored.text == "Research quantum computing"
        assert restored.request_id == "req-abc123"
        assert restored.task_id == "550e8400-e29b-41d4-a716-446655440000"
        assert restored.sender == "cli"
        assert restored.variables == {"topic": "quantum"}

    def test_minimal(self):
        import uuid as uuid_mod

        req = A2ARequest(text="hello", request_id="s1")
        assert req.task_id  # auto-generated UUIDv4
        uuid_mod.UUID(req.task_id)  # must be valid UUID format
        j = req.to_json()
        d = json.loads(j)
        assert "metadata" not in d["params"]
        assert d["params"]["message"]["taskId"] == req.task_id

        restored = A2ARequest.from_json(j)
        assert restored.text == "hello"
        assert restored.task_id == req.task_id
        assert restored.sender == ""
        assert restored.variables == {}

    def test_context_id_in_roundtrip(self):
        ctx = "ctx-11111111-2222-3333-4444-555555555555"
        req = A2ARequest(text="hello", request_id="r1", context_id=ctx, sender="cli")
        j = req.to_json()
        d = json.loads(j)
        assert d["params"]["message"]["contextId"] == ctx

        restored = A2ARequest.from_json(j)
        assert restored.context_id == ctx

    def test_context_id_auto_generated(self):
        import uuid as uuid_mod

        req = A2ARequest(text="hello", request_id="r1")
        assert req.context_id
        uuid_mod.UUID(req.context_id)  # must be valid UUID

    def test_context_id_preserved_from_json(self):
        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": "r1",
                "method": "message/send",
                "params": {
                    "message": {
                        "role": "user",
                        "parts": [{"type": "text", "text": "hi"}],
                        "taskId": "t1",
                        "contextId": "ctx-explicit",
                    }
                },
            }
        )
        req = A2ARequest.from_json(payload)
        assert req.context_id == "ctx-explicit"


class TestStatusEvent:
    def test_working_text(self):
        event = make_status_event(
            "req-1",
            "sess-abc123",
            "working",
            message="hello",
            metadata={"task_name": "research"},
        )
        d = json.loads(event)
        assert d["jsonrpc"] == "2.0"
        assert d["id"] == "req-1"
        assert d["result"]["type"] == "TaskStatusUpdateEvent"
        assert d["result"]["taskId"] == "sess-abc123"
        assert d["result"]["status"]["state"] == "working"
        msg = d["result"]["status"]["message"]
        assert msg["role"] == "agent"
        assert msg["parts"] == [{"type": "text", "text": "hello"}]
        assert "messageId" in msg  # REQUIRED per A2A v1.0.0 proto
        assert d["result"]["metadata"]["task_name"] == "research"
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
            metadata={"type": "tool_use", "task_name": "research"},
        )
        d = json.loads(event)
        assert d["result"]["metadata"]["type"] == "tool_use"
        assert d["result"]["metadata"]["task_name"] == "research"

        kind, content = classify_reply(d)
        assert kind == REPLY_TOOL
        assert content == "Read: file.py"

    def test_terminal_with_artifact(self):
        artifact_event = make_artifact_event(
            "req-2",
            "sess-def456",
            "Final answer",
            metadata={"task_name": "summarize"},
        )
        status_event = make_status_event("req-2", "sess-def456", "completed")
        da = json.loads(artifact_event)
        ds = json.loads(status_event)
        assert ds["result"]["status"]["state"] == "completed"
        assert da["result"]["artifact"]["parts"][0]["text"] == "Final answer"

        kind_a, content_a = classify_reply(da)
        assert kind_a == REPLY_ARTIFACT
        assert content_a == "Final answer"

        kind_s, content_s = classify_reply(ds)
        assert kind_s == REPLY_TERMINAL
        assert content_s == ""

    def test_error_reply(self):
        kind, content = classify_reply(
            {"error": {"code": -32004, "message": "Unknown agent"}}
        )
        assert kind == REPLY_ERROR
        assert content == "Unknown agent"

    def test_failed_reply(self):
        event = make_status_event(
            "req-3", "sess-xyz", "failed", message="Agent crashed"
        )
        d = json.loads(event)
        kind, content = classify_reply(d)
        assert kind == REPLY_FAILED
        assert content == "Agent crashed"

    def test_failed_reply_no_message_uses_default(self):
        """When a failed reply has no message, content defaults to 'Task failed'."""
        d = {
            "jsonrpc": "2.0",
            "id": "req-4",
            "result": {
                "type": "TaskStatusUpdateEvent",
                "taskId": "sess-xyz",
                "status": {"state": "failed"},
            },
        }
        kind, content = classify_reply(d)
        assert kind == REPLY_FAILED
        assert content == "Task failed"

    def test_canceled_reply(self):
        event = make_status_event(
            "req-5", "sess-xyz", "canceled", message="User canceled"
        )
        d = json.loads(event)
        kind, content = classify_reply(d)
        assert kind == REPLY_FAILED
        assert content == "User canceled"

    def test_rejected_reply(self):
        event = make_status_event(
            "req-6", "sess-xyz", "rejected", message="Request rejected"
        )
        d = json.loads(event)
        kind, content = classify_reply(d)
        assert kind == REPLY_FAILED
        assert content == "Request rejected"

    def test_unknown_message(self):
        kind, content = classify_reply({"something": "else"})
        assert kind == ""
        assert content == ""

    def test_status_event_with_context_id(self):
        event = make_status_event(
            "req-1", "sess-1", "working", message="hi", context_id="ctx-123"
        )
        d = json.loads(event)
        assert d["result"]["contextId"] == "ctx-123"

    def test_context_id_always_present(self):
        """contextId is REQUIRED per A2A v1.0.0 proto, always emitted."""
        event = make_status_event("req-1", "sess-1", "working", message="hi")
        d = json.loads(event)
        assert d["result"]["contextId"] == ""

    def test_artifact_event_has_context_id(self):
        """contextId is REQUIRED on TaskArtifactUpdateEvent per proto."""
        event = make_artifact_event("req-1", "t1", "result", context_id="ctx-1")
        d = json.loads(event)
        assert d["result"]["contextId"] == "ctx-1"
        # Also present when empty
        event2 = make_artifact_event("req-1", "t1", "result")
        d2 = json.loads(event2)
        assert d2["result"]["contextId"] == ""

    def test_input_required_is_stream_final(self):
        """input-required MUST be treated as stream-final (A2A-over-MQTT spec)."""
        from skitter.a2a import REPLY_INPUT_REQUIRED

        d = {
            "jsonrpc": "2.0",
            "id": "req-1",
            "result": {
                "type": "TaskStatusUpdateEvent",
                "taskId": "t1",
                "contextId": "ctx-1",
                "status": {
                    "state": "input-required",
                    "message": {
                        "role": "agent",
                        "parts": [{"type": "text", "text": "What is your name?"}],
                    },
                },
            },
        }
        kind, content = classify_reply(d)
        assert kind == REPLY_INPUT_REQUIRED
        assert content == "What is your name?"

    def test_auth_required_is_stream_final(self):
        """auth-required MUST be treated as stream-final (A2A-over-MQTT spec)."""
        from skitter.a2a import REPLY_INPUT_REQUIRED

        d = {
            "jsonrpc": "2.0",
            "id": "req-1",
            "result": {
                "type": "TaskStatusUpdateEvent",
                "taskId": "t1",
                "contextId": "",
                "status": {"state": "auth-required"},
            },
        }
        kind, content = classify_reply(d)
        assert kind == REPLY_INPUT_REQUIRED
        assert content == "auth-required"


class TestSpecDefaults:
    """Verify spec-mandated default values for retry/timeout profile."""

    def test_reply_first_timeout(self):
        from skitter.a2a import REPLY_FIRST_TIMEOUT

        assert REPLY_FIRST_TIMEOUT == 15.0

    def test_stream_idle_timeout(self):
        from skitter.a2a import STREAM_IDLE_TIMEOUT

        assert STREAM_IDLE_TIMEOUT == 30.0

    def test_max_attempts(self):
        from skitter.a2a import MAX_ATTEMPTS

        assert MAX_ATTEMPTS == 3


# --- A2A error codes ---


class TestErrorCodes:
    def test_error_codes_defined(self):
        assert A2A_REQUEST_EXPIRED == -32003
        assert A2A_RESPONDER_UNAVAILABLE == -32004
        assert A2A_TRANSPORT_PROTOCOL_ERROR == -32005

    def test_make_a2a_error_with_transport_code(self):
        err = make_a2a_error(-32004, "Agent offline")
        assert err["code"] == -32004
        assert err["message"] == "Agent offline"
        assert err["data"]["a2a_error"] == "responder_unavailable"

    def test_make_a2a_error_with_request_expired(self):
        err = make_a2a_error(-32003, "Timed out")
        assert err["data"]["a2a_error"] == "request_expired"

    def test_make_a2a_error_without_transport_code(self):
        err = make_a2a_error(-32602, "Invalid params")
        assert err["code"] == -32602
        assert "data" not in err


# --- Topic builders ---


class TestTopics:
    def test_request_per_agent(self):
        t = topic_request("researcher")
        assert "/request/" in t
        assert "/researcher" in t

    def test_a2a_event_topic(self):
        t = topic_a2a_event("skitter")
        assert t == "$a2a/v1/event/skitter/default/skitter"


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


# --- Session building (DB-backed coordinator) ---


class TestCoordinatorSession:
    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    def _make_coordinator(self):
        from skitter.coordinator import Coordinator

        return Coordinator(self.db)

    def test_create_session_from_graph(self):
        from skitter.db import App, AppVersion

        sup = self._make_coordinator()
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
                            },
                            {
                                "id": "review",
                                "agent": "writer",
                                "description": "Review results",
                                "needs": ["research"],
                                "terminal": True,
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
        # session_id is now internal; request_task_id holds the requester's Task.id
        assert state.session_id != req.task_id
        assert state.request_task_id == req.task_id
        assert "research" in state.graph
        assert "review" in state.graph
        assert state.graph["review"].needs == ["research"]
        assert "research" in state.pending
        assert "review" in state.pending

    def test_variable_interpolation(self):
        from skitter.db import App, AppVersion

        sup = self._make_coordinator()
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
                                "terminal": True,
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

    def test_user_request_stored_in_variables(self):
        from skitter.db import App, AppVersion

        sup = self._make_coordinator()
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
        req = A2ARequest(text="summarize the news", request_id="r1")
        state = sup.create_session_from_graph(
            graph_json=self.db.get_app_version("v1").graph_json,
            app_version_id="v1",
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        assert state.variables["user_request"] == "summarize the news"

    def test_user_request_does_not_override_explicit(self):
        from skitter.db import App, AppVersion

        sup = self._make_coordinator()
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
        req = A2ARequest(text="summarize the news", request_id="r1")
        state = sup.create_session_from_graph(
            graph_json=self.db.get_app_version("v1").graph_json,
            app_version_id="v1",
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
            variables={"user_request": "custom override"},
        )
        assert state.variables["user_request"] == "custom override"

    def test_session_stores_context_id(self):
        from skitter.db import App, AppVersion

        sup = self._make_coordinator()
        self.db.create_app(App(id="ctx-app", name="Ctx"))
        self.db.create_app_version(
            AppVersion(
                id="v1-ctx",
                app_id="ctx-app",
                version=1,
                graph_json=json.dumps(
                    {
                        "tasks": [
                            {
                                "id": "a",
                                "agent": "x",
                                "description": "do",
                                "needs": [],
                                "terminal": True,
                            }
                        ]
                    }
                ),
            )
        )
        req = A2ARequest(text="go", request_id="r1", context_id="ctx-explicit-123")
        state = sup.create_session_from_graph(
            graph_json=self.db.get_app_version("v1-ctx").graph_json,
            app_version_id="v1-ctx",
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        assert state.context_id == "ctx-explicit-123"

        db_session = self.db.get_session(state.session_id)
        assert db_session.context_id == "ctx-explicit-123"
        assert db_session.request_task_id == req.task_id


# --- A2A compliance: agent runner handle_request ---


class TestAgentRunnerCompliance:
    """Verify agent_runner.handle_request echoes req.task_id in all status events."""

    @pytest.mark.asyncio
    async def test_status_events_use_task_id(self):
        """All status events (submitted, working, completed) must carry req.task_id."""
        from unittest.mock import AsyncMock, MagicMock

        from skitter.agent_runner import handle_request

        agent = AgentDef(id="test", name="Test")
        task_id = "550e8400-e29b-41d4-a716-446655440000"
        req = A2ARequest(
            text="hello", request_id="rpc-1", task_id=task_id, sender="test"
        )

        mock_client = MagicMock()
        published: list[str] = []

        async def capture_publish(topic, payload, **kwargs):
            published.append(payload)

        mock_client.publish = AsyncMock(side_effect=capture_publish)
        semaphore = asyncio.Semaphore(1)

        with patch(
            "skitter.agent_runner._run_cli",
            new=AsyncMock(return_value="result text"),
        ):
            await handle_request(
                mock_client, agent, req, "reply/t", "corr-1", {}, semaphore
            )

        # Every published event must have taskId = req.task_id
        for raw in published:
            data = json.loads(raw)
            result = data.get("result", {})
            if result.get("type") == "TaskStatusUpdateEvent":
                assert result["taskId"] == task_id

    @pytest.mark.asyncio
    async def test_stream_qos_is_1(self):
        """Streaming updates must use QoS 1 per spec."""
        from unittest.mock import AsyncMock, MagicMock

        from skitter.agent_runner import handle_request

        agent = AgentDef(id="test", name="Test")
        req = A2ARequest(text="hello", request_id="rpc-1", sender="test")

        mock_client = MagicMock()
        qos_values: list[int] = []

        async def capture_publish(topic, payload, qos=0, **kwargs):
            qos_values.append(qos)

        mock_client.publish = AsyncMock(side_effect=capture_publish)
        semaphore = asyncio.Semaphore(1)

        async def streaming_cli(agent, prompt, publish_stream, env):
            await publish_stream("text", "chunk")
            return "done"

        with patch("skitter.agent_runner._run_cli", new=streaming_cli):
            await handle_request(
                mock_client, agent, req, "reply/t", "corr-1", {}, semaphore
            )

        # All publishes (submitted ack, stream chunk, terminal) should be QoS 1
        assert all(q == 1 for q in qos_values)


# --- A2A compliance: coordinator dispatch ---


class TestCoordinatorDispatchCompliance:
    """Verify coordinator dispatches with proper Task.id and cancel uses dispatch_task_id."""

    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    def _make_coordinator_with_app(self):
        from unittest.mock import AsyncMock, MagicMock

        from skitter.coordinator import Coordinator
        from skitter.runtime_api import create_app

        sup = Coordinator(self.db)
        mock_client = MagicMock()
        mock_client.publish = AsyncMock()
        mock_client.subscribe = AsyncMock()
        sup._client = mock_client

        create_app(
            self.db,
            app_id="test-app",
            name="Test",
            graph={
                "tasks": [
                    {
                        "id": "step",
                        "agent": "researcher",
                        "description": "Do it",
                        "needs": [],
                        "terminal": True,
                    }
                ]
            },
        )
        return sup, mock_client

    @pytest.mark.asyncio
    async def test_dispatched_request_has_uuid4_task_id(self):
        """Coordinator must generate a UUIDv4 task_id for dispatched A2A requests."""
        import uuid as uuid_mod

        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        await sup.dispatch_ready(state)

        # Find the A2A request published to the agent's request topic
        dispatched_payloads = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if "/request/" in str(call.args[0]) and "researcher" in str(call.args[0])
        ]
        assert len(dispatched_payloads) == 1
        dispatched = dispatched_payloads[0]

        # Must use message/send method
        assert dispatched["method"] == "message/send"

        # taskId in the message must be a valid UUID
        dispatched_task_id = dispatched["params"]["message"]["taskId"]
        uuid_mod.UUID(dispatched_task_id)  # raises if not valid UUID

        # The dispatched task_id must differ from the session's task_id
        assert dispatched_task_id != req.task_id

        # SessionTask must store the dispatch_task_id
        assert state.graph["step"].dispatch_task_id == dispatched_task_id

    @pytest.mark.asyncio
    async def test_dispatch_sets_correlation_data(self):
        """Dispatched A2A requests must include MQTT Correlation Data."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        await sup.dispatch_ready(state)

        # Find the publish call to the agent's request topic
        dispatch_calls = [
            call
            for call in mock_client.publish.call_args_list
            if "/request/" in str(call.args[0]) and "researcher" in str(call.args[0])
        ]
        assert len(dispatch_calls) == 1
        props = dispatch_calls[0].kwargs.get("properties")
        assert props is not None
        assert state.graph["step"].dispatch_correlation != ""

    @pytest.mark.asyncio
    async def test_reply_with_correct_correlation_is_accepted(self):
        """Replies with matching MQTT Correlation Data must be processed."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        await sup.dispatch_ready(state)
        correct_corr = state.graph["step"].dispatch_correlation

        reply_topic = f"$a2a/v1/reply/skitter/default/skitter/{state.session_id}/step"
        reply_payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": correct_corr,
                "result": {
                    "type": "TaskStatusUpdateEvent",
                    "taskId": state.graph["step"].dispatch_task_id,
                    "contextId": "",
                    "status": {"state": "completed"},
                },
            }
        )
        await sup.handle_reply(reply_topic, reply_payload, correct_corr)

        assert "step" not in state.inflight
        assert "step" in state.results

    @pytest.mark.asyncio
    async def test_reply_with_wrong_correlation_is_dropped(self):
        """Replies with mismatched MQTT Correlation Data must be dropped."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        await sup.dispatch_ready(state)
        assert "step" in state.inflight

        reply_topic = f"$a2a/v1/reply/skitter/default/skitter/{state.session_id}/step"
        reply_payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": "wrong-corr",
                "result": {
                    "type": "TaskStatusUpdateEvent",
                    "taskId": state.graph["step"].dispatch_task_id,
                    "contextId": "",
                    "status": {"state": "completed"},
                },
            }
        )
        await sup.handle_reply(reply_topic, reply_payload, "wrong-corr")

        assert "step" in state.inflight

    @pytest.mark.asyncio
    async def test_reply_with_missing_correlation_is_dropped(self):
        """Replies omitting MQTT Correlation Data must be dropped when expected."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        await sup.dispatch_ready(state)
        assert state.graph["step"].dispatch_correlation  # expected is set

        reply_topic = f"$a2a/v1/reply/skitter/default/skitter/{state.session_id}/step"
        reply_payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": "no-corr",
                "result": {
                    "type": "TaskStatusUpdateEvent",
                    "taskId": state.graph["step"].dispatch_task_id,
                    "contextId": "",
                    "status": {"state": "completed"},
                },
            }
        )
        await sup.handle_reply(reply_topic, reply_payload, "")

        assert "step" in state.inflight

    @pytest.mark.asyncio
    async def test_cancel_uses_a2a_task_id(self):
        """tasks/cancel must reference the dispatched Task.id, not the JSON-RPC id."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/caller",
            caller_correlation="corr-caller",
        )
        await sup.dispatch_ready(state)
        dispatch_task_id = state.graph["step"].dispatch_task_id
        assert dispatch_task_id  # must be set

        mock_client.publish.reset_mock()
        await sup._cancel_session_cleanup(state.session_id)

        # Find the tasks/cancel message
        cancel_calls = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if "/request/" in str(call.args[0])
        ]
        cancel_msgs = [c for c in cancel_calls if c.get("method") == "tasks/cancel"]
        assert len(cancel_msgs) == 1
        assert cancel_msgs[0]["params"]["id"] == dispatch_task_id

    @pytest.mark.asyncio
    async def test_handle_request_deduplicates_by_task_id(self):
        """Second request with same Task.id must reply with current state, not create a new session."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        # Session is now indexed by request_task_id for dedup
        assert sup._request_task_index[req.task_id] == state.session_id

        mock_client.publish.reset_mock()

        # Send another request with the same task_id and context_id (retry)
        dup_req = A2ARequest(
            text="go again",
            request_id="r2",
            task_id=req.task_id,
            context_id=req.context_id,
        )
        await sup.handle_request(dup_req, "reply/t2", "corr-2", "test-app")

        # Must reply with existing state, not create a new session
        replies = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/t2"
        ]
        assert len(replies) == 1
        assert replies[0]["result"]["status"]["state"] == "working"
        assert replies[0]["result"]["taskId"] == req.task_id
        # Still only one session
        assert len(sup._sessions) == 1

    @pytest.mark.asyncio
    async def test_dedup_completed_session_returns_stored_state(self):
        """Duplicate Task.id for a completed session must reply with completed state."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        # Simulate completion: remove from in-memory, mark DB as completed
        sup._sessions.pop(state.session_id)
        sup._request_task_index.pop(state.request_task_id, None)
        self.db.update_session_state(state.session_id, "completed")
        # Store a result on the terminal task
        task_row_id = f"{state.session_id}/step"
        self.db.update_task(task_row_id, state="completed", result="final answer")

        mock_client.publish.reset_mock()

        dup_req = A2ARequest(
            text="go again",
            request_id="r2",
            task_id=req.task_id,
            context_id=req.context_id,
        )
        await sup.handle_request(dup_req, "reply/t2", "corr-2", "test-app")

        replies = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/t2"
        ]
        # Dedup replays artifact + completed status for terminal sessions
        assert len(replies) == 2
        artifact_reply = next(
            r for r in replies if r["result"]["type"] == "TaskArtifactUpdateEvent"
        )
        status_reply = next(
            r for r in replies if r["result"]["type"] == "TaskStatusUpdateEvent"
        )
        assert status_reply["result"]["status"]["state"] == "completed"
        assert status_reply["result"]["taskId"] == req.task_id
        assert (
            "final answer" in artifact_reply["result"]["artifact"]["parts"][0]["text"]
        )

    @pytest.mark.asyncio
    async def test_context_id_mismatch_returns_error(self):
        """Duplicate Task.id with different context_id must return -32602."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1", context_id="ctx-original")
        version = self.db.get_current_version("test-app")
        sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )

        mock_client.publish.reset_mock()

        # Send with same task_id but different context_id
        dup_req = A2ARequest(
            text="go again",
            request_id="r2",
            task_id=req.task_id,
            context_id="ctx-different",
        )
        await sup.handle_request(dup_req, "reply/t2", "corr-2", "test-app")

        replies = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/t2"
        ]
        assert len(replies) == 1
        assert replies[0]["error"]["code"] == -32602
        assert "context_id mismatch" in replies[0]["error"]["message"]

    @pytest.mark.asyncio
    async def test_context_id_mismatch_db_session_returns_error(self):
        """Duplicate Task.id in DB with different context_id must return -32602."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1", context_id="ctx-original")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        # Move to DB (simulate completion)
        sup._sessions.pop(state.session_id)
        sup._request_task_index.pop(state.request_task_id, None)
        self.db.update_session_state(state.session_id, "completed")

        mock_client.publish.reset_mock()

        dup_req = A2ARequest(
            text="go again",
            request_id="r2",
            task_id=req.task_id,
            context_id="ctx-different",
        )
        await sup.handle_request(dup_req, "reply/t2", "corr-2", "test-app")

        replies = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/t2"
        ]
        assert len(replies) == 1
        assert replies[0]["error"]["code"] == -32602

    @pytest.mark.asyncio
    async def test_send_error_includes_a2a_error_data(self):
        """_send_error must include data.a2a_error for transport error codes."""
        sup, mock_client = self._make_coordinator_with_app()

        await sup._send_error(
            "reply/t", "corr-1", "Agent offline", code=A2A_RESPONDER_UNAVAILABLE
        )

        calls = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/t"
        ]
        assert len(calls) == 1
        err = calls[0]["error"]
        assert err["data"]["a2a_error"] == "responder_unavailable"

    @pytest.mark.asyncio
    async def test_forward_stream_qos_1(self):
        """Forwarded stream updates to caller must use QoS 1."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/caller",
            caller_correlation="corr-caller",
        )

        mock_client.publish.reset_mock()
        await sup._forward_stream(state, "step", "text", "thinking...")

        # Check QoS in the publish call
        assert mock_client.publish.call_count == 1
        call_kwargs = mock_client.publish.call_args
        assert (
            call_kwargs.kwargs.get(
                "qos", call_kwargs.args[2] if len(call_kwargs.args) > 2 else 0
            )
            == 1
        )

    @pytest.mark.asyncio
    async def test_dispatch_includes_user_request_in_prompt(self):
        """Dispatched A2A request must append user request to prompt text."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="find latest news", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        await sup.dispatch_ready(state)

        dispatched_payloads = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if "/request/" in str(call.args[0]) and "researcher" in str(call.args[0])
        ]
        assert len(dispatched_payloads) == 1
        prompt_text = dispatched_payloads[0]["params"]["message"]["parts"][0]["text"]
        assert "User request: find latest news" in prompt_text

    @pytest.mark.asyncio
    async def test_dispatch_omits_user_request_when_empty(self):
        """Dispatched prompt must not contain 'User request:' when request text is empty."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        await sup.dispatch_ready(state)

        dispatched_payloads = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if "/request/" in str(call.args[0]) and "researcher" in str(call.args[0])
        ]
        assert len(dispatched_payloads) == 1
        prompt_text = dispatched_payloads[0]["params"]["message"]["parts"][0]["text"]
        assert "User request:" not in prompt_text

    @pytest.mark.asyncio
    async def test_dispatched_request_has_context_id(self):
        """Dispatched A2A payload must include the session's contextId."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1", context_id="ctx-dispatch-test")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        await sup.dispatch_ready(state)

        dispatched_payloads = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if "/request/" in str(call.args[0]) and "researcher" in str(call.args[0])
        ]
        assert len(dispatched_payloads) == 1
        msg = dispatched_payloads[0]["params"]["message"]
        assert msg["contextId"] == "ctx-dispatch-test"

    @pytest.mark.asyncio
    async def test_submitted_ack_has_context_id(self):
        """Submitted ack event must include contextId."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1", context_id="ctx-ack-test")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/caller",
            caller_correlation="corr-caller",
        )
        await sup._start_session(state, "test")

        # Find submitted ack published to caller
        ack_calls = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/caller"
        ]
        submitted = [
            c
            for c in ack_calls
            if c.get("result", {}).get("status", {}).get("state") == "submitted"
        ]
        assert len(submitted) == 1
        assert submitted[0]["result"]["contextId"] == "ctx-ack-test"


# --- A2A compliance: backoff calculation ---


class TestBackoffDelay:
    def test_backoff_within_expected_range(self):
        from skitter.a2a import _backoff_delay

        for _ in range(50):
            d0 = _backoff_delay(0)
            assert 0.8 <= d0 <= 1.2  # base=1.0, +-20% jitter

            d1 = _backoff_delay(1)
            assert 1.6 <= d1 <= 2.4

            d2 = _backoff_delay(2)
            assert 3.2 <= d2 <= 4.8

    def test_backoff_clamps_at_max(self):
        from skitter.a2a import _backoff_delay

        # Attempt index beyond the list should clamp to last entry
        d = _backoff_delay(100)
        assert 3.2 <= d <= 4.8


# --- validate_a2a_request ---


class TestValidateA2ARequest:
    """Tests for the shared A2A request validation helper."""

    def _make_mqtt_msg(
        self,
        payload: str | bytes = b"",
        response_topic: str = "reply/t",
        correlation: str = "corr-1",
    ) -> MagicMock:
        msg = MagicMock()
        msg.payload = payload.encode() if isinstance(payload, str) else payload
        props = MagicMock()
        props.ResponseTopic = response_topic or None
        props.CorrelationData = correlation.encode() if correlation else None
        msg.properties = props
        return msg

    def _valid_payload(self, task_id: str = "tid-1") -> str:
        return json.dumps(
            {
                "jsonrpc": "2.0",
                "id": "rpc-1",
                "method": "message/send",
                "params": {
                    "message": {
                        "role": "user",
                        "parts": [{"type": "text", "text": "hello"}],
                        "taskId": task_id,
                    }
                },
            }
        )

    @pytest.mark.asyncio
    async def test_valid_request_returns_tuple(self):
        from unittest.mock import AsyncMock

        from skitter.a2a import validate_a2a_request

        msg = self._make_mqtt_msg(payload=self._valid_payload())
        client = AsyncMock()
        import logging

        result = await validate_a2a_request(msg, client, log=logging.getLogger("test"))
        assert result is not None
        req, reply_topic, correlation = result
        assert req.text == "hello"
        assert req.task_id == "tid-1"
        assert reply_topic == "reply/t"
        assert correlation == "corr-1"
        client.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_empty_payload_returns_none_silently(self):
        from unittest.mock import AsyncMock

        from skitter.a2a import validate_a2a_request

        msg = self._make_mqtt_msg(payload=b"")
        client = AsyncMock()
        import logging

        result = await validate_a2a_request(msg, client, log=logging.getLogger("test"))
        assert result is None
        client.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_response_topic_returns_none_no_error_sent(self):
        """No Response Topic means we can't reply at all."""
        from unittest.mock import AsyncMock

        from skitter.a2a import validate_a2a_request

        msg = self._make_mqtt_msg(
            payload=self._valid_payload(), response_topic="", correlation="corr-1"
        )
        client = AsyncMock()
        import logging

        result = await validate_a2a_request(msg, client, log=logging.getLogger("test"))
        assert result is None
        client.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_missing_correlation_sends_error_to_reply_topic(self):
        """Has Response Topic but no Correlation Data: send transport error."""
        from unittest.mock import AsyncMock

        from skitter.a2a import validate_a2a_request

        msg = self._make_mqtt_msg(
            payload=self._valid_payload(), response_topic="reply/t", correlation=""
        )
        client = AsyncMock()
        import logging

        result = await validate_a2a_request(msg, client, log=logging.getLogger("test"))
        assert result is None
        client.publish.assert_called_once()
        err = json.loads(client.publish.call_args.args[1])
        assert err["error"]["code"] == A2A_TRANSPORT_PROTOCOL_ERROR
        assert "Response Topic or Correlation Data" in err["error"]["message"]

    @pytest.mark.asyncio
    async def test_missing_both_props_returns_none_no_error_sent(self):
        """Neither Response Topic nor Correlation Data: can't reply, just drop."""
        from unittest.mock import AsyncMock

        from skitter.a2a import validate_a2a_request

        msg = self._make_mqtt_msg(
            payload=self._valid_payload(), response_topic="", correlation=""
        )
        client = AsyncMock()
        import logging

        result = await validate_a2a_request(msg, client, log=logging.getLogger("test"))
        assert result is None
        client.publish.assert_not_called()

    @pytest.mark.asyncio
    async def test_bad_json_sends_parse_error(self):
        """Malformed JSON sends -32700 parse error response."""
        from unittest.mock import AsyncMock

        from skitter.a2a import validate_a2a_request

        msg = self._make_mqtt_msg(payload="not json{{{")
        client = AsyncMock()
        import logging

        result = await validate_a2a_request(msg, client, log=logging.getLogger("test"))
        assert result is None
        client.publish.assert_called_once()
        err = json.loads(client.publish.call_args.args[1])
        assert err["error"]["code"] == -32700
        assert "Parse error" in err["error"]["message"]

    @pytest.mark.asyncio
    async def test_missing_task_id_sends_transport_error(self):
        """Empty taskId sends transport_protocol_error."""
        from unittest.mock import AsyncMock

        from skitter.a2a import validate_a2a_request

        msg = self._make_mqtt_msg(payload=self._valid_payload(task_id=""))
        client = AsyncMock()
        import logging

        result = await validate_a2a_request(msg, client, log=logging.getLogger("test"))
        assert result is None
        client.publish.assert_called_once()
        err = json.loads(client.publish.call_args.args[1])
        assert err["error"]["code"] == A2A_TRANSPORT_PROTOCOL_ERROR
        assert "Task.id" in err["error"]["message"]

    @pytest.mark.asyncio
    async def test_error_response_echoes_correlation(self):
        """All error responses must echo the Correlation Data."""
        from unittest.mock import AsyncMock

        from skitter.a2a import validate_a2a_request

        msg = self._make_mqtt_msg(
            payload=self._valid_payload(task_id=""), correlation="my-corr-99"
        )
        client = AsyncMock()
        import logging

        await validate_a2a_request(msg, client, log=logging.getLogger("test"))
        err = json.loads(client.publish.call_args.args[1])
        assert err["id"] == "my-corr-99"


# --- send_and_wait ---


class TestSendAndWait:
    """Tests for the A2A requester retry/timeout profile."""

    @pytest.mark.asyncio
    async def test_fire_and_forget_mode(self):
        """send_request publishes once and returns without subscribing."""
        from unittest.mock import AsyncMock, patch

        from skitter.a2a import send_request

        mock_client = AsyncMock()
        mock_client.subscribe = AsyncMock()
        mock_client.publish = AsyncMock()

        with patch("aiomqtt.Client") as MockClient:
            MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            await send_request("topic/req", '{"test": true}', "corr-1")

        mock_client.subscribe.assert_not_called()
        mock_client.publish.assert_called_once()
        # A2A protocol requires Response Topic even for fire-and-forget
        props = mock_client.publish.call_args.kwargs.get("properties")
        assert props is not None
        assert hasattr(props, "ResponseTopic") and props.ResponseTopic
        assert hasattr(props, "CorrelationData") and props.CorrelationData

    @pytest.mark.asyncio
    async def test_terminal_reply_stops_listening(self):
        """Generator stops after yielding a terminal kind."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from skitter.a2a import REPLY_TERMINAL, stream_replies, make_status_event

        # Build a terminal reply message
        terminal = make_status_event(
            request_id="corr-1", task_id="t1", state="completed", message="done"
        )

        # Mock MQTT client that yields one terminal message
        mock_msg = MagicMock()
        mock_msg.payload = terminal.encode()
        corr_props = MagicMock()
        corr_props.CorrelationData = b"corr-1"
        mock_msg.properties = corr_props

        mock_client = AsyncMock()
        mock_client.subscribe = AsyncMock()
        mock_client.publish = AsyncMock()

        async def fake_messages():
            yield mock_msg

        mock_client.messages = fake_messages()

        with patch("aiomqtt.Client") as MockClient:
            MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            replies = [
                (kind, content)
                async for kind, content in stream_replies("topic/req", "{}", "corr-1")
            ]

        assert len(replies) == 1
        assert replies[0][0] == REPLY_TERMINAL
        assert "done" in replies[0][1]

    @pytest.mark.asyncio
    async def test_timeout_triggers_retry_with_new_correlation(self):
        """When no reply arrives, stream_replies retries with new Correlation Data."""
        from unittest.mock import AsyncMock, patch

        from skitter.a2a import REPLY_TIMEOUT, stream_replies

        mock_client = AsyncMock()
        mock_client.subscribe = AsyncMock()
        mock_client.publish = AsyncMock()

        # Messages iterator that blocks forever (until timeout fires)
        async def blocking_messages():
            await asyncio.sleep(999)
            yield  # never reached; async gen that hangs

        with (
            patch("aiomqtt.Client") as MockClient,
            patch("skitter.a2a.REPLY_FIRST_TIMEOUT", 0.05),
            patch("skitter.a2a._backoff_delay", return_value=0.01),
            patch("skitter.a2a.MAX_ATTEMPTS", 3),
        ):
            MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            # Each access to .messages returns a fresh blocking generator
            type(mock_client).messages = property(lambda self: blocking_messages())

            replies = []
            async for kind, content in stream_replies("topic/req", "{}", "corr-orig"):
                replies.append((kind, content))

        # Should have published 3 times (initial + 2 retries)
        assert mock_client.publish.call_count == 3
        assert replies == [(REPLY_TIMEOUT, "")]

    @pytest.mark.asyncio
    async def test_ignores_mismatched_correlation(self):
        """Messages with wrong Correlation Data are silently skipped."""
        from unittest.mock import AsyncMock, MagicMock, patch

        from skitter.a2a import stream_replies, make_status_event

        # One message with wrong correlation, then one with correct
        wrong_msg = MagicMock()
        wrong_msg.payload = make_status_event(
            request_id="wrong", task_id="t1", state="completed", message="bad"
        ).encode()
        wrong_props = MagicMock()
        wrong_props.CorrelationData = b"wrong-corr"
        wrong_msg.properties = wrong_props

        right_msg = MagicMock()
        right_msg.payload = make_status_event(
            request_id="corr-1", task_id="t1", state="completed", message="good"
        ).encode()
        right_props = MagicMock()
        right_props.CorrelationData = b"corr-1"
        right_msg.properties = right_props

        mock_client = AsyncMock()
        mock_client.subscribe = AsyncMock()
        mock_client.publish = AsyncMock()

        async def fake_messages():
            yield wrong_msg
            yield right_msg

        mock_client.messages = fake_messages()

        with patch("aiomqtt.Client") as MockClient:
            MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            replies = [
                (kind, content)
                async for kind, content in stream_replies("topic/req", "{}", "corr-1")
            ]

        # Only the correctly-correlated message should reach the caller
        assert len(replies) == 1
        assert "good" in replies[0][1]


# --- Entry task detection ---


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
        assert len(card["metadata"]["tasks"]) == 1

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
        # supportedInterfaces replaces top-level url/protocolVersion per A2A v1.0.0
        ifaces = card["supportedInterfaces"]
        assert len(ifaces) == 1
        assert ifaces[0]["protocolVersion"] == "1.0.0"
        assert ifaces[0]["protocolBinding"] == "MQTTv5+JSONRPCv2"
        assert "url" in ifaces[0]
        assert card["capabilities"]["streaming"] is True
        assert card["capabilities"]["pushNotifications"] is False
        assert card["defaultInputModes"] == ["text/plain"]
        assert card["defaultOutputModes"] == ["text/plain"]
        assert card["skills"][0]["id"] == "researcher"
        assert card["skills"][0]["tags"] == ["researcher"]
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
        assert card["supportedInterfaces"][0]["url"] == "mqtt://custom:1883"

    def test_card_skills_have_tags(self):
        from skitter.discovery import build_card

        agent = AgentDef(
            id="coder",
            name="Coder",
            description="Writes code",
            tags=["code", "python"],
        )
        card = build_card(agent)
        assert card["skills"][0]["tags"] == ["code", "python"]

    def test_card_skills_default_tags(self):
        from skitter.discovery import build_card

        agent = AgentDef(id="writer", name="Writer", description="Writes")
        card = build_card(agent)
        assert card["skills"][0]["tags"] == ["writer"]


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
            claude_agent="researcher",
        )
        cmd = _build_cli_cmd(agent, "test prompt")
        assert cmd[0] == "claude"
        assert "-p" in cmd
        assert "test prompt" in cmd
        assert "--agent" in cmd
        assert "researcher" in cmd
        assert "--model" in cmd
        assert "sonnet" in cmd
        assert cmd[cmd.index("--permission-mode") + 1] == "auto"
        assert "--settings" in cmd
        assert "--dangerously-skip-permissions" not in cmd

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
        assert cmd[-1] == "code something"  # prompt must be last (positional)
        assert "--model" in cmd
        assert "gpt-5-nano" in cmd
        assert "--ephemeral" in cmd
        assert cmd[cmd.index("--color") + 1] == "never"
        assert "--full-auto" in cmd

    def test_build_codex_cmd_with_instructions(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(
            id="coder",
            name="Coder",
            runtime="codex",
            codex_instructions="You are a senior developer.",
        )
        cmd = _build_cli_cmd(agent, "write tests")
        assert "-c" in cmd
        # Find the -c arg that sets developer_instructions
        c_indices = [i for i, v in enumerate(cmd) if v == "-c"]
        dev_instr_args = [
            cmd[i + 1] for i in c_indices if "developer_instructions=" in cmd[i + 1]
        ]
        assert len(dev_instr_args) == 1
        assert dev_instr_args[0] == "developer_instructions=You are a senior developer."

    def test_build_codex_cmd_no_instructions(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(id="coder", name="Coder", runtime="codex")
        cmd = _build_cli_cmd(agent, "write tests")
        c_indices = [i for i, v in enumerate(cmd) if v == "-c"]
        dev_instr_args = [
            cmd[i + 1] for i in c_indices if "developer_instructions=" in cmd[i + 1]
        ]
        assert len(dev_instr_args) == 0

    def test_build_claude_cmd_default_agent_name(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(id="researcher", name="Researcher", runtime="claude")
        cmd = _build_cli_cmd(agent, "test")
        # When claude_agent is empty, uses agent.id
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
    def test_load_claude_agent(self, tmp_path):
        (tmp_path / "researcher.md").write_text(
            "---\n"
            "name: researcher\n"
            "description: Deep research\n"
            "model: sonnet\n"
            "---\n"
            "You are a researcher.\n"
        )
        from skitter.agent_runner import load_agent

        agent = load_agent(str(tmp_path / "researcher.md"))
        assert agent.id == "researcher"
        assert agent.name == "researcher"
        assert agent.description == "Deep research"
        assert agent.model == "sonnet"
        assert agent.runtime == "claude"
        assert agent.claude_agent == "researcher"

    def test_load_claude_agent_minimal(self, tmp_path):
        (tmp_path / "simple.md").write_text("---\nname: simple\n---\nBe brief.\n")
        from skitter.agent_runner import load_agent

        agent = load_agent(str(tmp_path / "simple.md"))
        assert agent.id == "simple"
        assert agent.description == ""
        assert agent.model == ""
        assert agent.runtime == "claude"
        assert agent.claude_agent == "simple"

    def test_load_claude_agent_name_differs_from_filename(self, tmp_path):
        """claude_agent should use frontmatter name, not filename stem."""
        (tmp_path / "my-copy.md").write_text(
            "---\nname: researcher\ndescription: Research\n---\nDo research.\n"
        )
        from skitter.agent_runner import load_agent

        agent = load_agent(str(tmp_path / "my-copy.md"))
        assert agent.id == "researcher"
        assert agent.claude_agent == "researcher"

    def test_load_codex_agent(self, tmp_path):
        (tmp_path / "coder.toml").write_text(
            'model = "gpt-5.1-codex-mini"\n'
            'developer_instructions = "You are a senior developer."\n'
        )
        from skitter.agent_runner import load_agent

        agent = load_agent(str(tmp_path / "coder.toml"))
        assert agent.id == "coder"
        assert agent.runtime == "codex"
        assert agent.model == "gpt-5.1-codex-mini"
        assert agent.description == "You are a senior developer."
        assert agent.codex_instructions == "You are a senior developer."


# --- Pull (save cards) ---


class TestPullCards:
    def test_save_card(self, tmp_path):
        from skitter.pull import save_cards

        cards = [
            {
                "_agent_id": "researcher",
                "name": "Researcher",
                "description": "Deep research",
                "capabilities": {"streaming": True},
                "skills": [{"id": "researcher", "name": "Researcher"}],
            }
        ]
        written = save_cards(cards, tmp_path)
        assert len(written) == 1
        data = json.loads((tmp_path / "researcher.json").read_text())
        assert data["name"] == "Researcher"
        assert data["description"] == "Deep research"
        assert data["capabilities"] == {"streaming": True}
        # Internal fields should be stripped
        assert "_agent_id" not in data

    def test_skip_skitter_agent(self, tmp_path):
        from skitter.pull import save_cards

        cards = [{"_agent_id": "skitter", "name": "Skitter Runtime"}]
        written = save_cards(cards, tmp_path)
        assert written == []

    def test_save_workflow_cards(self, tmp_path):
        from skitter.pull import save_cards

        cards = [
            {
                "_agent_id": "pipeline",
                "name": "Pipeline",
                "metadata": {"tasks": [{"id": "t1"}]},
            }
        ]
        written = save_cards(cards, tmp_path)
        assert len(written) == 1
        data = json.loads((tmp_path / "pipeline.json").read_text())
        assert data["name"] == "Pipeline"
        assert data["metadata"]["tasks"] == [{"id": "t1"}]

    def test_skip_existing_file(self, tmp_path):
        from skitter.pull import save_cards

        (tmp_path / "researcher.json").write_text("{}\n")
        cards = [{"_agent_id": "researcher", "name": "Researcher"}]
        written = save_cards(cards, tmp_path)
        assert written == []
        assert (tmp_path / "researcher.json").read_text() == "{}\n"


# --- Safe format ---


class TestSafeFormat:
    def test_unknown_vars_left_intact(self):
        from skitter.config import safe_format

        desc = safe_format("Research {topic}, output as {format}.", {"topic": "AI"})
        assert desc == "Research AI, output as {format}."


# --- Dependency resolution ---


class TestDependencyResolution:
    def test_variables_field_default(self):
        from skitter.coordinator import SessionState

        state = SessionState(
            session_id="s1", request_task_id="rtid-s1", app_version_id="v1"
        )
        assert state.variables == {}

    def test_compute_ready_no_needs(self):
        from skitter.coordinator import SessionState, SessionTask as ST, _compute_ready

        state = SessionState(
            session_id="s1", request_task_id="rtid-s1", app_version_id="v1"
        )
        state.graph["a"] = ST(agent="r", description="d", needs=[])
        state.graph["b"] = ST(agent="r", description="d", needs=["a"])
        state.pending = {"a", "b"}
        ready = _compute_ready(state)
        assert ready == ["a"]

    def test_compute_ready_after_completion(self):
        from skitter.coordinator import SessionState, SessionTask as ST, _compute_ready

        state = SessionState(
            session_id="s1", request_task_id="rtid-s1", app_version_id="v1"
        )
        state.graph["a"] = ST(agent="r", description="d", needs=[])
        state.graph["b"] = ST(agent="r", description="d", needs=["a"])
        state.results["a"] = "done"
        state.pending = {"b"}
        ready = _compute_ready(state)
        assert ready == ["b"]

    def test_propagate_failure(self):
        from skitter.coordinator import (
            SessionState,
            SessionTask as ST,
            _propagate_failure,
        )

        state = SessionState(
            session_id="s1", request_task_id="rtid-s1", app_version_id="v1"
        )
        state.graph["a"] = ST(agent="r", description="d", needs=[])
        state.graph["b"] = ST(agent="r", description="d", needs=["a"])
        state.graph["c"] = ST(agent="r", description="d", needs=["b"])
        state.failed.add("a")
        state.pending = {"b", "c"}
        newly_failed = _propagate_failure(state, "a")
        assert "b" in newly_failed
        assert "c" in newly_failed
        assert "b" in state.failed
        assert "c" in state.failed

    def test_find_terminal_tasks(self):
        from skitter.coordinator import (
            SessionState,
            SessionTask as ST,
            _find_terminal_tasks,
        )

        state = SessionState(
            session_id="s1", request_task_id="rtid-s1", app_version_id="v1"
        )
        state.graph["a"] = ST(agent="r", description="d")
        state.graph["b"] = ST(agent="r", description="d", terminal=True)
        state.graph["c"] = ST(agent="r", description="d", terminal=True)
        terminals = _find_terminal_tasks(state)
        assert set(terminals) == {"b", "c"}

    def test_build_context(self):
        from skitter.coordinator import (
            SessionState,
            SessionTask as ST,
            _build_context,
        )

        state = SessionState(
            session_id="s1", request_task_id="rtid-s1", app_version_id="v1"
        )
        state.results["a"] = "result A"
        state.results["b"] = "result B"
        task = ST(agent="w", description="d", needs=["a", "b"])
        ctx = _build_context(state, task)
        assert "result A" in ctx
        assert "result B" in ctx
        assert "task 'a'" in ctx


# --- Discovery registry ---


class TestDiscoveryRegistry:
    def test_update_and_get(self):
        from skitter.coordinator import DiscoveryRegistry

        reg = DiscoveryRegistry()
        reg.update("researcher", {"name": "Researcher"})
        assert reg.get("researcher") == {"name": "Researcher"}
        assert reg.get("unknown") is None

    def test_remove(self):
        from skitter.coordinator import DiscoveryRegistry

        reg = DiscoveryRegistry()
        reg.update("researcher", {"name": "Researcher"})
        reg.remove("researcher")
        assert reg.get("researcher") is None

    def test_list_agents_vs_apps(self):
        from skitter.coordinator import DiscoveryRegistry

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


class TestTaskTarget:
    def test_defaults(self):
        from skitter.a2a import TaskTarget

        t = TaskTarget(agent="researcher")
        assert t.mqtt_host == ""
        assert t.mqtt_port == 8883


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
                },
                {
                    "id": "analyze",
                    "agent": "analyzer",
                    "description": "Analyze data",
                    "needs": ["read"],
                    "terminal": True,
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
            "tasks": [{"id": "t1", "agent": "unknown", "needs": [], "terminal": True}]
        }
        with pytest.raises(GraphValidationError, match="unknown agent"):
            validate_graph(graph, {"reader"})

    def test_duplicate_task_id(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "t1", "agent": "a", "needs": [], "terminal": True},
                {"id": "t1", "agent": "a", "needs": [], "terminal": True},
            ]
        }
        with pytest.raises(GraphValidationError, match="Duplicate"):
            validate_graph(graph, {"a"})

    def test_cycle_detected(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "a", "agent": "x", "needs": ["b"]},
                {"id": "b", "agent": "y", "needs": ["a"], "terminal": True},
            ]
        }
        with pytest.raises(GraphValidationError, match="Cycle"):
            validate_graph(graph, {"x", "y"})

    def test_no_terminal_caught(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "t1", "agent": "a", "needs": []},
                {"id": "t2", "agent": "b", "needs": ["t1"]},
            ]
        }
        with pytest.raises(GraphValidationError, match="No terminal"):
            validate_graph(graph, {"a", "b"})

    def test_unknown_need(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "t1", "agent": "a", "needs": ["nonexistent"], "terminal": True},
            ]
        }
        with pytest.raises(GraphValidationError, match="unknown task"):
            validate_graph(graph, {"a"})

    def test_terminal_has_dependents(self):
        from skitter.graph_gen import GraphValidationError, validate_graph

        graph = {
            "tasks": [
                {"id": "t1", "agent": "a", "needs": [], "terminal": True},
                {"id": "t2", "agent": "b", "needs": ["t1"], "terminal": True},
            ]
        }
        with pytest.raises(GraphValidationError, match="must not have dependents"):
            validate_graph(graph, {"a", "b"})


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
                    },
                    {
                        "id": "analyze",
                        "agent": "analyzer",
                        "description": "Analyze the data",
                        "needs": ["read"],
                        "terminal": True,
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

        fenced = '```json\n{"tasks": [{"id": "t1", "agent": "reader", "description": "do it", "needs": [], "terminal": true}]}\n```'

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
                        "terminal": True,
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
                        "terminal": True,
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
                        "terminal": True,
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

    def test_runtime_card(self):
        from skitter.runtime_api import AGENT_ID, runtime_card

        card = runtime_card()
        assert card["name"] == "Skitter Runtime"
        assert card["skills"][0]["id"] == AGENT_ID

    @pytest.mark.asyncio
    async def test_create_app(self):
        from unittest.mock import AsyncMock

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
        from unittest.mock import AsyncMock, MagicMock

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
        from unittest.mock import AsyncMock, MagicMock

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
        sup, mock_client = self._make_coordinator()
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
        artifact_reply = next(
            r for r in parsed if r["result"]["type"] == "TaskArtifactUpdateEvent"
        )
        status_reply = next(
            r for r in parsed if r["result"]["type"] == "TaskStatusUpdateEvent"
        )
        assert status_reply["result"]["status"]["state"] == "completed"
        artifact = artifact_reply["result"]["artifact"]["parts"][0]["text"]
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
        await sup._handle_runtime_query(query_req, "reply/q", "corr-q2")

        reply_calls = [
            call
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/q"
        ]
        assert len(reply_calls) == 2
        parsed = [json.loads(c.args[1]) for c in reply_calls]
        artifact_reply = next(
            r for r in parsed if r["result"]["type"] == "TaskArtifactUpdateEvent"
        )
        artifact = artifact_reply["result"]["artifact"]["parts"][0]["text"]
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
        from unittest.mock import AsyncMock, MagicMock, patch

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
            patch("skitter.coordinator.aiomqtt.Client", return_value=mock_app_client),
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
            c for c in reply_calls if "TaskArtifactUpdateEvent" in str(c.args[1])
        )
        reply_data = json.loads(artifact_call.args[1])
        artifact = reply_data["result"]["artifact"]["parts"][0]["text"]
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
        sup.create_session_from_graph(
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
        artifact = reply_data["result"]["artifact"]["parts"][0]["text"]
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
        artifact = reply_data["result"]["artifact"]["parts"][0]["text"]
        result = json.loads(artifact)
        assert "not found" in result["error"].lower()


_SINGLE_STEP_GRAPH = {
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


def _make_wired_coordinator(db):
    """Create a Coordinator with a mocked MQTT client (publish + subscribe)."""
    from skitter.coordinator import Coordinator

    sup = Coordinator(db)
    mock_client = MagicMock()
    mock_client.publish = AsyncMock()
    mock_client.subscribe = AsyncMock()
    sup._client = mock_client
    return sup, mock_client


def _make_wired_coordinator_with_app(db):
    """Create a Coordinator with mocked client and a single-step test app."""
    from skitter.runtime_api import create_app

    sup, mock_client = _make_wired_coordinator(db)
    create_app(db, app_id="test-app", name="Test", graph=_SINGLE_STEP_GRAPH)
    return sup, mock_client


class TestWriteAheadDispatch:
    """Verify write-ahead persistence: DB task is updated before MQTT publish."""

    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    @pytest.mark.asyncio
    async def test_db_updated_before_publish(self):
        """DB task must have dispatch info persisted before MQTT publish fires."""
        sup, mock_client = _make_wired_coordinator_with_app(self.db)

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        sid = state.session_id

        db_state_at_publish: list[dict] = []

        async def capture_publish(topic, payload, **kwargs):
            if "/request/" in str(topic) and "researcher" in str(topic):
                task = self.db.get_task(f"{sid}/step")
                db_state_at_publish.append(
                    {
                        "state": task.state,
                        "dispatch_task_id": task.dispatch_task_id,
                        "dispatched_at": task.dispatched_at,
                    }
                )

        mock_client.publish = AsyncMock(side_effect=capture_publish)
        await sup.dispatch_ready(state)

        assert len(db_state_at_publish) == 1
        snap = db_state_at_publish[0]
        assert snap["state"] == "running"
        assert snap["dispatch_task_id"] != ""
        assert snap["dispatched_at"] != ""

    @pytest.mark.asyncio
    async def test_dispatch_persists_reply_topic(self):
        """Write-ahead must persist the reply_topic for recovery."""
        sup, mock_client = _make_wired_coordinator_with_app(self.db)

        req = A2ARequest(text="go", request_id="r1")
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

        task = self.db.get_task(f"{sid}/step")
        assert task.reply_topic != ""
        assert f"/{A2A_UNIT}/" in task.reply_topic


class TestSessionRecovery:
    """Characterize the coordinator's recovery behavior on startup."""

    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    def _seed_running_session(self):
        """Seed DB with a running session: one completed task, one dispatched."""
        from skitter.db import App, AppVersion, DBSession, DBTask

        self.db.create_app(App(id="app1", name="App", card_json='{"name":"App"}'))
        self.db.create_app_version(
            AppVersion(
                id="app1-v1",
                app_id="app1",
                version=1,
                graph_json=json.dumps(
                    {
                        "tasks": [
                            {
                                "id": "step-a",
                                "agent": "agent-a",
                                "description": "Do A",
                                "needs": [],
                            },
                            {
                                "id": "step-b",
                                "agent": "agent-b",
                                "description": "Do B",
                                "needs": ["step-a"],
                                "terminal": True,
                            },
                        ]
                    }
                ),
            )
        )
        self.db.create_session(
            DBSession(
                id="sess-1",
                app_version_id="app1-v1",
                request_task_id="rtid-1",
                context_id="ctx-recovery",
                request_json="{}",
                variables='{"user_request":"test"}',
                caller_reply_topic="reply/caller",
                caller_correlation="corr-caller",
                state="running",
            )
        )
        self.db.create_task(
            DBTask(
                id="sess-1/step-a",
                session_id="sess-1",
                node_id="step-a",
                agent="agent-a",
                description="Do A",
                needs="[]",
            )
        )
        self.db.update_task(
            "sess-1/step-a",
            state="completed",
            result="A done",
            dispatch_task_id="uuid-a",
        )
        self.db.create_task(
            DBTask(
                id="sess-1/step-b",
                session_id="sess-1",
                node_id="step-b",
                agent="agent-b",
                description="Do B",
                needs='["step-a"]',
                terminal="1",
            )
        )
        self.db.update_task(
            "sess-1/step-b",
            state="running",
            dispatch_task_id="uuid-b",
            reply_topic="$a2a/v1/reply/skitter/default/skitter/sess-1/step-b",
            dispatched_at="2026-01-01T00:00:00+00:00",
        )

    @pytest.mark.asyncio
    async def test_recover_rehydrates_session(self):
        """Recovery must reconstruct in-memory session state from DB."""
        self._seed_running_session()
        sup, _ = _make_wired_coordinator(self.db)
        await sup.recover()

        assert "sess-1" in sup._sessions
        state = sup._sessions["sess-1"]
        assert state.context_id == "ctx-recovery"
        assert state.caller_reply_topic == "reply/caller"
        assert state.caller_correlation == "corr-caller"
        assert "step-a" in state.graph
        assert "step-b" in state.graph

    @pytest.mark.asyncio
    async def test_recover_completed_task_in_results(self):
        """Completed tasks must be in state.results after recovery."""
        self._seed_running_session()
        sup, _ = _make_wired_coordinator(self.db)
        await sup.recover()

        state = sup._sessions["sess-1"]
        assert "step-a" in state.results
        assert state.results["step-a"] == "A done"
        assert "step-a" not in state.pending
        assert "step-a" not in state.inflight

    @pytest.mark.asyncio
    async def test_recover_running_dispatched_task_is_inflight(self):
        """Running+dispatched tasks must be in state.inflight after recovery."""
        self._seed_running_session()
        sup, _ = _make_wired_coordinator(self.db)
        await sup.recover()

        state = sup._sessions["sess-1"]
        assert "step-b" in state.inflight
        assert "step-b" not in state.pending

    @pytest.mark.asyncio
    async def test_recover_resubscribes_reply_topics(self):
        """Recovery must resubscribe to reply topics for inflight tasks."""
        self._seed_running_session()
        sup, mock_client = _make_wired_coordinator(self.db)
        await sup.recover()

        subscribe_topics = [
            str(call.args[0]) for call in mock_client.subscribe.call_args_list
        ]
        expected = "$a2a/v1/reply/skitter/default/skitter/sess-1/step-b"
        assert expected in subscribe_topics

    @pytest.mark.asyncio
    async def test_recover_skips_completed_sessions(self):
        """Completed sessions must not be rehydrated."""
        self._seed_running_session()
        self.db.update_session_state("sess-1", "completed")
        sup, _ = _make_wired_coordinator(self.db)
        await sup.recover()

        assert "sess-1" not in sup._sessions

    @pytest.mark.asyncio
    async def test_recover_dispatches_newly_ready_tasks(self):
        """Recovery must dispatch pending tasks whose dependencies are met."""
        from skitter.db import App, AppVersion, DBSession, DBTask

        self.db.create_app(App(id="app2", name="App2"))
        self.db.create_app_version(
            AppVersion(
                id="app2-v1",
                app_id="app2",
                version=1,
                graph_json=json.dumps(
                    {
                        "tasks": [
                            {
                                "id": "a",
                                "agent": "x",
                                "description": "A",
                                "needs": [],
                            },
                            {
                                "id": "b",
                                "agent": "y",
                                "description": "B",
                                "needs": ["a"],
                                "terminal": True,
                            },
                        ]
                    }
                ),
            )
        )
        self.db.create_session(
            DBSession(
                id="sess-2",
                app_version_id="app2-v1",
                request_task_id="rtid-2",
                state="running",
            )
        )
        self.db.create_task(
            DBTask(
                id="sess-2/a",
                session_id="sess-2",
                node_id="a",
                agent="x",
                needs="[]",
            )
        )
        self.db.update_task("sess-2/a", state="completed", result="A result")
        self.db.create_task(
            DBTask(
                id="sess-2/b",
                session_id="sess-2",
                node_id="b",
                agent="y",
                needs='["a"]',
                terminal="1",
            )
        )

        sup, mock_client = _make_wired_coordinator(self.db)
        await sup.recover()

        state = sup._sessions["sess-2"]
        assert "b" in state.inflight
        assert "b" not in state.pending

        dispatched = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if "/request/" in str(call.args[0]) and "y" in str(call.args[0])
        ]
        assert len(dispatched) == 1
        assert dispatched[0]["method"] == "message/send"

    @pytest.mark.asyncio
    async def test_recover_timeout_fails_inflight_task(self):
        """Recovered inflight tasks that get no reply within timeout must fail."""
        self._seed_running_session()
        sup, _ = _make_wired_coordinator(self.db)
        await sup.recover()

        state = sup._sessions["sess-1"]
        assert "step-b" in state.inflight

        with patch("skitter.coordinator.asyncio.sleep", new_callable=AsyncMock):
            await sup._timeout_inflight(state, "step-b", timeout=120.0)

        assert "step-b" in state.failed
        assert "step-b" not in state.inflight

        task = self.db.get_task("sess-1/step-b")
        assert task.state == "failed"
        assert "timed out" in task.error.lower()


class TestDedupContextEdgeCases:
    """Characterize context_id edge cases in dedup: empty context = untracked."""

    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    def _create_session(self, sup, context_id):
        version = self.db.get_current_version("test-app")
        req = A2ARequest(text="go", request_id="r1", context_id=context_id)
        sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        return req

    @pytest.mark.asyncio
    @pytest.mark.parametrize(
        "stored_ctx,incoming_ctx",
        [
            ("", "ctx-new"),
            ("ctx-original", ""),
            ("", ""),
        ],
        ids=["stored-empty", "incoming-empty", "both-empty"],
    )
    async def test_dedup_empty_context_allows_dedup(self, stored_ctx, incoming_ctx):
        """Empty context_id on either side allows dedup (untracked context)."""
        sup, mock_client = _make_wired_coordinator_with_app(self.db)
        req = self._create_session(sup, stored_ctx)

        mock_client.publish.reset_mock()
        dup = A2ARequest(
            text="retry",
            request_id="r2",
            task_id=req.task_id,
            context_id=incoming_ctx,
        )
        await sup.handle_request(dup, "reply/t2", "corr-2", "test-app")

        replies = [
            json.loads(call.args[1])
            for call in mock_client.publish.call_args_list
            if str(call.args[0]) == "reply/t2"
        ]
        assert len(replies) == 1
        assert "error" not in replies[0]

    @pytest.mark.asyncio
    async def test_identity_session_id_is_internal(self):
        """session_id is coordinator-generated; request_task_id holds requester's Task.id."""
        sup, _ = _make_wired_coordinator_with_app(self.db)

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )

        assert state.session_id != req.task_id
        assert state.request_task_id == req.task_id

        db_session = self.db.get_session(state.session_id)
        assert db_session is not None
        assert db_session.request_task_id == req.task_id

    @pytest.mark.asyncio
    async def test_dedup_looks_up_by_request_task_id(self):
        """Dedup: coordinator looks up session via _request_task_index by incoming Task.id."""
        sup, _ = _make_wired_coordinator_with_app(self.db)

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )

        assert req.task_id in sup._request_task_index
        assert sup._request_task_index[req.task_id] == state.session_id
