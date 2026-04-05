"""Coordinator unit tests."""

import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from skitter.a2a import (
    A2ARequest,
    A2A_RESPONDER_UNAVAILABLE,
    a2a_unit,
)
from skitter.coordinator import _parse_agent_id_from_topic  # noqa: F401


# --- Coordinator session management ---


class TestCoordinatorSession:
    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    def _make_coordinator(self):
        from skitter.coordinator import Coordinator

        return Coordinator(self.db)

    @pytest.mark.asyncio
    async def test_create_session_from_graph(self):
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
        state = await sup.create_session_from_graph(
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

    @pytest.mark.asyncio
    async def test_variable_interpolation(self):
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
        state = await sup.create_session_from_graph(
            graph_json=self.db.get_app_version("v1").graph_json,
            app_version_id="v1",
            request=req,
            caller_reply_topic="",
            caller_correlation="",
            variables={"topic": "quantum"},
        )
        assert "quantum" in state.graph["research"].description

    @pytest.mark.asyncio
    async def test_user_request_stored_in_variables(self):
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
        state = await sup.create_session_from_graph(
            graph_json=self.db.get_app_version("v1").graph_json,
            app_version_id="v1",
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        assert state.variables["user_request"] == "summarize the news"

    @pytest.mark.asyncio
    async def test_user_request_does_not_override_explicit(self):
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
        state = await sup.create_session_from_graph(
            graph_json=self.db.get_app_version("v1").graph_json,
            app_version_id="v1",
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
            variables={"user_request": "custom override"},
        )
        assert state.variables["user_request"] == "custom override"

    @pytest.mark.asyncio
    async def test_session_stores_context_id(self):
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
        state = await sup.create_session_from_graph(
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
        state = await sup.create_session_from_graph(
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

        # Must use SendMessage method
        assert dispatched["method"] == "SendMessage"

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
        state = await sup.create_session_from_graph(
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
        state = await sup.create_session_from_graph(
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
                    "statusUpdate": {
                        "taskId": state.graph["step"].dispatch_task_id,
                        "contextId": "",
                        "status": {"state": "TASK_STATE_COMPLETED"},
                    },
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
        state = await sup.create_session_from_graph(
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
                    "statusUpdate": {
                        "taskId": state.graph["step"].dispatch_task_id,
                        "contextId": "",
                        "status": {"state": "TASK_STATE_COMPLETED"},
                    },
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
        state = await sup.create_session_from_graph(
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
                    "statusUpdate": {
                        "taskId": state.graph["step"].dispatch_task_id,
                        "contextId": "",
                        "status": {"state": "TASK_STATE_COMPLETED"},
                    },
                },
            }
        )
        await sup.handle_reply(reply_topic, reply_payload, "")

        assert "step" in state.inflight

    @pytest.mark.asyncio
    async def test_cancel_uses_a2a_task_id(self):
        """CancelTask must reference the dispatched Task.id and include MQTT v5 properties."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = await sup.create_session_from_graph(
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

        # Find the CancelTask publish call
        cancel_publish_calls = [
            call
            for call in mock_client.publish.call_args_list
            if "/request/" in str(call.args[0])
        ]
        cancel_payloads = [json.loads(c.args[1]) for c in cancel_publish_calls]
        cancel_msgs = [c for c in cancel_payloads if c.get("method") == "CancelTask"]
        assert len(cancel_msgs) == 1
        assert cancel_msgs[0]["params"]["id"] == dispatch_task_id

        # CancelTask must include MQTT v5 properties (Response Topic + Correlation Data)
        cancel_call = next(
            c
            for c in cancel_publish_calls
            if json.loads(c.args[1]).get("method") == "CancelTask"
        )
        props = cancel_call.kwargs.get("properties")
        assert props is not None, "CancelTask must include MQTT v5 properties"
        assert getattr(props, "ResponseTopic", None), "Must set Response Topic"
        assert getattr(props, "CorrelationData", None), "Must set Correlation Data"

    @pytest.mark.asyncio
    async def test_handle_request_deduplicates_by_task_id(self):
        """Second request with same Task.id must reply with current state, not create a new session."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = await sup.create_session_from_graph(
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
        su = replies[0]["result"]["statusUpdate"]
        assert su["status"]["state"] == "TASK_STATE_WORKING"
        assert su["taskId"] == req.task_id
        # Still only one session
        assert len(sup._sessions) == 1

    @pytest.mark.asyncio
    async def test_dedup_completed_session_returns_stored_state(self):
        """Duplicate Task.id for a completed session must reply with completed state."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1")
        version = self.db.get_current_version("test-app")
        state = await sup.create_session_from_graph(
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
        artifact_reply = next(r for r in replies if "artifactUpdate" in r["result"])
        status_reply = next(r for r in replies if "statusUpdate" in r["result"])
        su = status_reply["result"]["statusUpdate"]
        assert su["status"]["state"] == "TASK_STATE_COMPLETED"
        assert su["taskId"] == req.task_id
        au = artifact_reply["result"]["artifactUpdate"]
        assert "final answer" in au["artifact"]["parts"][0]["text"]

    @pytest.mark.asyncio
    async def test_context_id_mismatch_returns_error(self):
        """Duplicate Task.id with different context_id must return -32602."""
        sup, mock_client = self._make_coordinator_with_app()

        req = A2ARequest(text="go", request_id="r1", context_id="ctx-original")
        version = self.db.get_current_version("test-app")
        await sup.create_session_from_graph(
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
        state = await sup.create_session_from_graph(
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
        state = await sup.create_session_from_graph(
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
        state = await sup.create_session_from_graph(
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
        state = await sup.create_session_from_graph(
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
        state = await sup.create_session_from_graph(
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
        state = await sup.create_session_from_graph(
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
            if c.get("result", {})
            .get("statusUpdate", {})
            .get("status", {})
            .get("state")
            == "TASK_STATE_SUBMITTED"
        ]
        assert len(submitted) == 1
        assert submitted[0]["result"]["statusUpdate"]["contextId"] == "ctx-ack-test"


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
        reg.update(
            "app1",
            {
                "name": "App1",
                "capabilities": {
                    "extensions": [
                        {
                            "uri": "urn:skitter:app",
                            "params": {"tasks": [{"id": "t1"}]},
                        }
                    ]
                },
            },
        )
        assert "agent1" in reg.list_agents()
        assert "app1" not in reg.list_agents()
        assert "app1" in reg.list_apps()
        assert "agent1" not in reg.list_apps()


# --- Helpers for write-ahead, recovery, and dedup edge case tests ---

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
    # Prevent recover() from opening real MQTT connections for app cards
    sup._start_app_connection = AsyncMock()
    return sup, mock_client


def _make_wired_coordinator_with_app(db):
    """Create a Coordinator with mocked client and a single-step test app."""
    from skitter.runtime_api import create_app

    sup, mock_client = _make_wired_coordinator(db)
    create_app(db, app_id="test-app", name="Test", graph=_SINGLE_STEP_GRAPH)
    return sup, mock_client


# --- Write-ahead dispatch ---


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
        state = await sup.create_session_from_graph(
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
        state = await sup.create_session_from_graph(
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
        assert f"/{a2a_unit()}/" in task.reply_topic


# --- Session recovery ---


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
        assert dispatched[0]["method"] == "SendMessage"

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


# --- Dedup context edge cases ---


class TestDedupContextEdgeCases:
    """Characterize context_id edge cases in dedup: empty context = untracked."""

    def setup_method(self):
        from skitter.db import SqliteDB

        self.db = SqliteDB(":memory:")

    def teardown_method(self):
        self.db.close()

    async def _create_session(self, sup, context_id):
        version = self.db.get_current_version("test-app")
        req = A2ARequest(text="go", request_id="r1", context_id=context_id)
        await sup.create_session_from_graph(
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
        req = await self._create_session(sup, stored_ctx)

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
        state = await sup.create_session_from_graph(
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
        state = await sup.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )

        assert req.task_id in sup._request_task_index
        assert sup._request_task_index[req.task_id] == state.session_id
