"""A2A protocol unit tests."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from skitter.a2a import (
    a2a_unit,
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
    topic_request,
)
from skitter.coordinator import _parse_agent_id_from_topic


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
        assert d["method"] == "SendMessage"
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
                "method": "SendMessage",
                "params": {
                    "message": {
                        "role": "ROLE_USER",
                        "parts": [{"text": "hi"}],
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
        su = d["result"]["statusUpdate"]
        assert su["taskId"] == "sess-abc123"
        assert su["status"]["state"] == "TASK_STATE_WORKING"
        msg = su["status"]["message"]
        assert msg["role"] == "ROLE_AGENT"
        assert msg["parts"] == [{"text": "hello"}]
        assert "messageId" in msg  # REQUIRED per A2A v1.0.0 proto
        assert su["metadata"]["task_name"] == "research"

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
        su = d["result"]["statusUpdate"]
        assert su["metadata"]["type"] == "tool_use"
        assert su["metadata"]["task_name"] == "research"

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
        assert ds["result"]["statusUpdate"]["status"]["state"] == "TASK_STATE_COMPLETED"
        au = da["result"]["artifactUpdate"]
        assert au["artifact"]["parts"][0]["text"] == "Final answer"

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
                "statusUpdate": {
                    "taskId": "sess-xyz",
                    "status": {"state": "TASK_STATE_FAILED"},
                },
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
        assert d["result"]["statusUpdate"]["contextId"] == "ctx-123"

    def test_context_id_always_present(self):
        """contextId is REQUIRED per A2A v1.0.0 proto, always emitted."""
        event = make_status_event("req-1", "sess-1", "working", message="hi")
        d = json.loads(event)
        assert d["result"]["statusUpdate"]["contextId"] == ""

    def test_artifact_event_has_context_id(self):
        """contextId is REQUIRED on TaskArtifactUpdateEvent per proto."""
        event = make_artifact_event("req-1", "t1", "result", context_id="ctx-1")
        d = json.loads(event)
        assert d["result"]["artifactUpdate"]["contextId"] == "ctx-1"
        # Also present when empty
        event2 = make_artifact_event("req-1", "t1", "result")
        d2 = json.loads(event2)
        assert d2["result"]["artifactUpdate"]["contextId"] == ""

    def test_input_required_is_stream_final(self):
        """input-required MUST be treated as stream-final (A2A-over-MQTT spec)."""
        from skitter.a2a import REPLY_INPUT_REQUIRED

        d = {
            "jsonrpc": "2.0",
            "id": "req-1",
            "result": {
                "statusUpdate": {
                    "taskId": "t1",
                    "contextId": "ctx-1",
                    "status": {
                        "state": "TASK_STATE_INPUT_REQUIRED",
                        "message": {
                            "role": "ROLE_AGENT",
                            "parts": [{"text": "What is your name?"}],
                        },
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
                "statusUpdate": {
                    "taskId": "t1",
                    "contextId": "",
                    "status": {"state": "TASK_STATE_AUTH_REQUIRED"},
                },
            },
        }
        kind, content = classify_reply(d)
        assert kind == REPLY_INPUT_REQUIRED
        assert content == "auth-required"

    def test_status_event_has_timestamp(self):
        """TaskStatus must include ISO 8601 timestamp per proto."""
        event = make_status_event("req-1", "t1", "working", message="hi")
        d = json.loads(event)
        ts = d["result"]["statusUpdate"]["status"]["timestamp"]
        assert ts  # non-empty
        # Must be ISO 8601 parseable
        from datetime import datetime

        datetime.fromisoformat(ts)

    def test_artifact_event_append_default_false(self):
        """append field omitted when False (proto3 default)."""
        event = make_artifact_event("req-1", "t1", "result")
        d = json.loads(event)
        assert "append" not in d["result"]["artifactUpdate"]

    def test_artifact_event_append_true(self):
        """append=True must be included in the wire format."""
        event = make_artifact_event("req-1", "t1", "chunk2", append=True)
        d = json.loads(event)
        assert d["result"]["artifactUpdate"]["append"] is True

    def test_classify_stream_response_task_completed(self):
        """StreamResponse.task with completed status maps to REPLY_TERMINAL."""
        d = {
            "jsonrpc": "2.0",
            "id": "r1",
            "result": {
                "task": {
                    "id": "t1",
                    "contextId": "ctx-1",
                    "status": {"state": "TASK_STATE_COMPLETED"},
                }
            },
        }
        kind, content = classify_reply(d)
        assert kind == REPLY_TERMINAL

    def test_classify_task_with_artifacts(self):
        """SendMessageResponse.task with artifacts extracts artifact text."""
        d = {
            "jsonrpc": "2.0",
            "id": "r1",
            "result": {
                "task": {
                    "id": "t1",
                    "status": {"state": "TASK_STATE_COMPLETED"},
                    "artifacts": [
                        {
                            "artifactId": "a1",
                            "parts": [{"text": "Final answer"}],
                        }
                    ],
                }
            },
        }
        kind, content = classify_reply(d)
        assert kind == REPLY_TERMINAL
        assert content == "Final answer"

    def test_classify_stream_response_task_failed(self):
        """StreamResponse.task with failed status maps to REPLY_FAILED."""
        d = {
            "jsonrpc": "2.0",
            "id": "r1",
            "result": {
                "task": {
                    "id": "t1",
                    "status": {
                        "state": "TASK_STATE_FAILED",
                        "message": {
                            "role": "ROLE_AGENT",
                            "parts": [{"text": "boom"}],
                        },
                    },
                }
            },
        }
        kind, content = classify_reply(d)
        assert kind == REPLY_FAILED
        assert content == "boom"

    def test_classify_stream_response_task_working(self):
        """StreamResponse.task with working status maps to REPLY_TEXT."""
        d = {
            "jsonrpc": "2.0",
            "id": "r1",
            "result": {
                "task": {
                    "id": "t1",
                    "status": {
                        "state": "TASK_STATE_WORKING",
                        "message": {
                            "role": "ROLE_AGENT",
                            "parts": [{"text": "thinking"}],
                        },
                    },
                }
            },
        }
        kind, content = classify_reply(d)
        assert kind == REPLY_TEXT
        assert content == "thinking"

    def test_classify_stream_response_message(self):
        """StreamResponse.message maps to REPLY_TEXT."""
        d = {
            "jsonrpc": "2.0",
            "id": "r1",
            "result": {
                "message": {
                    "messageId": "m1",
                    "role": "ROLE_AGENT",
                    "parts": [{"text": "hello from agent"}],
                }
            },
        }
        kind, content = classify_reply(d)
        assert kind == REPLY_TEXT
        assert content == "hello from agent"

    def test_classify_data_part(self):
        """Part with data field should serialize as JSON text."""
        d = {
            "jsonrpc": "2.0",
            "id": "r1",
            "result": {
                "message": {
                    "messageId": "m1",
                    "role": "ROLE_AGENT",
                    "parts": [{"data": {"key": "value"}}],
                }
            },
        }
        kind, content = classify_reply(d)
        assert kind == REPLY_TEXT
        assert '"key"' in content
        assert '"value"' in content

    def test_classify_url_part(self):
        """Part with url field should return the URL as text."""
        d = {
            "jsonrpc": "2.0",
            "id": "r1",
            "result": {
                "message": {
                    "messageId": "m1",
                    "role": "ROLE_AGENT",
                    "parts": [{"url": "https://example.com/file.pdf"}],
                }
            },
        }
        kind, content = classify_reply(d)
        assert kind == REPLY_TEXT
        assert content == "https://example.com/file.pdf"

    def test_classify_raw_part(self):
        """Part with raw field should return a placeholder."""
        d = {
            "jsonrpc": "2.0",
            "id": "r1",
            "result": {
                "message": {
                    "messageId": "m1",
                    "role": "ROLE_AGENT",
                    "parts": [{"raw": "AQID", "filename": "data.bin"}],
                }
            },
        }
        kind, content = classify_reply(d)
        assert kind == REPLY_TEXT
        assert content == "[binary: data.bin]"

    def test_classify_artifact_with_data_part(self):
        """Artifact with data part should serialize as JSON."""
        d = {
            "jsonrpc": "2.0",
            "id": "r1",
            "result": {
                "artifactUpdate": {
                    "taskId": "t1",
                    "artifact": {
                        "artifactId": "a1",
                        "parts": [{"data": [1, 2, 3]}],
                    },
                    "lastChunk": True,
                }
            },
        }
        kind, content = classify_reply(d)
        assert kind == REPLY_ARTIFACT
        assert content == "[1, 2, 3]"

    def test_classify_multi_part_message(self):
        """Multiple parts in a message are concatenated."""
        d = {
            "jsonrpc": "2.0",
            "id": "r1",
            "result": {
                "message": {
                    "messageId": "m1",
                    "role": "ROLE_AGENT",
                    "parts": [
                        {"text": "Result: "},
                        {"data": {"key": 1}},
                    ],
                }
            },
        }
        kind, content = classify_reply(d)
        assert kind == REPLY_TEXT
        assert content == 'Result: {"key": 1}'


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
        from skitter.a2a import a2a_org

        t = topic_a2a_event("skitter")
        assert t == f"$a2a/v1/event/{a2a_org()}/{a2a_unit()}/skitter"


# --- Topic parsing ---


class TestTopicParsing:
    def test_parse_agent_id(self):
        topic = "$a2a/v1/request/skitter/default/researcher"
        assert _parse_agent_id_from_topic(topic) == "researcher"

    def test_parse_app_id(self):
        topic = "$a2a/v1/request/skitter/default/quick-research"
        assert _parse_agent_id_from_topic(topic) == "quick-research"

    def test_parse_short_topic(self):
        assert _parse_agent_id_from_topic("too/short") == ""


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
                "method": "SendMessage",
                "params": {
                    "message": {
                        "role": "ROLE_USER",
                        "parts": [{"text": "hello"}],
                        "taskId": task_id,
                    }
                },
            }
        )

    @pytest.mark.asyncio
    async def test_valid_request_returns_tuple(self):

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
        from unittest.mock import patch

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
        from unittest.mock import MagicMock, patch

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
        from unittest.mock import patch

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
        from unittest.mock import MagicMock, patch

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


# --- Safe format ---


class TestSafeFormat:
    def test_unknown_vars_left_intact(self):
        from skitter.config import safe_format

        desc = safe_format("Research {topic}, output as {format}.", {"topic": "AI"})
        assert desc == "Research AI, output as {format}."


# --- A2A compliance: MQTT v5 property helpers ---


class TestMqttProperties:
    def test_make_properties_with_user_properties(self):
        from skitter.mqtt import make_properties

        props = make_properties(
            user_properties=[("a2a-status", "online"), ("a2a-status-source", "agent")],
        )
        assert props.UserProperty == [
            ("a2a-status", "online"),
            ("a2a-status-source", "agent"),
        ]

    def test_make_properties_without_user_properties(self):
        from skitter.mqtt import make_properties

        props = make_properties(correlation_data="corr-1")
        assert not hasattr(props, "UserProperty") or not props.UserProperty

    def test_make_will_properties(self):
        from paho.mqtt.packettypes import PacketTypes

        from skitter.mqtt import make_will_properties

        props = make_will_properties(
            user_properties=[
                ("a2a-status", "offline"),
                ("a2a-status-source", "lwt"),
            ],
        )
        assert props.packetType == PacketTypes.WILLMESSAGE
        assert props.UserProperty == [
            ("a2a-status", "offline"),
            ("a2a-status-source", "lwt"),
        ]

    def test_get_user_property(self):
        from skitter.mqtt import get_user_property

        msg = MagicMock()
        props = MagicMock()
        props.UserProperty = [("a2a-status", "online"), ("a2a-status-source", "agent")]
        msg.properties = props

        assert get_user_property(msg, "a2a-status") == "online"
        assert get_user_property(msg, "a2a-status-source") == "agent"
        assert get_user_property(msg, "nonexistent") is None

    def test_get_user_property_no_props(self):
        from skitter.mqtt import get_user_property

        msg = MagicMock()
        msg.properties = None
        assert get_user_property(msg, "a2a-status") is None


# --- A2A compliance: Client ID format ---


class TestClientIdFormat:
    """Client ID MUST be {org_id}/{unit_id}/{agent_id} per A2A-over-MQTT spec."""

    def test_mqtt_client_kwargs_protocol_v5(self):
        from skitter.mqtt import mqtt_client_kwargs

        kwargs = mqtt_client_kwargs()
        import aiomqtt

        assert kwargs["protocol"] == aiomqtt.ProtocolVersion.V5


# --- A2A compliance: retry Correlation Data rotation ---


class TestRetryCorrelationRotation:
    """Requester MUST generate new Correlation Data for each retry attempt."""

    @pytest.mark.asyncio
    async def test_each_retry_uses_different_correlation(self):
        """Each retry publish must have unique Correlation Data; Task.id stays the same."""
        from unittest.mock import patch

        from skitter.a2a import stream_replies

        mock_client = AsyncMock()
        mock_client.subscribe = AsyncMock()
        mock_client.publish = AsyncMock()

        async def blocking_messages():
            await asyncio.sleep(999)
            yield  # never reached

        with (
            patch("aiomqtt.Client") as MockClient,
            patch("skitter.a2a.REPLY_FIRST_TIMEOUT", 0.05),
            patch("skitter.a2a._backoff_delay", return_value=0.01),
            patch("skitter.a2a.MAX_ATTEMPTS", 3),
        ):
            MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)
            type(mock_client).messages = property(lambda self: blocking_messages())

            async for _ in stream_replies("topic/req", '{"test": 1}', "corr-orig"):
                pass

        # Extract Correlation Data from each publish call's properties
        correlations = []
        for call in mock_client.publish.call_args_list:
            props = call.kwargs.get("properties")
            if props and hasattr(props, "CorrelationData"):
                cd = props.CorrelationData
                correlations.append(cd.decode() if isinstance(cd, bytes) else cd)

        assert len(correlations) == 3
        # First attempt uses original, subsequent use new values
        assert correlations[0] == "corr-orig"
        assert correlations[1] != "corr-orig"
        assert correlations[2] != "corr-orig"
        # All three must be unique
        assert len(set(correlations)) == 3


# ---------------------------------------------------------------------------
# A2A lazy namespace resolution
# ---------------------------------------------------------------------------


class TestA2ALazyNamespace:
    """P1 regression: A2A_ORG/A2A_UNIT must resolve lazily, not at import time."""

    @staticmethod
    def _reset_namespace():
        import skitter.a2a as a2a_mod

        a2a_mod._ns_resolved = False

    def test_namespace_resolves_from_config(self, tmp_path):
        """Setting SKITTER_HOME before first access must affect the namespace."""
        import skitter.a2a as a2a_mod

        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("org: custom-org\nunit: custom-unit\n")

        self._reset_namespace()

        with patch.dict(
            "os.environ",
            {"SKITTER_HOME": str(tmp_path)},
            clear=False,
        ):
            org = a2a_mod.a2a_org()
            unit = a2a_mod.a2a_unit()
        assert org == "custom-org"
        assert unit == "custom-unit"

        self._reset_namespace()

    def test_topic_builders_use_lazy_namespace(self, tmp_path):
        import skitter.a2a as a2a_mod

        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("org: myorg\nunit: myunit\n")

        self._reset_namespace()

        with patch.dict(
            "os.environ",
            {"SKITTER_HOME": str(tmp_path)},
            clear=False,
        ):
            topic = a2a_mod.topic_request("test-agent")
        assert topic == "$a2a/v1/request/myorg/myunit/test-agent"

        self._reset_namespace()
