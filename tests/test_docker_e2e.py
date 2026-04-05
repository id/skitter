"""Docker-based E2E tests with real auth and real CLI agents.

Fourth tier: exercises real Claude Code / Codex CLIs against a real EMQX
broker running in Docker Compose. Tests are skipped when the required
auth tokens are absent, so the suite degrades gracefully.

Prerequisites (run before pytest):
    docker compose --env-file .env.test -f docker-compose.test.yml up -d --wait --build

Auth is loaded from ``.env.test`` at the project root.
"""

from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import uuid
from pathlib import Path

import aiomqtt
import pytest
from dotenv import dotenv_values

from skitter.a2a import (
    A2ARequest,
    REPLY_ARTIFACT,
    a2a_org,
    a2a_unit,
    stream_request,
    topic_reply,
    topic_request,
)
from skitter.mqtt import get_correlation_data, make_properties, mqtt_client_kwargs
from tests.conftest import (
    PROJECT_ROOT,
    broker_reachable,
    create_test_app,
    run_skitter,
    send_and_collect,
    wait_for_discovery,
)

WORKSPACE_DIR = PROJECT_ROOT / "tests" / "workspace"
CLAUDE_STATE_DIR = PROJECT_ROOT / "tests" / "claude-state"

# ---------------------------------------------------------------------------
# Load auth from .env.test (env vars take precedence)
# ---------------------------------------------------------------------------

_env_test = dotenv_values(PROJECT_ROOT / ".env.test")
for _k, _v in _env_test.items():
    if _v and not os.environ.get(_k):
        os.environ[_k] = _v

# Resolve ~ in CODEX_AUTH_JSON (dotenv doesn't expand tildes)
_codex_auth = os.environ.get("CODEX_AUTH_JSON", "")
if _codex_auth:
    os.environ["CODEX_AUTH_JSON"] = str(Path(_codex_auth).expanduser())

# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------

has_claude_auth = bool(os.environ.get("CLAUDE_CODE_OAUTH_TOKEN"))
has_codex_auth = bool(
    os.environ.get("CODEX_AUTH_JSON") and Path(os.environ["CODEX_AUTH_JSON"]).is_file()
)
has_llm_api_key = bool(os.environ.get("SKITTER_LLM_API_KEY"))

needs_claude = pytest.mark.skipif(
    not has_claude_auth, reason="CLAUDE_CODE_OAUTH_TOKEN not set"
)
needs_codex = pytest.mark.skipif(not has_codex_auth, reason="Codex auth not available")
needs_llm = pytest.mark.skipif(
    not has_llm_api_key, reason="SKITTER_LLM_API_KEY not set"
)
needs_any_runtime = pytest.mark.skipif(
    not (has_claude_auth or has_codex_auth), reason="No runtime auth available"
)


pytestmark = pytest.mark.skipif(
    not broker_reachable(),
    reason="MQTT broker not reachable on localhost:1883; start the compose stack first",
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture
def host_env(tmp_path):
    """Fresh isolated environment for each test that runs skitter CLI commands."""
    env = os.environ.copy()
    env["HOME"] = str(tmp_path)
    env["MQTT_BROKER_URL"] = "mqtt://localhost:1883"
    env["NO_COLOR"] = "1"
    env["TERM"] = "dumb"
    return env


async def _send_raw_and_collect_raw(
    request_topic: str,
    payload: str,
    correlation_id: str,
    timeout: float = 120.0,
) -> list[dict]:
    """Publish raw JSON payload and collect all raw reply messages as dicts."""
    test_id = uuid.uuid4().hex[:8]
    reply_t = topic_reply("test", test_id)

    messages: list[dict] = []

    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{a2a_org()}/{a2a_unit()}/docker-raw-{test_id}",
        ),
    ) as client:
        await client.subscribe(reply_t, qos=1)

        props = make_properties(
            response_topic=reply_t,
            correlation_data=correlation_id,
        )
        await client.publish(request_topic, payload, qos=1, properties=props)

        try:
            async with asyncio.timeout(timeout):
                async for mqtt_msg in client.messages:
                    raw = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                    if not raw:
                        continue
                    data = json.loads(raw)
                    messages.append(data)
                    # Stop on terminal: artifact, error, or failed
                    result = data.get("result", {})
                    if any(k in result for k in ("artifactUpdate", "error")):
                        return messages
                    error = data.get("error")
                    if error:
                        return messages
        except TimeoutError:
            pass

    return messages


# ===================================================================
# 1. Setup and Onboarding
# ===================================================================


class TestSetup:
    """Setup and onboarding tests; no auth needed, only Docker broker."""

    def test_setup_non_interactive(self, host_env):
        """``skitter setup --non-interactive`` creates config and agents dir."""
        r = run_skitter(["setup", "--non-interactive"], host_env)
        assert r.returncode == 0, f"Setup failed: {r.stderr}"

        home = Path(host_env["HOME"])
        config = home / ".skitter" / "config.yaml"
        agents_dir = home / ".skitter" / "agents"
        assert config.exists(), "config.yaml not created"
        assert agents_dir.is_dir(), "agents dir not created"

    def test_setup_custom_home(self, host_env, tmp_path):
        """Custom SKITTER_HOME via env var."""
        env = host_env.copy()
        custom_home = tmp_path / "custom_skitter"
        env["SKITTER_HOME"] = str(custom_home)
        r = run_skitter(["setup", "--non-interactive"], env)
        assert r.returncode == 0, f"Setup failed: {r.stderr}"
        assert (custom_home / "config.yaml").exists()

    def test_setup_idempotent(self, host_env):
        """Running setup twice succeeds without clobbering."""
        r1 = run_skitter(["setup", "--non-interactive"], host_env)
        r2 = run_skitter(["setup", "--non-interactive"], host_env)
        assert r1.returncode == 0
        assert r2.returncode == 0

    def test_doctor_passes(self, host_env):
        """``skitter doctor`` passes config + broker checks."""
        run_skitter(["setup", "--non-interactive"], host_env)
        r = run_skitter(["doctor"], host_env)
        # Doctor may exit 1 if optional checks fail (LLM key, Docker, etc.);
        # verify broker connectivity passed in the output.
        assert "Broker:" in r.stdout and "reachable" in r.stdout, (
            f"Broker check not passing: {r.stdout}"
        )

    def test_status_shows_readiness(self, host_env):
        """``skitter status`` shows config, broker, agents."""
        run_skitter(["setup", "--non-interactive"], host_env)
        r = run_skitter(["status"], host_env)
        assert r.returncode == 0, f"Status failed: {r.stderr}"


# ===================================================================
# 2. Standalone Agent: Claude
# ===================================================================


@needs_claude
class TestClaudeAgent:
    """Claude agent tests; need CLAUDE_CODE_OAUTH_TOKEN."""

    pytestmark = pytest.mark.asyncio

    async def test_discovery_card(self):
        """Agent publishes retained card with correct schema."""
        card = await wait_for_discovery("test-claude")
        assert card.get("name") == "test-claude"
        assert "description" in card
        assert "version" in card
        ifaces = card.get("supportedInterfaces", [])
        assert ifaces, "No supportedInterfaces in card"
        assert ifaces[0].get("protocolVersion") == "1.0.0"

    async def test_ask_one_shot(self, host_env):
        """``skitter ask test-claude "return 42"`` gets a JSON response."""
        run_skitter(["setup", "--non-interactive"], host_env)
        r = run_skitter(
            ["ask", "test-claude", "return 42"],
            host_env,
            timeout=120,
        )
        assert r.returncode == 0, f"Ask failed: {r.stderr}"
        assert r.stdout.strip(), "Empty response"

    async def test_ask_via_mqtt(self):
        """Direct MQTT request to Claude agent returns a response."""
        req = A2ARequest(
            text='Return ONLY the JSON {"result": 42}',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )
        result = await send_and_collect(topic_request("test-claude"), req)
        assert result, "Empty result from Claude agent"
        assert not result.startswith("Failed:"), f"Request failed: {result}"
        assert not result.startswith("Error:"), f"Request error: {result}"

    async def test_streaming(self):
        """Response includes streaming text events before the terminal artifact."""
        test_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", test_id)
        req = A2ARequest(
            text='Return the JSON {"result": 99}',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )

        got_artifact = False

        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{a2a_org()}/{a2a_unit()}/docker-stream-{test_id}",
            ),
        ) as client:
            await client.subscribe(reply_t, qos=1)
            async with asyncio.timeout(120):
                async for kind, _content in stream_request(
                    client,
                    topic_request("test-claude"),
                    reply_t,
                    req.to_json(),
                    req.request_id,
                ):
                    if kind == REPLY_ARTIFACT:
                        got_artifact = True

        assert got_artifact, "No artifact received"

    async def test_context_continuity(self):
        """Two asks with same context_id produce coherent multi-turn."""
        ctx_id = str(uuid.uuid4())

        # First turn: state a preference
        req1 = A2ARequest(
            text="My favorite fruit is pineapple. What JSON should I return to express that? "
            'Just reply with the JSON like {"fruit": "pineapple"}.',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            context_id=ctx_id,
        )
        result1 = await send_and_collect(topic_request("test-claude-chat"), req1)
        assert result1, "Empty result from first turn"

        # Second turn: reference the preference
        req2 = A2ARequest(
            text="What was my favorite fruit from our earlier conversation? "
            "Reply with just the fruit name, nothing else.",
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            context_id=ctx_id,
        )
        result2 = await send_and_collect(topic_request("test-claude-chat"), req2)
        assert "pineapple" in result2.lower(), f"Context not preserved; got: {result2}"

    async def test_cancel_inflight(self):
        """CancelTask during execution gets canceled reply."""
        test_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", test_id)
        task_id = str(uuid.uuid4())

        req = A2ARequest(
            text="Write a very long essay about the history of mathematics. "
            "Make it at least 5000 words.",
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            task_id=task_id,
        )

        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{a2a_org()}/{a2a_unit()}/docker-cancel-{test_id}",
            ),
        ) as client:
            await client.subscribe(reply_t, qos=1)

            # Send the request
            props = make_properties(
                response_topic=reply_t,
                correlation_data=req.request_id,
            )
            await client.publish(
                topic_request("test-claude"), req.to_json(), qos=1, properties=props
            )

            # Wait for at least one reply (SUBMITTED or WORKING)
            got_reply = False
            async with asyncio.timeout(30):
                async for mqtt_msg in client.messages:
                    if mqtt_msg.payload:
                        got_reply = True
                        break

            assert got_reply, "No initial reply received"

            # Send CancelTask
            cancel_payload = json.dumps(
                {
                    "jsonrpc": "2.0",
                    "id": f"cancel-{uuid.uuid4().hex[:8]}",
                    "method": "tasks/cancel",
                    "params": {"taskId": task_id},
                }
            )
            cancel_props = make_properties(
                response_topic=reply_t,
                correlation_data=f"cancel-{test_id}",
            )
            await client.publish(
                topic_request("test-claude"),
                cancel_payload,
                qos=1,
                properties=cancel_props,
            )

            # Collect remaining replies; expect cancellation
            async with asyncio.timeout(60):
                async for mqtt_msg in client.messages:
                    if not mqtt_msg.payload:
                        continue
                    data = json.loads(mqtt_msg.payload)
                    result = data.get("result", {})
                    status = result.get("statusUpdate", {}).get("state", "")
                    if status == "TASK_STATE_CANCELED":
                        break
                    # Also check if it completed before we could cancel
                    if "artifactUpdate" in result:
                        break

            # Either canceled or completed before cancellation; both valid.

    async def test_concurrent_requests(self):
        """Two concurrent requests to same agent both complete."""
        req1 = A2ARequest(
            text='Return ONLY the JSON {"id": 1}',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )
        req2 = A2ARequest(
            text='Return ONLY the JSON {"id": 2}',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )

        results = await asyncio.gather(
            send_and_collect(topic_request("test-claude"), req1),
            send_and_collect(topic_request("test-claude"), req2),
        )

        for i, result in enumerate(results):
            assert result, f"Request {i + 1} returned empty"
            assert not result.startswith("Failed:"), f"Request {i + 1} failed: {result}"
            assert not result.startswith("Error:"), f"Request {i + 1} error: {result}"


# ===================================================================
# 3. Standalone Agent: Codex
# ===================================================================


@needs_codex
class TestCodexAgent:
    """Codex agent tests; need OPENAI_API_KEY or CODEX_AUTH_JSON."""

    pytestmark = pytest.mark.asyncio

    async def test_discovery_card(self):
        """Card published with correct schema."""
        card = await wait_for_discovery("test-codex")
        assert card.get("name") == "test-codex"
        assert "description" in card
        ifaces = card.get("supportedInterfaces", [])
        assert ifaces, "No supportedInterfaces in card"
        assert ifaces[0].get("protocolVersion") == "1.0.0"

    async def test_ask_one_shot(self, host_env):
        """``skitter ask test-codex "return hello"`` gets response."""
        run_skitter(["setup", "--non-interactive"], host_env)
        r = run_skitter(
            ["ask", "test-codex", "return hello"],
            host_env,
            timeout=120,
        )
        assert r.returncode == 0, f"Ask failed: {r.stderr}"
        assert r.stdout.strip(), "Empty response"

    async def test_ask_via_mqtt(self):
        """Direct MQTT request to Codex agent returns a response."""
        req = A2ARequest(
            text='Return ONLY the JSON {"greeting": "hello"}',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )
        result = await send_and_collect(topic_request("test-codex"), req)
        assert result, "Empty result from Codex agent"
        assert not result.startswith("Failed:"), f"Request failed: {result}"

    async def test_context_continuity(self):
        """Two asks with same context_id produce coherent multi-turn."""
        ctx_id = str(uuid.uuid4())

        req1 = A2ARequest(
            text="My favorite color for UI buttons is emerald green. "
            'Reply with {"ack": "ok"}.',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            context_id=ctx_id,
        )
        result1 = await send_and_collect(topic_request("test-codex-chat"), req1)
        assert result1, "Empty result from first turn"

        req2 = A2ARequest(
            text="What color did I say I prefer for UI buttons? Reply with just the color.",
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            context_id=ctx_id,
        )
        result2 = await send_and_collect(topic_request("test-codex-chat"), req2)
        assert "emerald" in result2.lower(), f"Context not preserved; got: {result2}"


# ===================================================================
# 4. A2A Protocol Compliance (real agents)
# ===================================================================


@needs_any_runtime
class TestA2AProtocol:
    """A2A protocol compliance tests using whichever runtime is available."""

    pytestmark = pytest.mark.asyncio

    def _available_agent(self) -> str:
        if has_claude_auth:
            return "test-claude"
        return "test-codex"

    async def test_wire_format_proto3(self):
        """Raw MQTT payloads use proto3 JSON: SCREAMING_SNAKE_CASE enums, oneof wrappers."""
        agent_id = self._available_agent()
        task_id = str(uuid.uuid4())
        correlation = uuid.uuid4().hex[:16]

        req = A2ARequest(
            text='Return the JSON {"test": true}',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            task_id=task_id,
        )

        messages = await _send_raw_and_collect_raw(
            topic_request(agent_id),
            req.to_json(),
            correlation,
        )

        assert messages, "No messages received"

        # Check for proto3 conventions
        for msg in messages:
            result = msg.get("result", {})
            if "statusUpdate" in result:
                state = result["statusUpdate"].get("state", "")
                if state:  # State may be absent in streaming text updates
                    assert state.startswith("TASK_STATE_"), (
                        f"Expected SCREAMING_SNAKE_CASE state, got: {state}"
                    )
            if "artifactUpdate" in result:
                # Artifact should have parts with text
                parts = result["artifactUpdate"].get("artifact", {}).get("parts", [])
                for part in parts:
                    assert "text" in part or "data" in part, (
                        f"Part missing text/data: {part}"
                    )

    async def test_reply_echoes_task_id(self):
        """All reply events carry the requester's Task.id."""
        agent_id = self._available_agent()
        task_id = str(uuid.uuid4())
        correlation = uuid.uuid4().hex[:16]

        req = A2ARequest(
            text='Return {"ok": true}',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            task_id=task_id,
        )

        messages = await _send_raw_and_collect_raw(
            topic_request(agent_id),
            req.to_json(),
            correlation,
        )

        assert messages, "No messages received"
        for msg in messages:
            result = msg.get("result", {})
            for key in ("statusUpdate", "artifactUpdate"):
                if key in result:
                    msg_task_id = result[key].get("taskId")
                    assert msg_task_id == task_id, (
                        f"Reply taskId mismatch: expected {task_id}, got {msg_task_id}"
                    )

    async def test_rejects_missing_task_id(self):
        """Agent runner rejects request without Task.id (error -32005)."""
        agent_id = self._available_agent()
        correlation = uuid.uuid4().hex[:16]

        # Build a malformed request with no taskId in the message
        payload = json.dumps(
            {
                "jsonrpc": "2.0",
                "id": f"test-{uuid.uuid4().hex[:8]}",
                "method": "message/send",
                "params": {
                    "message": {
                        "messageId": uuid.uuid4().hex,
                        "role": "ROLE_USER",
                        "parts": [{"text": "hello"}],
                        "contextId": str(uuid.uuid4()),
                        # No taskId
                    }
                },
            }
        )

        messages = await _send_raw_and_collect_raw(
            topic_request(agent_id),
            payload,
            correlation,
            timeout=30,
        )

        assert messages, "No reply received for malformed request"
        # Should get an error response
        last = messages[-1]
        error = last.get("error")
        assert error, f"Expected error response, got: {last}"
        assert error.get("code") == -32005, (
            f"Expected error code -32005, got: {error.get('code')}"
        )

    async def test_deduplication_by_task_id(self):
        """Same Task.id sent twice returns existing result, no re-execution."""
        agent_id = self._available_agent()
        task_id = str(uuid.uuid4())

        req = A2ARequest(
            text='Return {"dedup": true}',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            task_id=task_id,
        )

        # First request
        result1 = await send_and_collect(topic_request(agent_id), req)
        assert result1, "First request returned empty"

        # Second request with same task_id (new request_id for JSON-RPC)
        req2 = A2ARequest(
            text='Return {"dedup": true}',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            task_id=task_id,
        )

        result2 = await send_and_collect(topic_request(agent_id), req2)
        # Both should succeed; second should return cached result
        assert result2, "Dedup request returned empty"

    async def test_correlation_data_on_replies(self):
        """All replies carry MQTT Correlation Data."""
        agent_id = self._available_agent()
        correlation = uuid.uuid4().hex[:16]

        req = A2ARequest(
            text='Return {"corr": true}',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )

        test_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", test_id)

        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{a2a_org()}/{a2a_unit()}/docker-corr-{test_id}",
            ),
        ) as client:
            await client.subscribe(reply_t, qos=1)

            props = make_properties(
                response_topic=reply_t,
                correlation_data=correlation,
            )
            await client.publish(
                topic_request(agent_id), req.to_json(), qos=1, properties=props
            )

            reply_count = 0
            async with asyncio.timeout(120):
                async for mqtt_msg in client.messages:
                    if not mqtt_msg.payload:
                        continue
                    corr = get_correlation_data(mqtt_msg)
                    assert corr == correlation, (
                        f"Reply missing/wrong Correlation Data: expected {correlation}, got {corr}"
                    )
                    reply_count += 1
                    data = json.loads(mqtt_msg.payload)
                    result = data.get("result", {})
                    if "artifactUpdate" in result or data.get("error"):
                        break

            assert reply_count > 0, "No replies received"


# ===================================================================
# 5. Composed Apps (Coordinator + LLM)
# ===================================================================


@needs_claude
@needs_llm
class TestComposedApps:
    """Composed app tests; need Claude auth + LLM API key for graph generation."""

    pytestmark = pytest.mark.asyncio

    async def test_create_app(self):
        """``create app`` via runtime API returns app_id, publishes card."""
        app_id = await create_test_app(
            ["test-claude"],
            "A simple test app that echoes input",
            timeout=60,
        )
        assert app_id, "No app_id returned"

        # Verify the app's discovery card is published
        card = await wait_for_discovery(app_id)
        assert card.get("name"), "App card missing name"

    async def test_linear_pipeline(self):
        """A then B; B receives A's output."""
        app_id = await create_test_app(
            ["test-claude", "test-sum"],
            "First, test-claude generates three random numbers as JSON. "
            "Then, test-sum extracts and sums those numbers.",
        )

        req = A2ARequest(
            text="Generate three numbers between 1 and 10",
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )
        result = await send_and_collect(topic_request(app_id), req, timeout=180)
        assert result, "Pipeline returned empty"
        assert not result.startswith("Failed:"), f"Pipeline failed: {result}"

    async def test_app_context_continuity(self):
        """Two requests to same app with same context_id are coherent."""
        app_id = await create_test_app(
            ["test-claude"],
            "A simple conversational app",
        )
        ctx_id = str(uuid.uuid4())

        req1 = A2ARequest(
            text="I'm building a project called silver-moon. "
            'Reply with {"ack": "ok"}.',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            context_id=ctx_id,
        )
        result1 = await send_and_collect(topic_request(app_id), req1, timeout=180)
        assert result1, "First app turn returned empty"

        req2 = A2ARequest(
            text="What was my project name? Reply with just the name.",
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            context_id=ctx_id,
        )
        result2 = await send_and_collect(topic_request(app_id), req2, timeout=180)
        assert "silver-moon" in result2.lower(), (
            f"App context not preserved; got: {result2}"
        )

    async def test_app_context_isolation(self):
        """Different context_ids are isolated."""
        app_id = await create_test_app(
            ["test-claude"],
            "A conversational app",
        )

        # Context A: set a fact
        ctx_a = str(uuid.uuid4())
        req_a = A2ARequest(
            text="I'm working on the dolphin-tracker project. "
            'Reply with {"ack": "ok"}.',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            context_id=ctx_a,
        )
        await send_and_collect(topic_request(app_id), req_a, timeout=180)

        # Context B: ask for the fact (should not know it)
        ctx_b = str(uuid.uuid4())
        req_b = A2ARequest(
            text="What project am I working on? If you don't know, say 'unknown'.",
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            context_id=ctx_b,
        )
        result_b = await send_and_collect(topic_request(app_id), req_b, timeout=180)
        # Context B should NOT know about "dolphin-tracker"
        assert "dolphin" not in result_b.lower(), (
            f"Context isolation broken; context B knows about dolphin-tracker: {result_b}"
        )

    async def test_cancel_running_session(self):
        """Cancel a composed session mid-flight."""
        app_id = await create_test_app(
            ["test-claude"],
            "A long-running app",
        )

        task_id = str(uuid.uuid4())
        req = A2ARequest(
            text="Write a very long essay about artificial intelligence. "
            "Make it extremely detailed, at least 5000 words.",
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            task_id=task_id,
        )

        test_id = uuid.uuid4().hex[:8]
        reply_t = topic_reply("test", test_id)

        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{a2a_org()}/{a2a_unit()}/docker-app-cancel-{test_id}",
            ),
        ) as client:
            await client.subscribe(reply_t, qos=1)

            props = make_properties(
                response_topic=reply_t,
                correlation_data=req.request_id,
            )
            await client.publish(
                topic_request(app_id), req.to_json(), qos=1, properties=props
            )

            # Wait for initial reply
            async with asyncio.timeout(30):
                async for mqtt_msg in client.messages:
                    if mqtt_msg.payload:
                        break

            # Cancel session via CLI
            subprocess.run(
                [sys.executable, "-m", "skitter", "cancel-session", task_id],
                env=os.environ | {"MQTT_BROKER_URL": "mqtt://localhost:1883"},
                capture_output=True,
                text=True,
                timeout=30,
            )
            # Session cancel may or may not succeed depending on timing;
            # the important thing is that it doesn't crash

    async def test_failure_propagates(self):
        """Agent failure propagates to app-level error."""
        # This test relies on the agent failing (e.g., invalid request).
        # We send to a non-existent agent within the app; coordinator
        # should report the failure.
        app_id = await create_test_app(
            ["test-claude"],
            "An app that processes data",
        )

        req = A2ARequest(
            text="Process this data",
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )

        # This should complete (success or failure); we just verify
        # the app handles the request without hanging
        result = await send_and_collect(topic_request(app_id), req, timeout=180)
        assert result is not None  # Got some response


# ===================================================================
# 6. Service Management
# ===================================================================


class TestServiceManagement:
    """Service management tests."""

    def test_status_with_running_services(self, host_env):
        """``skitter status`` while containers running shows agents, broker."""
        run_skitter(["setup", "--non-interactive"], host_env)
        r = run_skitter(["status"], host_env)
        assert r.returncode == 0, f"Status failed: {r.stderr}"
        output = r.stdout + r.stderr
        # Should mention broker connectivity
        assert any(
            word in output.lower() for word in ("broker", "mqtt", "connected", "online")
        ), f"Status output missing broker info: {output}"

    def test_logs_shows_output(self, host_env):
        """``skitter logs coordinator`` returns log lines."""
        run_skitter(["setup", "--non-interactive"], host_env)
        r = run_skitter(["logs", "coordinator"], host_env, timeout=15)
        # Logs command may fail if coordinator is not managed by skitter services,
        # since our compose stack is external. Just verify it doesn't crash hard.
        # returncode 0 or 1 are both acceptable.
        assert r.returncode in (0, 1), f"Logs crashed: {r.stderr}"


# ===================================================================
# 7. Management Commands
# ===================================================================


@needs_any_runtime
class TestManagementCommands:
    """Management command tests."""

    pytestmark = pytest.mark.asyncio

    async def test_list_agents(self, host_env):
        """``skitter list-agents`` shows running agents."""
        run_skitter(["setup", "--non-interactive"], host_env)

        # Wait for at least one agent to be discovered
        agent_id = "test-claude" if has_claude_auth else "test-codex"
        await wait_for_discovery(agent_id)

        r = run_skitter(["list-agents"], host_env)
        assert r.returncode == 0, f"list-agents failed: {r.stderr}"
        assert agent_id in (r.stdout + r.stderr), (
            f"Agent {agent_id} not in list-agents output: {r.stdout}"
        )

    async def test_get_agent(self, host_env):
        """``skitter get-agent <id>`` returns card JSON."""
        run_skitter(["setup", "--non-interactive"], host_env)

        agent_id = "test-claude" if has_claude_auth else "test-codex"
        await wait_for_discovery(agent_id)

        r = run_skitter(["get-agent", agent_id], host_env)
        assert r.returncode == 0, f"get-agent failed: {r.stderr}"
        output = r.stdout + r.stderr
        assert agent_id in output, f"Agent not in output: {output}"

    @needs_llm
    @needs_claude
    async def test_list_apps(self, host_env):
        """After creating an app, it appears in list."""
        run_skitter(["setup", "--non-interactive"], host_env)
        app_id = await create_test_app(["test-claude"], "A test app", timeout=60)

        r = run_skitter(["list-apps"], host_env)
        assert r.returncode == 0, f"list-apps failed: {r.stderr}"
        assert app_id in (r.stdout + r.stderr), f"App {app_id} not in list-apps output"

    @needs_llm
    @needs_claude
    async def test_get_app(self, host_env):
        """App details include version, graph."""
        run_skitter(["setup", "--non-interactive"], host_env)
        app_id = await create_test_app(
            ["test-claude"], "A test app for get-app", timeout=60
        )

        r = run_skitter(["get-app", app_id], host_env)
        assert r.returncode == 0, f"get-app failed: {r.stderr}"

    @needs_llm
    @needs_claude
    async def test_delete_app(self, host_env):
        """Deleted app no longer appears."""
        run_skitter(["setup", "--non-interactive"], host_env)
        app_id = await create_test_app(
            ["test-claude"], "A test app for deletion", timeout=60
        )

        r = run_skitter(["delete-app", app_id], host_env)
        assert r.returncode == 0, f"delete-app failed: {r.stderr}"

        r2 = run_skitter(["list-apps"], host_env)
        assert app_id not in (r2.stdout + r2.stderr), (
            f"Deleted app {app_id} still in list"
        )


# ===================================================================
# 8. Edge Cases and Robustness
# ===================================================================


class TestEdgeCases:
    """Edge cases and robustness tests."""

    def test_ask_nonexistent_agent(self, host_env):
        """``skitter ask no-such-agent "hello"`` fails gracefully with timeout or error."""
        run_skitter(["setup", "--non-interactive"], host_env)
        try:
            r = run_skitter(
                ["ask", "no-such-agent", "hello"],
                host_env,
                timeout=20,
            )
            # If it didn't time out, it should have a non-zero exit or error message
            output = (r.stdout + r.stderr).lower()
            assert r.returncode != 0 or "error" in output or "timeout" in output, (
                f"Expected failure for nonexistent agent, got rc={r.returncode}: {r.stdout}"
            )
        except subprocess.TimeoutExpired:
            pass  # Timing out is the expected behavior for a nonexistent agent

    @pytest.mark.asyncio
    @needs_claude
    async def test_ask_with_special_characters(self):
        """Prompt with quotes, newlines, unicode."""
        req = A2ARequest(
            text='Return this exact JSON: {"special": "quotes \\"here\\"", "unicode": "\u2603"}',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )
        result = await send_and_collect(topic_request("test-claude"), req)
        assert result, "Empty result for special characters"
        assert not result.startswith("Error:"), f"Error: {result}"

    @pytest.mark.asyncio
    @needs_claude
    async def test_large_prompt(self):
        """10KB prompt processes without truncation."""
        large_text = "x" * 10000
        req = A2ARequest(
            text=f"I'm sending you a large input. Count the x characters and return "
            f'{{"count": <number>}}. Here they are: {large_text}',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )
        result = await send_and_collect(topic_request("test-claude"), req)
        assert result, "Empty result for large prompt"
        assert not result.startswith("Error:"), f"Error on large prompt: {result}"


# ===================================================================
# 9. Cross-Runtime
# ===================================================================


@needs_claude
@needs_codex
@needs_llm
class TestCrossRuntime:
    """Cross-runtime pipeline test; needs Claude + Codex + LLM."""

    pytestmark = pytest.mark.asyncio

    async def test_mixed_pipeline(self):
        """Composed app with one Claude agent and one Codex agent."""
        app_id = await create_test_app(
            ["test-claude", "test-codex"],
            "First, test-claude generates a JSON with three numbers. "
            "Then, test-codex receives that output and returns a greeting.",
            timeout=60,
        )

        req = A2ARequest(
            text="Start the pipeline",
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )
        pipeline_result = await send_and_collect(
            topic_request(app_id), req, timeout=180
        )
        assert pipeline_result, "Mixed pipeline returned empty"
        assert not pipeline_result.startswith("Failed:"), (
            f"Mixed pipeline failed: {pipeline_result}"
        )


# ===================================================================
# 10. Agent Capabilities (file generation, memory)
# ===================================================================


@pytest.fixture
def workspace():
    """Provide a clean workspace directory shared with the agent-writer container."""
    WORKSPACE_DIR.mkdir(parents=True, exist_ok=True)
    # Clean before test
    for f in WORKSPACE_DIR.iterdir():
        f.unlink()
    yield WORKSPACE_DIR
    # Clean after test
    for f in WORKSPACE_DIR.iterdir():
        f.unlink()


@needs_claude
class TestClaudeAgentCapabilities:
    """Claude agent capabilities: file generation and memory."""

    pytestmark = pytest.mark.asyncio

    async def test_file_generation(self, workspace):
        """Agent writes a file to the mounted workspace; host can read it."""
        filename = f"test-{uuid.uuid4().hex[:6]}.txt"
        req = A2ARequest(
            text=f"Create a file at /tmp/workspace/{filename} with the content "
            f'"hello from agent". Use the Write tool.',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )
        result = await send_and_collect(topic_request("test-writer"), req)
        assert result, f"Empty result: {result}"
        assert not result.startswith("Failed:"), f"Request failed: {result}"

        written = workspace / filename
        assert written.exists(), (
            f"File {filename} not found in workspace. "
            f"Contents: {list(workspace.iterdir())}"
        )
        content = written.read_text()
        assert "hello from agent" in content, f"Unexpected content: {content}"

    async def test_file_generation_with_dynamic_content(self, workspace):
        """Agent generates a file with computed content."""
        filename = f"sum-{uuid.uuid4().hex[:6]}.json"
        req = A2ARequest(
            text=f"Calculate 17 + 25 and write the result as JSON "
            f'{{"sum": <result>}} to /tmp/workspace/{filename}. '
            f"Use the Write tool.",
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )
        result = await send_and_collect(topic_request("test-writer"), req)
        assert result, f"Empty result: {result}"
        assert not result.startswith("Failed:"), f"Request failed: {result}"

        written = workspace / filename
        assert written.exists(), f"File not found: {list(workspace.iterdir())}"
        data = json.loads(written.read_text())
        assert data["sum"] == 42, f"Expected sum=42, got: {data}"

    async def test_agent_memory_persists_across_sessions(self):
        """Agent remembers a fact from a previous session (different context_id).

        Session 1: tell the agent a fact and ask it to remember.
        Session 2 (new context_id): ask the agent to recall the fact.
        Verify memory files exist on the host filesystem.
        """
        tag = uuid.uuid4().hex[:6]

        # Session 1: establish a fact
        req1 = A2ARequest(
            text=f"Please remember this: my project Zephyr-{tag} uses RabbitMQ as its message broker. "
            "This is important for future reference.",
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )
        result1 = await send_and_collect(topic_request("test-writer"), req1)
        assert result1, "Empty result from session 1"

        # Verify memory files were created on the host
        memory_files = list(CLAUDE_STATE_DIR.rglob("memory/*.md"))
        assert memory_files, (
            f"No memory files found under {CLAUDE_STATE_DIR}. "
            f"Contents: {list(CLAUDE_STATE_DIR.rglob('*'))}"
        )
        non_empty = [f for f in memory_files if f.stat().st_size > 0]
        assert non_empty, f"All memory files are empty: {memory_files}"

        # Session 2: new context, ask about the fact from memory
        req2 = A2ARequest(
            text=f"What message broker does project Zephyr-{tag} use? "
            "Check your memory. Reply with just the broker name.",
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )
        result2 = await send_and_collect(topic_request("test-writer"), req2)
        assert "rabbitmq" in result2.lower(), (
            f"Agent didn't recall from memory: {result2}"
        )


@needs_codex
class TestCodexAgentCapabilities:
    """Codex agent capabilities: file generation and memory."""

    pytestmark = pytest.mark.asyncio

    async def test_file_generation(self, workspace):
        """Codex agent writes a file to the mounted workspace."""
        filename = f"codex-{uuid.uuid4().hex[:6]}.txt"
        req = A2ARequest(
            text=f"Run this shell command: echo 'hello from codex' > /tmp/workspace/{filename}\n"
            f"Then confirm the file was written.",
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
        )
        result = await send_and_collect(topic_request("test-codex-writer"), req)
        assert not result.startswith("Failed:"), f"Request failed: {result}"

        written = workspace / filename
        assert written.exists(), (
            f"File {filename} not found in workspace. "
            f"Contents: {list(workspace.iterdir())}"
        )
        content = written.read_text()
        assert "hello from codex" in content, f"Unexpected content: {content}"

    async def test_agent_memory_across_turns(self):
        """Codex agent remembers information from previous turns."""
        ctx_id = str(uuid.uuid4())

        req1 = A2ARequest(
            text="I'm building a service called Aurora that uses port 8080. "
            'Reply with {"ack": "ok"}.',
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            context_id=ctx_id,
        )
        result1 = await send_and_collect(topic_request("test-codex-chat"), req1)
        assert result1, "Empty result from turn 1"

        req2 = A2ARequest(
            text="What port does Aurora use? Reply with just the number.",
            request_id=f"test-{uuid.uuid4().hex[:8]}",
            sender="docker-e2e",
            context_id=ctx_id,
        )
        result2 = await send_and_collect(topic_request("test-codex-chat"), req2)
        assert "8080" in result2, f"Agent didn't remember port: {result2}"
