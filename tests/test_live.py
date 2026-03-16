"""Live e2e tests — agent-runners + coordinator in Docker + composed apps.

Both agent-runners and the coordinator run in Docker containers for full
isolation.  Credentials are loaded from .env.test at the project root.

Requires:
  - Docker with the skitter network (docker compose up -d)
  - MQTT broker on the skitter network
  - .env.test with CLAUDE_CODE_OAUTH_TOKEN and/or OPENAI_API_KEY

Usage:
  uv run pytest tests/test_live.py -v -s                    # auto-detect runtime
  uv run pytest tests/test_live.py -v -s --runtime claude   # claude only
  uv run pytest tests/test_live.py -v -s --runtime codex    # codex only
"""

from __future__ import annotations

import asyncio
import json
import os
import random
import uuid

import aiomqtt
import pytest

from skitter.mqtt import make_properties, mqtt_client_kwargs, topic_reply, topic_request
from skitter.types import (
    A2ARequest,
    REPLY_ERROR,
    REPLY_FAILED,
    REPLY_SUBMITTED,
    REPLY_TERMINAL,
    classify_reply,
)

from .conftest import (
    FakeAgent,
    _load_env_test,
    docker_available,
    needs_mqtt,
    runtime_available,
    send_and_collect,
    start_agent_runner,
    start_coordinator,
    stop_container,
    wait_for_discovery,
    write_agent_file,
)

MODELS = {
    "claude": os.environ.get("SKITTER_TEST_CLAUDE_MODEL", "haiku"),
    "codex": os.environ.get("SKITTER_TEST_CODEX_MODEL", ""),
}

needs_docker = pytest.mark.skipif(not docker_available(), reason="Docker not available")


def _has_llm_key() -> bool:
    env = {**_load_env_test(), **os.environ}
    return bool(env.get("ANTHROPIC_API_KEY") or env.get("OPENAI_API_KEY"))


needs_llm = pytest.mark.skipif(not _has_llm_key(), reason="No LLM API key")


def _pick_runtime(config) -> str:
    selected = config.getoption("--runtime", default=None)
    if selected:
        if not runtime_available(selected):
            pytest.skip(f"{selected} credentials not available")
        return selected
    for rt in ("claude", "codex"):
        if runtime_available(rt):
            return rt
    pytest.skip(
        "No runtime credentials (need CLAUDE_CODE_OAUTH_TOKEN or ~/.codex/auth.json)"
    )


@pytest.fixture(scope="module")
def runtime(request):
    return _pick_runtime(request.config)


@pytest.fixture(scope="module")
def coordinator():
    """Start coordinator in Docker."""
    container_id = start_coordinator(
        env_vars={"SKITTER_LLM_MODEL": "anthropic/claude-haiku-4-5"},
    )
    yield container_id
    stop_container(container_id)


@pytest.fixture(scope="module")
def agent(runtime, coordinator):
    """Start an agent-runner in Docker, wait for discovery."""
    agent_id = f"test-{runtime}"
    model = MODELS.get(runtime, "")
    agent_path = write_agent_file(agent_id, runtime, model)
    container_id = start_agent_runner(agent_path, runtime)
    yield agent_id
    stop_container(container_id)
    agent_path.unlink(missing_ok=True)


@needs_mqtt
@needs_docker
class TestLive:
    @pytest.mark.asyncio
    async def test_agent_discovery(self, agent):
        """Agent-runner publishes a spec-conformant discovery card."""
        card = await wait_for_discovery(agent)
        assert card["protocolVersion"] == "0.2.5"
        assert card["capabilities"]["streaming"] is True

    @pytest.mark.asyncio
    async def test_agent_query(self, agent):
        """Send a query to the agent, get a response."""

        req = A2ARequest(
            text="What is 2+2? Reply with just the number.",
            request_id=f"query-{uuid.uuid4().hex[:8]}",
            sender="test",
        )
        result = await send_and_collect(topic_request(agent), req, timeout=30.0)
        assert result, "Empty result"
        assert "4" in result
        print(f"\nResult: {result}")


# ---------------------------------------------------------------------------
# Helpers for fake-agent tests
# ---------------------------------------------------------------------------


def _merger_fn(text: str) -> str:
    """Extract all JSON objects from coordinator context and merge them."""
    merged: dict = {}
    for line in text.split("\n"):
        line = line.strip()
        if line.startswith("{"):
            try:
                merged.update(json.loads(line))
            except json.JSONDecodeError:
                pass
    return json.dumps(merged)


async def _create_app(agent_ids: list[str], instructions: str) -> str:
    """Create a composed app via the runtime API, return the app_id."""
    spec = json.dumps(
        {
            "name": f"Test-{uuid.uuid4().hex[:6]}",
            "instructions": instructions,
            "agents": agent_ids,
        }
    )
    create_req = A2ARequest(
        text=f"create app {spec}",
        request_id=f"create-{uuid.uuid4().hex[:8]}",
        sender="test",
    )
    result = await send_and_collect(topic_request("skitter"), create_req, timeout=30.0)
    assert result, "Create app returned empty"
    data = json.loads(result)
    assert "created_app" in data, f"Unexpected: {result}"
    return data["created_app"]["app_id"]


# ---------------------------------------------------------------------------
# Composed app: deterministic fake agents with JSON output
# ---------------------------------------------------------------------------


@needs_mqtt
@needs_docker
@needs_llm
class TestComposedApp:
    @pytest.mark.asyncio
    async def test_composed_app_merges_json(self, coordinator):
        """Two producers emit JSON, merger combines them. Verify the chain."""
        aid_a = f"producer-a-{uuid.uuid4().hex[:4]}"
        aid_b = f"producer-b-{uuid.uuid4().hex[:4]}"
        aid_m = f"merger-{uuid.uuid4().hex[:4]}"

        num_a = random.randint(1, 10000)
        num_b = random.randint(1, 10000)

        async with (
            FakeAgent(
                aid_a,
                description="Produces a JSON object with a random number",
                reply_fn=lambda _text, _a=aid_a, _n=num_a: json.dumps({_a: _n}),
            ),
            FakeAgent(
                aid_b,
                description="Produces a JSON object with a random number",
                reply_fn=lambda _text, _b=aid_b, _n=num_b: json.dumps({_b: _n}),
            ),
            FakeAgent(
                aid_m,
                description="Merges upstream JSON objects into one",
                reply_fn=_merger_fn,
            ),
        ):
            await wait_for_discovery(aid_a)
            await wait_for_discovery(aid_b)
            await wait_for_discovery(aid_m)
            await asyncio.sleep(2)

            print("\nCreating composed app...")
            app_id = await _create_app(
                [aid_a, aid_b, aid_m],
                f"{aid_a} and {aid_b} each produce a JSON object independently. "
                f"Then {aid_m} merges their outputs into a single JSON object.",
            )
            print(f"Created app '{app_id}'")
            await asyncio.sleep(1)

            app_req = A2ARequest(
                text="Go.",
                request_id=f"app-{uuid.uuid4().hex[:8]}",
                sender="test",
            )
            print(f"Sending request to '{app_id}'...")
            result = await send_and_collect(
                topic_request(app_id), app_req, timeout=60.0
            )
            assert result, "Empty result"

            merged = json.loads(result)
            print(f"Merged result: {merged}")
            assert merged[aid_a] == num_a, f"Expected {aid_a}={num_a}, got {merged}"
            assert merged[aid_b] == num_b, f"Expected {aid_b}={num_b}, got {merged}"


# ---------------------------------------------------------------------------
# Cancellation + failure
# ---------------------------------------------------------------------------


@needs_mqtt
@needs_docker
@needs_llm
class TestCancellation:
    @pytest.mark.asyncio
    async def test_cancel_running_session(self, coordinator):
        """Cancel a session while agents are still processing."""
        aid_a = f"fake-slow-a-{uuid.uuid4().hex[:4]}"
        aid_b = f"fake-slow-b-{uuid.uuid4().hex[:4]}"

        async with (
            FakeAgent(aid_a, reply_delay=60.0),
            FakeAgent(aid_b, reply_delay=60.0),
        ):
            await wait_for_discovery(aid_a)
            await wait_for_discovery(aid_b)
            await asyncio.sleep(2)

            app_id = await _create_app(
                [aid_a, aid_b],
                f"First {aid_a} greets. Then {aid_b} summarizes.",
            )
            await asyncio.sleep(1)

            # Send request and track the reply
            test_id = uuid.uuid4().hex[:8]
            reply_t = topic_reply("test", test_id)

            async with aiomqtt.Client(
                **mqtt_client_kwargs(
                    identifier=f"skitter/default/test-cancel-{test_id}",
                ),
            ) as client:
                await client.subscribe(reply_t, qos=1)

                app_req = A2ARequest(
                    text="Hello!",
                    request_id=f"app-{uuid.uuid4().hex[:8]}",
                    sender="test",
                )
                props = make_properties(
                    response_topic=reply_t,
                    correlation_data=app_req.request_id,
                )
                await client.publish(
                    topic_request(app_id),
                    app_req.to_json(),
                    qos=1,
                    properties=props,
                )

                # Wait for submitted ack to get session_id
                session_id = None
                async with asyncio.timeout(15.0):
                    async for msg in client.messages:
                        payload = msg.payload.decode() if msg.payload else ""
                        if not payload:
                            continue
                        data = json.loads(payload)
                        kind, content = classify_reply(data)
                        if kind == REPLY_SUBMITTED:
                            session_id = content
                            break

                assert session_id, "No submitted ack received"
                print(f"\nSession {session_id} started, cancelling...")

                # Cancel the session via runtime API (fire-and-forget)
                cancel_req = A2ARequest(
                    text=f"cancel session {session_id}",
                    request_id=f"cancel-{uuid.uuid4().hex[:8]}",
                    sender="test",
                )
                cancel_reply_t = topic_reply("test", f"cancel-{test_id}")
                cancel_props = make_properties(
                    response_topic=cancel_reply_t,
                    correlation_data=cancel_req.request_id,
                )
                await client.publish(
                    topic_request("skitter"),
                    cancel_req.to_json(),
                    qos=1,
                    properties=cancel_props,
                )

                # Wait for the cancelled reply on the original request
                async with asyncio.timeout(10.0):
                    async for msg in client.messages:
                        payload = msg.payload.decode() if msg.payload else ""
                        if not payload:
                            continue
                        data = json.loads(payload)
                        kind, content = classify_reply(data)
                        if kind == REPLY_FAILED:
                            assert "cancel" in content.lower()
                            print(f"Session cancelled: {content}")
                            return
                        if kind in (REPLY_TERMINAL, REPLY_ERROR):
                            # Possible race: session completed before cancel
                            print(f"Session ended with {kind}: {content[:100]}")
                            return

                pytest.fail("No cancellation reply received")


@needs_mqtt
@needs_docker
@needs_llm
class TestAgentFailure:
    @pytest.mark.asyncio
    async def test_failure_propagates(self, coordinator):
        """Agent failure propagates through the coordinator to the caller."""
        aid = f"fake-fail-{uuid.uuid4().hex[:4]}"

        async with FakeAgent(aid, reply_state="failed"):
            await wait_for_discovery(aid)
            await asyncio.sleep(2)

            app_id = await _create_app(
                [aid],
                f"Use {aid} to analyze the input.",
            )
            await asyncio.sleep(1)

            app_req = A2ARequest(
                text="Analyze this.",
                request_id=f"app-{uuid.uuid4().hex[:8]}",
                sender="test",
            )
            print(f"\nSending request to app '{app_id}' (expect failure)...")
            result = await send_and_collect(
                topic_request(app_id), app_req, timeout=30.0
            )
            assert result, "No result from failing app"
            assert "failed" in result.lower()
            print(f"Got expected failure: {result}")
