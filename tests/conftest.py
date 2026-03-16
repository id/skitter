"""Shared fixtures and helpers for tests.

Provides: MQTT helpers (send_and_collect, wait_for_discovery, create_test_app)
and skip conditions.
"""

from __future__ import annotations

import asyncio
import json
import socket
import uuid

import aiomqtt
import pytest

from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    MQTT_TLS,
    A2A_ORG,
    A2A_UNIT,
    make_properties,
    mqtt_client_kwargs,
    topic_discovery,
    topic_reply,
)
from skitter.types import (
    A2ARequest,
    REPLY_ERROR,
    REPLY_FAILED,
    REPLY_TERMINAL,
    REPLY_TEXT,
    classify_reply,
)


# ---------------------------------------------------------------------------
# Skip conditions
# ---------------------------------------------------------------------------


def mqtt_available() -> bool:
    try:
        s = socket.create_connection((MQTT_HOST, MQTT_PORT), timeout=2)
        if MQTT_TLS:
            import ssl

            ctx = ssl.create_default_context()
            s = ctx.wrap_socket(s, server_hostname=MQTT_HOST)
        s.close()
        return True
    except OSError:
        return False


needs_mqtt = pytest.mark.skipif(not mqtt_available(), reason="No MQTT broker")


# ---------------------------------------------------------------------------
# MQTT helpers
# ---------------------------------------------------------------------------


async def wait_for_discovery(agent_id: str, timeout: float = 30.0) -> dict:
    """Wait for an agent's discovery card to appear on the broker."""
    topic = topic_discovery(agent_id)
    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{A2A_ORG}/{A2A_UNIT}/test-disco-{uuid.uuid4().hex[:6]}",
        ),
    ) as client:
        await client.subscribe(topic, qos=1)
        try:
            async with asyncio.timeout(timeout):
                async for msg in client.messages:
                    if msg.payload:
                        return json.loads(msg.payload)
        except TimeoutError:
            pytest.fail(
                f"Discovery card for '{agent_id}' did not appear within {timeout}s"
            )
    return {}


async def send_and_collect(
    request_topic: str,
    msg: A2ARequest,
    timeout: float = 60.0,
) -> str:
    """Publish A2A request, stream replies, return terminal result."""
    test_id = uuid.uuid4().hex[:8]
    reply_t = topic_reply("test", test_id)

    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{A2A_ORG}/{A2A_UNIT}/test-client-{test_id}",
        ),
    ) as client:
        await client.subscribe(reply_t, qos=1)

        props = make_properties(
            response_topic=reply_t,
            correlation_data=msg.request_id,
        )
        await client.publish(
            request_topic,
            msg.to_json(),
            qos=1,
            properties=props,
        )

        try:
            async with asyncio.timeout(timeout):
                async for mqtt_msg in client.messages:
                    payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                    if not payload:
                        continue
                    try:
                        data = json.loads(payload)
                    except Exception:
                        continue

                    kind, content = classify_reply(data)
                    if kind == REPLY_TEXT:
                        print(content, end="", flush=True)
                    elif kind == REPLY_TERMINAL:
                        print()
                        return content
                    elif kind == REPLY_FAILED:
                        return f"Failed: {content}"
                    elif kind == REPLY_ERROR:
                        return f"Error: {content}"
        except TimeoutError:
            pytest.fail(f"Timed out after {timeout}s waiting for result")

    return ""


async def create_test_app(
    agent_ids: list[str],
    instructions: str,
    timeout: float = 30.0,
) -> str:
    """Create a composed app via the runtime API, return the app_id."""
    from skitter.mqtt import topic_request

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
    result = await send_and_collect(
        topic_request("skitter"), create_req, timeout=timeout
    )
    assert result, "Create app returned empty"
    data = json.loads(result)
    assert "created_app" in data, f"Unexpected: {result}"
    return data["created_app"]["app_id"]
