"""Standalone A2A agent process.

Reads an agent definition YAML, connects to the broker, publishes its
discovery card, and handles A2A requests by running a CLI tool (claude/codex)
as a subprocess.

    skitter agent-runner researcher

Fully independent — no supervisor, no shared state.
"""

import asyncio
import json
import logging
import os
import sys

import aiomqtt

from skitter.config import AgentDef, load_agents
from skitter.discovery import CardPublisher, build_card
from skitter.mqtt import (
    A2A_ORG,
    A2A_UNIT,
    get_correlation_data,
    get_response_topic,
    make_properties,
    mqtt_client_kwargs,
    topic_event,
    topic_request,
)
from skitter.spawn import worker_env
from skitter.types import (
    A2ARequest,
    A2AResponse,
    A2A_TRANSPORT_PROTOCOL_ERROR,
    make_status_event,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.agent_runner")

# Max concurrent requests per agent runner
_MAX_CONCURRENT = int(os.environ.get("SKITTER_AGENT_MAX_CONCURRENT", "4"))


def _build_cli_cmd(agent: AgentDef, prompt: str) -> list[str]:
    """Build the CLI command for the agent's runtime."""
    if agent.runtime == "codex":
        cmd = [
            "codex",
            "exec",
            "--json",
            "--full-auto",
            "--skip-git-repo-check",
            prompt,
        ]
        if agent.model:
            cmd.extend(["--model", agent.model])
    else:
        cmd = [
            "claude",
            "-p",
            prompt,
            "--output-format",
            "stream-json",
            "--verbose",
            "--dangerously-skip-permissions",
        ]
        agent_file = agent.agent_file or agent.id
        cmd.extend(["--agent", agent_file.removesuffix(".md")])
        if agent.model:
            cmd.extend(["--model", agent.model])
    return cmd


async def _run_cli(
    agent: AgentDef,
    prompt: str,
    publish_stream: "callable",
    env: dict[str, str],
) -> str:
    """Run the CLI tool as a subprocess, stream output, return final text."""
    cmd = _build_cli_cmd(agent, prompt)

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            limit=1024 * 1024,
        )
    except FileNotFoundError:
        binary = "codex" if agent.runtime == "codex" else "claude"
        return f"Error: {binary} CLI not found on PATH"

    texts: list[str] = []

    # Drain stderr concurrently to avoid deadlock if pipe buffer fills
    async def _drain_stderr() -> str:
        assert proc.stderr is not None
        data = await proc.stderr.read()
        return data.decode().strip()

    stderr_task = asyncio.create_task(_drain_stderr())

    assert proc.stdout is not None
    async for line in proc.stdout:
        line_str = line.decode().strip()
        if not line_str:
            continue
        try:
            event = json.loads(line_str)
        except json.JSONDecodeError:
            continue

        event_type = event.get("type", "")

        if event_type == "assistant":
            for block in event.get("message", {}).get("content", []):
                if block.get("type") == "text":
                    text = block.get("text", "")
                    texts.append(text)
                    await publish_stream("text", text)
                elif block.get("type") == "tool_use":
                    await publish_stream(
                        "tool_use",
                        f"{block.get('name', '?')}: {str(block.get('input', ''))[:100]}",
                    )
        elif event_type == "item.completed":
            item = event.get("item", {})
            if item.get("type") == "agent_message":
                text = item.get("text", "")
                if text:
                    texts.append(text)
                    await publish_stream("text", text)

    await proc.wait()
    stderr = await stderr_task
    if stderr:
        log.warning("stderr: %s", stderr[:500])

    if proc.returncode and proc.returncode != 0 and not texts:
        return f"(process exited with code {proc.returncode})"

    return "\n".join(texts) if texts else "(no response)"


def _mqtt_kwargs_for_agent(agent: AgentDef, **overrides) -> dict:
    """Build MQTT connection kwargs, using agent's broker config if set."""
    if agent.broker and agent.broker.host:
        return mqtt_client_kwargs(
            hostname=agent.broker.host,
            port=agent.broker.port or 8883,
            **overrides,
        )
    return mqtt_client_kwargs(**overrides)


async def handle_request(
    client: aiomqtt.Client,
    agent: AgentDef,
    payload: str,
    reply_topic: str,
    correlation: str,
    env: dict[str, str],
    semaphore: asyncio.Semaphore,
) -> None:
    """Handle a single A2A request: run CLI, stream results, send reply."""
    try:
        req = A2ARequest.from_json(payload)
    except Exception as e:
        log.error("Bad request JSON: %s", e)
        return

    log.info("Request %s: %.80s", req.request_id, req.text)

    # Send submitted ack
    ack = make_status_event(
        request_id=correlation,
        task_id=req.request_id,
        state="submitted",
    )
    props = make_properties(correlation_data=correlation)
    await client.publish(reply_topic, ack, qos=1, properties=props)

    # Stream callback
    async def publish_stream(item_type: str, content: str) -> None:
        event = make_status_event(
            request_id=correlation,
            task_id=req.request_id,
            state="working",
            message=content,
            message_type=item_type,
        )
        await client.publish(reply_topic, event, qos=0, properties=props)

    async with semaphore:
        result = await _run_cli(agent, req.text, publish_stream, env)

    # Send terminal result
    terminal = make_status_event(
        request_id=correlation,
        task_id=req.request_id,
        state="completed",
        artifact_text=result,
    )
    await client.publish(reply_topic, terminal, qos=1, properties=props)
    log.info("Request %s completed (%d chars)", req.request_id, len(result))


async def run(agent_name: str) -> None:
    """Main loop: publish card, listen for requests, handle them."""
    agents = load_agents()
    agent = agents.get(agent_name)
    if not agent:
        log.error("Agent '%s' not found in ~/.skitter/agents/", agent_name)
        sys.exit(1)

    log.info("Starting agent runner: %s (runtime=%s)", agent.id, agent.runtime)

    # Compute env once at startup
    env = worker_env()

    # Publish discovery card
    card = build_card(agent)
    card_json = json.dumps(card)
    publisher = CardPublisher(
        agent.id, card_json, mqtt_kwargs=_mqtt_kwargs_for_agent(agent)
    )
    await publisher.start()

    # LWT for crash detection
    lwt_topic = topic_event(agent.id, "dead")
    lwt_payload = json.dumps({"status": "dead", "agent": agent.id})
    will = aiomqtt.Will(topic=lwt_topic, payload=lwt_payload, qos=1)

    request_topic = topic_request(agent.id)
    semaphore = asyncio.Semaphore(_MAX_CONCURRENT)
    inflight: set[asyncio.Task] = set()

    try:
        async with aiomqtt.Client(
            **_mqtt_kwargs_for_agent(
                agent,
                identifier=f"{A2A_ORG}/{A2A_UNIT}/{agent.id}-runner",
                will=will,
            ),
        ) as client:
            await client.subscribe(request_topic, qos=1)
            log.info("Listening on %s", request_topic)

            async for mqtt_msg in client.messages:
                payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                if not payload:
                    continue

                reply_topic = get_response_topic(mqtt_msg) or ""
                correlation = get_correlation_data(mqtt_msg) or ""
                if not reply_topic or not correlation:
                    log.warning("Request missing Response Topic or Correlation Data")
                    if reply_topic:
                        resp = A2AResponse(
                            id=correlation,
                            error={
                                "code": A2A_TRANSPORT_PROTOCOL_ERROR,
                                "message": "Missing MQTT v5 Response Topic or Correlation Data",
                            },
                        )
                        await client.publish(reply_topic, resp.to_json(), qos=1)
                    continue

                task = asyncio.create_task(
                    handle_request(
                        client, agent, payload, reply_topic, correlation, env, semaphore
                    )
                )
                inflight.add(task)
                task.add_done_callback(inflight.discard)
    finally:
        # Cancel inflight tasks on shutdown
        for task in inflight:
            task.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
        await publisher.stop()


def main() -> None:
    if len(sys.argv) < 2:
        print("Usage: skitter agent-runner <agent_name>", file=sys.stderr)
        sys.exit(1)
    agent_name = sys.argv[-1]
    try:
        asyncio.run(run(agent_name))
    except KeyboardInterrupt:
        log.info("Agent runner shutting down")


if __name__ == "__main__":
    main()
