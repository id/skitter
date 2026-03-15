"""Standalone A2A agent process.

Reads a native agent definition (Claude .md or Codex .toml), connects
to the broker, publishes its discovery card, and handles A2A requests
by running the CLI tool as a subprocess.

    skitter agent-runner ~/.claude/agents/researcher.md

Fully independent — no coordinator, no shared state.
"""

import asyncio
import json
import logging
import os
import sys

import aiomqtt

from skitter.config import AgentDef
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
from skitter.types import (
    A2ARequest,
    A2AResponse,
    A2A_TRANSPORT_PROTOCOL_ERROR,
    make_status_event,
)


def agent_env() -> dict[str, str]:
    """Build env for agent processes — strip CLAUDECODE, prefer OAuth over API key."""
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    if env.get("CLAUDE_CODE_OAUTH_TOKEN"):
        env.pop("ANTHROPIC_API_KEY", None)
    return env


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

    try:
        async with semaphore:
            result = await _run_cli(agent, req.text, publish_stream, env)
    except Exception:
        log.exception("Request %s failed", req.request_id)
        result = "(internal error)"

    # Send terminal result
    terminal = make_status_event(
        request_id=correlation,
        task_id=req.request_id,
        state="completed",
        artifact_text=result,
    )
    await client.publish(reply_topic, terminal, qos=1, properties=props)
    log.info("Request %s completed (%d chars)", req.request_id, len(result))


def load_agent(path_str: str) -> AgentDef:
    """Load an agent definition from a Claude .md or Codex .toml file."""
    from pathlib import Path

    path = Path(path_str)
    if not path.is_file():
        log.error("Agent definition not found: %s", path)
        sys.exit(1)

    suffix = path.suffix.lower()
    if suffix == ".md":
        return _load_claude_agent(path)
    if suffix == ".toml":
        return _load_codex_agent(path)

    log.error("Unsupported agent file type: %s (expected .md or .toml)", suffix)
    sys.exit(1)


def _load_claude_agent(path) -> AgentDef:
    """Parse a Claude agent .md file (YAML frontmatter between --- delimiters)."""
    import yaml

    text = path.read_text()
    if not text.startswith("---"):
        log.error("Claude agent file must start with --- frontmatter: %s", path)
        sys.exit(1)

    end = text.find("\n---", 3)
    if end == -1:
        log.error("No closing --- in frontmatter: %s", path)
        sys.exit(1)

    frontmatter = yaml.safe_load(text[3:end])
    if not isinstance(frontmatter, dict):
        log.error("Invalid frontmatter in %s", path)
        sys.exit(1)

    agent_id = frontmatter.get("name", path.stem)
    return AgentDef(
        id=agent_id,
        name=frontmatter.get("name", agent_id),
        description=frontmatter.get("description", ""),
        runtime="claude",
        model=frontmatter.get("model", ""),
        agent_file=path.stem,
    )


def _load_codex_agent(path) -> AgentDef:
    """Parse a Codex agent .toml file."""
    import tomllib

    try:
        data = tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as e:
        log.error("Invalid TOML in %s: %s", path, e)
        sys.exit(1)
    agent_id = path.stem
    return AgentDef(
        id=agent_id,
        name=agent_id,
        description=data.get("developer_instructions", "")[:100],
        runtime="codex",
        model=data.get("model", ""),
    )


async def run(agent_name: str) -> None:
    """Main loop: publish card, listen for requests, handle them."""
    agent = load_agent(agent_name)

    log.info("Starting agent runner: %s (runtime=%s)", agent.id, agent.runtime)

    # Compute env once at startup
    env = agent_env()

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

                def _on_done(t: asyncio.Task) -> None:
                    inflight.discard(t)
                    if not t.cancelled() and t.exception():
                        log.error("Request handler failed: %s", t.exception())

                task = asyncio.create_task(
                    handle_request(
                        client, agent, payload, reply_topic, correlation, env, semaphore
                    )
                )
                inflight.add(task)
                task.add_done_callback(_on_done)
    finally:
        # Cancel inflight tasks on shutdown
        for task in inflight:
            task.cancel()
        if inflight:
            await asyncio.gather(*inflight, return_exceptions=True)
        await publisher.stop()


def main() -> None:
    # Via __main__.py: sys.argv = ['...', 'agent-runner', '<name>']
    # Via direct: sys.argv = ['agent_runner.py', '<name>']
    if len(sys.argv) < 3 and "agent-runner" in sys.argv:
        print("Usage: skitter agent-runner <agent.md|agent.toml>", file=sys.stderr)
        sys.exit(1)
    if len(sys.argv) < 2:
        print(
            "Usage: python -m skitter.agent_runner <agent.md|agent.toml>",
            file=sys.stderr,
        )
        sys.exit(1)
    # Take last arg — works for both dispatch paths
    agent_path = sys.argv[-1]
    try:
        asyncio.run(run(agent_path))
    except KeyboardInterrupt:
        log.info("Agent runner shutting down")


if __name__ == "__main__":
    main()
