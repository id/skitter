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
from datetime import datetime, timezone

import aiomqtt

from skitter.config import AgentDef
from skitter.discovery import build_card
from skitter.a2a import (
    A2A_ORG,
    A2A_UNIT,
    A2ARequest,
    make_artifact_event,
    make_status_event,
    topic_a2a_event,
    topic_discovery,
    topic_request,
    validate_a2a_request,
)
from skitter.mqtt import make_properties, mqtt_client_kwargs


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
# TTL for completed task deduplication (seconds)
_DEDUP_TTL = 300.0


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

    try:
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
    except asyncio.CancelledError:
        proc.kill()
        await proc.wait()
        stderr_task.cancel()
        raise

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
    req: A2ARequest,
    reply_topic: str,
    correlation: str,
    env: dict[str, str],
    semaphore: asyncio.Semaphore,
) -> str:
    """Handle a single A2A request: run CLI, stream results, send reply. Returns result text."""
    log.info("Request %s (task %s): %.80s", req.request_id, req.task_id, req.text)

    # Send submitted ack
    ack = make_status_event(
        request_id=correlation,
        task_id=req.task_id,
        state="submitted",
        context_id=req.context_id or "",
    )
    props = make_properties(correlation_data=correlation)
    await client.publish(reply_topic, ack, qos=1, properties=props)

    # Stream callback
    async def publish_stream(item_type: str, content: str) -> None:
        event = make_status_event(
            request_id=correlation,
            task_id=req.task_id,
            state="working",
            message=content,
            context_id=req.context_id or "",
            metadata={"type": item_type},
        )
        await client.publish(reply_topic, event, qos=1, properties=props)

    try:
        async with semaphore:
            result = await _run_cli(agent, req.text, publish_stream, env)
    except asyncio.CancelledError:
        canceled = make_status_event(
            request_id=correlation,
            task_id=req.task_id,
            state="canceled",
            message="Task canceled",
            context_id=req.context_id or "",
        )
        try:
            await client.publish(reply_topic, canceled, qos=1, properties=props)
        except Exception:
            pass
        log.info("Request %s canceled", req.request_id)
        return ""
    except Exception:
        log.exception("Request %s failed", req.request_id)
        failed = make_status_event(
            request_id=correlation,
            task_id=req.task_id,
            state="failed",
            message="Internal error",
            context_id=req.context_id or "",
        )
        await client.publish(reply_topic, failed, qos=1, properties=props)
        return ""

    # Send artifact then terminal status
    if result:
        artifact = make_artifact_event(
            request_id=correlation,
            task_id=req.task_id,
            artifact_text=result,
            context_id=req.context_id or "",
        )
        await client.publish(reply_topic, artifact, qos=1, properties=props)
    terminal = make_status_event(
        request_id=correlation,
        task_id=req.task_id,
        state="completed",
        context_id=req.context_id or "",
    )
    await client.publish(reply_topic, terminal, qos=1, properties=props)
    log.info("Request %s completed (%d chars)", req.request_id, len(result))
    return result


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
    """Main loop: load agent from file and start."""
    agent = load_agent(agent_name)
    await run_with_def(agent)


async def run_with_def(agent: AgentDef) -> None:
    """Main loop from an AgentDef (no file loading)."""
    log.info("Starting agent runner: %s (runtime=%s)", agent.id, agent.runtime)

    # Compute env once at startup
    env = agent_env()

    # Publish discovery card (retained, via main client below)
    card = build_card(agent)
    card_json = json.dumps(card)

    # LWT for crash detection
    lwt_topic = topic_a2a_event(agent.id)
    lwt_payload = json.dumps(
        {
            "event": "dead",
            "agent": agent.id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
    )
    will = aiomqtt.Will(topic=lwt_topic, payload=lwt_payload, qos=1)

    request_topic = topic_request(agent.id)
    semaphore = asyncio.Semaphore(_MAX_CONCURRENT)
    task_registry: dict[str, asyncio.Task] = {}  # task_id → asyncio.Task
    completed_tasks: dict[
        str, tuple[float, str, str]
    ] = {}  # task_id → (timestamp, state, result)

    try:
        async with aiomqtt.Client(
            **_mqtt_kwargs_for_agent(
                agent,
                identifier=f"{A2A_ORG}/{A2A_UNIT}/{agent.id}",
                will=will,
            ),
        ) as client:
            await client.subscribe(request_topic, qos=1)
            await client.publish(
                topic_discovery(agent.id),
                card_json,
                qos=1,
                retain=True,
            )
            log.info("Listening on %s", request_topic)

            async for mqtt_msg in client.messages:
                payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                if not payload:
                    continue

                # Parse method to handle tasks/cancel separately
                try:
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                method = data.get("method", "")

                if method == "tasks/cancel":
                    cancel_id = data.get("params", {}).get("id", "")
                    if cancel_id and cancel_id in task_registry:
                        task_registry[cancel_id].cancel()
                        log.info("Canceling task %s", cancel_id)
                    continue

                # message/send: validate MQTT v5 properties + Task.id
                validated = await validate_a2a_request(mqtt_msg, client, log=log)
                if not validated:
                    continue
                req, reply_topic, correlation = validated

                # Task.id deduplication: evict stale entries, return state for known tasks
                now = asyncio.get_running_loop().time()
                stale = [
                    k for k, v in completed_tasks.items() if now - v[0] > _DEDUP_TTL
                ]
                for k in stale:
                    del completed_tasks[k]

                if req.task_id in task_registry:
                    dedup_state, dedup_result = "working", ""
                elif req.task_id in completed_tasks:
                    _, dedup_state, dedup_result = completed_tasks[req.task_id]
                else:
                    dedup_state = None

                if dedup_state:
                    log.info(
                        "Duplicate Task.id %s (%s), returning %s state",
                        req.task_id,
                        "in-flight" if dedup_state == "working" else "done",
                        dedup_state,
                    )
                    ctx = req.context_id or ""
                    props = make_properties(correlation_data=correlation)
                    # Replay artifact for completed tasks so retrying requesters
                    # can recover the original output
                    if dedup_result:
                        artifact = make_artifact_event(
                            request_id=correlation,
                            task_id=req.task_id,
                            artifact_text=dedup_result,
                            context_id=ctx,
                        )
                        await client.publish(
                            reply_topic, artifact, qos=1, properties=props
                        )
                    event = make_status_event(
                        request_id=correlation,
                        task_id=req.task_id,
                        state=dedup_state,
                        context_id=ctx,
                    )
                    await client.publish(reply_topic, event, qos=1, properties=props)
                    continue

                def _on_done(t: asyncio.Task, tid: str = req.task_id) -> None:
                    task_registry.pop(tid, None)
                    if t.cancelled():
                        state, result = "canceled", ""
                    elif t.exception():
                        log.error("Request handler failed: %s", t.exception())
                        state, result = "failed", ""
                    else:
                        state, result = "completed", t.result() or ""
                    completed_tasks[tid] = (
                        asyncio.get_running_loop().time(),
                        state,
                        result,
                    )

                task = asyncio.create_task(
                    handle_request(
                        client, agent, req, reply_topic, correlation, env, semaphore
                    )
                )
                task_registry[req.task_id] = task
                task.add_done_callback(_on_done)
    finally:
        # Cancel inflight tasks on shutdown
        tasks = list(task_registry.values())
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)


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
