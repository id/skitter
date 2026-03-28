"""Standalone A2A agent process.

Reads a native agent definition (Claude .md or Codex .toml), connects
to the broker, publishes its discovery card, and handles A2A requests
by running the CLI tool as a subprocess.

    skitter agent-runner .claude/agents/researcher.md

Fully independent; no coordinator, no shared state.
"""

import asyncio
import json
import logging
import os
import sys
import tomllib
from pathlib import Path

import aiomqtt
import yaml

from skitter.config import AgentDef
from skitter.discovery import build_card
from skitter.a2a import (
    A2A_ORG,
    A2A_UNIT,
    A2A_INVALID_PARAMS,
    A2ARequest,
    A2AResponse,
    make_a2a_error,
    make_artifact_event,
    make_status_event,
    topic_discovery,
    topic_request,
    validate_a2a_request,
)
from skitter.mqtt import make_properties, make_will_properties, mqtt_client_kwargs


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

_SANDBOX_SETTINGS = json.dumps(
    {"sandbox": {"enabled": True, "filesystem": {"allowWrite": ["/tmp"]}}}
)


def _build_cli_cmd(agent: AgentDef, prompt: str) -> list[str]:
    """Build the CLI command for the agent's runtime."""
    if agent.runtime == "codex":
        cmd = [
            "codex",
            "exec",
            "--json",
            "--full-auto",
            "--ephemeral",
            "--color",
            "never",
            "--skip-git-repo-check",
            "-c",
            "approval_policy=never",
        ]
        if agent.model:
            cmd.extend(["--model", agent.model])
        if agent.codex_instructions:
            cmd.extend(["-c", f"developer_instructions={agent.codex_instructions}"])
        cmd.append(prompt)
    elif agent.runtime == "copilot":
        cmd = [
            "copilot",
            "-p",
            prompt,
            "--output-format",
            "json",
            "--allow-all",
        ]
        agent_name = agent.claude_agent or agent.id
        cmd.extend(["--agent", agent_name])
        if agent.model:
            cmd.extend(["--model", agent.model])
    elif agent.runtime == "qwen":
        cmd = [
            "qwen",
            "-p",
            prompt,
            "--output-format",
            "stream-json",
            "--sandbox",
            "--approval-mode",
            "auto-edit",
        ]
        if agent.model:
            cmd.extend(["--model", agent.model])
    else:
        # claude and other claude-compatible runtimes (gemini, etc.)
        binary = agent.runtime
        cmd = [
            binary,
            "-p",
            prompt,
            "--output-format",
            "stream-json",
            "--verbose",
            "--permission-mode",
            "auto",
            "--settings",
            _SANDBOX_SETTINGS,
        ]
        agent_name = agent.claude_agent or agent.id
        cmd.extend(["--agent", agent_name])
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
        binary = cmd[0]
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
                        texts.append(block.get("text", ""))
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

        await proc.wait()
    except asyncio.CancelledError:
        proc.kill()
        await proc.wait()
        stderr_task.cancel()
        raise

    stderr = await stderr_task
    if stderr:
        log.warning("stderr: %s", stderr[:500])

    if proc.returncode and not texts:
        return f"(process exited with code {proc.returncode})"

    return "\n".join(texts) if texts else "(no response)"


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
    """Load an agent definition from a .md or .toml file.

    The file extension determines the *parse format* (YAML frontmatter vs TOML).
    The ``runtime`` field inside the file determines which CLI tool runs the agent.
    If ``runtime`` is omitted, it defaults to ``claude`` for .md and ``codex`` for .toml.
    """
    path = Path(path_str)
    if not path.is_file():
        log.error("Agent definition not found: %s", path)
        sys.exit(1)

    suffix = path.suffix.lower()
    if suffix == ".md":
        return _load_md_agent(path)
    if suffix == ".toml":
        return _load_toml_agent(path)

    log.error("Unsupported agent file type: %s (expected .md or .toml)", suffix)
    sys.exit(1)


def _load_md_agent(path) -> AgentDef:
    """Parse an agent .md file (YAML frontmatter between --- delimiters)."""
    text = path.read_text()
    if not text.startswith("---"):
        log.error("Agent file must start with --- frontmatter: %s", path)
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
    runtime = frontmatter.get("runtime", "claude")
    return AgentDef(
        id=agent_id,
        name=frontmatter.get("name", agent_id),
        description=frontmatter.get("description", ""),
        runtime=runtime,
        model=frontmatter.get("model", ""),
        claude_agent=agent_id,
    )


def _load_toml_agent(path) -> AgentDef:
    """Parse an agent .toml file."""
    try:
        data = tomllib.loads(path.read_text())
    except tomllib.TOMLDecodeError as e:
        log.error("Invalid TOML in %s: %s", path, e)
        sys.exit(1)
    agent_id = data.get("name", path.stem)
    instructions = data.get("developer_instructions", "")
    runtime = data.get("runtime", "codex")
    return AgentDef(
        id=agent_id,
        name=agent_id,
        description=data.get("description", instructions[:100]),
        runtime=runtime,
        model=data.get("model", ""),
        codex_instructions=instructions,
    )


async def run(agent_name: str) -> None:
    """Main loop: load agent from file and start."""
    agent = load_agent(agent_name)
    await run_with_def(agent)


async def run_with_def(agent: AgentDef) -> None:
    """Main loop from an AgentDef (no file loading)."""
    log.info("Starting agent runner: %s (runtime=%s)", agent.id, agent.runtime)

    env = agent_env()
    card = build_card(agent)
    card_json = json.dumps(card)
    discovery_topic = topic_discovery(agent.id)

    lwt_props = make_will_properties(
        user_properties=[("a2a-status", "offline"), ("a2a-status-source", "lwt")],
    )
    will = aiomqtt.Will(
        topic=discovery_topic,
        payload=card_json,
        qos=1,
        retain=True,
        properties=lwt_props,
    )
    online_props = make_properties(
        user_properties=[("a2a-status", "online"), ("a2a-status-source", "agent")],
    )
    offline_props = make_properties(
        user_properties=[("a2a-status", "offline"), ("a2a-status-source", "agent")],
    )

    request_topic = topic_request(agent.id)
    semaphore = asyncio.Semaphore(_MAX_CONCURRENT)
    task_registry: dict[str, asyncio.Task] = {}  # task_id → asyncio.Task
    completed_tasks: dict[
        str, tuple[float, str, str]
    ] = {}  # task_id → (timestamp, state, result)
    task_context: dict[str, str] = {}  # task_id → context_id

    started = False

    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{A2A_ORG}/{A2A_UNIT}/{agent.id}",
            will=will,
        ),
    ) as client:
        try:
            await client.subscribe(request_topic, qos=1)
            await client.publish(
                discovery_topic,
                card_json,
                qos=1,
                retain=True,
                properties=online_props,
            )
            started = True
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
                    task_context.pop(k, None)

                if req.task_id in task_registry:
                    dedup_state, dedup_result = "working", ""
                elif req.task_id in completed_tasks:
                    _, dedup_state, dedup_result = completed_tasks[req.task_id]
                else:
                    dedup_state = None
                    dedup_result = None

                if dedup_state:
                    # Reject context_id mismatch per A2A-over-MQTT spec (-32602)
                    stored_ctx = task_context.get(req.task_id, "")
                    incoming_ctx = req.context_id or ""
                    if stored_ctx and incoming_ctx and incoming_ctx != stored_ctx:
                        log.warning(
                            "context_id mismatch for Task.id %s: stored=%s incoming=%s",
                            req.task_id,
                            stored_ctx,
                            incoming_ctx,
                        )
                        resp = A2AResponse(
                            id=correlation,
                            error=make_a2a_error(
                                A2A_INVALID_PARAMS,
                                "context_id mismatch: incoming context_id differs "
                                "from stored value for this Task.id",
                            ),
                        )
                        props = make_properties(correlation_data=correlation)
                        await client.publish(
                            reply_topic, resp.to_json(), qos=1, properties=props
                        )
                        continue

                    log.info(
                        "Duplicate Task.id %s (%s), returning %s state",
                        req.task_id,
                        "in-flight" if dedup_state == "working" else "done",
                        dedup_state,
                    )
                    ctx = req.context_id or ""
                    props = make_properties(correlation_data=correlation)
                    # Replay artifact so retrying requesters recover the original output
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

                task_context[req.task_id] = req.context_id or ""
                task = asyncio.create_task(
                    handle_request(
                        client, agent, req, reply_topic, correlation, env, semaphore
                    )
                )
                task_registry[req.task_id] = task
                task.add_done_callback(_on_done)
        finally:
            tasks = list(task_registry.values())
            for task in tasks:
                task.cancel()
            if tasks:
                await asyncio.gather(*tasks, return_exceptions=True)

            if started:
                try:
                    await client.publish(
                        discovery_topic,
                        card_json,
                        qos=1,
                        retain=True,
                        properties=offline_props,
                    )
                except Exception:
                    log.debug("Failed to publish offline status", exc_info=True)


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
