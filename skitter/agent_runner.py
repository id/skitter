"""Standalone A2A agent process.

Reads a native agent definition (Claude .md or Codex .toml), connects
to the broker, publishes its discovery card, and handles A2A requests
by running the CLI tool as a subprocess.

    skitter agent-runner ~/.skitter/agents/researcher.md

Supported runtimes: claude, codex.
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

from skitter.config import AgentDef, SkillDef
from skitter.discovery import build_card
from skitter.a2a import (
    a2a_org,
    a2a_unit,
    A2A_INVALID_PARAMS,
    A2ARequest,
    A2AResponse,
    TaskState,
    make_a2a_error,
    make_artifact_event,
    make_status_event,
    topic_discovery,
    topic_request,
    validate_a2a_request,
)
from skitter.mqtt import (
    get_correlation_data,
    get_response_topic,
    make_properties,
    make_will_properties,
    mqtt_client_kwargs,
)
from skitter.runtime_cli import extract_text, extract_session_id


def agent_env() -> dict[str, str]:
    """Build env for agent processes — strip CLAUDECODE, prefer OAuth over API key."""
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    if env.get("CLAUDE_CODE_OAUTH_TOKEN"):
        env.pop("ANTHROPIC_API_KEY", None)
    return env


log = logging.getLogger("skitter.agent_runner")

# Max concurrent requests per agent runner
_MAX_CONCURRENT = int(os.environ.get("SKITTER_AGENT_MAX_CONCURRENT", "4"))
# TTL for completed task deduplication (seconds)
_DEDUP_TTL = 300.0
_SESSION_MAP_FILE = "context_sessions.json"

_SANDBOX_SETTINGS = json.dumps(
    {"sandbox": {"enabled": True, "filesystem": {"allowWrite": ["/tmp"]}}}
)
_PERMISSION_MODE = os.environ.get("SKITTER_AGENT_PERMISSION_MODE", "auto")


def _build_cli_cmd(
    agent: AgentDef, prompt: str, resume_id: str | None = None
) -> list[str]:
    """Build the CLI command for the agent's runtime.

    When *resume_id* is provided (the CLI-native session ID):
    - Claude: appends ``--resume <resume_id>``.
    - Codex: uses ``codex exec resume <resume_id>``.
    """
    if agent.runtime == "codex":
        if _PERMISSION_MODE == "bypassPermissions":
            flags = [
                "--json",
                "--dangerously-bypass-approvals-and-sandbox",
                "--skip-git-repo-check",
            ]
        else:
            flags = [
                "--json",
                "--full-auto",
                "--skip-git-repo-check",
                "-c",
                "approval_policy=never",
            ]
        if agent.model:
            flags.extend(["--model", agent.model])
        if resume_id:
            cmd = ["codex", "exec", "resume"] + flags + [resume_id, prompt]
        else:
            cmd = ["codex", "exec"] + flags + ["--color", "never"]
            if agent.instructions:
                cmd.extend(["-c", f"developer_instructions={agent.instructions}"])
            cmd.append(prompt)
    else:
        full_prompt = prompt
        if agent.instructions:
            full_prompt = f"{agent.instructions}\n\n{prompt}"
        cmd = [
            "claude",
            "-p",
            full_prompt,
            "--output-format",
            "stream-json",
            "--verbose",
        ]
        if resume_id:
            cmd.extend(["--resume", resume_id])
        elif _PERMISSION_MODE == "bypassPermissions":
            cmd.extend(["--dangerously-skip-permissions"])
        else:
            cmd.extend(
                ["--permission-mode", _PERMISSION_MODE, "--settings", _SANDBOX_SETTINGS]
            )
        if agent.model:
            cmd.extend(["--model", agent.model])
        if agent.max_turns:
            cmd.extend(["--max-turns", str(agent.max_turns)])
        if agent.tools:
            cmd.extend(["--allowedTools", ",".join(agent.tools)])
    return cmd


async def _run_cli(
    agent: AgentDef,
    prompt: str,
    publish_stream: "callable",
    env: dict[str, str],
    resume_id: str | None = None,
    cwd: Path | None = None,
) -> tuple[str, str]:
    """Run the CLI tool as a subprocess, stream output.

    Returns ``(result_text, cli_session_id)`` where *cli_session_id* is
    the native session identifier reported by the CLI (used for resume).
    """
    cmd = _build_cli_cmd(agent, prompt, resume_id=resume_id)

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            env=env,
            cwd=cwd,
            limit=1024 * 1024,
        )
    except FileNotFoundError:
        binary = cmd[0]
        return f"Error: {binary} CLI not found on PATH", ""

    texts: list[str] = []
    session_id = ""

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

            if not session_id:
                sid = extract_session_id(event)
                if sid:
                    session_id = sid

            text = extract_text(event, agent.runtime)
            if text:
                texts.append(text)

            # Forward tool_use events for streaming visibility
            if event.get("type") == "assistant":
                for block in event.get("message", {}).get("content", []):
                    if block.get("type") == "tool_use":
                        await publish_stream(
                            "tool_use",
                            f"{block.get('name', '?')}: {str(block.get('input', ''))[:100]}",
                        )

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
        return f"(process exited with code {proc.returncode})", session_id

    result = "\n".join(texts) if texts else "(no response)"
    return result, session_id


async def handle_request(
    client: aiomqtt.Client,
    agent: AgentDef,
    req: A2ARequest,
    reply_topic: str,
    correlation: str,
    env: dict[str, str],
    semaphore: asyncio.Semaphore,
    cwd: Path | None = None,
    resume_id: str | None = None,
) -> tuple[str, str]:
    """Handle a single A2A request: run CLI, stream results, send reply.

    Returns ``(result_text, cli_session_id)``.
    """
    log.info("Request %s (task %s): %.80s", req.request_id, req.task_id, req.text)

    # Send submitted ack
    ack = make_status_event(
        request_id=correlation,
        task_id=req.task_id,
        state=TaskState.SUBMITTED,
        context_id=req.context_id or "",
    )
    props = make_properties(correlation_data=correlation)
    log.debug("MQTT → %s (submitted ack)", reply_topic)
    await client.publish(reply_topic, ack, qos=1, properties=props)

    # Stream callback
    async def publish_stream(item_type: str, content: str) -> None:
        event = make_status_event(
            request_id=correlation,
            task_id=req.task_id,
            state=TaskState.WORKING,
            message=content,
            context_id=req.context_id or "",
            metadata={"type": item_type},
        )
        log.debug("MQTT → %s (working: %s)", reply_topic, item_type)
        await client.publish(reply_topic, event, qos=1, properties=props)

    try:
        async with semaphore:
            result, cli_session_id = await _run_cli(
                agent, req.text, publish_stream, env, resume_id=resume_id, cwd=cwd
            )
    except asyncio.CancelledError:
        canceled = make_status_event(
            request_id=correlation,
            task_id=req.task_id,
            state=TaskState.CANCELED,
            message="Task canceled",
            context_id=req.context_id or "",
        )
        try:
            await client.publish(reply_topic, canceled, qos=1, properties=props)
        except Exception:
            pass
        log.info("Request %s canceled", req.request_id)
        return "", ""
    except Exception:
        log.exception("Request %s failed", req.request_id)
        failed = make_status_event(
            request_id=correlation,
            task_id=req.task_id,
            state=TaskState.FAILED,
            message="Internal error",
            context_id=req.context_id or "",
        )
        await client.publish(reply_topic, failed, qos=1, properties=props)
        return "", ""

    # Send artifact then terminal status
    if result:
        artifact = make_artifact_event(
            request_id=correlation,
            task_id=req.task_id,
            artifact_text=result,
            context_id=req.context_id or "",
        )
        log.debug("MQTT → %s (artifact, %d bytes)", reply_topic, len(artifact))
        await client.publish(reply_topic, artifact, qos=1, properties=props)
    terminal = make_status_event(
        request_id=correlation,
        task_id=req.task_id,
        state=TaskState.COMPLETED,
        context_id=req.context_id or "",
    )
    log.debug("MQTT → %s (completed)", reply_topic)
    await client.publish(reply_topic, terminal, qos=1, properties=props)
    log.info("Request %s completed (%d chars)", req.request_id, len(result))
    return result, cli_session_id


def _parse_frontmatter(text: str) -> tuple[dict, str] | None:
    """Parse YAML frontmatter from a ``---`` delimited markdown file.

    Returns ``(frontmatter_dict, body)`` or ``None`` on parse failure.
    """
    if not text.startswith("---"):
        return None
    end = text.find("\n---", 3)
    if end == -1:
        return None
    fm = yaml.safe_load(text[3:end])
    if not isinstance(fm, dict):
        return None
    body = text[end + 4 :].strip()
    return fm, body


def _coerce_list(value: object) -> list[str]:
    """Coerce a string or list value to ``list[str]``."""
    if isinstance(value, str):
        return [s.strip() for s in value.split(",") if s.strip()]
    if isinstance(value, list):
        return value
    return []


def _load_skills(skill_refs: list[str]) -> list[SkillDef]:
    """Load skill metadata from ~/.skitter/skills/ for each referenced name."""
    from skitter.config import skills_dir

    base = skills_dir()
    results: list[SkillDef] = []
    for name in skill_refs:
        skill_file = base / name / "SKILL.md"
        if not skill_file.is_file():
            log.warning("Skill '%s' not found at %s", name, skill_file)
            continue
        parsed = _parse_frontmatter(skill_file.read_text())
        if not parsed:
            log.warning("Skill '%s' has invalid frontmatter: %s", name, skill_file)
            continue
        fm, _ = parsed
        results.append(
            SkillDef(
                id=name,
                name=fm.get("name", name),
                description=fm.get("description", ""),
            )
        )
    return results


def _load_session_map(resource_dir: Path | None) -> dict[str, str]:
    if not resource_dir:
        return {}
    try:
        return json.loads((resource_dir / _SESSION_MAP_FILE).read_text())
    except (json.JSONDecodeError, OSError):
        return {}


def _save_session_map(resource_dir: Path | None, mapping: dict[str, str]) -> None:
    if not resource_dir:
        return
    path = resource_dir / _SESSION_MAP_FILE
    try:
        path.write_text(json.dumps(mapping))
    except OSError:
        log.warning("Failed to persist session map to %s", path)


_RUNTIME_SKILLS_PATH: dict[str, str] = {
    "claude": ".claude/skills",
    "codex": ".agents/skills",
}


def _setup_skill_links(agent: AgentDef, resource_dir: Path) -> None:
    """Create runtime-native symlinks for referenced skills.

    For Claude: ``<resource_dir>/.claude/skills/<name>`` -> ``~/.skitter/skills/<name>``
    For Codex:  ``<resource_dir>/.agents/skills/<name>`` -> ``~/.skitter/skills/<name>``

    Idempotent: skips correct symlinks, fixes stale ones.
    """
    from skitter.config import skills_dir

    rel = _RUNTIME_SKILLS_PATH.get(agent.runtime)
    if not rel:
        return
    skills_parent = resource_dir / rel
    skills_base = skills_dir()

    wanted: set[str] = set()
    for name in agent.skill_refs:
        target = skills_base / name
        if not target.is_dir():
            continue
        wanted.add(name)
        target_resolved = target.resolve()
        link = skills_parent / name
        if link.is_symlink():
            if link.resolve() == target_resolved:
                continue
            link.unlink()
        elif link.exists():
            continue  # not a symlink; don't overwrite real dirs
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(target)
        log.info("Linked skill %s -> %s", link, target)

    # Remove stale symlinks for skills no longer referenced
    if skills_parent.is_dir():
        for entry in skills_parent.iterdir():
            if entry.is_symlink() and entry.name not in wanted:
                entry.unlink()
                log.info("Removed stale skill link: %s", entry)


def scan_agents() -> list[tuple[str, str, str]]:
    """Scan ~/.skitter/agents/ for agent definitions.

    Returns list of ``(agent_id, filename, runtime)`` tuples.
    Uses the same parsing logic as :func:`load_agent`.
    """
    from skitter.config import skitter_home

    agents_dir = skitter_home() / "agents"
    if not agents_dir.is_dir():
        return []

    results: list[tuple[str, str, str]] = []
    for path in sorted(agents_dir.iterdir()):
        if path.suffix == ".md":
            parsed = _parse_frontmatter(path.read_text())
            if parsed:
                fm, _ = parsed
                agent_id = fm.get("name", path.stem)
                runtime = fm.get("runtime", "claude")
                results.append((agent_id, path.name, runtime))
        elif path.suffix == ".toml":
            try:
                data = tomllib.loads(path.read_text())
                agent_id = data.get("name", path.stem)
                runtime = data.get("runtime", "codex")
                results.append((agent_id, path.name, runtime))
            except Exception:
                log.warning("Skipping invalid TOML: %s", path)
    return results


def load_agent(path_str: str) -> AgentDef:
    """Load an agent definition from a file path or agent name.

    Accepts either a path to a .md/.toml file, or a bare agent name which is
    resolved from ``SKITTER_HOME/agents/`` (tries ``<name>.md`` then ``<name>.toml``).
    """
    path = Path(path_str)
    if not path.is_file():
        from skitter.config import skitter_home

        agents_dir = skitter_home() / "agents"
        for suffix in (".md", ".toml"):
            candidate = agents_dir / f"{path_str}{suffix}"
            if candidate.is_file():
                path = candidate
                break
    if not path.is_file():
        log.error("Agent definition not found: %s", path)
        sys.exit(1)

    suffix = path.suffix.lower()
    if suffix == ".md":
        agent = _load_md_agent(path)
    elif suffix == ".toml":
        agent = _load_toml_agent(path)
    else:
        log.error("Unsupported agent file type: %s (expected .md or .toml)", suffix)
        sys.exit(1)

    if agent.skill_refs:
        agent.skills = _load_skills(agent.skill_refs)
    return agent


def _load_md_agent(path) -> AgentDef:
    """Parse an agent .md file (YAML frontmatter between --- delimiters)."""
    parsed = _parse_frontmatter(path.read_text())
    if not parsed:
        log.error("Invalid or missing frontmatter in %s", path)
        sys.exit(1)

    fm, body = parsed
    agent_id = fm.get("name", path.stem)
    return AgentDef(
        id=agent_id,
        name=agent_id,
        description=fm.get("description", ""),
        runtime=fm.get("runtime", "claude"),
        model=fm.get("model", ""),
        instructions=body,
        max_turns=int(fm.get("maxTurns", 0)),
        tools=_coerce_list(fm.get("tools", [])),
        skill_refs=_coerce_list(fm.get("skills", [])),
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
    return AgentDef(
        id=agent_id,
        name=agent_id,
        description=data.get("description", instructions[:100]),
        runtime=data.get("runtime", "codex"),
        model=data.get("model", ""),
        instructions=instructions,
        skill_refs=_coerce_list(data.get("skills", [])),
    )


async def run(agent_name: str) -> None:
    """Main loop: load agent from file and start."""
    agent = load_agent(agent_name)
    # Runtime working directory: writable location for subprocess cwd and skill
    # links.  Kept under skitter_home()/run/ so it works on both host and inside
    # containers (where the agents/ mount is read-only).
    from skitter.config import skitter_home

    resource_dir = skitter_home() / "run" / agent.id
    await run_with_def(agent, resource_dir=resource_dir)


async def run_with_def(agent: AgentDef, *, resource_dir: Path | None = None) -> None:
    """Main loop from an AgentDef (no file loading)."""
    log.info("Starting agent runner: %s (runtime=%s)", agent.id, agent.runtime)

    # Ensure the resource directory exists (used as subprocess cwd)
    if resource_dir:
        resource_dir.mkdir(parents=True, exist_ok=True)
        if agent.skill_refs:
            _setup_skill_links(agent, resource_dir)
            log.info("Skills linked in %s", resource_dir)

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
    context_active: dict[str, str] = {}  # context_id → task_id (running)
    context_session: dict[str, str] = _load_session_map(resource_dir)

    started = False

    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{a2a_org()}/{a2a_unit()}/{agent.id}",
            will=will,
        ),
    ) as client:
        try:
            await client.subscribe(request_topic, qos=1)
            log.debug("MQTT → %s (discovery card, online)", discovery_topic)
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
                log.debug("MQTT ← %s (%d bytes)", mqtt_msg.topic, len(payload))
                if not payload:
                    continue

                # Parse method to handle CancelTask separately
                try:
                    data = json.loads(payload)
                except json.JSONDecodeError:
                    continue
                method = data.get("method", "")

                if method == "CancelTask":
                    cancel_id = data.get("params", {}).get("id", "")
                    if cancel_id and cancel_id in task_registry:
                        task_registry[cancel_id].cancel()
                        log.info("Canceling task %s", cancel_id)
                    # Reply to CancelTask per A2A spec
                    cancel_reply_t = get_response_topic(mqtt_msg) or ""
                    cancel_corr = get_correlation_data(mqtt_msg) or ""
                    if cancel_reply_t and cancel_corr:
                        rpc_id = data.get("id", "")
                        cancel_state = (
                            TaskState.CANCELED
                            if cancel_id in task_registry
                            else TaskState.FAILED
                        )
                        resp = make_status_event(
                            request_id=rpc_id,
                            task_id=cancel_id or "",
                            state=cancel_state,
                        )
                        props = make_properties(correlation_data=cancel_corr)
                        await client.publish(
                            cancel_reply_t, resp, qos=1, properties=props
                        )
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
                    dedup_state, dedup_result = TaskState.WORKING, ""
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
                        "in-flight" if dedup_state == TaskState.WORKING else "done",
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

                def _on_done(
                    t: asyncio.Task,
                    tid: str = req.task_id,
                    ctx: str = req.context_id or "",
                ) -> None:
                    task_registry.pop(tid, None)
                    # Only clear context_active if this task is still the active one
                    if ctx and context_active.get(ctx) == tid:
                        del context_active[ctx]
                    if t.cancelled():
                        state, result = TaskState.CANCELED, ""
                    elif t.exception():
                        log.error("Request handler failed: %s", t.exception())
                        state, result = TaskState.FAILED, ""
                    else:
                        result, cli_sid = t.result() or ("", "")
                        state = TaskState.COMPLETED
                        if ctx and cli_sid:
                            context_session[ctx] = cli_sid
                            _save_session_map(resource_dir, context_session)
                    completed_tasks[tid] = (
                        asyncio.get_running_loop().time(),
                        state,
                        result,
                    )

                ctx_id = req.context_id or ""
                task_context[req.task_id] = ctx_id

                # Cancel-and-replace: if another task is already running
                # for this context_id, cancel it before starting the new one.
                if ctx_id:
                    prev_tid = context_active.get(ctx_id)
                    if prev_tid and prev_tid in task_registry:
                        log.info(
                            "Canceling task %s (superseded by %s for context %s)",
                            prev_tid,
                            req.task_id,
                            ctx_id,
                        )
                        task_registry[prev_tid].cancel()

                resume = context_session.get(ctx_id) if ctx_id else None

                task = asyncio.create_task(
                    handle_request(
                        client,
                        agent,
                        req,
                        reply_topic,
                        correlation,
                        env,
                        semaphore,
                        cwd=resource_dir,
                        resume_id=resume,
                    )
                )
                task_registry[req.task_id] = task
                if ctx_id:
                    context_active[ctx_id] = req.task_id
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
        print("Usage: skitter agent-runner <name|file>", file=sys.stderr)
        sys.exit(1)
    if len(sys.argv) < 2:
        print(
            "Usage: python -m skitter.agent_runner <name|file>",
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
