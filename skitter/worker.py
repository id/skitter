"""Self-coordinating worker — reads session spec, waits for upstream results,
runs agent as CLI subprocess, publishes results.

Join workers subscribe to upstream chain result topics
and sleep until all inputs arrive via MQTT retained messages.
"""

import asyncio
import json
import logging
import os
import sys

import aiomqtt

from skitter.config import AGENT_HOMES_DIR, WORKSPACES_DIR
from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    A2A_ORG,
    A2A_UNIT,
    make_properties,
    topic_chain_result,
    topic_event,
    topic_request_cancel,
    topic_state_session,
    topic_state_task,
    topic_state_usage,
)
from skitter.types import (
    AgentMessage,
    Session,
    StreamItem,
    TaskStatusUpdate,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.worker")


def resolve_agent_home(agent_id: str) -> str:
    """Return the agent's home directory path."""
    home = AGENT_HOMES_DIR / agent_id
    home.mkdir(parents=True, exist_ok=True)
    return str(home)


def build_system_prompt(task: AgentMessage) -> str:
    """Build system prompt from soul + skills + context + budget."""
    parts = []
    if task.soul:
        parts.append(f"# Identity\n{task.soul}")
    if task.skills:
        parts.append(f"# Skills & Constraints\n{task.skills}")
    if task.context:
        parts.append(f"# Context from upstream tasks\n{task.context}")
    if task.max_turns > 0:
        parts.append(
            f"# Resource Budget\n"
            f"You have {task.max_turns} tool-use turns. "
            f"After that, your execution stops immediately.\n\n"
            f"Plan accordingly:\n"
            f"- Do NOT spend all turns on gathering — reserve time to write up findings.\n"
            f"- Your FINAL text output IS your deliverable. Tool results alone are not visible to reviewers.\n"
            f"- When ~2 turns remain, stop gathering and write a comprehensive summary of everything you found."
        )
    return "\n\n".join(parts)


async def run_agent(
    task: AgentMessage,
    workspace: str,
    publish_stream_item,
    cancel_event: asyncio.Event,
) -> tuple[str, dict | None, float | None]:
    """Run any agent runtime as a subprocess, parse JSONL stdout."""
    if task.runtime == "codex":
        # Build prompt from soul + skills + context + description
        prompt_parts = []
        if task.soul:
            prompt_parts.append(task.soul)
        if task.skills:
            prompt_parts.append(task.skills)
        if task.context:
            prompt_parts.append(f"Context:\n{task.context}")
        prompt_parts.append(task.description)
        prompt = "\n\n".join(prompt_parts)

        cmd = ["codex", "exec", "--json", "--full-auto", "--skip-git-repo-check", prompt]
        if task.model:
            cmd.extend(["--model", task.model])
    else:
        # Claude CLI
        cmd = [
            "claude",
            "-p",
            task.description,
            "--output-format",
            "stream-json",
            "--verbose",
            "--max-turns",
            str(task.max_turns),
            "--dangerously-skip-permissions",
        ]
        if task.model:
            cmd.extend(["--model", task.model])
        if task.max_turns == 0:
            cmd.extend(["--allowedTools", ""])

        system_prompt = build_system_prompt(task)
        if system_prompt:
            cmd.extend(["--append-system-prompt", system_prompt])

    # Filter CLAUDECODE to allow nested claude sessions
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=workspace,
            env=env,
        )
    except FileNotFoundError:
        binary = "codex" if task.runtime == "codex" else "claude"
        error_msg = f"Error: {binary} CLI not found on PATH"
        log.error(error_msg)
        return error_msg, None, None

    texts: list[str] = []
    usage: dict | None = None
    cost_usd: float | None = None
    seq = 0

    assert proc.stdout is not None
    async for line in proc.stdout:
        if cancel_event.is_set():
            proc.terminate()
            break

        line_str = line.decode().strip()
        if not line_str:
            continue
        try:
            event = json.loads(line_str)
        except json.JSONDecodeError:
            continue

        seq += 1
        event_type = event.get("type", "")

        # Claude stream-json: {"type":"assistant","message":{"content":[...]}}
        if event_type == "assistant":
            message = event.get("message", {})
            for block in message.get("content", []):
                if block.get("type") == "text":
                    text = block.get("text", "")
                    texts.append(text)
                    await publish_stream_item("text", text, seq)
                elif block.get("type") == "tool_use":
                    await publish_stream_item(
                        "tool_use",
                        f"{block.get('name', '?')}: {str(block.get('input', ''))[:100]}",
                        seq,
                    )
        # Codex JSONL: {"type":"item.completed","item":{"type":"agent_message","text":"..."}}
        elif event_type == "item.completed":
            item = event.get("item", {})
            if item.get("type") == "agent_message":
                text = item.get("text", "")
                if text:
                    texts.append(text)
                    await publish_stream_item("text", text, seq)
        # Codex: {"type":"turn.completed","usage":{...}}
        elif event_type == "turn.completed":
            usage = event.get("usage")
        # Claude: {"type":"result","total_cost_usd":...,"usage":{...}}
        elif event_type == "result":
            cost_usd = event.get("total_cost_usd")
            usage = event.get("usage")

    await proc.wait()

    # Log stderr for debugging (auth errors, etc.)
    if proc.stderr:
        stderr = (await proc.stderr.read()).decode().strip()
        if stderr:
            log.warning("stderr: %s", stderr[:500])

    if proc.returncode and proc.returncode != 0 and not texts:
        return f"(process exited with code {proc.returncode})", None, None

    response_text = "\n".join(texts) if texts else "(no response)"
    return response_text, usage, cost_usd


async def read_retained_session(
    client: aiomqtt.Client, session_id: str
) -> Session | None:
    """Read the retained session spec from MQTT."""
    session_topic = topic_state_session(session_id)
    await client.subscribe(session_topic, qos=1)

    try:
        async with asyncio.timeout(30.0):
            async for mqtt_msg in client.messages:
                payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                if not payload:
                    continue
                session = Session.from_json(payload)
                await client.unsubscribe(session_topic)
                return session
    except TimeoutError:
        log.error("Timed out waiting for session %s", session_id)

    await client.unsubscribe(session_topic)
    return None


async def wait_for_needs(
    client: aiomqtt.Client, session: Session, task_name: str
) -> str:
    """Subscribe to upstream chain result topics and wait until all arrive."""
    my_task = session.tasks[task_name]
    needed: dict[str, str] = {}  # source_task_id -> need_id
    for need_id in my_task.needs:
        need_task = session.tasks.get(need_id)
        if need_task:
            needed[need_task.task_id] = need_id
            await client.subscribe(
                topic_chain_result(session.session_id, need_task.task_id), qos=1
            )

    if not needed:
        return ""

    log.info("Waiting for %d upstream results: %s", len(needed), list(needed.values()))
    results: dict[str, str] = {}

    async for mqtt_msg in client.messages:
        payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
        if not payload:
            continue
        try:
            data = json.loads(payload)
        except Exception:
            continue

        source_tid = data.get("task_id", "")
        if source_tid in needed:
            need_id = needed[source_tid]
            results[need_id] = data.get("result", "")
            log.info(
                "Got result from '%s' (%d/%d)",
                need_id,
                len(results),
                len(needed),
            )
            if len(results) == len(needed):
                break

    # Unsubscribe from chain topics
    for need_id in my_task.needs:
        need_task = session.tasks.get(need_id)
        if need_task:
            await client.unsubscribe(
                topic_chain_result(session.session_id, need_task.task_id)
            )

    parts = [
        f"## Result from '{need_id}':\n{result}" for need_id, result in results.items()
    ]
    return "\n\n".join(parts)


async def publish_task_status(
    client: aiomqtt.Client,
    session_id: str,
    task_id: str,
    status: str,
    result: str = "",
) -> None:
    """Publish per-task status as a retained message on its own topic."""
    payload = {"task_id": task_id, "session_id": session_id, "status": status}
    if result:
        payload["result"] = result
    await client.publish(
        topic_state_task(session_id, task_id),
        json.dumps(payload),
        qos=1,
        retain=True,
    )


async def publish_terminal_result(
    client: aiomqtt.Client,
    session: Session,
    task_name: str,
    result: str,
) -> None:
    """Publish result to caller."""
    my_task = session.tasks[task_name]
    status = TaskStatusUpdate(
        task_id=my_task.task_id,
        state="completed",
        result=result,
    )

    # Publish to caller
    if session.caller_reply_topic:
        props = make_properties(correlation_data=session.caller_correlation)
        await client.publish(
            session.caller_reply_topic,
            status.to_json(),
            qos=1,
            properties=props,
        )



async def run(agent: str, session_id: str, task_id: str) -> None:
    log.info("[worker:%s:%s] Starting", agent, task_id)

    alive_topic = topic_event(agent, "alive")
    done_topic = topic_event(agent, "done")
    lwt_topic = topic_event(agent, "dead")
    usage_topic = topic_state_usage(session_id, task_id)

    lwt_payload = json.dumps(
        {
            "status": "dead",
            "task_id": task_id,
            "agent": agent,
            "session_id": session_id,
        }
    )
    will = aiomqtt.Will(topic=lwt_topic, payload=lwt_payload, qos=1)

    async with aiomqtt.Client(
        MQTT_HOST,
        MQTT_PORT,
        identifier=f"{A2A_ORG}/{A2A_UNIT}/{agent}-{task_id}",
        will=will,
        protocol=aiomqtt.ProtocolVersion.V5,
    ) as client:
        # Announce alive
        await client.publish(
            alive_topic,
            json.dumps({"status": "alive", "task_id": task_id, "agent": agent}),
            qos=1,
        )

        # 1. Read session spec (retained)
        session = await read_retained_session(client, session_id)
        if session is None:
            log.error("[worker:%s:%s] No session found, exiting", agent, task_id)
            return

        # Find my task by task_id
        task_name = None
        for tid, st in session.tasks.items():
            if st.task_id == task_id:
                task_name = tid
                break

        if task_name is None:
            log.error("[worker:%s:%s] Task not found in session", agent, task_id)
            return

        my_spec = session.task_dispatches.get(task_name)
        if my_spec is None:
            log.error("[worker:%s:%s] No dispatch spec in session", agent, task_id)
            return

        # 2. Wait for upstream results (join coordination)
        my_task = session.tasks[task_name]
        context = ""
        if my_task.needs:
            await publish_task_status(client, session_id, task_id, "waiting")
            context = await wait_for_needs(client, session, task_name)

        # 3. Build AgentMessage from pre-materialized spec + context
        spec_with_context = {**my_spec, "context": context}
        task_msg = AgentMessage(
            task_id=spec_with_context["task_id"],
            session_id=spec_with_context["session_id"],
            description=spec_with_context["description"],
            soul=spec_with_context.get("soul", ""),
            skills=spec_with_context.get("skills", ""),
            context=spec_with_context.get("context", ""),
            max_turns=spec_with_context.get("max_turns", 10),
            model=spec_with_context.get("model", ""),
            runtime=spec_with_context.get("runtime", "claude"),
            next=spec_with_context.get("next", ""),
            caller_reply_topic=spec_with_context.get("caller_reply_topic", ""),
            caller_correlation=spec_with_context.get("caller_correlation", ""),
        )

        await publish_task_status(client, session_id, task_id, "running")

        log.info(
            "[worker:%s:%s] Processing (runtime=%s, model=%s, max_turns=%d): %.80s",
            agent,
            task_id,
            task_msg.runtime,
            task_msg.model or "default",
            task_msg.max_turns,
            task_msg.description,
        )

        # 4. Resolve workspace
        workspace = WORKSPACES_DIR / task_id
        workspace.mkdir(parents=True, exist_ok=True)

        # Per-agent HOME (only used in docker mode to isolate agent configs)
        # In subprocess mode, keep the real HOME so claude CLI finds OAuth creds
        worker_mode = os.environ.get("SKITTER_WORKER_MODE", "subprocess")
        if worker_mode == "docker":
            agent_home = resolve_agent_home(agent)
            os.environ["HOME"] = agent_home

        # Streaming target
        stream_topic = task_msg.caller_reply_topic
        stream_correlation = task_msg.caller_correlation

        async def publish_stream_item(item_type: str, content: str, seq: int) -> None:
            if not stream_topic:
                return
            item = StreamItem(task_id=task_id, seq=seq, type=item_type, content=content)
            props = make_properties(correlation_data=stream_correlation)
            await client.publish(stream_topic, item.to_json(), qos=0, properties=props)

        # Cancel listener
        cancel_event = asyncio.Event()
        cancel_topic = topic_request_cancel(agent)
        listener_task: asyncio.Task | None = None

        async def cancel_listener() -> None:
            try:
                async with aiomqtt.Client(
                    MQTT_HOST,
                    MQTT_PORT,
                    identifier=f"{A2A_ORG}/{A2A_UNIT}/{agent}-{task_id}-ctrl",
                    protocol=aiomqtt.ProtocolVersion.V5,
                ) as ctrl_client:
                    await ctrl_client.subscribe(cancel_topic, qos=1)
                    async for msg in ctrl_client.messages:
                        msg_payload = msg.payload.decode() if msg.payload else ""
                        if not msg_payload:
                            continue
                        try:
                            data = json.loads(msg_payload)
                            if (
                                data.get("params", {}).get("task_id") == task_id
                                or data.get("task_id") == task_id
                            ):
                                log.info(
                                    "[worker:%s:%s] Cancel received", agent, task_id
                                )
                                cancel_event.set()
                                return
                        except Exception:
                            pass
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.warning(
                    "[worker:%s:%s] Cancel listener error: %s", agent, task_id, e
                )

        if task_msg.max_turns != 0:
            listener_task = asyncio.create_task(cancel_listener())

        # 5. Run agent
        usage: dict | None = None
        cost_usd: float | None = None
        try:
            response_text, usage, cost_usd = await run_agent(
                task_msg,
                str(workspace),
                publish_stream_item,
                cancel_event,
            )
        finally:
            if listener_task and not listener_task.done():
                listener_task.cancel()
                try:
                    await listener_task
                except asyncio.CancelledError:
                    pass

        # 6. Publish result
        if task_msg.next and task_msg.next != "output":
            # Non-terminal: publish retained chain result
            await client.publish(
                topic_chain_result(session_id, task_id),
                json.dumps(
                    {
                        "task_id": task_id,
                        "session_id": session_id,
                        "result": response_text,
                    }
                ),
                qos=1,
                retain=True,
            )
            log.info(
                "[worker:%s:%s] Chain result published for next task '%s'",
                agent,
                task_id,
                task_msg.next,
            )

            await publish_task_status(client, session_id, task_id, "done")
        else:
            # Terminal: publish to caller
            await publish_terminal_result(client, session, task_name, response_text)
            await publish_task_status(
                client, session_id, task_id, "done", result=response_text
            )
            log.info(
                "[worker:%s:%s] Terminal result published",
                agent,
                task_id,
            )

        # Publish usage
        if usage or cost_usd is not None:
            usage_payload = json.dumps(
                {
                    "task_id": task_id,
                    "session_id": session_id,
                    "usage": usage,
                    "cost_usd": cost_usd,
                }
            )
            await client.publish(usage_topic, usage_payload, qos=1)

        # Announce done
        await client.publish(
            done_topic,
            json.dumps({"status": "done", "task_id": task_id, "agent": agent}),
            qos=1,
        )
        log.info("[worker:%s:%s] Done", agent, task_id)


def main() -> None:
    if len(sys.argv) < 4:
        print(
            "Usage: python -m skitter.worker <agent> <session_id> <task_id>",
            file=sys.stderr,
        )
        sys.exit(1)
    agent = sys.argv[1]
    session_id = sys.argv[2]
    task_id = sys.argv[3]
    try:
        asyncio.run(run(agent, session_id, task_id))
    except KeyboardInterrupt:
        log.info("[worker:%s:%s] Interrupted", agent, task_id)


if __name__ == "__main__":
    main()
