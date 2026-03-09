"""Self-coordinating worker — reads session spec, waits for upstream results,
runs agent as CLI subprocess, publishes results.

Join workers subscribe to upstream result topics
and sleep until all inputs arrive via MQTT retained messages.
"""

import asyncio
import json
import logging
import os
import sys

import aiomqtt

from skitter.config import WORKSPACES_DIR
from skitter.mqtt import (
    A2A_ORG,
    A2A_UNIT,
    make_properties,
    mqtt_client_kwargs,
    topic_event,
    topic_request_cancel,
    topic_result,
    topic_session,
    topic_status,
    topic_usage,
)
from skitter.types import (
    Session,
    SessionTask,
    make_status_event,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.worker")


async def run_agent(
    task: SessionTask,
    context: str,
    workspace: str,
    publish_stream_item,
    cancel_event: asyncio.Event,
) -> tuple[str, dict | None, float | None]:
    """Run any agent runtime as a subprocess, parse JSONL stdout."""
    if task.runtime == "codex":
        prompt_parts = []
        if context:
            prompt_parts.append(f"Context:\n{context}")
        prompt_parts.append(task.description)
        prompt = "\n\n".join(prompt_parts)

        cmd = [
            "codex",
            "exec",
            "--json",
            "--full-auto",
            "--skip-git-repo-check",
            prompt,
        ]
        if task.model:
            cmd.extend(["--model", task.model])
    else:
        cmd = [
            "claude",
            "-p",
            task.description,
            "--output-format",
            "stream-json",
            "--verbose",
            "--dangerously-skip-permissions",
        ]
        if task.agent:
            cmd.extend(["--agent", task.agent])
        if task.model:
            cmd.extend(["--model", task.model])
        if context:
            cmd.extend(
                [
                    "--append-system-prompt",
                    f"# Context from upstream tasks\n{context}",
                ]
            )

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

        event_type = event.get("type", "")

        if event_type == "assistant":
            message = event.get("message", {})
            for block in message.get("content", []):
                if block.get("type") == "text":
                    text = block.get("text", "")
                    texts.append(text)
                    await publish_stream_item("text", text)
                elif block.get("type") == "tool_use":
                    await publish_stream_item(
                        "tool_use",
                        f"{block.get('name', '?')}: {str(block.get('input', ''))[:100]}",
                    )
        elif event_type == "item.completed":
            item = event.get("item", {})
            if item.get("type") == "agent_message":
                text = item.get("text", "")
                if text:
                    texts.append(text)
                    await publish_stream_item("text", text)
        elif event_type == "turn.completed":
            usage = event.get("usage")
        elif event_type == "result":
            cost_usd = event.get("total_cost_usd")
            usage = event.get("usage")

    await proc.wait()

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
    session_topic = topic_session(session_id)
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
    """Subscribe to upstream result topics and wait until all arrive."""
    my_task = session.tasks[task_name]
    needed: set[str] = set()
    for need_id in my_task.needs:
        need_task = session.tasks.get(need_id)
        if need_task:
            needed.add(need_id)
            await client.subscribe(
                topic_result(session.workflow_id, need_id, session.session_id),
                qos=1,
            )

    if not needed:
        return ""

    log.info("Waiting for %d upstream results: %s", len(needed), sorted(needed))
    results: dict[str, str] = {}

    async for mqtt_msg in client.messages:
        payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
        if not payload:
            continue
        try:
            data = json.loads(payload)
        except Exception:
            continue

        source_task = data.get("task", "")
        if source_task in needed:
            results[source_task] = data.get("result", "")
            log.info(
                "Got result from '%s' (%d/%d)",
                source_task,
                len(results),
                len(needed),
            )
            if len(results) == len(needed):
                break

    for need_id in needed:
        await client.unsubscribe(
            topic_result(session.workflow_id, need_id, session.session_id)
        )

    parts = [
        f"## Result from '{need_id}':\n{result}" for need_id, result in results.items()
    ]
    return "\n\n".join(parts)


async def publish_task_state(
    client: aiomqtt.Client,
    workflow_id: str,
    session_id: str,
    task_name: str,
    status: str,
) -> None:
    """Publish per-task status (waiting/running) as a retained message."""
    await client.publish(
        topic_status(workflow_id, task_name, session_id),
        json.dumps({"task": task_name, "session_id": session_id, "status": status}),
        qos=1,
        retain=True,
    )


async def publish_terminal_result(
    client: aiomqtt.Client,
    session: Session,
    task_name: str,
    result: str,
) -> None:
    """Publish terminal TaskStatusUpdateEvent to caller."""
    event = make_status_event(
        request_id=session.caller_correlation,
        task_id=task_name,
        state="completed",
        artifact_text=result,
    )

    if session.caller_reply_topic:
        props = make_properties(correlation_data=session.caller_correlation)
        await client.publish(
            session.caller_reply_topic,
            event,
            qos=1,
            properties=props,
        )


async def run(agent: str, session_id: str, task_name: str) -> str:
    log.info("[worker:%s:%s] Starting", agent, task_name)

    alive_topic = topic_event(agent, "alive")
    lwt_topic = topic_event(agent, "dead")

    lwt_payload = json.dumps(
        {
            "status": "dead",
            "task": task_name,
            "agent": agent,
            "session_id": session_id,
        }
    )
    will = aiomqtt.Will(topic=lwt_topic, payload=lwt_payload, qos=1)

    async with aiomqtt.Client(
        **mqtt_client_kwargs(
            identifier=f"{A2A_ORG}/{A2A_UNIT}/{agent}-{task_name}-{session_id[:8]}",
            will=will,
        ),
    ) as client:
        await client.publish(
            alive_topic,
            json.dumps({"status": "alive", "task": task_name, "agent": agent}),
            qos=1,
        )

        session = await read_retained_session(client, session_id)
        if session is None:
            log.error("[worker:%s:%s] No session found, exiting", agent, task_name)
            return "(no session found)"

        my_task = session.tasks.get(task_name)
        if my_task is None:
            log.error("[worker:%s:%s] Task not found in session", agent, task_name)
            return "(task not found)"

        # Wait for upstream results (join coordination)
        context = ""
        if my_task.needs:
            await publish_task_state(
                client, session.workflow_id, session.session_id, task_name, "waiting"
            )
            context = await wait_for_needs(client, session, task_name)

        await publish_task_state(
            client, session.workflow_id, session.session_id, task_name, "running"
        )

        log.info(
            "[worker:%s:%s] Processing (runtime=%s, model=%s): %.80s",
            agent,
            task_name,
            my_task.runtime,
            my_task.model or "default",
            my_task.description,
        )

        workspace = WORKSPACES_DIR / f"{session_id}-{task_name}"
        workspace.mkdir(parents=True, exist_ok=True)

        # Streaming
        stream_topic = session.caller_reply_topic
        stream_correlation = session.caller_correlation
        stream_props = make_properties(correlation_data=stream_correlation)

        async def publish_stream_item(item_type: str, content: str) -> None:
            if not stream_topic:
                return
            event = make_status_event(
                request_id=stream_correlation,
                task_id=task_name,
                state="working",
                message=content,
                message_type=item_type,
            )
            await client.publish(stream_topic, event, qos=0, properties=stream_props)

        # Cancel listener
        cancel_event = asyncio.Event()
        cancel_topic = topic_request_cancel(agent)
        listener_task: asyncio.Task | None = None

        async def cancel_listener() -> None:
            try:
                async with aiomqtt.Client(
                    **mqtt_client_kwargs(
                        identifier=f"{A2A_ORG}/{A2A_UNIT}/{agent}-{task_name}-{session_id[:8]}-ctrl",
                    ),
                ) as ctrl_client:
                    await ctrl_client.subscribe(cancel_topic, qos=1)
                    async for msg in ctrl_client.messages:
                        msg_payload = msg.payload.decode() if msg.payload else ""
                        if not msg_payload:
                            continue
                        try:
                            data = json.loads(msg_payload)
                            if (
                                data.get("params", {}).get("task") == task_name
                                or data.get("task") == task_name
                            ):
                                log.info(
                                    "[worker:%s:%s] Cancel received",
                                    agent,
                                    task_name,
                                )
                                cancel_event.set()
                                return
                        except Exception:
                            pass
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.warning(
                    "[worker:%s:%s] Cancel listener error: %s", agent, task_name, e
                )

        listener_task = asyncio.create_task(cancel_listener())

        # Run agent
        usage_data: dict | None = None
        cost_usd: float | None = None
        try:
            response_text, usage_data, cost_usd = await run_agent(
                my_task,
                context,
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

        # Publish retained result (all tasks — used by dashboard and downstream workers)
        await client.publish(
            topic_result(session.workflow_id, task_name, session_id),
            json.dumps(
                {
                    "task": task_name,
                    "session_id": session_id,
                    "result": response_text,
                }
            ),
            qos=1,
            retain=True,
        )

        # Terminal tasks also reply directly to the caller
        is_terminal = not my_task.next or my_task.next == "output"
        if is_terminal:
            await publish_terminal_result(client, session, task_name, response_text)

        log.info(
            "[worker:%s:%s] Result published%s",
            agent,
            task_name,
            " (terminal)" if is_terminal else f" for next task '{my_task.next}'",
        )

        # Publish usage
        if usage_data or cost_usd is not None:
            usage_payload = json.dumps(
                {
                    "task": task_name,
                    "session_id": session_id,
                    "usage": usage_data,
                    "cost_usd": cost_usd,
                }
            )
            await client.publish(
                topic_usage(session.workflow_id, task_name, session_id),
                usage_payload,
                qos=1,
            )

        log.info("[worker:%s:%s] Done", agent, task_name)

    return response_text


def main() -> None:
    if len(sys.argv) >= 4:
        agent, session_id, task_name = sys.argv[1:4]
    else:
        agent = os.environ.get("AGENT_NAME", "")
        session_id = os.environ.get("SESSION_ID", "")
        task_name = os.environ.get("TASK", "")

    if not agent or not session_id or not task_name:
        print(
            "Usage: python -m skitter.worker <agent> <session_id> <task>\n"
            "   or: AGENT_NAME=... SESSION_ID=... TASK=... python -m skitter.worker",
            file=sys.stderr,
        )
        sys.exit(1)
    try:
        asyncio.run(run(agent, session_id, task_name))
    except KeyboardInterrupt:
        log.info("[worker:%s:%s] Interrupted", agent, task_name)


if __name__ == "__main__":
    main()
