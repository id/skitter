import asyncio
import json
import logging
import sys
import time

import aiomqtt
import claude_agent_sdk

from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    TOPIC_CANCEL,
    TOPIC_FEEDBACK,
    TOPIC_RESULTS,
    TOPIC_STREAM,
    TOPIC_STREAM_SNAPSHOT,
    TOPIC_TASKS,
    TOPIC_USAGE,
    TOPIC_WORKER_STATUS,
)
from skitter.types import (
    FeedbackSignal,
    StreamChunk,
    StreamSnapshot,
    TaskMessage,
    TaskResultMessage,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.worker")

SNAPSHOT_CHUNK_INTERVAL = 5
SNAPSHOT_TIME_INTERVAL = 10  # seconds
SNAPSHOT_TEXT_CAP = 50_000
TOOL_RESULT_PREVIEW = 200
TOOL_OUTPUT_PER_ITEM = 1500  # chars per tool result in final output
TOOL_OUTPUT_TOTAL = 20_000  # max total chars of tool results in final output


async def run(agent: str, chat_id: str, task_id: str) -> None:
    log.info("[worker:%s:%s] Starting", agent, task_id)

    status_topic = TOPIC_WORKER_STATUS.format(chat_id=chat_id, task_id=task_id)
    task_topic = TOPIC_TASKS.format(agent=agent, chat_id=chat_id, task_id=task_id)
    result_topic = TOPIC_RESULTS.format(chat_id=chat_id, task_id=task_id)
    stream_topic = TOPIC_STREAM.format(chat_id=chat_id, task_id=task_id)
    snapshot_topic = TOPIC_STREAM_SNAPSHOT.format(chat_id=chat_id, task_id=task_id)
    feedback_topic = TOPIC_FEEDBACK.format(chat_id=chat_id, task_id=task_id)
    cancel_topic = TOPIC_CANCEL.format(chat_id=chat_id, task_id=task_id)

    lwt_payload = json.dumps({"status": "dead", "task_id": task_id})
    will = aiomqtt.Will(topic=status_topic, payload=lwt_payload, qos=1)

    async with aiomqtt.Client(
        MQTT_HOST,
        MQTT_PORT,
        identifier=f"skitter-worker-{task_id}",
        will=will,
    ) as client:
        # Announce alive
        await client.publish(
            status_topic,
            json.dumps({"status": "alive", "task_id": task_id}),
            qos=1,
        )

        # Subscribe to our specific task topic to get the retained message
        await client.subscribe(task_topic, qos=1)
        log.info("[worker:%s:%s] Subscribed to %s", agent, task_id, task_topic)

        # Receive the retained task message
        task: TaskMessage | None = None
        async for mqtt_msg in client.messages:
            try:
                payload = mqtt_msg.payload.decode()
                task = TaskMessage.from_json(payload)
                break
            except Exception as e:
                log.error("[worker:%s:%s] Failed to parse task: %s", agent, task_id, e)
                break

        if task is None:
            log.error("[worker:%s:%s] No task received, exiting", agent, task_id)
            return

        log.info(
            "[worker:%s:%s] Processing task (model=%s, max_turns=%d): %.80s",
            agent,
            task_id,
            task.model or "default",
            task.max_turns,
            task.description,
        )

        # Build system prompt from soul + skills + context + constraints
        system_parts = []
        if task.soul:
            system_parts.append(f"# Identity\n{task.soul}")
        if task.skills:
            system_parts.append(f"# Skills & Constraints\n{task.skills}")
        if task.context:
            system_parts.append(f"# Context from upstream tasks\n{task.context}")
        if task.max_turns > 0:
            system_parts.append(
                f"# Resource Budget\n"
                f"You have {task.max_turns} tool-use turns. "
                f"After that, your execution stops immediately.\n\n"
                f"Plan accordingly:\n"
                f"- Do NOT spend all turns on gathering — reserve time to write up findings.\n"
                f"- Your FINAL text output IS your deliverable. Tool results alone are not visible to reviewers.\n"
                f"- When ~2 turns remain, stop gathering and write a comprehensive summary of everything you found."
            )
        system_prompt = "\n\n".join(system_parts) if system_parts else None

        # --- Streaming state ---
        seq = 0
        started_at = time.time()
        accumulated_text: list[str] = []
        tool_log: list[str] = []
        tool_output_parts: list[str] = []
        tool_output_chars = 0
        tool_calls = 0
        errors = 0
        last_snapshot_time = started_at
        last_snapshot_seq = 0

        # --- Feedback / cancel state (shared with hooks and listener) ---
        pending_feedback: list[str | None] = [None]  # mutable container for closure
        cancel_event = asyncio.Event()

        async def publish_chunk(chunk_type: str, content: str) -> None:
            nonlocal seq
            seq += 1
            chunk = StreamChunk(
                task_id=task_id, chat_id=chat_id, seq=seq,
                type=chunk_type, content=content,
            )
            await client.publish(stream_topic, chunk.to_json(), qos=0)

        async def publish_snapshot() -> None:
            nonlocal last_snapshot_time, last_snapshot_seq
            now = time.time()
            last_snapshot_time = now
            last_snapshot_seq = seq
            full_text = "\n".join(accumulated_text)
            if len(full_text) > SNAPSHOT_TEXT_CAP:
                full_text = full_text[-SNAPSHOT_TEXT_CAP:]
            snapshot = StreamSnapshot(
                task_id=task_id, chat_id=chat_id, seq=seq,
                text=full_text, tool_log=tool_log[-20:],
                tool_calls=tool_calls, errors=errors,
                started_at=started_at, elapsed_s=now - started_at,
            )
            await client.publish(
                snapshot_topic, snapshot.to_json(), qos=1, retain=True,
            )

        async def maybe_publish_snapshot() -> None:
            now = time.time()
            if (
                seq - last_snapshot_seq >= SNAPSHOT_CHUNK_INTERVAL
                or now - last_snapshot_time >= SNAPSHOT_TIME_INTERVAL
            ):
                await publish_snapshot()

        # --- Hooks ---
        async def post_tool_use(event, _match, _ctx):
            nonlocal tool_calls, errors, tool_output_chars
            tool_name = event["tool_name"]
            tool_response = event.get("tool_response", "")
            tool_calls += 1
            response_str = str(tool_response)
            is_error = isinstance(tool_response, dict) and tool_response.get(
                "is_error", False
            )
            if is_error:
                errors += 1
            tool_log.append(f"{tool_name} → {'error' if is_error else 'ok'}")
            await publish_chunk("tool_result", response_str[:TOOL_RESULT_PREVIEW])
            await maybe_publish_snapshot()

            # Capture tool output for inclusion in final result
            if not is_error and tool_output_chars < TOOL_OUTPUT_TOTAL:
                trimmed = response_str[:TOOL_OUTPUT_PER_ITEM]
                entry = f"[{tool_name}]: {trimmed}"
                tool_output_parts.append(entry)
                tool_output_chars += len(entry)

            return {}

        async def pre_tool_use(event, _match, _ctx):
            if cancel_event.is_set():
                return {"continue_": False, "reason": "Task cancelled"}
            feedback = pending_feedback[0]
            if feedback:
                pending_feedback[0] = None
                return {
                    "systemMessage": (
                        "A periodic QA review flagged a potential concern with "
                        "your current approach:\n\n"
                        f'"{feedback}"\n\n'
                        "Briefly consider whether this is valid. If so, adjust. "
                        "If you believe you're on track, continue — the reviewer "
                        "may be mistaken."
                    )
                }
            return {}

        # --- Feedback / cancel listener (second MQTT client) ---
        listener_task: asyncio.Task | None = None

        async def feedback_listener() -> None:
            try:
                async with aiomqtt.Client(
                    MQTT_HOST,
                    MQTT_PORT,
                    identifier=f"skitter-worker-{task_id}-ctrl",
                ) as ctrl_client:
                    await ctrl_client.subscribe(feedback_topic, qos=1)
                    await ctrl_client.subscribe(cancel_topic, qos=1)
                    async for msg in ctrl_client.messages:
                        msg_payload = msg.payload.decode() if msg.payload else ""
                        if not msg_payload:
                            continue
                        msg_topic = str(msg.topic)
                        try:
                            data = json.loads(msg_payload)
                            if msg_topic == feedback_topic:
                                pending_feedback[0] = data.get("feedback", "")
                                log.info(
                                    "[worker:%s:%s] Received QA feedback: %.80s",
                                    agent, task_id, pending_feedback[0],
                                )
                            elif msg_topic == cancel_topic:
                                log.info(
                                    "[worker:%s:%s] Cancel signal: %s",
                                    agent, task_id, data.get("reason", ""),
                                )
                                cancel_event.set()
                                return
                        except Exception as e:
                            log.warning(
                                "[worker:%s:%s] Bad control message: %s",
                                agent, task_id, e,
                            )
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.warning("[worker:%s:%s] Feedback listener error: %s", agent, task_id, e)

        # Only start listener for tasks that use tools (max_turns > 0)
        if task.max_turns != 0:
            listener_task = asyncio.create_task(feedback_listener())

        # --- Run Claude agent ---
        usage: dict | None = None
        cost_usd: float | None = None
        try:
            texts: list[str] = []
            options = claude_agent_sdk.ClaudeAgentOptions(
                max_turns=task.max_turns,
                permission_mode="bypassPermissions",
            )
            if task.max_turns == 0:
                options.allowed_tools = []
                options.tools = []
            else:
                # Add hooks for tasks that use tools
                options.hooks = {
                    "PreToolUse": [
                        claude_agent_sdk.HookMatcher(
                            matcher=None, hooks=[pre_tool_use], timeout=None,
                        )
                    ],
                    "PostToolUse": [
                        claude_agent_sdk.HookMatcher(
                            matcher=None, hooks=[post_tool_use], timeout=None,
                        )
                    ],
                }
            if task.model:
                options.model = task.model
            if system_prompt:
                options.system_prompt = system_prompt

            async for message in claude_agent_sdk.query(
                prompt=task.description,
                options=options,
            ):
                if isinstance(message, claude_agent_sdk.AssistantMessage):
                    for block in message.content:
                        if isinstance(block, claude_agent_sdk.TextBlock):
                            texts.append(block.text)
                            accumulated_text.append(block.text)
                            await publish_chunk("text", block.text)
                        elif isinstance(block, claude_agent_sdk.ToolUseBlock):
                            input_summary = str(block.input)[:100]
                            await publish_chunk(
                                "tool_use", f"{block.name}: {input_summary}",
                            )
                    await maybe_publish_snapshot()
                elif isinstance(message, claude_agent_sdk.ResultMessage):
                    usage = message.usage
                    cost_usd = message.total_cost_usd
                    log.info(
                        "[worker:%s:%s] Result: is_error=%s, turns=%s, cost=$%s",
                        agent,
                        task_id,
                        message.is_error,
                        message.num_turns,
                        f"{cost_usd:.4f}" if cost_usd else "?",
                    )

            response_text = "\n".join(texts) if texts else "(no response)"

            # Append tool results so QA and synthesizer see actual work product
            if tool_output_parts:
                tool_section = "\n\n".join(tool_output_parts)
                response_text += (
                    f"\n\n---\n## Tool Results ({tool_calls} calls)\n\n"
                    + tool_section
                )
        except Exception as e:
            log.error("[worker:%s:%s] Agent error: %s", agent, task_id, e)
            # Use accumulated text + tool outputs as partial result
            if accumulated_text or tool_output_parts:
                parts = [f"[PARTIAL — agent error after {time.time() - started_at:.0f}s]"]
                if accumulated_text:
                    parts.append("\n".join(accumulated_text))
                if tool_output_parts:
                    tool_section = "\n\n".join(tool_output_parts)
                    parts.append(
                        f"---\n## Tool Results ({tool_calls} calls)\n\n"
                        + tool_section
                    )
                response_text = "\n\n".join(parts)
            else:
                response_text = f"Error: {e}"
        finally:
            # Stop feedback listener
            if listener_task and not listener_task.done():
                listener_task.cancel()
                try:
                    await listener_task
                except asyncio.CancelledError:
                    pass

        # Publish result
        result_msg = TaskResultMessage(
            task_id=task_id,
            chat_id=task.chat_id,
            result=response_text,
        )
        await client.publish(result_topic, result_msg.to_json(), qos=1)
        log.info("[worker:%s:%s] Published result to %s", agent, task_id, result_topic)

        # Publish usage
        if usage or cost_usd is not None:
            usage_topic = TOPIC_USAGE.format(chat_id=chat_id, task_id=task_id)
            usage_payload = json.dumps({
                "task_id": task_id,
                "chat_id": chat_id,
                "usage": usage,
                "cost_usd": cost_usd,
            })
            await client.publish(usage_topic, usage_payload, qos=1)
            log.info("[worker:%s:%s] Published usage to %s", agent, task_id, usage_topic)

        # Publish final snapshot
        if seq > 0:
            await publish_snapshot()

        # Clear retained topics
        await client.publish(task_topic, b"", qos=1, retain=True)
        await client.publish(snapshot_topic, b"", qos=1, retain=True)
        await client.publish(feedback_topic, b"", qos=1, retain=True)
        await client.publish(cancel_topic, b"", qos=1, retain=True)

        # Announce done
        await client.publish(
            status_topic,
            json.dumps({"status": "done", "task_id": task_id}),
            qos=1,
        )
        log.info("[worker:%s:%s] Done", agent, task_id)


def main() -> None:
    if len(sys.argv) < 4:
        print(
            "Usage: python -m skitter.worker <agent> <chat_id> <task_id>",
            file=sys.stderr,
        )
        sys.exit(1)
    agent = sys.argv[1]
    chat_id = sys.argv[2]
    task_id = sys.argv[3]
    try:
        asyncio.run(run(agent, chat_id, task_id))
    except KeyboardInterrupt:
        log.info("[worker:%s:%s] Interrupted", agent, task_id)


if __name__ == "__main__":
    main()
