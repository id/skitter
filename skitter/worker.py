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
    A2A_ORG,
    A2A_UNIT,
    make_properties,
    topic_chain_result,
    topic_event,
    topic_request_cancel,
    topic_state_dispatch,
    topic_state_usage,
)
from skitter import config as skitter_config
from skitter.types import (
    AgentMessage,
    StreamItem,
    TaskStatusUpdate,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.worker")

TOOL_RESULT_PREVIEW = 200
TOOL_OUTPUT_PER_ITEM = 1500
TOOL_OUTPUT_TOTAL = 20_000


async def run_claude_agent(
    task: AgentMessage,
    workspace: str,
    publish_stream_item,
    cancel_event: asyncio.Event,
) -> tuple[str, dict | None, float | None]:
    """Run a task using claude-agent-sdk. Returns (response_text, usage, cost_usd)."""
    seq_holder = [0]
    started_at = time.time()
    accumulated_text: list[str] = []
    tool_log: list[str] = []
    tool_output_parts: list[str] = []
    tool_output_chars = 0
    tool_calls = 0
    errors = 0

    async def _publish(item_type: str, content: str) -> None:
        seq_holder[0] += 1
        await publish_stream_item(item_type, content, seq_holder[0])

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
        await _publish("tool_result", response_str[:TOOL_RESULT_PREVIEW])

        if not is_error and tool_output_chars < TOOL_OUTPUT_TOTAL:
            trimmed = response_str[:TOOL_OUTPUT_PER_ITEM]
            entry = f"[{tool_name}]: {trimmed}"
            tool_output_parts.append(entry)
            tool_output_chars += len(entry)

        return {}

    async def pre_tool_use(event, _match, _ctx):
        if cancel_event.is_set():
            return {"continue_": False, "reason": "Task cancelled"}
        return {}

    # Build system prompt
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

    usage: dict | None = None
    cost_usd: float | None = None
    response_text = "(no response)"
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
            options.hooks = {
                "PreToolUse": [
                    claude_agent_sdk.HookMatcher(
                        matcher=None,
                        hooks=[pre_tool_use],
                        timeout=None,
                    )
                ],
                "PostToolUse": [
                    claude_agent_sdk.HookMatcher(
                        matcher=None,
                        hooks=[post_tool_use],
                        timeout=None,
                    )
                ],
            }
        if task.model:
            options.model = task.model
        if system_prompt:
            options.system_prompt = system_prompt
        options.cwd = workspace

        async for message in claude_agent_sdk.query(
            prompt=task.description,
            options=options,
        ):
            if isinstance(message, claude_agent_sdk.AssistantMessage):
                for block in message.content:
                    if isinstance(block, claude_agent_sdk.TextBlock):
                        texts.append(block.text)
                        accumulated_text.append(block.text)
                        await _publish("text", block.text)
                    elif isinstance(block, claude_agent_sdk.ToolUseBlock):
                        input_summary = str(block.input)[:100]
                        await _publish(
                            "tool_use",
                            f"{block.name}: {input_summary}",
                        )
            elif isinstance(message, claude_agent_sdk.ResultMessage):
                usage = message.usage
                cost_usd = message.total_cost_usd
                log.info(
                    "[worker] Result: is_error=%s, turns=%s, cost=$%s",
                    message.is_error,
                    message.num_turns,
                    f"{cost_usd:.4f}" if cost_usd else "?",
                )

        response_text = "\n".join(texts) if texts else "(no response)"

        if tool_output_parts:
            tool_section = "\n\n".join(tool_output_parts)
            response_text += (
                f"\n\n---\n## Tool Results ({tool_calls} calls)\n\n" + tool_section
            )
    except Exception as e:
        log.error("[worker] Claude agent error: %s", e)
        if accumulated_text or tool_output_parts:
            parts = [f"[PARTIAL — agent error after {time.time() - started_at:.0f}s]"]
            if accumulated_text:
                parts.append("\n".join(accumulated_text))
            if tool_output_parts:
                tool_section = "\n\n".join(tool_output_parts)
                parts.append(
                    f"---\n## Tool Results ({tool_calls} calls)\n\n" + tool_section
                )
            response_text = "\n\n".join(parts)
        else:
            response_text = f"Error: {e}"

    return response_text, usage, cost_usd


async def run_codex_agent(
    task: AgentMessage,
    workspace: str,
    publish_stream_item,
) -> tuple[str, dict | None, float | None]:
    """Run a task using Codex CLI. Returns (response_text, usage, cost_usd)."""
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

    cmd = ["codex", "exec", "--json", "--full-auto", prompt]
    if task.model:
        cmd.extend(["--model", task.model])

    seq_holder = [0]

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
            cwd=workspace,
        )
    except FileNotFoundError:
        error_msg = (
            "Error: codex CLI not installed. Install with: npm i -g @openai/codex"
        )
        log.error("[worker] %s", error_msg)
        return error_msg, None, None

    texts: list[str] = []
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
        if event_type == "message" and event.get("role") == "assistant":
            for block in event.get("content", []):
                if block.get("type") == "output_text":
                    text = block.get("text", "")
                    texts.append(text)
                    seq_holder[0] += 1
                    await publish_stream_item("text", text, seq_holder[0])
                elif block.get("type") == "tool_use":
                    seq_holder[0] += 1
                    await publish_stream_item(
                        "tool_use",
                        f"{block.get('name', '?')}: {str(block.get('input', ''))[:100]}",
                        seq_holder[0],
                    )

    await proc.wait()
    response_text = "\n".join(texts) if texts else "(no response from codex)"
    return response_text, None, None


async def run(agent: str, session_id: str, task_id: str) -> None:
    log.info("[worker:%s:%s] Starting", agent, task_id)

    # A2A topics — use agent_id-based event topics
    alive_topic = topic_event(agent, "alive")
    done_topic = topic_event(agent, "done")
    lwt_topic = topic_event(agent, "dead")
    dispatch_topic = topic_state_dispatch(task_id)
    cancel_topic = topic_request_cancel(agent)
    usage_topic = topic_state_usage(session_id, task_id)

    lwt_payload = json.dumps({"status": "dead", "task_id": task_id, "agent": agent})
    will = aiomqtt.Will(topic=lwt_topic, payload=lwt_payload, qos=1)

    async with aiomqtt.Client(
        MQTT_HOST,
        MQTT_PORT,
        identifier=f"{A2A_ORG}/{A2A_UNIT}/{agent}-{task_id}",
        will=will,
        protocol=aiomqtt.ProtocolVersion.V5,
    ) as client:
        # Subscribe to retained dispatch topic
        await client.subscribe(dispatch_topic, qos=1)

        # Announce alive (monitoring only)
        await client.publish(
            alive_topic,
            json.dumps({"status": "alive", "task_id": task_id, "agent": agent}),
            qos=1,
        )
        log.info(
            "[worker:%s:%s] Alive, subscribed to %s", agent, task_id, dispatch_topic
        )

        # Read retained dispatch from coordinator
        task: AgentMessage | None = None
        response_topic: str | None = None
        correlation_data: str | None = None

        try:
            async with asyncio.timeout(30.0):
                async for mqtt_msg in client.messages:
                    try:
                        payload = mqtt_msg.payload.decode()
                        data = json.loads(payload)
                        task = AgentMessage.from_json(json.dumps(data["task"]))
                        response_topic = data["reply_topic"]
                        correlation_data = data["correlation"]
                        break
                    except Exception as e:
                        log.error(
                            "[worker:%s:%s] Failed to parse dispatch: %s",
                            agent,
                            task_id,
                            e,
                        )
                        break
        except TimeoutError:
            log.error(
                "[worker:%s:%s] Timed out waiting for retained dispatch",
                agent,
                task_id,
            )

        if task is None or response_topic is None:
            log.error(
                "[worker:%s:%s] No task or no response topic, exiting", agent, task_id
            )
            return

        # Clear retained dispatch
        await client.publish(dispatch_topic, b"", qos=1, retain=True)

        log.info(
            "[worker:%s:%s] Processing task (runtime=%s, model=%s, max_turns=%d): %.80s",
            agent,
            task_id,
            task.runtime,
            task.model or "default",
            task.max_turns,
            task.description,
        )

        # Resolve workspace
        workspace = skitter_config.WORKSPACES_DIR / task_id
        workspace.mkdir(parents=True, exist_ok=True)
        workspace_str = str(workspace)

        # Determine streaming target: direct to caller
        stream_topic = task.caller_reply_topic or response_topic
        stream_correlation = task.caller_correlation or correlation_data

        async def publish_stream_item(item_type: str, content: str, seq: int) -> None:
            item = StreamItem(
                task_id=task_id,
                seq=seq,
                type=item_type,
                content=content,
            )
            props = make_properties(correlation_data=stream_correlation)
            await client.publish(
                stream_topic,
                item.to_json(),
                qos=0,
                properties=props,
            )

        # Cancel listener
        cancel_event = asyncio.Event()
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
                                    "[worker:%s:%s] Cancel signal received",
                                    agent,
                                    task_id,
                                )
                                cancel_event.set()
                                return
                        except Exception as e:
                            log.warning(
                                "[worker:%s:%s] Bad cancel message: %s",
                                agent,
                                task_id,
                                e,
                            )
            except asyncio.CancelledError:
                pass
            except Exception as e:
                log.warning(
                    "[worker:%s:%s] Cancel listener error: %s",
                    agent,
                    task_id,
                    e,
                )

        if task.max_turns != 0:
            listener_task = asyncio.create_task(cancel_listener())

        # --- Run agent based on runtime ---
        usage: dict | None = None
        cost_usd: float | None = None
        try:
            if task.runtime == "codex":
                response_text, usage, cost_usd = await run_codex_agent(
                    task, workspace_str, publish_stream_item
                )
            else:
                response_text, usage, cost_usd = await run_claude_agent(
                    task, workspace_str, publish_stream_item, cancel_event
                )
        finally:
            if listener_task and not listener_task.done():
                listener_task.cancel()
                try:
                    await listener_task
                except asyncio.CancelledError:
                    pass

        # --- Post-completion routing ---
        final_status = TaskStatusUpdate(
            task_id=task_id,
            state="completed",
            result=response_text,
        )

        if task.next == "" or task.next == "output":
            # Terminal: publish result to caller + bookkeeping to coordinator
            reply_topic = task.caller_reply_topic or response_topic
            reply_corr = task.caller_correlation or correlation_data
            props = make_properties(correlation_data=reply_corr)
            await client.publish(
                reply_topic,
                final_status.to_json(),
                qos=1,
                properties=props,
            )
            # Bookkeeping to coordinator
            props = make_properties(correlation_data=correlation_data)
            await client.publish(
                response_topic,
                final_status.to_json(),
                qos=1,
                properties=props,
            )
            log.info(
                "[worker:%s:%s] Terminal result published to %s",
                agent,
                task_id,
                reply_topic,
            )
        else:
            # Non-terminal: publish retained chain result for coordinator
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
                task.next,
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
