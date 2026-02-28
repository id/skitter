import asyncio
import json
import logging
import sys

import aiomqtt
import claude_agent_sdk

from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    TOPIC_TASKS,
    TOPIC_RESULTS,
    TOPIC_WORKER_STATUS,
)
from skitter.types import TaskMessage, TaskResultMessage

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.worker")


async def run(agent: str, chat_id: str, task_id: str) -> None:
    log.info("[worker:%s:%s] Starting", agent, task_id)

    status_topic = TOPIC_WORKER_STATUS.format(chat_id=chat_id, task_id=task_id)
    task_topic = TOPIC_TASKS.format(agent=agent, chat_id=chat_id, task_id=task_id)
    result_topic = TOPIC_RESULTS.format(chat_id=chat_id, task_id=task_id)

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

        # Build system prompt from soul + skills + context
        system_parts = []
        if task.soul:
            system_parts.append(f"# Identity\n{task.soul}")
        if task.skills:
            system_parts.append(f"# Skills & Constraints\n{task.skills}")
        if task.context:
            system_parts.append(f"# Context from upstream tasks\n{task.context}")
        system_prompt = "\n\n".join(system_parts) if system_parts else None

        # Run Claude agent
        try:
            texts: list[str] = []
            options = claude_agent_sdk.ClaudeAgentOptions(
                max_turns=task.max_turns,
                permission_mode="bypassPermissions",
            )
            if task.max_turns == 0:
                options.allowed_tools = []
                options.tools = []
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
                elif isinstance(message, claude_agent_sdk.ResultMessage):
                    log.info(
                        "[worker:%s:%s] Result: is_error=%s, turns=%s",
                        agent,
                        task_id,
                        message.is_error,
                        message.num_turns,
                    )

            response_text = "\n".join(texts) if texts else "(no response)"
        except Exception as e:
            log.error("[worker:%s:%s] Agent error: %s", agent, task_id, e)
            response_text = f"Error: {e}"

        # Publish result
        result_msg = TaskResultMessage(
            task_id=task_id,
            chat_id=task.chat_id,
            result=response_text,
        )
        await client.publish(result_topic, result_msg.to_json(), qos=1)
        log.info("[worker:%s:%s] Published result to %s", agent, task_id, result_topic)

        # Clear retained task message
        await client.publish(task_topic, b"", qos=1, retain=True)

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
