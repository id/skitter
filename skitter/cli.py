"""Interactive CLI for chatting with skitter via A2A-over-MQTT."""

import asyncio
import json
import sys
import uuid

import aiomqtt

from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    A2A_ORG,
    A2A_UNIT,
    make_properties,
    topic_reply,
    topic_request,
)
from skitter.types import InboundMessage, StreamItem, TaskStatusUpdate


async def run_chat(session_id: str) -> None:
    mqtt_session = uuid.uuid4().hex[:12]
    reply_t = topic_reply("cli", mqtt_session)
    default_request = topic_request("skitter")

    print(f"Connecting to {MQTT_HOST}:{MQTT_PORT}")
    print(f"Session ID: {session_id}")
    print(f"Reply topic: {reply_t}")
    print("Type or paste a message. /send to send, /drop to discard.")
    print("Ctrl+C to exit.\n")

    async with aiomqtt.Client(
        MQTT_HOST,
        MQTT_PORT,
        identifier=f"{A2A_ORG}/{A2A_UNIT}/cli-{mqtt_session}",
        protocol=aiomqtt.ProtocolVersion.V5,
    ) as client:
        await client.subscribe(reply_t, qos=1)

        async def listen() -> None:
            seen_seqs: set[tuple[str, int]] = set()
            async for msg in client.messages:
                try:
                    payload = msg.payload.decode() if msg.payload else ""
                    if not payload:
                        continue
                    data = json.loads(payload)

                    # Stream item (with dedup for QoS 1 redelivery)
                    if "seq" in data and "type" in data:
                        item = StreamItem.from_json(payload)
                        dedup_key = (item.task_id, item.seq)
                        if dedup_key in seen_seqs:
                            continue
                        seen_seqs.add(dedup_key)
                        if item.type == "text":
                            print(f"\r\033[K{item.content}", end="", flush=True)
                        elif item.type == "tool_use":
                            print(f"\r\033[K  [tool] {item.content}")
                        elif item.type == "tool_result":
                            print(f"\r\033[K  [result] {item.content[:80]}")
                        continue

                    # Terminal status
                    if "state" in data and "task_id" in data:
                        status = TaskStatusUpdate.from_json(payload)
                        print(f"\r\033[K\n{status.result}")
                        print("> ", end="", flush=True)
                        continue

                    # Error response (A2AResponse)
                    if "error" in data:
                        print(
                            f"\r\033[KError: {data['error'].get('message', data['error'])}"
                        )
                        print("> ", end="", flush=True)
                        continue

                except Exception:
                    pass

        listener = asyncio.create_task(listen())
        loop = asyncio.get_event_loop()

        def read_message() -> str:
            lines = []
            while True:
                prompt = "> " if not lines else ". "
                line = input(prompt)
                if line.strip() == "/send" and lines:
                    break
                if line.strip() == "/drop":
                    print("Discarded.")
                    return ""
                lines.append(line)
            return "\n".join(lines).strip()

        try:
            while True:
                text = await loop.run_in_executor(None, read_message)
                if not text:
                    continue

                # Parse invocation commands
                workflow_id = ""
                workflow_vars: dict[str, str] = {}
                agent_id = ""
                if text.startswith("/workflow "):
                    parts = text.split()
                    workflow_id = parts[1] if len(parts) > 1 else ""
                    i = 2
                    while i < len(parts):
                        if parts[i] == "--var" and i + 1 < len(parts):
                            kv = parts[i + 1]
                            if "=" in kv:
                                k, v = kv.split("=", 1)
                                workflow_vars[k] = v
                            i += 2
                        else:
                            i += 1
                    text = f"Workflow '{workflow_id}'"
                elif text.startswith("/agent "):
                    parts = text.split(None, 2)
                    agent_id = parts[1] if len(parts) > 1 else ""
                    text = parts[2] if len(parts) > 2 else ""
                    if not agent_id or not text:
                        print("Usage: /agent <agent_id> <description>")
                        continue
                # else: no prefix — goes to default agent (skitter)

                msg = InboundMessage(
                    text=text,
                    sender="cli",
                    session_id=session_id,
                    workflow_id=workflow_id,
                    workflow_vars=workflow_vars,
                    agent_id=agent_id,
                )

                # Route to the correct agent/workflow request topic
                if workflow_id:
                    request_topic = topic_request(f"workflow-{workflow_id}")
                elif agent_id:
                    request_topic = topic_request(agent_id)
                else:
                    request_topic = default_request

                props = make_properties(
                    response_topic=reply_t,
                    correlation_data=session_id,
                )
                await client.publish(
                    request_topic,
                    msg.to_json(),
                    qos=1,
                    properties=props,
                )
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
        finally:
            listener.cancel()


def main() -> None:
    session_id = f"cli-{uuid.uuid4().hex[:8]}"

    args = sys.argv[2:]  # skip "skitter" and "chat"
    i = 0
    while i < len(args):
        if args[i] == "--session-id" and i + 1 < len(args):
            session_id = args[i + 1]
            i += 2
        else:
            print(f"Unknown argument: {args[i]}", file=sys.stderr)
            sys.exit(1)

    asyncio.run(run_chat(session_id))
