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
    mqtt_client_kwargs,
    topic_reply,
    topic_request,
)
from skitter.types import (
    A2ARequest,
    REPLY_ERROR,
    REPLY_FAILED,
    REPLY_TERMINAL,
    REPLY_TEXT,
    REPLY_TOOL,
    classify_reply,
)


async def run_chat(session_id: str) -> None:
    mqtt_session = uuid.uuid4().hex[:12]
    reply_t = topic_reply("cli", mqtt_session)
    default_request = topic_request("coordinator")

    print(f"Connecting to {MQTT_HOST}:{MQTT_PORT}")
    print(f"Session ID: {session_id}")
    print(f"Reply topic: {reply_t}")
    print("Type or paste a message. /send to send, /drop to discard.")
    print("Ctrl+C to exit.\n")

    async with aiomqtt.Client(
        **mqtt_client_kwargs(identifier=f"{A2A_ORG}/{A2A_UNIT}/cli-{mqtt_session}"),
    ) as client:
        await client.subscribe(reply_t, qos=1)

        async def listen() -> None:
            async for msg in client.messages:
                try:
                    payload = msg.payload.decode() if msg.payload else ""
                    if not payload:
                        continue
                    data = json.loads(payload)

                    kind, content = classify_reply(data)
                    if kind == REPLY_TEXT:
                        print(f"\r\033[K{content}", end="", flush=True)
                    elif kind == REPLY_TOOL:
                        print(f"\r\033[K  [tool] {content}")
                    elif kind == REPLY_TERMINAL:
                        print(f"\r\033[K\n{content}")
                        print("> ", end="", flush=True)
                    elif kind == REPLY_FAILED:
                        print(f"\r\033[KFailed: {content}")
                        print("> ", end="", flush=True)
                    elif kind == REPLY_ERROR:
                        print(f"\r\033[KError: {content}")
                        print("> ", end="", flush=True)

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
                agent_id = ""
                if text.startswith("/agent "):
                    parts = text.split(None, 2)
                    agent_id = parts[1] if len(parts) > 1 else ""
                    text = parts[2] if len(parts) > 2 else ""
                    if not agent_id or not text:
                        print("Usage: /agent <agent_id> <description>")
                        continue

                request_id = f"{session_id}-{uuid.uuid4().hex[:6]}"
                req = A2ARequest(
                    text=text,
                    request_id=request_id,
                    sender="cli",
                )

                if agent_id:
                    request_topic = topic_request(agent_id)
                else:
                    request_topic = default_request

                props = make_properties(
                    response_topic=reply_t,
                    correlation_data=request_id,
                )
                await client.publish(
                    request_topic,
                    req.to_json(),
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
