"""Interactive CLI for chatting with skitter via MQTT."""

import asyncio
import sys
import uuid

import aiomqtt

from skitter.mqtt import MQTT_HOST, MQTT_PORT, TOPIC_INBOUND, TOPIC_OUTBOUND
from skitter.types import InboundMessage, OutboundMessage


async def run_chat(chat_id: str) -> None:
    outbound_topic = TOPIC_OUTBOUND.format(chat_id=chat_id)
    inbound_topic = TOPIC_INBOUND.format(chat_id=chat_id)

    print(f"Connecting to {MQTT_HOST}:{MQTT_PORT}")
    print(f"Chat ID: {chat_id}")
    print("Type or paste a message. /send to send, /drop to discard.")
    print("Ctrl+C to exit.\n")

    async with aiomqtt.Client(MQTT_HOST, MQTT_PORT) as client:
        await client.subscribe(outbound_topic)

        async def listen() -> None:
            async for msg in client.messages:
                try:
                    out = OutboundMessage.from_json(msg.payload.decode())
                    print(f"\r\033[K{out.text}")
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
                msg = InboundMessage(text=text, sender="cli", chat_id=chat_id)
                await client.publish(inbound_topic, msg.to_json())
        except (EOFError, KeyboardInterrupt):
            print("\nBye!")
        finally:
            listener.cancel()


def main() -> None:
    chat_id = f"cli-{uuid.uuid4().hex[:8]}"

    args = sys.argv[2:]  # skip "skitter" and "chat"
    i = 0
    while i < len(args):
        if args[i] == "--chat-id" and i + 1 < len(args):
            chat_id = args[i + 1]
            i += 2
        else:
            print(f"Unknown argument: {args[i]}", file=sys.stderr)
            sys.exit(1)

    asyncio.run(run_chat(chat_id))
