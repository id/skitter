"""Publish reload signal to supervisor via MQTT."""

import asyncio
import json

import aiomqtt

from skitter.mqtt import MQTT_HOST, MQTT_PORT, topic_reload


async def _reload() -> None:
    async with aiomqtt.Client(
        MQTT_HOST, MQTT_PORT, protocol=aiomqtt.ProtocolVersion.V5
    ) as client:
        await client.publish(
            topic_reload(),
            json.dumps({"action": "reload"}),
            qos=1,
        )
        print("Reload signal sent.")


def main() -> None:
    asyncio.run(_reload())


if __name__ == "__main__":
    main()
