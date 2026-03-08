"""Publish reload signal to supervisor via MQTT."""

import asyncio
import json

import aiomqtt

from skitter.mqtt import mqtt_client_kwargs, topic_reload


async def _reload() -> None:
    async with aiomqtt.Client(**mqtt_client_kwargs()) as client:
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
