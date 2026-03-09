"""Fly Machines API client for creating ephemeral worker machines."""

import json
import logging
import os
from urllib.error import HTTPError
from urllib.request import Request, urlopen

log = logging.getLogger("skitter.fly")

FLY_API_TOKEN = os.environ.get("FLY_API_TOKEN", "")
FLY_API_HOST = os.environ.get("FLY_API_HOST", "https://api.machines.dev")
FLY_APP = os.environ.get("FLY_APP", "skitter")
FLY_WORKER_IMAGE = os.environ.get("FLY_WORKER_IMAGE", "registry.fly.io/skitter:latest")
FLY_REGION = os.environ.get("FLY_REGION", "iad")


def create_machine(
    app: str,
    image: str,
    env: dict[str, str],
    region: str = "",
    guest: dict | None = None,
    cmd: list[str] | None = None,
) -> dict:
    """Create an ephemeral Fly Machine (auto_destroy, restart once on failure)."""
    if not FLY_API_TOKEN:
        raise RuntimeError("FLY_API_TOKEN not configured")

    config: dict = {
        "image": image,
        "auto_destroy": True,
        "restart": {"policy": "on-failure", "max_retries": 1},
        "env": env,
        "guest": guest
        if guest is not None
        else {"cpu_kind": "shared", "cpus": 1, "memory_mb": 512},
    }
    if cmd:
        config["init"] = {"cmd": cmd}

    body: dict = {"config": config, "region": region or FLY_REGION}

    url = f"{FLY_API_HOST.rstrip('/')}/v1/apps/{app}/machines"
    req = Request(
        url,
        data=json.dumps(body).encode(),
        headers={
            "Content-Type": "application/json",
            "Authorization": f"Bearer {FLY_API_TOKEN}",
        },
        method="POST",
    )

    try:
        with urlopen(req, timeout=30) as resp:
            result = json.loads(resp.read())
            log.info("Created machine %s in %s", result.get("id", "?"), app)
            return result
    except HTTPError as e:
        body = e.read().decode(errors="replace")[:500]
        log.error("Fly create_machine failed: %s %s", e.code, body)
        raise


def create_worker(agent: str, session_id: str, task_id: str) -> dict:
    """Create an ephemeral worker machine for a single task."""
    return create_machine(
        app=FLY_APP,
        image=FLY_WORKER_IMAGE,
        env={
            "AGENT_NAME": agent,
            "SESSION_ID": session_id,
            "TASK_ID": task_id,
        },
    )
