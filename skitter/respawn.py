"""Respawn crashed workers on LWT dead events."""

import json
import logging

from skitter.spawn import spawn_worker

log = logging.getLogger("skitter.respawn")


async def handle_dead_event(payload: str) -> None:
    """Re-spawn a crashed worker. Retained dispatch still exists on broker."""
    try:
        data = json.loads(payload)
    except Exception:
        return

    task_id = data.get("task_id", "")
    agent = data.get("agent", "")
    session_id = data.get("session_id", "")

    if not task_id or not agent or not session_id:
        log.warning("Dead event missing task_id, agent, or session_id: %s", data)
        return

    log.warning("Worker dead for task %s — respawning", task_id)
    spawn_worker(agent, session_id, task_id)
