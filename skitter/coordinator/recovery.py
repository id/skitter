"""Startup recovery for the coordinator.

Standalone async functions extracted from Coordinator. Rehydrate DB state on
startup and handle timeouts for recovered inflight tasks.
"""

from __future__ import annotations

import asyncio
import logging
from typing import TYPE_CHECKING

from skitter.a2a import TaskTarget
from skitter.coordinator.models import SessionState, SessionTask
from skitter.coordinator.reply_handler import fail_task

if TYPE_CHECKING:
    from skitter.coordinator.service import Coordinator

log = logging.getLogger("skitter.coordinator")


async def recover(coord: Coordinator) -> None:
    """Recover state from DB on startup."""
    # 1. Start dedicated connections for composed apps (card + request topic)
    for app in await coord._adb.list_apps():
        if app.card_json:
            await coord._start_app_connection(app.id, app.card_json)

    # 2. Rehydrate inflight sessions
    for db_session in await coord._adb.list_sessions():
        if db_session.state != "running":
            continue

        tasks = await coord._adb.list_tasks(db_session.id)
        if not tasks:
            continue

        app_version = await coord._adb.get_app_version(db_session.app_version_id)
        app_id = app_version.app_id if app_version else ""

        state = SessionState(
            session_id=db_session.id,
            request_task_id=db_session.request_task_id,
            app_version_id=db_session.app_version_id,
            app_id=app_id,
            context_id=db_session.context_id,
            caller_reply_topic=db_session.caller_reply_topic,
            caller_correlation=db_session.caller_correlation,
            variables=db_session.variables,
        )

        for t in tasks:
            # dispatch_correlation is not persisted; recovered tasks
            # skip correlation validation (bounded by 120s timeout)
            state.graph[t.node_id] = SessionTask(
                agent=t.agent,
                description=t.description,
                needs=t.needs,
                terminal=bool(t.terminal),
                target=TaskTarget(agent=t.agent),
                dispatch_task_id=t.dispatch_task_id,
            )

            if t.state == "completed":
                state.results[t.node_id] = t.result
            elif t.state == "failed":
                state.failed.add(t.node_id)
            elif t.state == "running" and t.dispatched_at:
                state.inflight.add(t.node_id)
                # Resubscribe to reply topic
                if t.reply_topic and coord._client:
                    if t.reply_topic not in coord._reply_subscriptions:
                        await coord._client.subscribe(t.reply_topic, qos=1)
                        coord._reply_subscriptions.add(t.reply_topic)
            else:
                state.pending.add(t.node_id)

        coord._sessions[state.session_id] = state
        coord._request_task_index[state.request_task_id] = state.session_id

        # Restore cancel-and-replace tracking so a new request with the
        # same (app_id, context_id) supersedes this recovered session.
        if state.context_id and state.app_id:
            coord._context_active[(state.app_id, state.context_id)] = state.session_id

        # Dispatch any newly ready tasks
        await coord.dispatch_ready(state)

        # Schedule timeout for recovered inflight tasks; if no reply
        # arrives within the timeout, the task is assumed lost.
        if state.inflight:
            for tid in list(state.inflight):
                asyncio.create_task(timeout_inflight(coord, state, tid, timeout=120.0))

        log.info(
            "Recovered session %s (%d tasks, %d inflight, %d pending)",
            state.session_id,
            len(state.graph),
            len(state.inflight),
            len(state.pending),
        )


async def timeout_inflight(
    coord: Coordinator, state: SessionState, node_id: str, timeout: float
) -> None:
    """Fail a recovered inflight task if no reply arrives within timeout."""
    await asyncio.sleep(timeout)
    if state.session_id in coord._sessions and node_id in state.inflight:
        log.warning(
            "Recovered task %s/%s timed out after %.0fs; failing",
            state.session_id,
            node_id,
            timeout,
        )
        await fail_task(
            coord,
            state,
            node_id,
            f"Task timed out during recovery (no reply within {timeout:.0f}s)",
        )
