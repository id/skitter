"""Reply handling for the coordinator.

Standalone async functions extracted from Coordinator to keep service.py focused
on wiring and state. Each function receives the coordinator instance as its first
argument.
"""

from __future__ import annotations

import json
import logging
from datetime import datetime, timezone
from typing import TYPE_CHECKING

from skitter.a2a import (
    REPLY_ARTIFACT,
    REPLY_ERROR,
    REPLY_FAILED,
    REPLY_INPUT_REQUIRED,
    REPLY_TERMINAL,
    REPLY_TEXT,
    REPLY_TOOL,
    TaskState,
    classify_reply,
    make_status_event,
)
from skitter.coordinator.models import (
    SessionState,
    find_terminal_tasks,
    propagate_failure,
)
from skitter.mqtt import make_properties

if TYPE_CHECKING:
    from skitter.coordinator.service import Coordinator

log = logging.getLogger("skitter.coordinator")


async def handle_reply(
    coord: Coordinator, topic: str, payload: str, correlation: str = ""
) -> None:
    """Process an A2A reply from an agent."""
    # Parse topic: $a2a/v1/reply/{org}/{unit}/skitter/{session_id}/{node_id}
    parts = topic.split("/")
    if len(parts) < 7:
        return
    session_id = parts[-2]
    node_id = parts[-1]

    state = coord._sessions.get(session_id)
    if not state:
        return

    # Validate MQTT Correlation Data if we have an expected value
    task = state.graph.get(node_id)
    expected = task.dispatch_correlation if task else ""
    if expected and correlation != expected:
        log.warning(
            "Dropping reply for %s/%s: correlation mismatch", session_id, node_id
        )
        return

    try:
        data = json.loads(payload)
    except Exception:
        return

    kind, content = classify_reply(data)
    log.debug("Reply %s/%s: kind=%s content=%.120s", session_id, node_id, kind, content)

    if kind == REPLY_ARTIFACT:
        # Accumulate artifact content; terminal status follows separately
        state.results[node_id] = content
    elif kind == REPLY_TERMINAL:
        # Prefer artifact content (preceding REPLY_ARTIFACT) over status message
        result = state.results.get(node_id, "") or content
        await complete_task(coord, state, node_id, result)
    elif kind == REPLY_INPUT_REQUIRED:
        # Interrupted state: multi-turn not yet supported for sub-agents
        await fail_task(coord, state, node_id, f"Agent requires input: {content}")
    elif kind in (REPLY_FAILED, REPLY_ERROR):
        await fail_task(coord, state, node_id, content)
    elif kind in (REPLY_TEXT, REPLY_TOOL):
        # Forward streaming updates to caller
        await _forward_stream(coord, state, node_id, kind, content)


async def _forward_stream(
    coord: Coordinator,
    state: SessionState,
    node_id: str,
    msg_type: str,
    content: str,
) -> None:
    """Forward streaming updates from agents to the session's caller."""
    if not state.caller_reply_topic or not coord._client:
        return
    event = make_status_event(
        request_id=state.caller_correlation,
        task_id=state.request_task_id,
        state=TaskState.WORKING,
        message=content,
        context_id=state.context_id,
        metadata={"type": msg_type, "task_name": node_id},
    )
    props = make_properties(correlation_data=state.caller_correlation)
    log.debug("MQTT → %s (stream forward)", state.caller_reply_topic)
    await coord._client.publish(
        state.caller_reply_topic, event, qos=1, properties=props
    )


async def complete_task(
    coord: Coordinator, state: SessionState, node_id: str, result: str
) -> None:
    """Handle successful task completion."""
    state.results[node_id] = result
    state.inflight.discard(node_id)

    # Update DB
    db_task_row_id = f"{state.session_id}/{node_id}"
    await coord._adb.update_task(
        db_task_row_id,
        state=TaskState.COMPLETED,
        result=result,
        completed_at=datetime.now(timezone.utc).isoformat(),
    )

    log.info("Task %s/%s completed", state.session_id, node_id)
    await coord._publish_event("task_completed", state.session_id, task_id=node_id)

    # Check if session is complete
    if not state.inflight and not state.pending:
        await complete_session(coord, state)
    else:
        # Dispatch newly ready tasks
        await coord.dispatch_ready(state)


async def fail_task(
    coord: Coordinator, state: SessionState, node_id: str, error: str
) -> None:
    """Handle task failure and propagate."""
    state.inflight.discard(node_id)
    state.failed.add(node_id)

    # Update DB
    db_task_row_id = f"{state.session_id}/{node_id}"
    await coord._adb.update_task(
        db_task_row_id,
        state=TaskState.FAILED,
        error=error,
        completed_at=datetime.now(timezone.utc).isoformat(),
    )

    # Propagate failure to downstream tasks
    newly_failed = propagate_failure(state, node_id)
    for ftid in newly_failed:
        cascade_error = f"Skipped: upstream task '{node_id}' failed"
        await coord._adb.update_task(
            f"{state.session_id}/{ftid}",
            state=TaskState.FAILED,
            error=cascade_error,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )

    log.error(
        "Task %s/%s failed: %s (cascaded to %d tasks)",
        state.session_id,
        node_id,
        error[:100],
        len(newly_failed),
    )
    await coord._publish_event(
        "task_failed",
        state.session_id,
        task_id=node_id,
        data={"error": error[:200]},
    )

    # Check if session is done (all inflight finished)
    if not state.inflight:
        await fail_session(coord, state, error)


async def complete_session(coord: Coordinator, state: SessionState) -> None:
    """Finalize a completed session; send result to caller."""
    if state.session_id not in coord._sessions:
        return  # already finalized (race with timeout/failure)

    # Find terminal task results
    terminal_tids = find_terminal_tasks(state)
    result_parts = []
    for tid in terminal_tids:
        if tid in state.results:
            result_parts.append(state.results[tid])

    result_text = "\n\n".join(result_parts) if result_parts else "(no result)"

    # Persist result on session for conversation continuity
    await coord._adb.update_session_state(
        state.session_id, TaskState.COMPLETED, result=result_text
    )

    await coord._publish_completed(
        state.caller_reply_topic,
        state.caller_correlation,
        state.request_task_id,
        state.context_id,
        artifact_text=result_text,
    )

    await coord._publish_event("session_completed", state.session_id)
    coord._sessions.pop(state.session_id, None)
    coord._request_task_index.pop(state.request_task_id, None)
    coord._clear_context_active(state)
    log.info("Session %s completed", state.session_id)


async def fail_session(coord: Coordinator, state: SessionState, error: str) -> None:
    """Finalize a failed session."""
    if state.session_id not in coord._sessions:
        return  # already finalized (race with timeout/failure)
    await coord._adb.update_session_state(state.session_id, TaskState.FAILED)

    if state.caller_reply_topic and coord._client:
        event = make_status_event(
            request_id=state.caller_correlation,
            task_id=state.request_task_id,
            state=TaskState.FAILED,
            message=error,
            context_id=state.context_id,
        )
        props = make_properties(correlation_data=state.caller_correlation)
        await coord._client.publish(
            state.caller_reply_topic, event, qos=1, properties=props
        )

    await coord._publish_event(
        "session_failed",
        state.session_id,
        data={"error": error[:200]},
    )
    coord._sessions.pop(state.session_id, None)
    coord._request_task_index.pop(state.request_task_id, None)
    coord._clear_context_active(state)
    log.info("Session %s failed", state.session_id)
