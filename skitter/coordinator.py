"""Coordinator — pure A2A orchestrator.

DB-backed session management, dependency resolution, and task dispatch.
Sends A2A requests to agents, collects replies, manages the DAG.
"""

import asyncio
import json
import logging
import uuid
from dataclasses import dataclass, field
from datetime import datetime, timezone

import aiomqtt

from skitter.config import safe_format
from skitter.db import (
    DB,
    DBSession,
    DBTask,
    open_db,
)
from skitter.discovery import is_workflow_card, parse_card
from skitter.runtime_api import (
    AGENT_ID as RUNTIME_AGENT_ID,
    CancelSessionResult,
    CreateAppResult,
    DeleteAppResult,
    handle_query as runtime_query,
    runtime_card,
)
from skitter.a2a import (
    A2A_ORG,
    A2A_UNIT,
    A2A_INVALID_PARAMS,
    A2ARequest,
    A2AResponse,
    A2A_RESPONDER_UNAVAILABLE,
    REPLY_ARTIFACT,
    REPLY_ERROR,
    REPLY_FAILED,
    REPLY_INPUT_REQUIRED,
    REPLY_TERMINAL,
    REPLY_TEXT,
    REPLY_TOOL,
    TaskTarget,
    classify_reply,
    make_a2a_error,
    make_artifact_event,
    make_status_event,
    topic_a2a_event,
    topic_coordinator_lock,
    topic_discovery,
    topic_discovery_wildcard,
    topic_reply,
    topic_request,
    validate_a2a_request,
)
from skitter.mqtt import (
    make_properties,
    mqtt_client_kwargs,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.coordinator")


# --- In-memory session state for dependency resolution ---


@dataclass
class SessionTask:
    """Per-task state within a session."""

    agent: str
    description: str
    needs: list[str] = field(default_factory=list)
    terminal: bool = False
    target: TaskTarget | None = None
    dispatch_correlation: str = ""  # MQTT Correlation Data sent with dispatch
    dispatch_task_id: str = ""  # A2A Task.id sent to agent; used for tasks/cancel


@dataclass
class SessionState:
    """In-memory state for an active session."""

    session_id: str  # internal; coordinator-generated UUID
    request_task_id: str  # incoming A2A Task.id; used for dedup and wire replies
    app_version_id: str
    context_id: str = ""
    caller_reply_topic: str = ""
    caller_correlation: str = ""
    graph: dict[str, SessionTask] = field(default_factory=dict)
    results: dict[str, str] = field(default_factory=dict)
    pending: set[str] = field(default_factory=set)
    inflight: set[str] = field(default_factory=set)
    failed: set[str] = field(default_factory=set)
    variables: dict[str, str] = field(default_factory=dict)

    @property
    def a2a_state(self) -> str:
        """Derive A2A task state from session progress."""
        if self.pending or self.inflight:
            return "working"
        if self.failed:
            return "failed"
        return "completed"


def _compute_ready(state: SessionState) -> list[str]:
    """Return node_ids that are pending and have all needs satisfied."""
    ready = []
    for tid in list(state.pending):
        task = state.graph[tid]
        if all(n in state.results or n in state.failed for n in task.needs):
            # If any need failed, this task fails too
            if any(n in state.failed for n in task.needs):
                continue
            ready.append(tid)
    return ready


def _propagate_failure(state: SessionState, failed_tid: str) -> list[str]:
    """Mark all transitively dependent tasks as failed. Returns newly failed node_ids."""
    newly_failed = []
    queue = [failed_tid]
    while queue:
        tid = queue.pop(0)
        for other_tid, task in state.graph.items():
            if other_tid in state.failed:
                continue
            if tid in task.needs:
                state.failed.add(other_tid)
                state.pending.discard(other_tid)
                newly_failed.append(other_tid)
                queue.append(other_tid)
    return newly_failed


def _build_context(state: SessionState, task: SessionTask) -> str:
    """Build context string from upstream results for a join task."""
    if not task.needs:
        return ""
    parts = []
    for need_id in task.needs:
        if need_id in state.results:
            parts.append(f"## Input from task '{need_id}'\n{state.results[need_id]}")
    return "\n\n".join(parts)


def _is_graph_task_terminal(t: dict) -> bool:
    """True if the graph task dict has ``terminal: true``."""
    return bool(t.get("terminal", False))


def _find_terminal_tasks(state: SessionState) -> list[str]:
    """Find terminal tasks in a session."""
    return [tid for tid, task in state.graph.items() if task.terminal]


# --- Discovery registry ---


class DiscoveryRegistry:
    """In-memory registry of discovered A2A agent cards."""

    def __init__(self) -> None:
        self._cards: dict[str, dict] = {}  # agent_id → card dict

    def update(self, agent_id: str, card: dict) -> None:
        self._cards[agent_id] = card
        log.info("Registry: updated card for %s", agent_id)

    def remove(self, agent_id: str) -> None:
        if agent_id in self._cards:
            del self._cards[agent_id]
            log.info("Registry: removed card for %s", agent_id)

    def get(self, agent_id: str) -> dict | None:
        return self._cards.get(agent_id)

    def list_agents(self) -> list[str]:
        return [aid for aid, card in self._cards.items() if not is_workflow_card(card)]

    def list_apps(self) -> list[str]:
        return [aid for aid, card in self._cards.items() if is_workflow_card(card)]


# --- Coordinator ---


class Coordinator:
    """A2A orchestrator with DB-backed state."""

    def __init__(self, db: DB) -> None:
        self._db = db
        self._sessions: dict[str, SessionState] = {}  # session_id → state
        self._request_task_index: dict[str, str] = {}  # request_task_id → session_id
        self._registry = DiscoveryRegistry()
        self._client: aiomqtt.Client | None = None
        self._reply_subscriptions: set[str] = set()
        self._app_clients: dict[str, aiomqtt.Client] = {}  # app_id -> dedicated client
        self._app_tasks: dict[str, asyncio.Task] = {}  # app_id -> forwarding task

    @property
    def registry(self) -> DiscoveryRegistry:
        return self._registry

    # --- Session events ---

    async def _publish_event(
        self,
        event_type: str,
        session_id: str,
        task_id: str = "",
        data: dict | None = None,
    ) -> None:
        """Publish a session lifecycle event on the A2A event topic.

        Best-effort: failures are logged but never propagate to callers,
        so ACL denials or transient disconnects cannot stall orchestration.
        """
        if not self._client:
            return
        payload = {
            "event": event_type,
            "session_id": session_id,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }
        if task_id:
            payload["task_id"] = task_id
        if data:
            payload["data"] = data
        try:
            await self._client.publish(
                topic_a2a_event(RUNTIME_AGENT_ID),
                json.dumps(payload),
                qos=1,
            )
        except Exception:
            log.warning(
                "Failed to publish %s event for session %s", event_type, session_id
            )

    # --- Session creation ---

    def create_session_from_graph(
        self,
        graph_json: str,
        app_version_id: str,
        request: A2ARequest,
        caller_reply_topic: str,
        caller_correlation: str,
        variables: dict[str, str] | None = None,
    ) -> SessionState:
        """Create a new session from an orchestration graph."""
        session_id = str(uuid.uuid4())
        request_task_id = request.task_id
        variables = variables or {}
        variables.setdefault("user_request", request.text or "")

        graph = json.loads(graph_json)
        tasks = graph.get("tasks", [])

        # Create DB session
        db_session = DBSession(
            id=session_id,
            app_version_id=app_version_id,
            request_task_id=request_task_id,
            context_id=request.context_id or "",
            request_json=request.to_json(),
            variables=json.dumps(variables),
            caller_reply_topic=caller_reply_topic,
            caller_correlation=caller_correlation,
        )
        self._db.create_session(db_session)

        # Build in-memory state
        state = SessionState(
            session_id=session_id,
            request_task_id=request_task_id,
            app_version_id=app_version_id,
            context_id=request.context_id or "",
            caller_reply_topic=caller_reply_topic,
            caller_correlation=caller_correlation,
            variables=variables,
        )

        for t in tasks:
            tid = t["id"]
            description = safe_format(t.get("description", ""), variables)
            needs = t.get("needs", [])
            agent = t.get("agent", "")
            terminal = _is_graph_task_terminal(t)
            target = TaskTarget(agent=agent)

            state.graph[tid] = SessionTask(
                agent=agent,
                description=description,
                needs=needs,
                terminal=terminal,
                target=target,
            )
            state.pending.add(tid)

            # Create DB task
            db_task = DBTask(
                id=f"{session_id}/{tid}",
                session_id=session_id,
                node_id=tid,
                agent=agent,
                description=description,
                needs=json.dumps(needs),
                terminal="1" if terminal else "",
                target_json=json.dumps(
                    {"agent": target.agent, "mqtt_host": target.mqtt_host}
                ),
            )
            self._db.create_task(db_task)

        self._sessions[session_id] = state
        self._request_task_index[request_task_id] = session_id
        return state

    # --- Task dispatch ---

    async def dispatch_ready(self, state: SessionState) -> None:
        """Dispatch all ready tasks in a session."""
        ready = _compute_ready(state)
        for tid in ready:
            await self._dispatch_task(state, tid)

    async def _dispatch_task(self, state: SessionState, node_id: str) -> None:
        """Send an A2A request for a single task."""
        task = state.graph[node_id]
        target = task.target or TaskTarget(agent=task.agent)

        context = _build_context(state, task)
        prompt = task.description
        if context:
            prompt = f"{context}\n\n{prompt}"

        user_request = state.variables.get("user_request", "")
        if user_request:
            prompt = f"{prompt}\n\nUser request: {user_request}"

        correlation = uuid.uuid4().hex[:16]
        reply_t = topic_reply("skitter", f"{state.session_id}/{node_id}")

        # Generate the A2A Task.id for the dispatched request
        dispatch_task_id = str(uuid.uuid4())
        task.dispatch_correlation = correlation
        task.dispatch_task_id = dispatch_task_id

        # Write-ahead: persist dispatch info before sending
        db_task_row_id = f"{state.session_id}/{node_id}"
        self._db.update_task(
            db_task_row_id,
            dispatch_task_id=dispatch_task_id,
            reply_topic=reply_t,
            dispatched_at=datetime.now(timezone.utc).isoformat(),
            state="running",
        )

        state.pending.discard(node_id)
        state.inflight.add(node_id)

        # Subscribe to reply topic
        if self._client and reply_t not in self._reply_subscriptions:
            await self._client.subscribe(reply_t, qos=1)
            self._reply_subscriptions.add(reply_t)
        a2a_req = A2ARequest(
            text=prompt,
            request_id=correlation,
            task_id=dispatch_task_id,
            context_id=state.context_id,
            sender="skitter",
        )
        request_topic = topic_request(target.agent)
        props = make_properties(
            response_topic=reply_t,
            correlation_data=correlation,
        )
        if self._client:
            await self._client.publish(
                request_topic, a2a_req.to_json(), qos=1, properties=props
            )

        log.info(
            "Dispatched task %s/%s → %s (req=%s)",
            state.session_id,
            node_id,
            target.agent,
            correlation,
        )
        await self._publish_event("task_started", state.session_id, task_id=node_id)

    # --- Reply handling ---

    async def handle_reply(
        self, topic: str, payload: str, correlation: str = ""
    ) -> None:
        """Process an A2A reply from an agent."""
        # Parse topic: $a2a/v1/reply/{org}/{unit}/skitter/{session_id}/{node_id}
        parts = topic.split("/")
        if len(parts) < 7:
            return
        session_id = parts[-2]
        node_id = parts[-1]

        state = self._sessions.get(session_id)
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

        if kind == REPLY_ARTIFACT:
            # Accumulate artifact content; terminal status follows separately
            state.results[node_id] = content
        elif kind == REPLY_TERMINAL:
            # Prefer artifact content (preceding REPLY_ARTIFACT) over status message
            result = state.results.get(node_id, "") or content
            await self._complete_task(state, node_id, result)
        elif kind == REPLY_INPUT_REQUIRED:
            # Interrupted state: multi-turn not yet supported for sub-agents
            await self._fail_task(state, node_id, f"Agent requires input: {content}")
        elif kind in (REPLY_FAILED, REPLY_ERROR):
            await self._fail_task(state, node_id, content)
        elif kind in (REPLY_TEXT, REPLY_TOOL):
            # Forward streaming updates to caller
            await self._forward_stream(state, node_id, kind, content)

    async def _forward_stream(
        self, state: SessionState, node_id: str, msg_type: str, content: str
    ) -> None:
        """Forward streaming updates from agents to the session's caller."""
        if not state.caller_reply_topic or not self._client:
            return
        event = make_status_event(
            request_id=state.caller_correlation,
            task_id=state.request_task_id,
            state="working",
            message=content,
            context_id=state.context_id,
            metadata={"type": msg_type, "task_name": node_id},
        )
        props = make_properties(correlation_data=state.caller_correlation)
        await self._client.publish(
            state.caller_reply_topic, event, qos=1, properties=props
        )

    async def _complete_task(
        self, state: SessionState, node_id: str, result: str
    ) -> None:
        """Handle successful task completion."""
        state.results[node_id] = result
        state.inflight.discard(node_id)

        # Update DB
        db_task_row_id = f"{state.session_id}/{node_id}"
        self._db.update_task(
            db_task_row_id,
            state="completed",
            result=result,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )

        log.info("Task %s/%s completed", state.session_id, node_id)
        await self._publish_event("task_completed", state.session_id, task_id=node_id)

        # Check if session is complete
        if not state.inflight and not state.pending:
            await self._complete_session(state)
        else:
            # Dispatch newly ready tasks
            await self.dispatch_ready(state)

    async def _fail_task(self, state: SessionState, node_id: str, error: str) -> None:
        """Handle task failure and propagate."""
        state.inflight.discard(node_id)
        state.failed.add(node_id)

        # Update DB
        db_task_row_id = f"{state.session_id}/{node_id}"
        self._db.update_task(
            db_task_row_id,
            state="failed",
            error=error,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )

        # Propagate failure to downstream tasks
        newly_failed = _propagate_failure(state, node_id)
        for ftid in newly_failed:
            cascade_error = f"Skipped: upstream task '{node_id}' failed"
            self._db.update_task(
                f"{state.session_id}/{ftid}",
                state="failed",
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
        await self._publish_event(
            "task_failed",
            state.session_id,
            task_id=node_id,
            data={"error": error[:200]},
        )

        # Check if session is done (all inflight finished)
        if not state.inflight:
            await self._fail_session(state, error)

    async def _complete_session(self, state: SessionState) -> None:
        """Finalize a completed session; send result to caller."""
        if state.session_id not in self._sessions:
            return  # already finalized (race with timeout/failure)
        self._db.update_session_state(state.session_id, "completed")

        # Find terminal task results
        terminal_tids = _find_terminal_tasks(state)
        result_parts = []
        for tid in terminal_tids:
            if tid in state.results:
                result_parts.append(state.results[tid])

        result_text = "\n\n".join(result_parts) if result_parts else "(no result)"

        await self._publish_completed(
            state.caller_reply_topic,
            state.caller_correlation,
            state.request_task_id,
            state.context_id,
            artifact_text=result_text,
        )

        await self._publish_event("session_completed", state.session_id)
        self._sessions.pop(state.session_id, None)
        self._request_task_index.pop(state.request_task_id, None)
        log.info("Session %s completed", state.session_id)

    async def _fail_session(self, state: SessionState, error: str) -> None:
        """Finalize a failed session."""
        if state.session_id not in self._sessions:
            return  # already finalized (race with timeout/failure)
        self._db.update_session_state(state.session_id, "failed")

        if state.caller_reply_topic and self._client:
            event = make_status_event(
                request_id=state.caller_correlation,
                task_id=state.request_task_id,
                state="failed",
                message=error,
                context_id=state.context_id,
            )
            props = make_properties(correlation_data=state.caller_correlation)
            await self._client.publish(
                state.caller_reply_topic, event, qos=1, properties=props
            )

        await self._publish_event(
            "session_failed",
            state.session_id,
            data={"error": error[:200]},
        )
        self._sessions.pop(state.session_id, None)
        self._request_task_index.pop(state.request_task_id, None)
        log.info("Session %s failed", state.session_id)

    # --- Runtime API ---

    async def _handle_runtime_query(
        self,
        req: A2ARequest,
        reply_topic: str,
        correlation: str,
    ) -> None:
        """Handle a runtime state query (list apps, get session, etc.).

        Caller must pass a validated A2ARequest (v5 props and Task.id already checked).
        """
        result = await runtime_query(self._db, req.text, self._registry)

        try:
            if isinstance(result, CancelSessionResult):
                await self._cancel_session_cleanup(result.session_id)
            elif isinstance(result, CreateAppResult):
                await self._start_app_connection(result.app_id, result.card_json)
            elif isinstance(result, DeleteAppResult):
                await self._delete_app_cleanup(result.app_id)
        except Exception:
            log.exception("Runtime query post-action failed")

        await self._publish_completed(
            reply_topic,
            correlation,
            req.task_id,
            req.context_id or "",
            artifact_text=json.dumps(result.to_dict()),
        )

    async def _cancel_session_cleanup(self, session_id: str) -> None:
        """Clean up in-memory state after a session is canceled in the DB.

        Sends A2A tasks/cancel to agents with inflight tasks (best-effort).
        """
        state = self._sessions.pop(session_id, None)
        if not state:
            return
        self._request_task_index.pop(state.request_task_id, None)
        now = datetime.now(timezone.utc).isoformat()

        # Send tasks/cancel to agents with inflight tasks
        for tid in list(state.inflight):
            task_def = state.graph.get(tid)
            cancel_id = task_def.dispatch_task_id if task_def else ""
            if cancel_id and self._client:
                cancel_msg = json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": f"cancel-{cancel_id}",
                        "method": "tasks/cancel",
                        "params": {"id": cancel_id},
                    }
                )
                try:
                    await self._client.publish(
                        topic_request(task_def.agent), cancel_msg, qos=1
                    )
                    log.info(
                        "Sent tasks/cancel for %s/%s → %s",
                        session_id,
                        tid,
                        task_def.agent,
                    )
                except Exception:
                    log.warning(
                        "Failed to send tasks/cancel for %s/%s", session_id, tid
                    )

        for tid in state.pending | state.inflight:
            self._db.update_task(
                f"{session_id}/{tid}", state="canceled", completed_at=now
            )
        if state.caller_reply_topic and self._client:
            event = make_status_event(
                request_id=state.caller_correlation,
                task_id=state.request_task_id,
                state="canceled",
                message="Session canceled via runtime API",
                context_id=state.context_id,
            )
            props = make_properties(correlation_data=state.caller_correlation)
            await self._client.publish(
                state.caller_reply_topic, event, qos=1, properties=props
            )

    async def _delete_app_cleanup(self, app_id: str) -> None:
        """Clear retained discovery card and tear down the app's MQTT connection."""
        client = self._app_clients.get(app_id)
        if client:
            try:
                await client.publish(topic_discovery(app_id), b"", qos=1, retain=True)
            except Exception:
                log.warning("Failed to clear discovery card for %s", app_id)
        await self._stop_app_connection(app_id)
        self._registry.remove(app_id)

    async def _start_app_connection(self, app_id: str, card_json: str) -> None:
        """Open a dedicated MQTT connection for an app.

        Publishes the retained discovery card and subscribes to the app's
        request topic. A background task forwards incoming requests to
        handle_request. The client ID matches the card's agent_id so the
        broker accepts the card publish per EIP-0033.
        """
        # Tear down any existing connection for this app (idempotent on recovery)
        await self._stop_app_connection(app_id)

        client = aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{A2A_ORG}/{A2A_UNIT}/{app_id}",
            ),
        )
        await client.__aenter__()
        self._app_clients[app_id] = client

        await client.publish(topic_discovery(app_id), card_json, qos=1, retain=True)
        await client.subscribe(topic_request(app_id), qos=1)

        task = asyncio.create_task(self._app_message_loop(app_id, client))
        self._app_tasks[app_id] = task
        log.info("App %s: dedicated connection started", app_id)

    async def _app_message_loop(self, app_id: str, client: aiomqtt.Client) -> None:
        """Forward requests arriving on an app's dedicated connection."""
        try:
            async for mqtt_msg in client.messages:
                topic = str(mqtt_msg.topic)
                payload_bytes = mqtt_msg.payload
                payload = payload_bytes.decode() if payload_bytes else ""
                if not payload or "/request/" not in topic or "/cancel" in topic:
                    continue

                validated = await validate_a2a_request(mqtt_msg, client, log=log)
                if not validated:
                    continue
                req, caller_reply, caller_corr = validated

                await self.handle_request(req, caller_reply, caller_corr, app_id)
        except asyncio.CancelledError:
            return
        except Exception:
            log.exception("App %s: message loop crashed", app_id)

    async def _stop_app_connection(self, app_id: str) -> None:
        """Tear down an app's dedicated MQTT connection."""
        task = self._app_tasks.pop(app_id, None)
        if task:
            task.cancel()
            try:
                await task
            except (asyncio.CancelledError, Exception):
                pass
        client = self._app_clients.pop(app_id, None)
        if client:
            try:
                await client.__aexit__(None, None, None)
            except Exception:
                pass

    # --- Inbound request handling ---

    async def _start_session(self, state: SessionState, label: str) -> None:
        """Send submitted ack, publish session_created event, dispatch ready tasks."""
        if state.caller_reply_topic and self._client:
            ack = make_status_event(
                request_id=state.caller_correlation,
                task_id=state.request_task_id,
                state="submitted",
                context_id=state.context_id,
            )
            props = make_properties(correlation_data=state.caller_correlation)
            await self._client.publish(
                state.caller_reply_topic, ack, qos=1, properties=props
            )
        await self._publish_event("session_created", state.session_id)
        await self.dispatch_ready(state)
        log.info(
            "%s session %s started (%d tasks)",
            label,
            state.session_id,
            len(state.graph),
        )

    async def handle_request(
        self,
        req: A2ARequest,
        caller_reply_topic: str,
        caller_correlation: str,
        agent_id: str,
    ) -> None:
        """Handle an inbound A2A request for a composed app.

        Caller must pass a validated A2ARequest (v5 props and Task.id already checked).
        """
        # Deduplication: if a session with this Task.id exists, reply with
        # current state (A2A-over-MQTT spec: MUST return existing task state)
        incoming_ctx = req.context_id or ""
        existing_session_id = self._request_task_index.get(req.task_id)
        existing = (
            self._sessions.get(existing_session_id) if existing_session_id else None
        )
        if existing:
            if await self._reject_context_mismatch(
                existing.context_id,
                incoming_ctx,
                caller_reply_topic,
                caller_correlation,
            ):
                return
            log.info(
                "Duplicate Task.id %s (in-flight), returning current state", req.task_id
            )
            await self._reply_existing_state(
                existing, caller_reply_topic, caller_correlation
            )
            return
        db_session = self._db.get_session_by_request_task_id(req.task_id)
        if db_session:
            if await self._reject_context_mismatch(
                db_session.context_id,
                incoming_ctx,
                caller_reply_topic,
                caller_correlation,
            ):
                return
            log.info(
                "Duplicate Task.id %s (DB: %s), returning stored state",
                req.task_id,
                db_session.state,
            )
            await self._reply_existing_db_state(
                db_session, caller_reply_topic, caller_correlation
            )
            return

        app = self._db.get_app(agent_id)
        if not app or not app.card_json:
            await self._send_error(
                caller_reply_topic,
                caller_correlation,
                f"Unknown app: {agent_id}",
                code=A2A_RESPONDER_UNAVAILABLE,
            )
            return

        version = self._db.get_current_version(agent_id)
        if not version:
            await self._send_error(
                caller_reply_topic,
                caller_correlation,
                f"App '{agent_id}' has no published version",
                code=A2A_RESPONDER_UNAVAILABLE,
            )
            return

        state = self.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic=caller_reply_topic,
            caller_correlation=caller_correlation,
            variables=req.variables,
        )
        await self._start_session(state, f"App '{agent_id}'")

    async def _publish_completed(
        self,
        reply_topic: str,
        correlation: str,
        task_id: str,
        context_id: str,
        artifact_text: str = "",
    ) -> None:
        """Publish artifact event (if any) followed by completed status event."""
        if not reply_topic or not self._client:
            return
        props = make_properties(correlation_data=correlation)
        if artifact_text:
            artifact = make_artifact_event(
                request_id=correlation,
                task_id=task_id,
                artifact_text=artifact_text,
                context_id=context_id,
            )
            await self._client.publish(reply_topic, artifact, qos=1, properties=props)
        event = make_status_event(
            request_id=correlation,
            task_id=task_id,
            state="completed",
            context_id=context_id,
        )
        await self._client.publish(reply_topic, event, qos=1, properties=props)

    async def _reply_existing_state(
        self,
        state: SessionState,
        reply_topic: str,
        correlation: str,
    ) -> None:
        """Reply with current in-memory session state (for dedup of in-flight sessions)."""
        if not reply_topic or not self._client:
            return
        event = make_status_event(
            request_id=correlation,
            task_id=state.request_task_id,
            state=state.a2a_state,
            context_id=state.context_id,
        )
        props = make_properties(correlation_data=correlation)
        await self._client.publish(reply_topic, event, qos=1, properties=props)

    async def _reply_existing_db_state(
        self,
        db_session: DBSession,
        reply_topic: str,
        correlation: str,
    ) -> None:
        """Replay stored session state for dedup.

        Replays the artifact (for completed) or error message (for failed)
        so retrying requesters can recover the original output. The
        "running" -> "working" mapping covers sessions not yet rehydrated
        after a restart.
        """
        if not reply_topic or not self._client:
            return
        reply_state = "working" if db_session.state == "running" else db_session.state
        props = make_properties(correlation_data=correlation)
        wire_task_id = db_session.request_task_id

        # Replay artifact/error content for terminal sessions
        error_msg = ""
        if db_session.state == "completed":
            tasks = sorted(self._db.list_tasks(db_session.id), key=lambda t: t.node_id)
            results = [t.result for t in tasks if t.terminal and t.result]
            artifact_text = "\n\n".join(results) if results else ""
            if artifact_text:
                artifact = make_artifact_event(
                    request_id=correlation,
                    task_id=wire_task_id,
                    artifact_text=artifact_text,
                    context_id=db_session.context_id,
                )
                await self._client.publish(
                    reply_topic, artifact, qos=1, properties=props
                )
        elif db_session.state in ("failed", "canceled"):
            tasks = sorted(self._db.list_tasks(db_session.id), key=lambda t: t.node_id)
            errors = [t.error for t in tasks if t.error]
            error_msg = "; ".join(errors) if errors else ""

        event = make_status_event(
            request_id=correlation,
            task_id=wire_task_id,
            state=reply_state,
            message=error_msg,
            context_id=db_session.context_id,
        )
        await self._client.publish(reply_topic, event, qos=1, properties=props)

    async def _reject_context_mismatch(
        self,
        stored_ctx: str,
        incoming_ctx: str,
        reply_topic: str,
        correlation: str,
    ) -> bool:
        """Return True (and send error) if context_id mismatches on dedup.

        Empty context_id on either side is allowed (untracked context).
        """
        if stored_ctx and incoming_ctx and incoming_ctx != stored_ctx:
            await self._send_error(
                reply_topic,
                correlation,
                "context_id mismatch: incoming context_id differs "
                "from stored value for this Task.id",
                code=A2A_INVALID_PARAMS,
            )
            return True
        return False

    async def _send_error(
        self,
        reply_topic: str,
        correlation: str,
        message: str,
        code: int = A2A_INVALID_PARAMS,
    ) -> None:
        if reply_topic and self._client:
            resp = A2AResponse(
                id=correlation or "",
                error=make_a2a_error(code, message),
            )
            props = make_properties(correlation_data=correlation)
            await self._client.publish(
                reply_topic, resp.to_json(), qos=1, properties=props
            )

    # --- Discovery subscription ---

    def handle_discovery(self, topic: str, payload: bytes) -> None:
        """Process a discovery card update from the broker."""
        agent_id = topic.split("/")[-1]
        if agent_id == RUNTIME_AGENT_ID:
            return
        if not payload:
            self._registry.remove(agent_id)
            return
        try:
            card = parse_card(payload)
            self._registry.update(agent_id, card)
        except Exception:
            log.warning("Failed to parse discovery card for %s", agent_id)

    # --- Startup recovery ---

    async def recover(self) -> None:
        """Recover state from DB on startup."""
        # 1. Start dedicated connections for composed apps (card + request topic)
        for app in self._db.list_apps():
            if app.card_json:
                await self._start_app_connection(app.id, app.card_json)

        # 2. Rehydrate inflight sessions
        for db_session in self._db.list_sessions():
            if db_session.state != "running":
                continue

            tasks = self._db.list_tasks(db_session.id)
            if not tasks:
                continue

            state = SessionState(
                session_id=db_session.id,
                request_task_id=db_session.request_task_id,
                app_version_id=db_session.app_version_id,
                context_id=db_session.context_id,
                caller_reply_topic=db_session.caller_reply_topic,
                caller_correlation=db_session.caller_correlation,
                variables=json.loads(db_session.variables)
                if db_session.variables
                else {},
            )

            for t in tasks:
                needs = json.loads(t.needs) if t.needs else []
                # dispatch_correlation is not persisted; recovered tasks
                # skip correlation validation (bounded by 120s timeout)
                state.graph[t.node_id] = SessionTask(
                    agent=t.agent,
                    description=t.description,
                    needs=needs,
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
                    if t.reply_topic and self._client:
                        if t.reply_topic not in self._reply_subscriptions:
                            await self._client.subscribe(t.reply_topic, qos=1)
                            self._reply_subscriptions.add(t.reply_topic)
                else:
                    state.pending.add(t.node_id)

            self._sessions[state.session_id] = state
            self._request_task_index[state.request_task_id] = state.session_id

            # Dispatch any newly ready tasks
            await self.dispatch_ready(state)

            # Schedule timeout for recovered inflight tasks; if no reply
            # arrives within the timeout, the task is assumed lost.
            if state.inflight:
                for tid in list(state.inflight):
                    asyncio.create_task(
                        self._timeout_inflight(state, tid, timeout=120.0)
                    )

            log.info(
                "Recovered session %s (%d tasks, %d inflight, %d pending)",
                state.session_id,
                len(state.graph),
                len(state.inflight),
                len(state.pending),
            )

    async def _timeout_inflight(
        self, state: SessionState, node_id: str, timeout: float
    ) -> None:
        """Fail a recovered inflight task if no reply arrives within timeout."""
        await asyncio.sleep(timeout)
        if state.session_id in self._sessions and node_id in state.inflight:
            log.warning(
                "Recovered task %s/%s timed out after %.0fs; failing",
                state.session_id,
                node_id,
                timeout,
            )
            await self._fail_task(
                state,
                node_id,
                f"Task timed out during recovery (no reply within {timeout:.0f}s)",
            )

    # --- Coordinator lock ---

    async def _check_coordinator_lock(self) -> None:
        """Fail fast if another coordinator is already running on this broker."""
        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{A2A_ORG}/{A2A_UNIT}/{RUNTIME_AGENT_ID}-lock-check",
            ),
        ) as client:
            await client.subscribe(topic_coordinator_lock(), qos=1)
            try:
                async with asyncio.timeout(2.0):
                    async for msg in client.messages:
                        if msg.payload:
                            raise SystemExit(
                                "Another coordinator is already running on this broker. "
                                "Only one coordinator per org/unit is supported."
                            )
                        break
            except TimeoutError:
                pass  # No retained lock message; safe to proceed

    # --- Main loop ---

    async def run(self) -> None:
        """Main coordinator loop."""
        await self._check_coordinator_lock()

        instance_id = uuid.uuid4().hex[:8]
        lwt = aiomqtt.Will(
            topic=topic_coordinator_lock(), payload=b"", qos=1, retain=True
        )

        try:
            async with aiomqtt.Client(
                **mqtt_client_kwargs(
                    identifier=f"{A2A_ORG}/{A2A_UNIT}/{RUNTIME_AGENT_ID}",
                    will=lwt,
                ),
            ) as client:
                self._client = client

                # Publish coordinator lock (retained; LWT clears it on crash)
                await client.publish(
                    topic_coordinator_lock(), instance_id, qos=1, retain=True
                )

                # Subscribe to discovery + runtime API
                await client.subscribe(topic_discovery_wildcard(), qos=1)
                await client.subscribe(topic_request(RUNTIME_AGENT_ID), qos=1)

                # Publish runtime API card (retained)
                rt_card_json = json.dumps(runtime_card())
                await client.publish(
                    topic_discovery(RUNTIME_AGENT_ID),
                    rt_card_json,
                    qos=1,
                    retain=True,
                )

                # Best-effort LLM connectivity check; warn but don't block
                # (only create-app needs the LLM, not runtime queries or recovery)
                try:
                    from skitter.llm import check as llm_check

                    async with asyncio.timeout(10):
                        await llm_check()
                except Exception as exc:
                    log.warning("LLM check failed (create-app will not work): %s", exc)

                # Recover apps (subscribe + republish cards) and inflight sessions
                await self.recover()
                log.info("Coordinator ready (lock=%s)", instance_id)

                async for mqtt_msg in client.messages:
                    topic = str(mqtt_msg.topic)
                    payload_bytes = mqtt_msg.payload
                    payload = payload_bytes.decode() if payload_bytes else ""

                    if "/discovery/" in topic:
                        self.handle_discovery(topic, payload_bytes or b"")

                    elif "/request/" in topic and "/cancel" not in topic:
                        # Main connection only handles runtime API requests;
                        # app requests arrive on dedicated per-app connections.
                        validated = await validate_a2a_request(
                            mqtt_msg, client, log=log
                        )
                        if not validated:
                            continue
                        req, caller_reply, caller_corr = validated

                        await self._handle_runtime_query(req, caller_reply, caller_corr)

                    elif "/reply/" in topic and "/skitter/" in topic:
                        if payload:
                            corr_bytes = getattr(
                                mqtt_msg.properties, "CorrelationData", None
                            )
                            corr = corr_bytes.decode() if corr_bytes else ""
                            await self.handle_reply(topic, payload, corr)
        finally:
            # Tear down per-app connections
            for app_id in list(self._app_tasks):
                await self._stop_app_connection(app_id)

            # Clear coordinator lock on clean shutdown
            try:
                async with aiomqtt.Client(
                    **mqtt_client_kwargs(
                        identifier=f"{A2A_ORG}/{A2A_UNIT}/{RUNTIME_AGENT_ID}-cleanup",
                    ),
                ) as client:
                    await client.publish(
                        topic_coordinator_lock(), b"", qos=1, retain=True
                    )
            except Exception:
                pass  # Best-effort; LWT handles crash case
            self._client = None

    async def stop(self) -> None:
        """Stop the coordinator and clean up."""
        for app_id in list(self._app_tasks):
            await self._stop_app_connection(app_id)
        self._db.close()


def _parse_agent_id_from_topic(topic: str) -> str:
    """Extract agent_id from $a2a/v1/request/{org}/{unit}/{agent_id}."""
    parts = topic.split("/")
    return parts[5] if len(parts) >= 6 else ""


# --- Entry point ---


def main() -> None:
    db = open_db()
    coord = Coordinator(db)
    try:
        asyncio.run(coord.run())
    except KeyboardInterrupt:
        log.info("Coordinator shutting down")
    finally:
        db.close()


if __name__ == "__main__":
    main()
