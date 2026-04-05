"""Coordinator — pure A2A orchestrator.

DB-backed session management, dependency resolution, and task dispatch.
Sends A2A requests to agents, collects replies, manages the DAG.
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone

import aiomqtt

from skitter.config import safe_format
from skitter.coordinator.models import (
    SessionState,
    SessionTask,
    build_context,
    compute_ready,
    is_graph_task_terminal,
)
from skitter.coordinator.registry import DiscoveryRegistry
from skitter.db import (
    AsyncDB,
    DB,
    DBSession,
    DBTask,
    open_db,
)
from skitter.discovery import parse_card
from skitter.runtime_api import (
    AGENT_ID as RUNTIME_AGENT_ID,
    CancelSessionResult,
    CreateAppResult,
    DeleteAppResult,
    handle_query as runtime_query,
    coordinator_card,
)
from skitter.a2a import (
    a2a_org,
    a2a_unit,
    A2A_INVALID_PARAMS,
    A2ARequest,
    A2AResponse,
    A2A_RESPONDER_UNAVAILABLE,
    TaskState,
    TaskTarget,
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

log = logging.getLogger("skitter.coordinator")


# --- Coordinator ---


class Coordinator:
    """A2A orchestrator with DB-backed state."""

    _MAX_HISTORY_TURNS = 10  # recent completed sessions to replay
    _MAX_RESULT_CHARS = 2000  # truncate per-turn result in history

    def __init__(self, db: DB) -> None:
        self._adb = AsyncDB(db)
        self._sessions: dict[str, SessionState] = {}  # session_id → state
        self._request_task_index: dict[str, str] = {}  # request_task_id → session_id
        self._registry = DiscoveryRegistry()
        self._client: aiomqtt.Client | None = None
        self._reply_subscriptions: set[str] = set()
        self._app_clients: dict[str, aiomqtt.Client] = {}  # app_id -> dedicated client
        self._app_tasks: dict[str, asyncio.Task] = {}  # app_id -> forwarding task
        # (app_id, context_id) → session_id for cancel-and-replace
        self._context_active: dict[tuple[str, str], str] = {}

    @property
    def registry(self) -> DiscoveryRegistry:
        return self._registry

    def _clear_context_active(self, state: SessionState) -> None:
        """Remove context_active entry if this session is still the active one."""
        key = (state.app_id, state.context_id)
        if key[1] and self._context_active.get(key) == state.session_id:
            del self._context_active[key]

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
            event_topic = topic_a2a_event(RUNTIME_AGENT_ID)
            log.debug("MQTT → %s (event=%s)", event_topic, event_type)
            await self._client.publish(
                event_topic,
                json.dumps(payload),
                qos=1,
            )
        except Exception:
            log.warning(
                "Failed to publish %s event for session %s", event_type, session_id
            )

    # --- Conversation continuity ---

    async def _build_conversation_history(self, app_id: str, context_id: str) -> str:
        """Build a conversation history block from prior completed sessions."""
        if not context_id:
            return ""
        sessions = await self._adb.list_context_sessions(
            app_id, context_id, limit=self._MAX_HISTORY_TURNS
        )
        if not sessions:
            return ""

        turns: list[str] = []
        for sess in sessions:
            user_text = ""
            if sess.request_json:
                try:
                    user_text = A2ARequest.from_json(sess.request_json).text
                except Exception:
                    log.warning("Failed to parse request_json for session %s", sess.id)
            result_text = sess.result or "(no result)"
            if len(result_text) > self._MAX_RESULT_CHARS:
                result_text = result_text[: self._MAX_RESULT_CHARS] + "..."
            n = len(turns) + 1
            turns.append(
                f"### Turn {n}\n**User:** {user_text}\n**Response:** {result_text}"
            )
        return "## Conversation history\n\n" + "\n\n".join(turns)

    # --- Session creation ---

    async def create_session_from_graph(
        self,
        graph_json: str,
        app_version_id: str,
        request: A2ARequest,
        caller_reply_topic: str,
        caller_correlation: str,
        variables: dict[str, str] | None = None,
        app_id: str = "",
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
            variables=variables,
            caller_reply_topic=caller_reply_topic,
            caller_correlation=caller_correlation,
        )
        await self._adb.create_session(db_session)

        # Build in-memory state
        state = SessionState(
            session_id=session_id,
            request_task_id=request_task_id,
            app_version_id=app_version_id,
            app_id=app_id,
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
            terminal = is_graph_task_terminal(t)
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
                needs=needs,
                terminal=terminal,
            )
            await self._adb.create_task(db_task)

        self._sessions[session_id] = state
        self._request_task_index[request_task_id] = session_id
        return state

    # --- Task dispatch ---

    async def dispatch_ready(self, state: SessionState) -> None:
        """Dispatch all ready tasks in a session."""
        ready = compute_ready(state)
        for tid in ready:
            await self._dispatch_task(state, tid)

    async def _dispatch_task(self, state: SessionState, node_id: str) -> None:
        """Send an A2A request for a single task."""
        task = state.graph[node_id]
        target = task.target or TaskTarget(agent=task.agent)

        parts: list[str] = []
        if state.conversation_history:
            parts.append(state.conversation_history)
        context = build_context(state, task)
        if context:
            parts.append(context)
        parts.append(task.description)
        user_request = state.variables.get("user_request", "")
        if user_request:
            parts.append(f"User request: {user_request}")
        prompt = "\n\n".join(parts)

        correlation = uuid.uuid4().hex[:16]
        reply_t = topic_reply("skitter", f"{state.session_id}/{node_id}")

        # Generate the A2A Task.id for the dispatched request
        dispatch_task_id = str(uuid.uuid4())
        task.dispatch_correlation = correlation
        task.dispatch_task_id = dispatch_task_id

        # Write-ahead: persist dispatch info before sending
        db_task_row_id = f"{state.session_id}/{node_id}"
        await self._adb.update_task(
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
            req_json = a2a_req.to_json()
            log.debug("MQTT → %s (%d bytes)", request_topic, len(req_json))
            await self._client.publish(request_topic, req_json, qos=1, properties=props)

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
        from skitter.coordinator.reply_handler import handle_reply

        await handle_reply(self, topic, payload, correlation)

    async def _forward_stream(
        self, state: SessionState, node_id: str, msg_type: str, content: str
    ) -> None:
        from skitter.coordinator.reply_handler import _forward_stream

        await _forward_stream(self, state, node_id, msg_type, content)

    async def _complete_task(
        self, state: SessionState, node_id: str, result: str
    ) -> None:
        from skitter.coordinator.reply_handler import complete_task

        await complete_task(self, state, node_id, result)

    async def _fail_task(self, state: SessionState, node_id: str, error: str) -> None:
        from skitter.coordinator.reply_handler import fail_task

        await fail_task(self, state, node_id, error)

    async def _complete_session(self, state: SessionState) -> None:
        from skitter.coordinator.reply_handler import complete_session

        await complete_session(self, state)

    async def _fail_session(self, state: SessionState, error: str) -> None:
        from skitter.coordinator.reply_handler import fail_session

        await fail_session(self, state, error)
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
        result = await runtime_query(self._adb, req.text, self._registry)

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

        Sends A2A CancelTask to agents with inflight tasks (best-effort).
        """
        state = self._sessions.pop(session_id, None)
        if not state:
            return
        self._request_task_index.pop(state.request_task_id, None)
        self._clear_context_active(state)
        now = datetime.now(timezone.utc).isoformat()

        # Send CancelTask to agents with inflight tasks
        cancel_reply_t = topic_reply(RUNTIME_AGENT_ID, f"cancel-{session_id[:8]}")
        for tid in list(state.inflight):
            task_def = state.graph.get(tid)
            cancel_id = task_def.dispatch_task_id if task_def else ""
            if cancel_id and self._client:
                correlation = uuid.uuid4().hex[:16]
                cancel_msg = json.dumps(
                    {
                        "jsonrpc": "2.0",
                        "id": f"cancel-{cancel_id}",
                        "method": "CancelTask",
                        "params": {"id": cancel_id},
                    }
                )
                props = make_properties(
                    response_topic=cancel_reply_t,
                    correlation_data=correlation,
                )
                try:
                    await self._client.publish(
                        topic_request(task_def.agent),
                        cancel_msg,
                        qos=1,
                        properties=props,
                    )
                    log.info(
                        "Sent CancelTask for %s/%s -> %s",
                        session_id,
                        tid,
                        task_def.agent,
                    )
                except Exception:
                    log.warning("Failed to send CancelTask for %s/%s", session_id, tid)

        for tid in state.pending | state.inflight:
            await self._adb.update_task(
                f"{session_id}/{tid}", state=TaskState.CANCELED, completed_at=now
            )
        if state.caller_reply_topic and self._client:
            event = make_status_event(
                request_id=state.caller_correlation,
                task_id=state.request_task_id,
                state=TaskState.CANCELED,
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
                identifier=f"{a2a_org()}/{a2a_unit()}/{app_id}",
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
                log.debug("MQTT ← %s [app=%s] (%d bytes)", topic, app_id, len(payload))
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
                state=TaskState.SUBMITTED,
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
        db_session = await self._adb.get_session_by_request_task_id(req.task_id)
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

        app = await self._adb.get_app(agent_id)
        if not app or not app.card_json:
            await self._send_error(
                caller_reply_topic,
                caller_correlation,
                f"Unknown app: {agent_id}",
                code=A2A_RESPONDER_UNAVAILABLE,
            )
            return

        version = await self._adb.get_current_version(agent_id)
        if not version:
            await self._send_error(
                caller_reply_topic,
                caller_correlation,
                f"App '{agent_id}' has no published version",
                code=A2A_RESPONDER_UNAVAILABLE,
            )
            return

        # Cancel-and-replace: if a session is already running for this
        # (app_id, context_id), cancel it before starting the new one.
        if incoming_ctx:
            ctx_key = (agent_id, incoming_ctx)
            prev_sid = self._context_active.get(ctx_key)
            if prev_sid and prev_sid in self._sessions:
                log.info(
                    "Canceling session %s (superseded by new request for %s/%s)",
                    prev_sid,
                    agent_id,
                    incoming_ctx,
                )
                await self._adb.update_session_state(prev_sid, TaskState.CANCELED)
                await self._cancel_session_cleanup(prev_sid)

        history = await self._build_conversation_history(agent_id, incoming_ctx)
        state = await self.create_session_from_graph(
            graph_json=version.graph_json,
            app_version_id=version.id,
            request=req,
            caller_reply_topic=caller_reply_topic,
            caller_correlation=caller_correlation,
            variables=req.variables,
            app_id=agent_id,
        )
        state.conversation_history = history
        if incoming_ctx:
            self._context_active[(agent_id, incoming_ctx)] = state.session_id
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
            log.debug("MQTT → %s (artifact, %d bytes)", reply_topic, len(artifact))
            await self._client.publish(reply_topic, artifact, qos=1, properties=props)
        event = make_status_event(
            request_id=correlation,
            task_id=task_id,
            state=TaskState.COMPLETED,
            context_id=context_id,
        )
        log.debug("MQTT → %s (completed)", reply_topic)
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
        reply_state = (
            TaskState.WORKING
            if db_session.state == "running"
            else TaskState(db_session.state)
        )
        props = make_properties(correlation_data=correlation)
        wire_task_id = db_session.request_task_id

        # Replay artifact/error content for terminal sessions
        error_msg = ""
        if db_session.state == "completed":
            tasks = sorted(
                await self._adb.list_tasks(db_session.id), key=lambda t: t.node_id
            )
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
            tasks = sorted(
                await self._adb.list_tasks(db_session.id), key=lambda t: t.node_id
            )
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
        from skitter.coordinator.recovery import recover

        await recover(self)

    async def _timeout_inflight(
        self, state: SessionState, node_id: str, timeout: float
    ) -> None:
        from skitter.coordinator.recovery import timeout_inflight

        await timeout_inflight(self, state, node_id, timeout)

    # --- Coordinator lock ---

    async def _check_coordinator_lock(self) -> None:
        """Fail fast if another coordinator is already running on this broker."""
        async with aiomqtt.Client(
            **mqtt_client_kwargs(
                identifier=f"{a2a_org()}/{a2a_unit()}/{RUNTIME_AGENT_ID}-lock-check",
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
                    identifier=f"{a2a_org()}/{a2a_unit()}/{RUNTIME_AGENT_ID}",
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
                rt_card_json = json.dumps(coordinator_card())
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
                    log.debug("MQTT ← %s (%d bytes)", topic, len(payload))

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
                        identifier=f"{a2a_org()}/{a2a_unit()}/{RUNTIME_AGENT_ID}-cleanup",
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
