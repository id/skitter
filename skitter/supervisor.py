"""Supervisor — pure A2A orchestrator.

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

from skitter.config import (
    WorkflowDef,
    load_workflows,
    safe_format,
)
from skitter.db import (
    App,
    AppVersion,
    DBSession,
    DBTask,
    SqliteDB,
    open_db,
)
from skitter.discovery import CardPublisher, build_card, is_workflow_card, parse_card
from skitter.runtime_api import (
    AGENT_ID as RUNTIME_AGENT_ID,
    CANCEL_KEY,
    handle_query as runtime_query,
    runtime_card,
)
from skitter.mqtt import (
    A2A_ORG,
    A2A_UNIT,
    get_correlation_data,
    get_response_topic,
    make_properties,
    mqtt_client_kwargs,
    topic_discovery,
    topic_discovery_wildcard,
    topic_request,
    topic_request_wildcard,
    topic_reply,
    topic_result,
)
from skitter.types import (
    A2ARequest,
    A2AResponse,
    A2A_RESPONDER_UNAVAILABLE,
    A2A_TRANSPORT_PROTOCOL_ERROR,
    TaskTarget,
    classify_reply,
    make_status_event,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.supervisor")


# --- In-memory session state for dependency resolution ---


@dataclass
class SessionTask:
    """Per-task state within a session."""

    task_id: str
    agent: str
    description: str
    needs: list[str] = field(default_factory=list)
    next: str = ""
    target: TaskTarget | None = None


@dataclass
class SessionState:
    """In-memory state for an active session."""

    session_id: str
    app_version_id: str
    caller_reply_topic: str = ""
    caller_correlation: str = ""
    graph: dict[str, SessionTask] = field(default_factory=dict)
    results: dict[str, str] = field(default_factory=dict)
    pending: set[str] = field(default_factory=set)
    inflight: set[str] = field(default_factory=set)
    failed: set[str] = field(default_factory=set)


def _compute_ready(state: SessionState) -> list[str]:
    """Return task_ids that are pending and have all needs satisfied."""
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
    """Mark all transitively dependent tasks as failed. Returns newly failed task_ids."""
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
            parts.append(f"## Result from '{need_id}':\n{state.results[need_id]}")
    return "\n\n".join(parts)


def _find_terminal_tasks(state: SessionState) -> list[str]:
    """Find tasks whose next is empty or 'output'."""
    return [
        tid
        for tid, task in state.graph.items()
        if not task.next or task.next == "output"
    ]


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


# --- Target resolution ---


def resolve_target(agent_id: str, registry: DiscoveryRegistry) -> TaskTarget:
    """Resolve an agent reference to a TaskTarget using the discovery registry."""
    card = registry.get(agent_id)
    if card:
        # For now, all discovered agents are on the local broker
        return TaskTarget(agent=agent_id)
    # Agent not in registry — assume local broker, will fail at dispatch time
    return TaskTarget(agent=agent_id)


# --- Supervisor ---


class Supervisor:
    """Pure A2A orchestrator with DB-backed state."""

    def __init__(self, db: SqliteDB) -> None:
        self._db = db
        self._sessions: dict[str, SessionState] = {}
        self._registry = DiscoveryRegistry()
        self._publishers: dict[str, CardPublisher] = {}
        self._client: aiomqtt.Client | None = None
        self._reply_subscriptions: set[str] = set()

    @property
    def registry(self) -> DiscoveryRegistry:
        return self._registry

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
        session_id = uuid.uuid4().hex[:16]
        variables = variables or {}

        graph = json.loads(graph_json)
        tasks = graph.get("tasks", [])

        # Create DB session
        db_session = DBSession(
            id=session_id,
            app_version_id=app_version_id,
            request_json=request.to_json(),
            variables=json.dumps(variables),
            caller_reply_topic=caller_reply_topic,
            caller_correlation=caller_correlation,
        )
        self._db.create_session(db_session)

        # Build in-memory state
        state = SessionState(
            session_id=session_id,
            app_version_id=app_version_id,
            caller_reply_topic=caller_reply_topic,
            caller_correlation=caller_correlation,
        )

        for t in tasks:
            tid = t["id"]
            description = safe_format(t.get("description", ""), variables)
            needs = t.get("needs", [])
            agent = t.get("agent", "")
            target = resolve_target(agent, self._registry)

            state.graph[tid] = SessionTask(
                task_id=tid,
                agent=agent,
                description=description,
                needs=needs,
                next=t.get("next", ""),
                target=target,
            )
            state.pending.add(tid)

            # Create DB task
            db_task = DBTask(
                id=f"{session_id}/{tid}",
                session_id=session_id,
                task_id=tid,
                agent=agent,
                description=description,
                needs=json.dumps(needs),
                next=t.get("next", ""),
                target_json=json.dumps(
                    {"agent": target.agent, "mqtt_host": target.mqtt_host}
                ),
            )
            self._db.create_task(db_task)

        self._sessions[session_id] = state
        return state

    # --- Task dispatch ---

    async def dispatch_ready(self, state: SessionState) -> None:
        """Dispatch all ready tasks in a session."""
        ready = _compute_ready(state)
        for tid in ready:
            await self._dispatch_task(state, tid)

    async def _dispatch_task(self, state: SessionState, task_id: str) -> None:
        """Send an A2A request for a single task."""
        task = state.graph[task_id]
        target = task.target or TaskTarget(agent=task.agent)

        context = _build_context(state, task)
        prompt = task.description
        if context:
            prompt = f"{context}\n\n{prompt}"

        request_id = uuid.uuid4().hex[:16]
        reply_t = topic_reply("supervisor", f"{state.session_id}/{task_id}")

        # Write-ahead: persist dispatch info before sending
        db_task_id = f"{state.session_id}/{task_id}"
        self._db.update_task(
            db_task_id,
            request_id=request_id,
            reply_topic=reply_t,
            dispatched_at=datetime.now(timezone.utc).isoformat(),
            state="running",
        )

        state.pending.discard(task_id)
        state.inflight.add(task_id)

        # Subscribe to reply topic
        if self._client and reply_t not in self._reply_subscriptions:
            await self._client.subscribe(reply_t, qos=1)
            self._reply_subscriptions.add(reply_t)

        # Build and send A2A request
        a2a_req = A2ARequest(
            text=prompt,
            request_id=request_id,
            sender="supervisor",
        )
        request_topic = topic_request(target.agent)
        props = make_properties(
            response_topic=reply_t,
            correlation_data=request_id,
        )
        if self._client:
            await self._client.publish(
                request_topic, a2a_req.to_json(), qos=1, properties=props
            )

        log.info(
            "Dispatched task %s/%s → %s (req=%s)",
            state.session_id,
            task_id,
            target.agent,
            request_id,
        )

    # --- Reply handling ---

    async def handle_reply(self, topic: str, payload: str) -> None:
        """Process an A2A reply from an agent."""
        # Parse topic: $a2a/v1/reply/{org}/{unit}/supervisor/{session_id}/{task_id}
        parts = topic.split("/")
        if len(parts) < 7:
            return
        # reply topic format: $a2a/v1/reply/{org}/{unit}/supervisor/{sid}/{tid}
        session_id = parts[-2]
        task_id = parts[-1]

        state = self._sessions.get(session_id)
        if not state:
            return

        try:
            data = json.loads(payload)
        except Exception:
            return

        kind, content = classify_reply(data)

        if kind == "terminal":
            await self._complete_task(state, task_id, content)
        elif kind == "error":
            await self._fail_task(state, task_id, content)
        elif kind in ("text", "tool_use"):
            # Forward streaming updates to caller
            await self._forward_stream(state, task_id, kind, content)

    async def _forward_stream(
        self, state: SessionState, task_id: str, msg_type: str, content: str
    ) -> None:
        """Forward streaming updates from agents to the session's caller."""
        if not state.caller_reply_topic or not self._client:
            return
        event = make_status_event(
            request_id=state.caller_correlation,
            task_id=state.session_id,
            state="working",
            message=content,
            message_type=msg_type,
            task_name=task_id,
        )
        props = make_properties(correlation_data=state.caller_correlation)
        await self._client.publish(
            state.caller_reply_topic, event, qos=0, properties=props
        )

    async def _complete_task(
        self, state: SessionState, task_id: str, result: str
    ) -> None:
        """Handle successful task completion."""
        state.results[task_id] = result
        state.inflight.discard(task_id)

        # Update DB
        db_task_id = f"{state.session_id}/{task_id}"
        self._db.update_task(
            db_task_id,
            state="completed",
            result=result,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )

        # Publish retained result for observability
        task = state.graph.get(task_id)
        if task and self._client:
            await self._client.publish(
                topic_result(state.app_version_id, task_id, state.session_id),
                json.dumps(
                    {
                        "task": task_id,
                        "session_id": state.session_id,
                        "result": result,
                    }
                ),
                qos=1,
                retain=True,
            )

        log.info("Task %s/%s completed", state.session_id, task_id)

        # Check if session is complete
        if not state.inflight and not state.pending:
            await self._complete_session(state)
        else:
            # Dispatch newly ready tasks
            await self.dispatch_ready(state)

    async def _fail_task(self, state: SessionState, task_id: str, error: str) -> None:
        """Handle task failure and propagate."""
        state.inflight.discard(task_id)
        state.failed.add(task_id)

        # Update DB
        db_task_id = f"{state.session_id}/{task_id}"
        self._db.update_task(
            db_task_id,
            state="failed",
            error=error,
            completed_at=datetime.now(timezone.utc).isoformat(),
        )

        # Publish retained failed result for observability
        if self._client:
            await self._client.publish(
                topic_result(state.app_version_id, task_id, state.session_id),
                json.dumps(
                    {
                        "task": task_id,
                        "session_id": state.session_id,
                        "result": error,
                        "failed": True,
                    }
                ),
                qos=1,
                retain=True,
            )

        # Propagate failure to downstream tasks
        newly_failed = _propagate_failure(state, task_id)
        for ftid in newly_failed:
            cascade_error = f"Skipped: upstream task '{task_id}' failed"
            self._db.update_task(
                f"{state.session_id}/{ftid}",
                state="failed",
                error=cascade_error,
                completed_at=datetime.now(timezone.utc).isoformat(),
            )

        log.error(
            "Task %s/%s failed: %s (cascaded to %d tasks)",
            state.session_id,
            task_id,
            error[:100],
            len(newly_failed),
        )

        # Check if session is done (all inflight finished)
        if not state.inflight:
            await self._fail_session(state, error)

    async def _complete_session(self, state: SessionState) -> None:
        """Finalize a completed session — send result to caller."""
        self._db.update_session_state(state.session_id, "completed")

        # Find terminal task results
        terminal_tids = _find_terminal_tasks(state)
        result_parts = []
        for tid in terminal_tids:
            if tid in state.results:
                result_parts.append(state.results[tid])

        result_text = "\n\n".join(result_parts) if result_parts else "(no result)"

        if state.caller_reply_topic and self._client:
            event = make_status_event(
                request_id=state.caller_correlation,
                task_id=state.session_id,
                state="completed",
                artifact_text=result_text,
            )
            props = make_properties(correlation_data=state.caller_correlation)
            await self._client.publish(
                state.caller_reply_topic, event, qos=1, properties=props
            )

        del self._sessions[state.session_id]
        log.info("Session %s completed", state.session_id)

    async def _fail_session(self, state: SessionState, error: str) -> None:
        """Finalize a failed session."""
        self._db.update_session_state(state.session_id, "failed")

        if state.caller_reply_topic and self._client:
            event = make_status_event(
                request_id=state.caller_correlation,
                task_id=state.session_id,
                state="failed",
                message=error,
            )
            props = make_properties(correlation_data=state.caller_correlation)
            await self._client.publish(
                state.caller_reply_topic, event, qos=1, properties=props
            )

        del self._sessions[state.session_id]
        log.info("Session %s failed", state.session_id)

    # --- Runtime API ---

    async def _handle_runtime_query(
        self,
        payload: str,
        reply_topic: str,
        correlation: str,
    ) -> None:
        """Handle a runtime state query (list apps, get session, etc.)."""
        try:
            req = A2ARequest.from_json(payload)
        except Exception as e:
            log.error("Bad runtime query JSON: %s", e)
            return

        result_json = runtime_query(self._db, req.text)

        # For cancel: clean up in-memory state and notify original caller
        try:
            result = json.loads(result_json)
            if CANCEL_KEY in result:
                await self._cancel_session_cleanup(result[CANCEL_KEY])
        except Exception:
            log.exception("Cancel cleanup failed")

        # Reply to the query caller
        if reply_topic and self._client:
            event = make_status_event(
                request_id=correlation,
                task_id=req.request_id,
                state="completed",
                artifact_text=result_json,
            )
            props = make_properties(correlation_data=correlation)
            await self._client.publish(reply_topic, event, qos=1, properties=props)

    async def _cancel_session_cleanup(self, session_id: str) -> None:
        """Clean up in-memory state after a session is cancelled in the DB."""
        state = self._sessions.pop(session_id, None)
        if not state:
            return
        now = datetime.now(timezone.utc).isoformat()
        for tid in state.pending | state.inflight:
            self._db.update_task(
                f"{session_id}/{tid}", state="cancelled", completed_at=now
            )
        if state.caller_reply_topic and self._client:
            event = make_status_event(
                request_id=state.caller_correlation,
                task_id=session_id,
                state="cancelled",
                message="Session cancelled via runtime API",
            )
            props = make_properties(correlation_data=state.caller_correlation)
            await self._client.publish(
                state.caller_reply_topic, event, qos=1, properties=props
            )

    # --- Inbound request handling ---

    async def handle_request(
        self,
        payload: str,
        caller_reply_topic: str,
        caller_correlation: str,
        agent_id: str,
    ) -> None:
        """Handle an inbound A2A request."""
        try:
            req = A2ARequest.from_json(payload)
        except Exception as e:
            log.error("Bad inbound JSON-RPC: %s", e)
            return

        # Check if this is a composed app (has a published card)
        app = self._db.get_app(agent_id)
        if app and app.card_json:
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

            # Send initial ack
            if caller_reply_topic:
                ack = make_status_event(
                    request_id=caller_correlation,
                    task_id=state.session_id,
                    state="submitted",
                )
                props = make_properties(correlation_data=caller_correlation)
                if self._client:
                    await self._client.publish(
                        caller_reply_topic, ack, qos=1, properties=props
                    )

            await self.dispatch_ready(state)
            log.info(
                "App '%s' session %s started (%d tasks)",
                agent_id,
                state.session_id,
                len(state.graph),
            )
            return

        # Check if agent exists in discovery registry
        if not self._registry.get(agent_id):
            # Try loading from workflows for backward compat
            workflows = load_workflows()
            if agent_id in workflows:
                wf = workflows[agent_id]
                graph = self._workflow_to_graph(wf, req.variables)
                # Create a temporary app version
                av_id = f"wf-{agent_id}-{uuid.uuid4().hex[:8]}"
                av = AppVersion(
                    id=av_id,
                    app_id=agent_id,
                    version=0,
                    graph_json=json.dumps(graph),
                )
                # Ensure app exists
                if not self._db.get_app(agent_id):
                    self._db.create_app(
                        App(id=agent_id, name=wf.name, description=wf.description)
                    )
                self._db.create_app_version(av)

                state = self.create_session_from_graph(
                    graph_json=av.graph_json,
                    app_version_id=av_id,
                    request=req,
                    caller_reply_topic=caller_reply_topic,
                    caller_correlation=caller_correlation,
                    variables=req.variables,
                )

                if caller_reply_topic:
                    ack = make_status_event(
                        request_id=caller_correlation,
                        task_id=state.session_id,
                        state="submitted",
                    )
                    props = make_properties(correlation_data=caller_correlation)
                    if self._client:
                        await self._client.publish(
                            caller_reply_topic, ack, qos=1, properties=props
                        )

                await self.dispatch_ready(state)
                log.info(
                    "Workflow '%s' session %s started (%d tasks)",
                    agent_id,
                    state.session_id,
                    len(state.graph),
                )
                return

            await self._send_error(
                caller_reply_topic,
                caller_correlation,
                f"Unknown agent or app: {agent_id}",
                code=A2A_RESPONDER_UNAVAILABLE,
            )
            return

        # Single agent — create a single-task session
        graph = {
            "tasks": [
                {
                    "id": agent_id,
                    "agent": agent_id,
                    "description": req.text,
                    "needs": [],
                    "next": "output",
                }
            ]
        }
        av_id = f"agent-{agent_id}-{uuid.uuid4().hex[:8]}"
        if not self._db.get_app(agent_id):
            self._db.create_app(App(id=agent_id, name=agent_id))
        av = AppVersion(
            id=av_id,
            app_id=agent_id,
            version=0,
            graph_json=json.dumps(graph),
        )
        self._db.create_app_version(av)

        state = self.create_session_from_graph(
            graph_json=av.graph_json,
            app_version_id=av_id,
            request=req,
            caller_reply_topic=caller_reply_topic,
            caller_correlation=caller_correlation,
        )

        if caller_reply_topic:
            ack = make_status_event(
                request_id=caller_correlation,
                task_id=state.session_id,
                state="submitted",
            )
            props = make_properties(correlation_data=caller_correlation)
            if self._client:
                await self._client.publish(
                    caller_reply_topic, ack, qos=1, properties=props
                )

        await self.dispatch_ready(state)
        log.info(
            "Agent '%s' session %s started",
            agent_id,
            state.session_id,
        )

    def _workflow_to_graph(
        self, wf: WorkflowDef, variables: dict[str, str] | None = None
    ) -> dict:
        """Convert a WorkflowDef to graph JSON."""
        variables = variables or {}
        tasks = []
        for t in wf.tasks:
            task_dict: dict = {
                "id": t.id,
                "agent": t.agent,
                "description": safe_format(t.description, variables),
                "needs": list(t.needs),
                "next": t.next,
            }
            if t.model:
                task_dict["model"] = t.model
            tasks.append(task_dict)
        graph: dict = {"tasks": tasks}
        if wf.workspace:
            graph["workspace"] = wf.workspace
        if wf.variables:
            graph["variables"] = wf.variables
        return graph

    async def _send_error(
        self,
        reply_topic: str,
        correlation: str,
        message: str,
        code: int = -32602,
    ) -> None:
        if reply_topic and self._client:
            resp = A2AResponse(
                id=correlation or "",
                error={"code": code, "message": message},
            )
            props = make_properties(correlation_data=correlation)
            await self._client.publish(
                reply_topic, resp.to_json(), qos=1, properties=props
            )

    # --- Discovery subscription ---

    def handle_discovery(self, topic: str, payload: bytes) -> None:
        """Process a discovery card update from the broker."""
        agent_id = topic.split("/")[-1]
        if agent_id in ("supervisor", RUNTIME_AGENT_ID):
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
        # 1. Republish composed app cards
        for app in self._db.list_apps():
            if app.card_json:
                pub = CardPublisher(app.id, app.card_json)
                self._publishers[app.id] = pub
                await pub.start()
                log.info("Republished card for app %s", app.id)

        # 2. Rehydrate inflight sessions
        for db_session in self._db.list_sessions():
            if db_session.state != "running":
                continue

            tasks = self._db.list_tasks(db_session.id)
            if not tasks:
                continue

            state = SessionState(
                session_id=db_session.id,
                app_version_id=db_session.app_version_id,
                caller_reply_topic=db_session.caller_reply_topic,
                caller_correlation=db_session.caller_correlation,
            )

            for t in tasks:
                needs = json.loads(t.needs) if t.needs else []
                state.graph[t.task_id] = SessionTask(
                    task_id=t.task_id,
                    agent=t.agent,
                    description=t.description,
                    needs=needs,
                    next=t.next,
                    target=resolve_target(t.agent, self._registry),
                )

                if t.state == "completed":
                    state.results[t.task_id] = t.result
                elif t.state == "failed":
                    state.failed.add(t.task_id)
                elif t.state == "running" and t.dispatched_at:
                    state.inflight.add(t.task_id)
                    # Resubscribe to reply topic
                    if t.reply_topic and self._client:
                        if t.reply_topic not in self._reply_subscriptions:
                            await self._client.subscribe(t.reply_topic, qos=1)
                            self._reply_subscriptions.add(t.reply_topic)
                else:
                    state.pending.add(t.task_id)

            self._sessions[state.session_id] = state

            # Dispatch any newly ready tasks
            await self.dispatch_ready(state)

            # Schedule timeout for recovered inflight tasks — if no reply
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
        self, state: SessionState, task_id: str, timeout: float
    ) -> None:
        """Fail a recovered inflight task if no reply arrives within timeout."""
        await asyncio.sleep(timeout)
        if state.session_id in self._sessions and task_id in state.inflight:
            log.warning(
                "Recovered task %s/%s timed out after %.0fs — failing",
                state.session_id,
                task_id,
                timeout,
            )
            await self._fail_task(
                state,
                task_id,
                f"Task timed out during recovery (no reply within {timeout:.0f}s)",
            )

    # --- Workflow card publishing ---

    async def publish_workflow_cards(self) -> None:
        """Publish discovery cards for configured workflows."""
        from skitter.config import AgentDef

        workflows = load_workflows()
        for wf in workflows.values():
            wf_agent = AgentDef(id=wf.id, name=wf.name, description=wf.description)
            card_json = json.dumps(build_card(wf_agent, workflow=wf))
            if wf.id not in self._publishers:
                pub = CardPublisher(wf.id, card_json)
                self._publishers[wf.id] = pub
                await pub.start()

    async def cleanup_stale_cards(self) -> None:
        """Remove retained discovery cards for workflows no longer in config."""
        active_ids = set(self._publishers.keys())
        try:
            async with aiomqtt.Client(
                **mqtt_client_kwargs(
                    identifier=f"{A2A_ORG}/{A2A_UNIT}/supervisor-cleanup"
                ),
            ) as client:
                discovered: set[str] = set()
                await client.subscribe(topic_discovery_wildcard(), qos=1)
                try:
                    async with asyncio.timeout(3.0):
                        async for msg in client.messages:
                            card_id = str(msg.topic).split("/")[-1]
                            if msg.payload:
                                discovered.add(card_id)
                except TimeoutError:
                    pass

                for card_id in discovered:
                    if card_id not in active_ids and card_id != "supervisor":
                        # Only clean up workflow cards (not standalone agents)
                        try:
                            card = parse_card(msg.payload)
                            if is_workflow_card(card):
                                await client.publish(
                                    topic_discovery(card_id),
                                    b"",
                                    qos=1,
                                    retain=True,
                                )
                                log.info("Cleared stale workflow card: %s", card_id)
                        except Exception:
                            pass
        except Exception:
            log.warning("Failed to clean up stale discovery cards")

    # --- Main loop ---

    async def run(self) -> None:
        """Main supervisor loop."""
        # Publish workflow cards + runtime API card
        await self.publish_workflow_cards()
        rt_card_json = json.dumps(runtime_card())
        pub = CardPublisher(RUNTIME_AGENT_ID, rt_card_json)
        self._publishers[RUNTIME_AGENT_ID] = pub
        await pub.start()

        # Clean up stale cards
        await self.cleanup_stale_cards()

        try:
            async with aiomqtt.Client(
                **mqtt_client_kwargs(identifier=f"{A2A_ORG}/{A2A_UNIT}/supervisor"),
            ) as client:
                self._client = client

                # Subscribe to discovery, requests
                await client.subscribe(topic_discovery_wildcard(), qos=1)
                await client.subscribe(topic_request_wildcard(), qos=1)
                log.info("Supervisor ready, listening on discovery and requests")

                # Recover inflight sessions from DB
                await self.recover()

                async for mqtt_msg in client.messages:
                    topic = str(mqtt_msg.topic)
                    payload_bytes = mqtt_msg.payload
                    payload = payload_bytes.decode() if payload_bytes else ""

                    if "/discovery/" in topic:
                        self.handle_discovery(topic, payload_bytes or b"")

                    elif "/request/" in topic and "/cancel" not in topic:
                        agent_id = _parse_agent_id_from_topic(topic)
                        if agent_id == "supervisor":
                            continue

                        caller_reply = get_response_topic(mqtt_msg) or ""
                        caller_corr = get_correlation_data(mqtt_msg) or ""

                        if not caller_reply or not caller_corr:
                            log.warning(
                                "Request missing Response Topic or Correlation Data"
                            )
                            if caller_reply:
                                await self._send_error(
                                    caller_reply,
                                    caller_corr,
                                    "Request must include MQTT v5 Response Topic "
                                    "and Correlation Data",
                                    code=A2A_TRANSPORT_PROTOCOL_ERROR,
                                )
                            continue

                        if agent_id == RUNTIME_AGENT_ID:
                            await self._handle_runtime_query(
                                payload, caller_reply, caller_corr
                            )
                            continue

                        await self.handle_request(
                            payload, caller_reply, caller_corr, agent_id
                        )

                    elif "/reply/" in topic and "/supervisor/" in topic:
                        if payload:
                            await self.handle_reply(topic, payload)
        finally:
            self._client = None
            for pub in self._publishers.values():
                await pub.stop()

    async def stop(self) -> None:
        """Stop the supervisor and clean up."""
        for pub in self._publishers.values():
            await pub.stop()
        self._db.close()


def _parse_agent_id_from_topic(topic: str) -> str:
    """Extract agent_id from $a2a/v1/request/{org}/{unit}/{agent_id}."""
    parts = topic.split("/")
    return parts[5] if len(parts) >= 6 else ""


# --- Entry point ---


def main() -> None:
    db = open_db()
    supervisor = Supervisor(db)
    try:
        asyncio.run(supervisor.run())
    except KeyboardInterrupt:
        log.info("Supervisor shutting down")
    finally:
        db.close()


if __name__ == "__main__":
    main()
