import asyncio
import json
import logging
import os
import subprocess
import sys
import uuid

import aiomqtt

from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    A2A_ORG,
    A2A_UNIT,
    get_correlation_data,
    get_response_topic,
    make_properties,
    topic_chain_result,
    topic_chain_wildcard,
    topic_control_reload,
    topic_discovery,
    topic_event_wildcard,
    topic_reply,
    topic_request,
    topic_state_dispatch,
    topic_state_session,
    topic_state_session_wildcard,
)
from skitter.config import (
    AgentDef,
    PipelineDef,
    agent_def_to_card,
    load_agents,
    load_pipelines,
    safe_format,
)
from skitter.types import (
    A2AResponse,
    AgentMessage,
    InboundMessage,
    Session,
    SessionTask,
    TaskStatusUpdate,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.coordinator")

DEFAULT_MODELS = "haiku:Fast and cheap, good for simple tasks and summaries|sonnet:Balanced, good for research and analysis|opus:Most capable, use for complex reasoning and coding"


def load_models() -> dict[str, str]:
    raw = os.environ.get("SKITTER_MODELS", DEFAULT_MODELS)
    models = {}
    for entry in raw.split("|"):
        entry = entry.strip()
        if ":" in entry:
            name, desc = entry.split(":", 1)
            models[name.strip()] = desc.strip()
    return models


def create_session(
    session_id: str,
    label: str,
    pipeline: PipelineDef | None = None,
    variables: dict[str, str] | None = None,
    agent_id: str = "",
    text: str = "",
    models: dict[str, str] | None = None,
    agents: dict[str, AgentDef] | None = None,
) -> Session:
    """Create a Session with SessionTasks. Handles both pipeline and direct agent calls."""
    models = models or {}
    agents = agents or {}
    variables = variables or {}
    default_model = list(models.keys())[0] if models else ""

    session = Session(
        session_id=session_id,
        pipeline_id=pipeline.id if pipeline else "",
        agent_id=agent_id if not pipeline else "",
        label=label,
        variables=variables,
    )

    if pipeline:
        for pt in pipeline.tasks:
            task_id = uuid.uuid4().hex[:12]
            agent_def = agents.get(pt.agent)

            description = safe_format(pt.description, variables)
            model = pt.model or (agent_def.model if agent_def else "") or default_model
            if models and model not in models:
                log.warning(
                    "[supervisor] Unknown model '%s' for task '%s', falling back to '%s'",
                    model,
                    pt.id,
                    default_model,
                )
                model = default_model

            session.tasks[pt.id] = SessionTask(
                id=pt.id,
                task_id=task_id,
                agent=pt.agent,
                description=description,
                model=model,
                next=pt.next,
                needs=list(pt.needs),
            )
    else:
        # Direct agent call — single-task session
        task_id = uuid.uuid4().hex[:12]
        agent_def = agents.get(agent_id)
        model = (agent_def.model if agent_def else "") or default_model
        if models and model not in models:
            model = default_model

        session.tasks[agent_id] = SessionTask(
            id=agent_id,
            task_id=task_id,
            agent=agent_id,
            description=text,
            model=model,
            next="output",
        )

    return session


# --- Worker spawning ---

WORKER_MODE = os.environ.get("SKITTER_WORKER_MODE", "subprocess")
WORKER_IMAGE = os.environ.get("SKITTER_WORKER_IMAGE", "skitter-worker:latest")
DOCKER_NETWORK = os.environ.get("SKITTER_DOCKER_NETWORK", "skitter")


def spawn_worker(agent: str, session_id: str, task_id: str) -> None:
    if WORKER_MODE == "docker":
        _spawn_worker_docker(agent, session_id, task_id)
    else:
        _spawn_worker_subprocess(agent, session_id, task_id)


def _spawn_worker_subprocess(agent: str, session_id: str, task_id: str) -> None:
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    subprocess.Popen(
        [sys.executable, "-m", "skitter.worker", agent, session_id, task_id],
        env=env,
    )
    log.info("[supervisor] Spawned %s worker subprocess for task %s", agent, task_id)


def _spawn_worker_docker(agent: str, session_id: str, task_id: str) -> None:
    env_args: list[str] = []
    env_args.extend(
        ["-e", f"MQTT_HOST={os.environ.get('SKITTER_DOCKER_MQTT_HOST', 'emqx')}"]
    )
    env_args.extend(["-e", f"MQTT_PORT={os.environ.get('MQTT_PORT', '1883')}"])
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key:
        env_args.extend(["-e", f"ANTHROPIC_API_KEY={api_key}"])
    openai_key = os.environ.get("OPENAI_API_KEY", "")
    if openai_key:
        env_args.extend(["-e", f"OPENAI_API_KEY={openai_key}"])
    subprocess.Popen(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            DOCKER_NETWORK,
            *env_args,
            WORKER_IMAGE,
            agent,
            session_id,
            task_id,
        ],
    )
    log.info("[supervisor] Spawned %s worker container for task %s", agent, task_id)


# --- Session/task helpers ---


def find_task_by_task_id(session: Session, task_id: str) -> SessionTask | None:
    for task in session.tasks.values():
        if task.task_id == task_id:
            return task
    return None


def find_id_by_task_id(session: Session, task_id: str) -> str | None:
    for tid, task in session.tasks.items():
        if task.task_id == task_id:
            return tid
    return None


def get_entry_tasks(session: Session) -> list[SessionTask]:
    """Find tasks with no dependencies (entry points)."""
    return [t for t in session.tasks.values() if not t.needs and t.status == "pending"]


def build_context_for_join(
    join_inputs: dict[str, str],
    needs: list[str],
    session: Session,
) -> str:
    """Build context string from accumulated join inputs."""
    parts = []
    for need_id in needs:
        # Find the task_id for this logical need
        need_task = session.tasks.get(need_id)
        if need_task and need_task.task_id in join_inputs:
            parts.append(
                f"## Result from '{need_id}':\n{join_inputs[need_task.task_id]}"
            )
    return "\n\n".join(parts)


# --- Recovery ---


async def recover_sessions(client: aiomqtt.Client) -> dict[str, Session]:
    """Subscribe to retained session specs and drain them to rebuild state."""
    sessions: dict[str, Session] = {}
    await client.subscribe(topic_state_session_wildcard(), qos=1)
    try:
        async with asyncio.timeout(1.0):
            async for mqtt_msg in client.messages:
                payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                if not payload:
                    continue
                try:
                    session = Session.from_json(payload)
                    sessions[session.session_id] = session
                    log.info(
                        "[supervisor] Recovered session %s (%d tasks)",
                        session.session_id,
                        len(session.tasks),
                    )
                except Exception as e:
                    log.warning("[supervisor] Failed to parse retained session: %s", e)
    except TimeoutError:
        pass
    await client.unsubscribe(topic_state_session_wildcard())
    return sessions


async def recover_chain_results(
    client: aiomqtt.Client,
) -> dict[tuple[str, str], str]:
    """Drain retained chain results. Returns {(session_id, source_task_id): result}."""
    results: dict[tuple[str, str], str] = {}
    await client.subscribe(topic_chain_wildcard(), qos=1)
    try:
        async with asyncio.timeout(1.0):
            async for mqtt_msg in client.messages:
                payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                if not payload:
                    continue
                try:
                    data = json.loads(payload)
                    session_id = data["session_id"]
                    source_task_id = data["task_id"]
                    results[(session_id, source_task_id)] = data["result"]
                except Exception as e:
                    log.warning("[supervisor] Failed to parse chain result: %s", e)
    except TimeoutError:
        pass
    await client.unsubscribe(topic_chain_wildcard())
    return results


def rebuild_task_map(sessions: dict[str, Session]) -> dict[str, str]:
    """Rebuild task_id -> session_id from recovered sessions."""
    task_to_session: dict[str, str] = {}
    for session_id, session in sessions.items():
        for task in session.tasks.values():
            if task.status == "running":
                task_to_session[task.task_id] = session_id
    return task_to_session


# --- Coordinator ---


class Coordinator:
    def __init__(self) -> None:
        self.sessions: dict[str, Session] = {}
        self.task_to_session: dict[str, str] = {}
        self.join_inputs: dict[tuple[str, str], dict[str, str]] = {}
        self.agents: dict[str, AgentDef] = {}
        self.pipelines: dict[str, PipelineDef] = {}
        self.client: aiomqtt.Client | None = None
        self.reply_topic: str = ""

    async def dispatch_task(
        self, session: Session, task_name: str, context: str = ""
    ) -> None:
        """Build AgentMessage and publish as retained dispatch."""
        assert self.client is not None
        st = session.tasks[task_name]
        agent_def = self.agents.get(st.agent)

        # For pipeline calls, get the PipelineTask for per-task overrides
        pt = None
        if session.pipeline_id:
            pipeline = self.pipelines.get(session.pipeline_id)
            if pipeline:
                pt = next((t for t in pipeline.tasks if t.id == task_name), None)

        # Resolve fields from agent_def + pipeline_task override
        soul = (
            pt.soul if pt and pt.soul else agent_def.soul if agent_def else ""
        ) or ""
        skills = (
            pt.skills if pt and pt.skills else agent_def.skills if agent_def else ""
        ) or ""
        max_turns = (
            pt.max_turns
            if pt and pt.max_turns
            else agent_def.max_turns
            if agent_def
            else 10
        )
        runtime = (agent_def.runtime if agent_def else "claude") or "claude"

        # Determine next_needs
        next_needs: list[str] = []
        if session.pipeline_id and st.next and st.next != "output":
            pipeline = self.pipelines.get(session.pipeline_id)
            if pipeline:
                next_pt = next((t for t in pipeline.tasks if t.id == st.next), None)
                if next_pt:
                    next_needs = list(next_pt.needs)

        agent_msg = AgentMessage(
            task_id=st.task_id,
            session_id=session.session_id,
            description=st.description,
            soul=soul,
            skills=skills,
            context=context,
            max_turns=max_turns,
            model=st.model,
            runtime=runtime,
            next=st.next,
            next_needs=next_needs,
            caller_reply_topic=session.caller_reply_topic,
            caller_correlation=session.caller_correlation,
        )
        dispatch_payload = json.dumps(
            {
                "task": json.loads(agent_msg.to_json()),
                "reply_topic": self.reply_topic,
                "correlation": st.task_id,
            }
        )
        await self.client.publish(
            topic_state_dispatch(st.task_id),
            dispatch_payload,
            qos=1,
            retain=True,
        )
        log.info(
            "[supervisor] Dispatched task %s (%s) via retained dispatch",
            st.id,
            st.task_id,
        )

    async def dispatch_and_spawn(
        self, session: Session, task_name: str, context: str = ""
    ) -> None:
        """Dispatch task (retained) and spawn worker."""
        st = session.tasks[task_name]
        st.status = "running"
        self.task_to_session[st.task_id] = session.session_id
        await self.dispatch_task(session, task_name, context)
        spawn_worker(st.agent, session.session_id, st.task_id)

    async def handle_inbound(self, mqtt_msg, payload: str) -> None:
        """Handle inbound request to coordinator."""
        assert self.client is not None
        caller_reply_topic = get_response_topic(mqtt_msg)
        caller_correlation = get_correlation_data(mqtt_msg)

        if not caller_reply_topic or not caller_correlation:
            log.warning(
                "[supervisor] Inbound request missing Response Topic or Correlation Data"
            )
            if caller_reply_topic:
                await self._send_error(
                    caller_reply_topic,
                    caller_correlation,
                    "Request must include MQTT v5 Response Topic and Correlation Data",
                    code=-32005,
                )
            return

        try:
            data = json.loads(payload)
        except Exception as e:
            log.error("[supervisor] Bad inbound JSON: %s", e)
            return

        method = data.get("method", "")

        if method == "tasks/spawn":
            await self._handle_spawn_request(
                data, caller_reply_topic, caller_correlation
            )
            return

        # Standard inbound request
        try:
            msg = InboundMessage.from_json(payload)
        except Exception:
            try:
                msg = InboundMessage(
                    text=data.get("text", ""),
                    sender=data.get("sender", "unknown"),
                    session_id=data.get(
                        "session_id",
                        f"req-{uuid.uuid4().hex[:8]}",
                    ),
                    pipeline_id=data.get("pipeline_id", ""),
                    pipeline_vars=data.get("pipeline_vars", {}),
                    agent_id=data.get("agent_id", ""),
                )
            except Exception as e:
                log.error("[supervisor] Bad inbound message: %s", e)
                return

        log.info(
            "[supervisor] Received message from %s: %.80s",
            msg.session_id,
            msg.text,
        )

        session = None
        if msg.agent_id:
            if msg.agent_id not in self.agents:
                await self._send_error(
                    caller_reply_topic,
                    caller_correlation,
                    f"Unknown agent: {msg.agent_id}",
                )
                return

            models = load_models()
            session = create_session(
                session_id=msg.session_id,
                label=msg.text,
                agent_id=msg.agent_id,
                text=msg.text,
                agents=self.agents,
                models=models,
            )

        elif msg.pipeline_id:
            pipeline = self.pipelines.get(msg.pipeline_id)
            if pipeline is None:
                await self._send_error(
                    caller_reply_topic,
                    caller_correlation,
                    f"Unknown pipeline: {msg.pipeline_id}",
                )
                return

            models = load_models()
            session = create_session(
                session_id=msg.session_id,
                label=msg.text,
                pipeline=pipeline,
                variables=msg.pipeline_vars,
                models=models,
                agents=self.agents,
            )

        else:
            await self._send_error(
                caller_reply_topic,
                caller_correlation,
                "Specify agent_id or pipeline_id. Use: "
                "skitter agent run <id> '<prompt>' or "
                "skitter pipeline run <id> --var key=value",
            )
            return

        # Store caller info on session
        session.caller_reply_topic = caller_reply_topic or ""
        session.caller_correlation = caller_correlation or ""
        self.sessions[msg.session_id] = session
        await self._publish_session(session)

        # Find and dispatch entry tasks
        entry = get_entry_tasks(session)
        for task in entry:
            await self.dispatch_and_spawn(session, task.id)

        await self._publish_session(session)

        label = (
            f"agent '{msg.agent_id}'"
            if msg.agent_id
            else f"pipeline '{msg.pipeline_id}'"
        )
        log.info(
            "[supervisor] %s started for %s (%d tasks, %d entry)",
            label.capitalize(),
            msg.session_id,
            len(session.tasks),
            len(entry),
        )

    async def _handle_spawn_request(
        self, data: dict, caller_reply_topic, caller_correlation
    ) -> None:
        """Handle A2A tasks/spawn request."""
        assert self.client is not None
        params = data.get("params", {})
        request_id = data.get("id", "")
        spawn_agent_id = params.get("agent_id", "")
        spawn_description = params.get("description", "")
        spawn_reply_to = params.get("reply_to", caller_reply_topic or "")
        spawn_sid = params.get("session_id", f"spawn-{uuid.uuid4().hex[:8]}")

        if not spawn_agent_id:
            if spawn_reply_to and request_id:
                err = A2AResponse(
                    id=request_id,
                    error={"code": -32602, "message": "Missing agent_id"},
                )
                await self.client.publish(spawn_reply_to, err.to_json(), qos=1)
            return

        spawn_task_id = uuid.uuid4().hex[:12]
        agent_def = self.agents.get(spawn_agent_id)
        models = load_models()
        default_model = list(models.keys())[0] if models else ""
        model = (agent_def.model if agent_def else "") or default_model

        spawn_session = Session(
            session_id=spawn_sid,
            agent_id=spawn_agent_id,
            label=spawn_description,
            caller_reply_topic=spawn_reply_to or "",
            caller_correlation=caller_correlation or "",
            spawn_request_id=request_id,
        )
        spawn_task = SessionTask(
            id="spawn_task",
            task_id=spawn_task_id,
            agent=spawn_agent_id,
            description=spawn_description,
            model=model,
            next="output",
        )
        spawn_session.tasks["spawn_task"] = spawn_task
        self.sessions[spawn_sid] = spawn_session

        await self.dispatch_and_spawn(spawn_session, "spawn_task")
        await self._publish_session(spawn_session)

        log.info(
            "[supervisor] Spawn request: %s for agent '%s' task %s",
            request_id,
            spawn_agent_id,
            spawn_task_id,
        )

    async def handle_chain_result(self, payload: str) -> None:
        """Handle retained chain result — marks source done, dispatches next task."""
        assert self.client is not None
        try:
            data = json.loads(payload)
        except Exception:
            return

        cr_sid = data.get("session_id", "")
        cr_source_tid = data.get("task_id", "")
        cr_result = data.get("result", "")

        session = self.sessions.get(cr_sid)
        if not session:
            return

        # Find source task and mark done
        source_id = find_id_by_task_id(session, cr_source_tid)
        if not source_id:
            return
        source_task = session.tasks[source_id]
        source_task.status = "done"
        self.task_to_session.pop(cr_source_tid, None)

        # Clear retained dispatch for source task
        await self.client.publish(
            topic_state_dispatch(cr_source_tid), b"", qos=1, retain=True
        )

        next_id = source_task.next
        if not next_id or next_id == "output":
            # Check session completion
            if all(t.status == "done" for t in session.tasks.values()):
                log.info("[supervisor] Session complete for %s", cr_sid)
            await self._publish_session(session)
            return

        next_task = session.tasks.get(next_id)
        if not next_task or next_task.status != "pending":
            await self._publish_session(session)
            return

        if len(next_task.needs) <= 1:
            # Simple chain: dispatch immediately
            context = f"## Result from '{source_id}':\n{cr_result}"
            await self.dispatch_and_spawn(session, next_id, context)

            # Clear retained chain result
            await self.client.publish(
                topic_chain_result(cr_sid, cr_source_tid), b"", qos=1, retain=True
            )
            await self._publish_session(session)
            log.info(
                "[supervisor] Chain: '%s' -> '%s' for session %s",
                source_id,
                next_id,
                cr_sid,
            )
        else:
            # Join: accumulate inputs
            key = (cr_sid, next_id)
            if key not in self.join_inputs:
                self.join_inputs[key] = {}
            self.join_inputs[key][cr_source_tid] = cr_result

            # Check if all needs satisfied
            all_satisfied = all(
                session.tasks.get(need_id) is not None
                and session.tasks[need_id].task_id in self.join_inputs[key]
                for need_id in next_task.needs
            )

            if all_satisfied:
                context = build_context_for_join(
                    self.join_inputs[key], next_task.needs, session
                )
                await self.dispatch_and_spawn(session, next_id, context)

                # Clear retained chain results
                for need_id in next_task.needs:
                    need_task = session.tasks.get(need_id)
                    if need_task:
                        await self.client.publish(
                            topic_chain_result(cr_sid, need_task.task_id),
                            b"",
                            qos=1,
                            retain=True,
                        )
                del self.join_inputs[key]

                log.info(
                    "[supervisor] Join task '%s' ready — all %d inputs collected",
                    next_id,
                    len(next_task.needs),
                )

            await self._publish_session(session)

    async def handle_reply(self, mqtt_msg, payload: str) -> None:
        """Handle reply from worker — terminal task bookkeeping."""
        assert self.client is not None
        corr_data = get_correlation_data(mqtt_msg)
        if not corr_data:
            return

        try:
            data = json.loads(payload)
        except Exception:
            return

        # Skip stream items
        if "seq" in data and "type" in data:
            return

        # Terminal status update
        if "state" not in data or "task_id" not in data:
            return

        status_update = TaskStatusUpdate.from_json(payload)
        task_id = status_update.task_id
        result_text = status_update.result

        if task_id not in self.task_to_session:
            return

        sid = self.task_to_session[task_id]
        session = self.sessions.get(sid)
        if session is None:
            return

        tid = find_id_by_task_id(session, task_id)
        if tid is None:
            return

        session.tasks[tid].status = "done"
        self.task_to_session.pop(task_id, None)

        # Clear retained dispatch
        await self.client.publish(
            topic_state_dispatch(task_id), b"", qos=1, retain=True
        )

        log.info(
            "[supervisor] Task '%s' (%s) done for session %s",
            tid,
            task_id,
            sid,
        )

        session_topic = topic_state_session(sid)

        # Spawn task completed: route back to requesting agent
        if tid == "spawn_task":
            spawn_req_id = session.spawn_request_id
            if session.caller_reply_topic:
                resp = A2AResponse(
                    id=spawn_req_id or "",
                    result={"output": result_text},
                )
                props = make_properties(correlation_data=session.caller_correlation)
                await self.client.publish(
                    session.caller_reply_topic,
                    resp.to_json(),
                    qos=1,
                    properties=props,
                )
            await self.client.publish(session_topic, b"", qos=1, retain=True)
            self.sessions.pop(sid, None)
            log.info("[supervisor] Spawn result routed for %s", sid)
            return

        # Store result from terminal tasks
        st = session.tasks[tid]
        if not st.next or st.next == "output":
            session.result = result_text

        # Check if session is complete
        if all(t.status == "done" for t in session.tasks.values()):
            log.info("[supervisor] Session complete for %s", sid)

        await self._publish_session(session)

    async def _publish_pipeline_cards(self) -> None:
        """Publish pipeline discovery cards as retained MQTT messages."""
        assert self.client is not None
        for pipeline_id, pipeline_def in self.pipelines.items():
            agents = list({t.agent for t in pipeline_def.tasks})
            card_payload = json.dumps(
                {
                    "pipeline_id": pipeline_id,
                    "name": pipeline_def.name,
                    "description": pipeline_def.description,
                    "variables": pipeline_def.variables,
                    "tasks": [
                        {"id": t.id, "agent": t.agent} for t in pipeline_def.tasks
                    ],
                    "agents": agents,
                }
            )
            await self.client.publish(
                topic_discovery(f"pipeline/{pipeline_id}"),
                card_payload,
                qos=1,
                retain=True,
            )

    async def handle_reload(self) -> None:
        """Reload agents and pipelines from disk."""
        assert self.client is not None
        log.info("[supervisor] Reload signal received")
        self.agents = load_agents()
        self.pipelines = load_pipelines()
        for agent_id, agent_def in self.agents.items():
            card = agent_def_to_card(agent_def)
            await self.client.publish(
                topic_discovery(agent_id),
                card.to_json(),
                qos=1,
                retain=True,
            )
        await self._publish_pipeline_cards()
        log.info(
            "[supervisor] Reloaded %d agents, %d pipelines",
            len(self.agents),
            len(self.pipelines),
        )

    async def handle_event(self, payload: str) -> None:
        """Handle agent events (alive/done/dead)."""
        assert self.client is not None
        try:
            status = json.loads(payload)
        except Exception:
            return

        wk_state = status.get("status", "")
        wk_task_id = status.get("task_id", "")

        if not wk_task_id:
            return

        if wk_state == "alive":
            log.info("[supervisor] Worker alive for task %s", wk_task_id)

        elif wk_state == "done":
            log.info("[supervisor] Worker done for task %s", wk_task_id)

        elif wk_state == "dead":
            if wk_task_id in self.task_to_session:
                wk_sid = self.task_to_session[wk_task_id]
                log.warning(
                    "[supervisor] Worker DEAD for task %s — respawning",
                    wk_task_id,
                )
                session = self.sessions.get(wk_sid)
                if session:
                    task_name = find_id_by_task_id(session, wk_task_id)
                    if task_name:
                        # Re-publish retained dispatch and respawn
                        await self.dispatch_task(session, task_name)
                spawn_worker(
                    status.get("agent", "unknown"),
                    wk_sid,
                    wk_task_id,
                )

    async def _send_error(
        self,
        reply_topic,
        correlation,
        message: str,
        code: int = -32602,
    ) -> None:
        """Send error response to caller.

        A2A error codes:
          -32602: invalid_params (default)
          -32003: request_expired
          -32004: responder_unavailable
          -32005: transport_protocol_error
        """
        assert self.client is not None
        if reply_topic:
            resp = A2AResponse(
                id=correlation or "",
                error={"code": code, "message": message},
            )
            props = make_properties(correlation_data=correlation)
            await self.client.publish(
                reply_topic, resp.to_json(), qos=1, properties=props
            )
        else:
            log.warning("[supervisor] No reply topic for error response")

    async def _publish_session(self, session: Session) -> None:
        """Publish retained session state."""
        assert self.client is not None
        await self.client.publish(
            topic_state_session(session.session_id),
            session.to_json(),
            qos=1,
            retain=True,
        )

    async def run(self) -> None:
        coordinator_session_id = uuid.uuid4().hex[:12]

        self.agents = load_agents()
        self.pipelines = load_pipelines()
        if self.agents:
            log.info(
                "[supervisor] Loaded %d agents: %s",
                len(self.agents),
                ", ".join(self.agents),
            )
        if self.pipelines:
            log.info(
                "[supervisor] Loaded %d pipelines: %s",
                len(self.pipelines),
                ", ".join(self.pipelines),
            )

        self.reply_topic = topic_reply("coordinator", coordinator_session_id)

        async with aiomqtt.Client(
            MQTT_HOST,
            MQTT_PORT,
            identifier=f"{A2A_ORG}/{A2A_UNIT}/coordinator-{coordinator_session_id}",
            protocol=aiomqtt.ProtocolVersion.V5,
        ) as client:
            self.client = client

            # --- Publish Agent Cards ---
            for agent_id, agent_def in self.agents.items():
                card = agent_def_to_card(agent_def)
                await client.publish(
                    topic_discovery(agent_id),
                    card.to_json(),
                    qos=1,
                    retain=True,
                )
            coord_card_payload = json.dumps(
                {
                    "agent_id": "coordinator",
                    "name": "Skitter Supervisor",
                    "description": "Pipeline supervisor — spawns workers, handles joins, respawns on crash",
                    "capabilities": ["orchestration", "spawn"],
                    "model": "",
                    "max_turns": 0,
                }
            )
            await client.publish(
                topic_discovery("coordinator"),
                coord_card_payload,
                qos=1,
                retain=True,
            )
            # --- Publish Pipeline Cards ---
            await self._publish_pipeline_cards()
            log.info(
                "[supervisor] Published %d Agent Cards, %d Pipeline Cards",
                len(self.agents) + 1,
                len(self.pipelines),
            )

            # --- Recovery phase ---
            self.sessions = await recover_sessions(client)
            if self.sessions:
                self.task_to_session = rebuild_task_map(self.sessions)
                # Recover chain results
                chain_results = await recover_chain_results(client)
                for (cr_sid, cr_source_tid), cr_result in chain_results.items():
                    cr_session = self.sessions.get(cr_sid)
                    if cr_session:
                        for jt in cr_session.tasks.values():
                            if len(jt.needs) > 1:
                                for need_id in jt.needs:
                                    need_task = cr_session.tasks.get(need_id)
                                    if need_task and need_task.task_id == cr_source_tid:
                                        key = (cr_sid, jt.id)
                                        if key not in self.join_inputs:
                                            self.join_inputs[key] = {}
                                        self.join_inputs[key][cr_source_tid] = cr_result

                # Respawn workers for tasks that were running
                for session in self.sessions.values():
                    for task in session.tasks.values():
                        if task.status == "running":
                            log.info(
                                "[supervisor] Respawning %s worker for task %s (recovery)",
                                task.agent,
                                task.task_id,
                            )
                            task_name = find_id_by_task_id(session, task.task_id)
                            if task_name:
                                await self.dispatch_task(session, task_name)
                            spawn_worker(task.agent, session.session_id, task.task_id)

                # Check if any joins are now satisfiable
                for (ji_sid, ji_task_id), ji_inputs in list(self.join_inputs.items()):
                    ji_session = self.sessions.get(ji_sid)
                    if not ji_session:
                        continue
                    ji_task = ji_session.tasks.get(ji_task_id)
                    if not ji_task or ji_task.status != "pending":
                        continue
                    all_satisfied = all(
                        ji_session.tasks.get(need_id) is not None
                        and ji_session.tasks[need_id].task_id in ji_inputs
                        for need_id in ji_task.needs
                    )
                    if all_satisfied:
                        context = build_context_for_join(
                            ji_inputs, ji_task.needs, ji_session
                        )
                        await self.dispatch_and_spawn(ji_session, ji_task_id, context)

                log.info(
                    "[supervisor] Recovery complete: %d sessions, %d running tasks",
                    len(self.sessions),
                    len(self.task_to_session),
                )
            else:
                log.info("[supervisor] No sessions to recover")

            # --- Subscribe ---
            await client.subscribe(topic_request("coordinator"), qos=1)
            await client.subscribe(self.reply_topic, qos=1)
            await client.subscribe(topic_event_wildcard(), qos=1)
            await client.subscribe(topic_chain_wildcard(), qos=1)
            await client.subscribe(topic_control_reload(), qos=1)
            log.info("[supervisor] Subscribed and ready (reply=%s)", self.reply_topic)

            async for mqtt_msg in client.messages:
                topic = str(mqtt_msg.topic)
                payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                if not payload:
                    continue

                if topic == topic_request("coordinator"):
                    await self.handle_inbound(mqtt_msg, payload)
                elif "/chain/" in topic:
                    await self.handle_chain_result(payload)
                elif topic == self.reply_topic:
                    await self.handle_reply(mqtt_msg, payload)
                elif topic == topic_control_reload():
                    await self.handle_reload()
                elif "/event/" in topic:
                    await self.handle_event(payload)


async def run() -> None:
    coordinator = Coordinator()
    await coordinator.run()


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("[supervisor] Shutting down")


if __name__ == "__main__":
    main()
