"""Supervisor — creates sessions, spawns workers.

Long-lived MQTT subscriber. Subscribes to $a2a/v1/request/+
and skitter/event/+/dead wildcards.
"""

import asyncio
import json
import logging
import uuid
from datetime import datetime, timezone

import aiomqtt

from skitter.config import (
    AgentDef,
    WorkflowDef,
    load_agents,
    load_workflows,
    safe_format,
)
from skitter.discovery import CardPublisher, build_card
from skitter.mqtt import (
    A2A_ORG,
    A2A_UNIT,
    get_correlation_data,
    get_response_topic,
    make_properties,
    mqtt_client_kwargs,
    topic_dead_wildcard,
    topic_discovery,
    topic_discovery_wildcard,
    topic_request_wildcard,
    topic_result,
    topic_session,
    topic_status,
)
from skitter.spawn import spawn_worker
from skitter.types import (
    A2ARequest,
    A2AResponse,
    A2A_RESPONDER_UNAVAILABLE,
    A2A_TRANSPORT_PROTOCOL_ERROR,
    Session,
    SessionTask,
    make_status_event,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.supervisor")


# --- Core logic (shared by all modes) ---


def _resolve_runtime(agents: dict[str, AgentDef], agent_id: str) -> str:
    agent_def = agents.get(agent_id)
    return (agent_def.runtime if agent_def else "claude") or "claude"


def create_session(
    session_id: str,
    label: str,
    workflow: WorkflowDef | None = None,
    variables: dict[str, str] | None = None,
    agent_id: str = "",
    text: str = "",
    agents: dict[str, AgentDef] | None = None,
) -> Session:
    """Create a Session with SessionTasks."""
    agents = agents or {}
    variables = variables or {}

    workspace_slug = workflow.workspace if workflow else ""

    session = Session(
        session_id=session_id,
        workflow_id=workflow.id if workflow else agent_id,
        agent_id=agent_id if not workflow else "",
        label=label,
        variables=variables,
    )

    if workflow:
        for pt in workflow.tasks:
            description = safe_format(pt.description, variables)
            task_ws = workspace_slug
            session.tasks[pt.id] = SessionTask(
                id=pt.id,
                agent=pt.agent,
                description=description,
                model=pt.model,
                runtime=_resolve_runtime(agents, pt.agent),
                next=pt.next,
                needs=list(pt.needs),
                workspace=task_ws,
            )
    else:
        session.tasks[agent_id] = SessionTask(
            id=agent_id,
            agent=agent_id,
            description=text,
            runtime=_resolve_runtime(agents, agent_id),
            next="output",
        )

    return session


def _parse_agent_id_from_topic(topic: str) -> str:
    """Extract agent_id from $a2a/v1/request/{org}/{unit}/{agent_id}."""
    parts = topic.split("/")
    return parts[5] if len(parts) >= 6 else ""


async def handle_request(
    client: aiomqtt.Client,
    payload: str,
    caller_reply_topic: str,
    caller_correlation: str,
    agents: dict[str, AgentDef],
    workflows: dict[str, WorkflowDef],
    agent_id: str = "",
) -> None:
    """Create session, publish it, spawn all workers."""
    try:
        req = A2ARequest.from_json(payload)
    except Exception as e:
        log.error("Bad inbound JSON-RPC: %s", e)
        return

    session_id = uuid.uuid4().hex[:16]

    # Detect workflow by checking if agent_id matches a workflow definition
    if agent_id in workflows:
        workflow = workflows[agent_id]
        workflow_id = agent_id
        agent_id = ""
        session = create_session(
            session_id,
            req.text,
            workflow=workflow,
            variables=req.variables,
            agents=agents,
        )
    else:
        agent_id = agent_id or "skitter"
        if agent_id not in agents:
            await _send_error(
                client,
                caller_reply_topic,
                caller_correlation,
                f"Unknown agent: {agent_id}",
                code=A2A_RESPONDER_UNAVAILABLE,
                a2a_error="responder_unavailable",
            )
            return
        session = create_session(
            session_id,
            req.text,
            agent_id=agent_id,
            text=req.text,
            agents=agents,
        )

    session.caller_reply_topic = caller_reply_topic
    session.caller_correlation = caller_correlation or req.request_id

    for st in session.tasks.values():
        st.status = "running"

    await client.publish(
        topic_session(session.session_id),
        session.to_json(),
        qos=1,
        retain=True,
    )

    # Send initial ack with server-generated Task.id (A2A spec §Request/Reply)
    if caller_reply_topic:
        ack = make_status_event(
            request_id=caller_correlation,
            task_id=session.session_id,
            state="submitted",
        )
        props = make_properties(correlation_data=caller_correlation)
        await client.publish(caller_reply_topic, ack, qos=1, properties=props)

    for task_name, st in session.tasks.items():
        spawn_worker(st.agent, session.session_id, task_name)

    label = f"agent '{agent_id}'" if agent_id else f"workflow '{workflow_id}'"
    log.info(
        "%s started for %s (%d tasks, all spawned)",
        label.capitalize(),
        session.session_id,
        len(session.tasks),
    )


async def _send_error(
    client: aiomqtt.Client,
    reply_topic: str,
    correlation: str,
    message: str,
    code: int = -32602,
    a2a_error: str = "",
) -> None:
    if reply_topic:
        error_data: dict = {"code": code, "message": message}
        if a2a_error:
            error_data["data"] = {"a2a_error": a2a_error}
        resp = A2AResponse(
            id=correlation or "",
            error=error_data,
        )
        props = make_properties(correlation_data=correlation)
        await client.publish(reply_topic, resp.to_json(), qos=1, properties=props)


# --- Mode: listen (long-lived MQTT subscriber) ---


async def handle_dead_event(client: aiomqtt.Client, payload: str) -> None:
    """Handle a worker death (LWT).

    Local mode: respawn the worker.
    Fly mode: Fly already retried (on-failure, max_retries=1). Check if a
    result was published; if not, the worker crashed permanently. Publish a
    failed result and status so downstream workers unblock.
    """
    from skitter.spawn import SPAWN_MODE

    try:
        data = json.loads(payload)
    except Exception:
        return
    task = data.get("task", "")
    agent = data.get("agent", "")
    session_id = data.get("session_id", "")
    if not task or not agent or not session_id:
        log.warning("Dead event missing task, agent, or session_id: %s", data)
        return

    if SPAWN_MODE != "fly":
        log.warning("Worker dead for task %s -- respawning", task)
        spawn_worker(agent, session_id, task)
        return

    # Fly mode: check if a result was already published (normal exit fires
    # LWT too because auto_destroy kills the connection).
    # Single probe connection reads both session and result topics.
    session, has_result = await _probe_task_state(session_id, task)

    if has_result:
        log.info("Worker for task %s exited normally (result exists)", task)
        return

    # No result -- worker crashed permanently. Fail this task and propagate.
    log.error("Worker for task %s crashed with no result -- failing task", task)
    error_msg = f"Task '{task}' failed: worker crashed after all retries"

    if session:
        await _fail_task(client, session, task, error_msg)
    else:
        log.error("Cannot read session %s to propagate failure", session_id)


async def _probe_task_state(
    session_id: str, task: str, timeout: float = 3.0
) -> tuple[Session | None, bool]:
    """Check if a session and result exist using a single probe connection."""
    session_topic = topic_session(session_id)
    session: Session | None = None
    has_result = False

    async with aiomqtt.Client(
        **mqtt_client_kwargs(identifier=f"{A2A_ORG}/{A2A_UNIT}/supervisor-probe"),
    ) as probe:
        # Subscribe to session first to learn the workflow_id
        await probe.subscribe(session_topic, qos=1)
        try:
            async with asyncio.timeout(timeout):
                async for msg in probe.messages:
                    payload = msg.payload.decode() if msg.payload else ""
                    if payload:
                        session = Session.from_json(payload)
                    break
        except TimeoutError:
            return None, False

        if session is None:
            return None, False

        # Now subscribe to the result topic
        result_topic = topic_result(session.workflow_id, task, session_id)
        await probe.unsubscribe(session_topic)
        await probe.subscribe(result_topic, qos=1)
        try:
            async with asyncio.timeout(timeout):
                async for msg in probe.messages:
                    if str(msg.topic) == result_topic:
                        has_result = bool(msg.payload)
                        break
        except TimeoutError:
            pass

    return session, has_result


async def _fail_task(
    client: aiomqtt.Client, session: Session, task: str, error_msg: str
) -> None:
    """Publish failed result and status for a task, then propagate to downstream."""
    workflow_id = session.workflow_id
    session_id = session.session_id

    # Publish failed result (unblocks downstream workers who check "failed" field)
    await client.publish(
        topic_result(workflow_id, task, session_id),
        json.dumps(
            {
                "task": task,
                "session_id": session_id,
                "result": error_msg,
                "failed": True,
            }
        ),
        qos=1,
        retain=True,
    )
    await client.publish(
        topic_status(workflow_id, task, session_id),
        json.dumps(
            {
                "task": task,
                "session_id": session_id,
                "status": "failed",
                "last_active": datetime.now(timezone.utc).isoformat(),
            }
        ),
        qos=1,
        retain=True,
    )

    # Find the terminal task in this pipeline and notify the caller
    failed_task = session.tasks.get(task)
    if not failed_task:
        return

    terminal = _find_terminal_task(session, task)
    if terminal and session.caller_reply_topic:
        event = make_status_event(
            request_id=session.caller_correlation,
            task_id=session_id,
            state="failed",
            message=error_msg,
            task_name=terminal,
        )
        props = make_properties(correlation_data=session.caller_correlation)
        await client.publish(session.caller_reply_topic, event, qos=1, properties=props)


def _find_terminal_task(session: Session, start_task: str) -> str:
    """Walk the task graph from start_task to find the terminal task."""
    current = start_task
    last_valid = start_task
    seen: set[str] = set()
    while current in session.tasks and current not in seen:
        seen.add(current)
        last_valid = current
        nxt = session.tasks[current].next
        if not nxt or nxt == "output":
            return current
        current = nxt
    return last_valid


def _build_workflow_cards(
    workflows: dict[str, WorkflowDef],
) -> dict[str, str]:
    """Build card JSON for workflow/composed-app cards only.

    Standalone agents self-register via agent_runner — the supervisor
    does not publish cards for them.
    """
    cards: dict[str, str] = {}
    for wf in workflows.values():
        wf_agent = AgentDef(id=wf.id, name=wf.name, description=wf.description)
        cards[wf.id] = json.dumps(build_card(wf_agent, workflow=wf))
    return cards


async def _cleanup_stale_cards(active_ids: set[str]) -> None:
    """Remove retained discovery cards for workflows no longer in config."""
    try:
        async with aiomqtt.Client(
            **mqtt_client_kwargs(identifier=f"{A2A_ORG}/{A2A_UNIT}/supervisor-cleanup"),
        ) as client:
            discovered: dict[str, bool] = {}
            await client.subscribe(topic_discovery_wildcard(), qos=1)
            try:
                async with asyncio.timeout(3.0):
                    async for msg in client.messages:
                        card_id = str(msg.topic).split("/")[-1]
                        if msg.payload:
                            discovered[card_id] = True
            except TimeoutError:
                pass

            for card_id in discovered:
                if card_id not in active_ids and card_id != "supervisor":
                    await client.publish(
                        topic_discovery(card_id), b"", qos=1, retain=True
                    )
                    log.info("Cleared stale discovery card: %s", card_id)
    except Exception:
        log.warning("Failed to clean up stale discovery cards")


async def run_listen() -> None:
    """Long-lived MQTT subscriber — local mode."""
    agents = load_agents()
    workflows = load_workflows()

    if agents:
        log.info("Loaded %d agents: %s", len(agents), ", ".join(agents))
    if workflows:
        log.info("Loaded %d workflows: %s", len(workflows), ", ".join(workflows))

    cards = _build_workflow_cards(workflows)
    publishers: dict[str, CardPublisher] = {}
    for card_id, card_json in cards.items():
        pub = CardPublisher(card_id, card_json)
        publishers[card_id] = pub
        await pub.start()

    # Clean up stale workflow cards (workflows removed from config)
    await _cleanup_stale_cards(set(cards.keys()))

    try:
        async with aiomqtt.Client(
            **mqtt_client_kwargs(identifier=f"{A2A_ORG}/{A2A_UNIT}/supervisor"),
        ) as client:
            await client.subscribe(topic_request_wildcard(), qos=1)
            await client.subscribe(topic_dead_wildcard(), qos=1)
            log.info("Supervisor ready, listening on request/+, event/+/dead")

            async for mqtt_msg in client.messages:
                topic = str(mqtt_msg.topic)
                payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                if not payload:
                    continue

                if "/request/" in topic and "/cancel" not in topic:
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
                            await _send_error(
                                client,
                                caller_reply,
                                caller_corr,
                                "Request must include MQTT v5 Response Topic and Correlation Data",
                                code=A2A_TRANSPORT_PROTOCOL_ERROR,
                                a2a_error="transport_protocol_error",
                            )
                        continue
                    await handle_request(
                        client,
                        payload,
                        caller_reply,
                        caller_corr,
                        agents,
                        workflows,
                        agent_id=agent_id,
                    )

                elif topic.startswith("skitter/event/"):
                    await handle_dead_event(client, payload)
    finally:
        for pub in publishers.values():
            await pub.stop()


# --- Entry point ---


def main() -> None:
    try:
        asyncio.run(run_listen())
    except KeyboardInterrupt:
        log.info("Supervisor shutting down")


if __name__ == "__main__":
    main()
