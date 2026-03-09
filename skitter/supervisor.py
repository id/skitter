"""Supervisor — creates sessions, spawns workers.

Long-lived MQTT subscriber. Subscribes to $a2a/v1/request/+
and skitter/event/+/dead wildcards.
"""

import asyncio
import json
import logging
import uuid

import aiomqtt

from skitter.config import (
    AgentDef,
    WorkflowDef,
    load_agents,
    load_workflows,
    safe_format,
)
from skitter.discovery import build_cards
from skitter.mqtt import (
    A2A_ORG,
    A2A_UNIT,
    get_correlation_data,
    get_response_topic,
    make_properties,
    mqtt_client_kwargs,
    topic_dead_wildcard,
    topic_discovery,
    topic_reload,
    topic_request_wildcard,
    topic_session,
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
            session.tasks[pt.id] = SessionTask(
                id=pt.id,
                agent=pt.agent,
                description=description,
                model=pt.model,
                runtime=_resolve_runtime(agents, pt.agent),
                next=pt.next,
                needs=list(pt.needs),
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

    workflow_id = ""
    if agent_id.startswith("workflow-"):
        workflow_id = agent_id.removeprefix("workflow-")
        agent_id = ""

    if workflow_id:
        workflow = workflows.get(workflow_id)
        if not workflow:
            await _send_error(
                client,
                caller_reply_topic,
                caller_correlation,
                f"Unknown workflow: {workflow_id}",
                code=A2A_RESPONDER_UNAVAILABLE,
                a2a_error="responder_unavailable",
            )
            return
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


async def _publish_discovery(
    client: aiomqtt.Client,
    cards: dict[str, str],
) -> None:
    for card_id, card_json in cards.items():
        await client.publish(
            topic_discovery(card_id),
            card_json,
            qos=1,
            retain=True,
        )
    log.info("Published %d discovery cards", len(cards))


async def handle_dead_event(payload: str) -> None:
    """Re-spawn a crashed worker. Retained session still exists on broker.

    Skipped when SPAWN_MODE=fly — Fly handles restarts via its own
    restart policy (on-failure, max_retries=1). The LWT dead event fires
    on normal exit too (auto_destroy kills the connection), so respawning
    would create an infinite loop.
    """
    from skitter.spawn import SPAWN_MODE

    if SPAWN_MODE == "fly":
        return

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
    log.warning("Worker dead for task %s — respawning", task)
    spawn_worker(agent, session_id, task)


async def run_listen() -> None:
    """Long-lived MQTT subscriber — local mode."""
    agents = load_agents()
    workflows = load_workflows()

    if agents:
        log.info("Loaded %d agents: %s", len(agents), ", ".join(agents))
    if workflows:
        log.info("Loaded %d workflows: %s", len(workflows), ", ".join(workflows))

    async with aiomqtt.Client(
        **mqtt_client_kwargs(identifier=f"{A2A_ORG}/{A2A_UNIT}/supervisor"),
    ) as client:
        await _publish_discovery(client, build_cards(agents, workflows))

        await client.subscribe(topic_request_wildcard(), qos=1)
        await client.subscribe(topic_dead_wildcard(), qos=1)
        await client.subscribe(topic_reload(), qos=1)
        log.info("Supervisor ready, listening on request/+, event/+/dead")

        async for mqtt_msg in client.messages:
            topic = str(mqtt_msg.topic)
            payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
            if not payload:
                continue

            if topic == topic_reload():
                agents = load_agents()
                workflows = load_workflows()
                await _publish_discovery(client, build_cards(agents, workflows))
                log.info(
                    "Reloaded %d agents, %d workflows", len(agents), len(workflows)
                )
                continue

            if "/request/" in topic and "/cancel" not in topic:
                agent_id = _parse_agent_id_from_topic(topic)
                if agent_id == "supervisor":
                    continue
                caller_reply = get_response_topic(mqtt_msg) or ""
                caller_corr = get_correlation_data(mqtt_msg) or ""
                if not caller_reply or not caller_corr:
                    log.warning("Request missing Response Topic or Correlation Data")
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
                await handle_dead_event(payload)


# --- Entry point ---


def main() -> None:
    try:
        asyncio.run(run_listen())
    except KeyboardInterrupt:
        log.info("Supervisor shutting down")


if __name__ == "__main__":
    main()
