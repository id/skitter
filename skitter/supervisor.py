"""Supervisor — listens for agent requests via wildcard, creates sessions,
spawns workers, monitors liveness via LWT events.

Not an A2A agent itself. Invisible infrastructure that subscribes to
request/{o}/{u}/+ and event/{o}/{u}/+/+ wildcards.
"""

import asyncio
import json
import logging
import uuid

import aiomqtt

from skitter.config import (
    AgentDef,
    WorkflowDef,
    safe_format,
)
from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    A2A_ORG,
    A2A_UNIT,
    get_correlation_data,
    get_response_topic,
    make_properties,
    topic_discovery,
    topic_event_wildcard,
    topic_reload,
    topic_request_wildcard,
    topic_session,
)
from skitter.respawn import handle_dead_event
from skitter.spawn import spawn_worker
from skitter.storage import load_agents, load_cards, load_workflows
from skitter.types import (
    A2AResponse,
    A2A_RESPONDER_UNAVAILABLE,
    A2A_TRANSPORT_PROTOCOL_ERROR,
    InboundMessage,
    Session,
    SessionTask,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.supervisor")


def build_dispatch_spec(
    session: Session,
    task_name: str,
    agents: dict[str, AgentDef],
) -> dict:
    """Pre-materialize a task's dispatch spec (AgentMessage fields as dict)."""
    st = session.tasks[task_name]
    agent_def = agents.get(st.agent)
    runtime = (agent_def.runtime if agent_def else "claude") or "claude"

    return {
        "task_id": st.task_id,
        "session_id": session.session_id,
        "description": st.description,
        "agent": st.agent,
        "model": st.model,
        "runtime": runtime,
        "next": st.next,
        "caller_reply_topic": session.caller_reply_topic,
        "caller_correlation": session.caller_correlation,
    }


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
        workflow_id=workflow.id if workflow else "",
        agent_id=agent_id if not workflow else "",
        label=label,
        variables=variables,
    )

    if workflow:
        for pt in workflow.tasks:
            task_id = uuid.uuid4().hex[:12]
            description = safe_format(pt.description, variables)

            session.tasks[pt.id] = SessionTask(
                id=pt.id,
                task_id=task_id,
                agent=pt.agent,
                description=description,
                model=pt.model,
                next=pt.next,
                needs=list(pt.needs),
            )
    else:
        task_id = uuid.uuid4().hex[:12]

        session.tasks[agent_id] = SessionTask(
            id=agent_id,
            task_id=task_id,
            agent=agent_id,
            description=text,
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
    """Create session, pre-materialize all task specs, spawn all workers."""
    try:
        data = json.loads(payload)
    except Exception as e:
        log.error("Bad inbound JSON: %s", e)
        return

    # Standard inbound request
    try:
        msg = InboundMessage.from_json(payload)
    except Exception:
        msg = InboundMessage(
            text=data.get("text", ""),
            sender=data.get("sender", "unknown"),
            session_id=data.get("session_id", f"req-{uuid.uuid4().hex[:8]}"),
            workflow_id=data.get("workflow_id", ""),
            workflow_vars=data.get("workflow_vars", {}),
            agent_id=data.get("agent_id", ""),
        )

    # Agent ID comes from the topic, fall back to payload
    if agent_id:
        msg.agent_id = agent_id

    # Check if this is a workflow request (agent_id starts with "workflow-")
    if msg.agent_id and msg.agent_id.startswith("workflow-"):
        msg.workflow_id = msg.agent_id.removeprefix("workflow-")
        msg.agent_id = ""

    if msg.agent_id:
        if msg.agent_id not in agents:
            await _send_error(
                client,
                caller_reply_topic,
                caller_correlation,
                f"Unknown agent: {msg.agent_id}",
                code=A2A_RESPONDER_UNAVAILABLE,
                a2a_error="responder_unavailable",
            )
            return
        session = create_session(
            msg.session_id,
            msg.text,
            agent_id=msg.agent_id,
            text=msg.text,
            agents=agents,
        )
    elif msg.workflow_id:
        workflow = workflows.get(msg.workflow_id)
        if not workflow:
            await _send_error(
                client,
                caller_reply_topic,
                caller_correlation,
                f"Unknown workflow: {msg.workflow_id}",
                code=A2A_RESPONDER_UNAVAILABLE,
                a2a_error="responder_unavailable",
            )
            return
        session = create_session(
            msg.session_id,
            msg.text,
            workflow=workflow,
            variables=msg.workflow_vars,
            agents=agents,
        )
    else:
        default_agent = "skitter"
        if default_agent not in agents:
            await _send_error(
                client,
                caller_reply_topic,
                caller_correlation,
                "No default agent. Specify agent_id or workflow_id, "
                "or run 'skitter init' to create the default agent.",
            )
            return
        msg.agent_id = default_agent
        session = create_session(
            msg.session_id,
            msg.text,
            agent_id=msg.agent_id,
            text=msg.text,
            agents=agents,
        )

    session.caller_reply_topic = caller_reply_topic
    session.caller_correlation = caller_correlation

    # Pre-materialize dispatch specs for every task
    for task_name in session.tasks:
        session.task_dispatches[task_name] = build_dispatch_spec(
            session, task_name, agents
        )

    # Publish session as retained event
    await client.publish(
        topic_session(session.session_id),
        session.to_json(),
        qos=1,
        retain=True,
    )

    # Spawn ALL workers (entry tasks run immediately, join tasks wait)
    for task_name, st in session.tasks.items():
        st.status = "running"
        spawn_worker(st.agent, session.session_id, st.task_id)

    # Re-publish session with updated statuses
    await client.publish(
        topic_session(session.session_id),
        session.to_json(),
        qos=1,
        retain=True,
    )

    label = (
        f"agent '{msg.agent_id}'" if msg.agent_id else f"workflow '{msg.workflow_id}'"
    )
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


async def _publish_discovery(
    client: aiomqtt.Client,
    cards: dict[str, str],
) -> None:
    """Publish pre-built Agent/Workflow Cards as retained discovery messages."""
    for card_id, card_json in cards.items():
        await client.publish(
            topic_discovery(card_id),
            card_json,
            qos=1,
            retain=True,
        )
    log.info("Published %d discovery cards", len(cards))


async def run() -> None:
    """Run the supervisor as a long-lived MQTT subscriber (local mode)."""
    agents = load_agents()
    workflows = load_workflows()
    cards = load_cards()

    if agents:
        log.info("Loaded %d agents: %s", len(agents), ", ".join(agents))
    if workflows:
        log.info("Loaded %d workflows: %s", len(workflows), ", ".join(workflows))

    async with aiomqtt.Client(
        MQTT_HOST,
        MQTT_PORT,
        identifier=f"{A2A_ORG}/{A2A_UNIT}/supervisor",
        protocol=aiomqtt.ProtocolVersion.V5,
    ) as client:
        await _publish_discovery(client, cards)

        # Subscribe to all agent requests, events (wildcards), and reload
        await client.subscribe(topic_request_wildcard(), qos=1)
        await client.subscribe(topic_event_wildcard(), qos=1)
        await client.subscribe(topic_reload(), qos=1)
        log.info("Supervisor ready, listening on request/+, event/+/+")

        async for mqtt_msg in client.messages:
            topic = str(mqtt_msg.topic)
            payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
            if not payload:
                continue

            # Reload signal
            if topic == topic_reload():
                agents = load_agents()
                workflows = load_workflows()
                cards = load_cards()
                await _publish_discovery(client, cards)
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

            elif "/event/" in topic and topic.endswith("/dead"):
                await handle_dead_event(payload)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Supervisor shutting down")


if __name__ == "__main__":
    main()
