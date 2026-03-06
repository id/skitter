"""Gateway — creates sessions, pre-materializes dispatch specs, spawns all workers.

In local mode, runs as a long-lived MQTT subscriber.
In serverless mode, triggered by EMQX webhook (Phase 2).
"""

import asyncio
import json
import logging
import os
import uuid

import aiomqtt

from skitter.config import (
    AgentDef,
    WorkflowDef,
    agent_def_to_card,
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
    topic_request,
    topic_state_session,
)
from skitter.respawn import handle_dead_event
from skitter.spawn import spawn_worker
from skitter.storage import load_agents, load_workflows
from skitter.types import (
    A2AResponse,
    InboundMessage,
    Session,
    SessionTask,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.gateway")

DEFAULT_MODELS = "haiku:Fast and cheap|sonnet:Balanced|opus:Most capable"


def _load_models() -> dict[str, str]:
    raw = os.environ.get("SKITTER_MODELS", DEFAULT_MODELS)
    return {
        name.strip(): desc.strip()
        for entry in raw.split("|")
        if ":" in entry
        for name, desc in [entry.split(":", 1)]
    }


def build_dispatch_spec(
    session: Session,
    task_name: str,
    agents: dict[str, AgentDef],
    workflows: dict[str, WorkflowDef],
) -> dict:
    """Pre-materialize a task's full dispatch spec (AgentMessage fields as dict)."""
    st = session.tasks[task_name]
    agent_def = agents.get(st.agent)

    # Resolve workflow task overrides
    pt = None
    if session.workflow_id:
        workflow = workflows.get(session.workflow_id)
        if workflow:
            pt = next((t for t in workflow.tasks if t.id == task_name), None)

    soul = (pt.soul if pt and pt.soul else agent_def.soul if agent_def else "") or ""
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

    return {
        "task_id": st.task_id,
        "session_id": session.session_id,
        "description": st.description,
        "soul": soul,
        "skills": skills,
        "max_turns": max_turns,
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
    models: dict[str, str] | None = None,
    agents: dict[str, AgentDef] | None = None,
) -> Session:
    """Create a Session with SessionTasks."""
    models = models or {}
    agents = agents or {}
    variables = variables or {}
    default_model = list(models.keys())[0] if models else ""

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
            agent_def = agents.get(pt.agent)
            description = safe_format(pt.description, variables)
            model = pt.model or (agent_def.model if agent_def else default_model)

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
        task_id = uuid.uuid4().hex[:12]
        agent_def = agents.get(agent_id)
        model = agent_def.model if agent_def else default_model

        session.tasks[agent_id] = SessionTask(
            id=agent_id,
            task_id=task_id,
            agent=agent_id,
            description=text,
            model=model,
            next="output",
        )

    return session


async def handle_request(
    client: aiomqtt.Client,
    payload: str,
    caller_reply_topic: str,
    caller_correlation: str,
    agents: dict[str, AgentDef],
    workflows: dict[str, WorkflowDef],
) -> None:
    """Create session, pre-materialize all task specs, spawn all workers."""
    try:
        data = json.loads(payload)
    except Exception as e:
        log.error("Bad inbound JSON: %s", e)
        return

    method = data.get("method", "")

    # A2A tasks/spawn
    if method == "tasks/spawn":
        await _handle_spawn(
            client, data, caller_reply_topic, caller_correlation, agents
        )
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

    models = _load_models()

    if msg.agent_id:
        if msg.agent_id not in agents:
            await _send_error(
                client,
                caller_reply_topic,
                caller_correlation,
                f"Unknown agent: {msg.agent_id}",
            )
            return
        session = create_session(
            msg.session_id,
            msg.text,
            agent_id=msg.agent_id,
            text=msg.text,
            agents=agents,
            models=models,
        )
    elif msg.workflow_id:
        workflow = workflows.get(msg.workflow_id)
        if not workflow:
            await _send_error(
                client,
                caller_reply_topic,
                caller_correlation,
                f"Unknown workflow: {msg.workflow_id}",
            )
            return
        session = create_session(
            msg.session_id,
            msg.text,
            workflow=workflow,
            variables=msg.workflow_vars,
            models=models,
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
            models=models,
        )

    session.caller_reply_topic = caller_reply_topic
    session.caller_correlation = caller_correlation

    # Pre-materialize dispatch specs for every task
    for task_name in session.tasks:
        session.task_dispatches[task_name] = build_dispatch_spec(
            session, task_name, agents, workflows
        )

    # Publish session as retained
    await client.publish(
        topic_state_session(session.session_id),
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
        topic_state_session(session.session_id),
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


async def _handle_spawn(
    client: aiomqtt.Client,
    data: dict,
    caller_reply_topic: str,
    caller_correlation: str,
    agents: dict[str, AgentDef],
) -> None:
    """Handle A2A tasks/spawn request."""
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
            await client.publish(spawn_reply_to, err.to_json(), qos=1)
        return

    models = _load_models()
    session = create_session(
        spawn_sid,
        spawn_description,
        agent_id=spawn_agent_id,
        text=spawn_description,
        agents=agents,
        models=models,
    )
    session.caller_reply_topic = spawn_reply_to
    session.caller_correlation = caller_correlation
    session.spawn_request_id = request_id

    # Rename the task key from agent_id to "spawn_task" for consistency
    agent_task = session.tasks.pop(spawn_agent_id)
    agent_task.id = "spawn_task"
    session.tasks["spawn_task"] = agent_task

    # Pre-materialize
    session.task_dispatches["spawn_task"] = build_dispatch_spec(
        session, "spawn_task", agents, {}
    )

    await client.publish(
        topic_state_session(session.session_id),
        session.to_json(),
        qos=1,
        retain=True,
    )

    agent_task.status = "running"
    spawn_worker(agent_task.agent, session.session_id, agent_task.task_id)

    await client.publish(
        topic_state_session(session.session_id),
        session.to_json(),
        qos=1,
        retain=True,
    )

    log.info(
        "Spawn request: %s for agent '%s' task %s",
        request_id,
        spawn_agent_id,
        agent_task.task_id,
    )


async def _send_error(
    client: aiomqtt.Client,
    reply_topic: str,
    correlation: str,
    message: str,
    code: int = -32602,
) -> None:
    if reply_topic:
        resp = A2AResponse(
            id=correlation or "",
            error={"code": code, "message": message},
        )
        props = make_properties(correlation_data=correlation)
        await client.publish(reply_topic, resp.to_json(), qos=1, properties=props)


async def _publish_discovery(
    client: aiomqtt.Client,
    agents: dict[str, AgentDef],
    workflows: dict[str, WorkflowDef],
) -> None:
    """Publish Agent Cards and Workflow Cards as retained discovery messages."""
    for agent_id, agent_def in agents.items():
        card = agent_def_to_card(agent_def)
        await client.publish(
            topic_discovery(agent_id),
            card.to_json(),
            qos=1,
            retain=True,
        )

    # Gateway card
    gateway_card = json.dumps(
        {
            "agent_id": "gateway",
            "name": "Skitter Gateway",
            "description": "Creates sessions and spawns workers",
            "capabilities": ["orchestration", "spawn"],
            "model": "",
            "max_turns": 0,
        }
    )
    await client.publish(
        topic_discovery("gateway"),
        gateway_card,
        qos=1,
        retain=True,
    )

    for workflow_id, workflow_def in workflows.items():
        card_payload = json.dumps(
            {
                "workflow_id": workflow_id,
                "name": workflow_def.name,
                "description": workflow_def.description,
                "variables": workflow_def.variables,
                "tasks": [
                    {
                        "id": t.id,
                        "agent": t.agent,
                        "next": t.next or "",
                        "needs": list(t.needs),
                        "model": t.model or "",
                    }
                    for t in workflow_def.tasks
                ],
                "agents": list({t.agent for t in workflow_def.tasks}),
            }
        )
        await client.publish(
            topic_discovery(f"workflow/{workflow_id}"),
            card_payload,
            qos=1,
            retain=True,
        )

    log.info(
        "Published %d Agent Cards, %d Workflow Cards",
        len(agents) + 1,
        len(workflows),
    )


async def run() -> None:
    """Run the gateway as a long-lived MQTT subscriber (local mode)."""
    gateway_id = uuid.uuid4().hex[:12]
    agents = load_agents()
    workflows = load_workflows()

    if agents:
        log.info("Loaded %d agents: %s", len(agents), ", ".join(agents))
    if workflows:
        log.info("Loaded %d workflows: %s", len(workflows), ", ".join(workflows))

    request_topic = topic_request("gateway")

    async with aiomqtt.Client(
        MQTT_HOST,
        MQTT_PORT,
        identifier=f"{A2A_ORG}/{A2A_UNIT}/gateway-{gateway_id}",
        protocol=aiomqtt.ProtocolVersion.V5,
    ) as client:
        await _publish_discovery(client, agents, workflows)

        # Subscribe to inbound requests and worker events
        await client.subscribe(request_topic, qos=1)
        await client.subscribe(topic_event_wildcard(), qos=1)
        log.info("Gateway ready, listening on %s", request_topic)

        async for mqtt_msg in client.messages:
            topic = str(mqtt_msg.topic)
            payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
            if not payload:
                continue

            if topic == request_topic:
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
                            code=-32005,
                        )
                    continue
                await handle_request(
                    client,
                    payload,
                    caller_reply,
                    caller_corr,
                    agents,
                    workflows,
                )

            elif "/event/" in topic and topic.endswith("/dead"):
                await handle_dead_event(payload)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("Gateway shutting down")


if __name__ == "__main__":
    main()
