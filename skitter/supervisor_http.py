"""HTTP-mode supervisor — handles EMQX webhook POSTs.

Triggered by EMQX webhook rules on:
  - request/{o}/{u}/+  → new agent/workflow requests
  - event/{o}/{u}/+/+  → dead events for crash recovery

Publishes responses via EMQX REST API (no persistent MQTT connection).
This module exposes a WSGI app for deployment as a Cloudflare Worker
or any HTTP server.
"""

import json
import logging

from skitter import emqx
from skitter.config import AgentDef, WorkflowDef
from skitter.mqtt import topic_discovery, topic_session
from skitter.storage import load_agents, load_cards, load_workflows
from skitter.supervisor import build_dispatch_spec, create_session
from skitter.types import (
    A2ARequest,
    A2AResponse,
    A2A_RESPONDER_UNAVAILABLE,
    A2A_TRANSPORT_PROTOCOL_ERROR,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.supervisor_http")

# Cached config (loaded once, refreshed on reload)
_agents: dict[str, AgentDef] = {}
_workflows: dict[str, WorkflowDef] = {}
_cards: dict[str, str] = {}


def reload_config() -> None:
    """Reload agents, workflows, and cards from storage."""
    global _agents, _workflows, _cards
    _agents = load_agents()
    _workflows = load_workflows()
    _cards = load_cards()
    log.info(
        "Loaded %d agents, %d workflows, %d cards",
        len(_agents),
        len(_workflows),
        len(_cards),
    )


def _ensure_loaded() -> None:
    """Load config on first request if not yet loaded."""
    if not _agents and not _workflows:
        reload_config()


def publish_discovery() -> None:
    """Publish all discovery cards via EMQX REST API."""
    for card_id, card_json in _cards.items():
        emqx.publish(topic_discovery(card_id), card_json, qos=1, retain=True)
    log.info("Published %d discovery cards", len(_cards))


def _parse_agent_id_from_topic(topic: str) -> str:
    """Extract agent_id from $a2a/v1/request/{org}/{unit}/{agent_id}."""
    parts = topic.split("/")
    return parts[5] if len(parts) >= 6 else ""


def _send_error(
    reply_topic: str,
    correlation: str,
    message: str,
    code: int = -32602,
    a2a_error: str = "",
) -> None:
    """Publish error response via EMQX REST API."""
    if not reply_topic:
        return
    error_data: dict = {"code": code, "message": message}
    if a2a_error:
        error_data["data"] = {"a2a_error": a2a_error}
    resp = A2AResponse(id=correlation or "", error=error_data)
    props = {"correlation_data": correlation} if correlation else None
    emqx.publish(reply_topic, resp.to_json(), qos=1, properties=props)


def handle_request(
    topic: str,
    payload: str,
    response_topic: str,
    correlation_data: str,
) -> dict:
    """Handle an inbound request webhook. Returns spawn instructions.

    Returns a dict with:
        {"session_id": str, "workers": [{"agent": str, "session_id": str, "task_id": str}, ...]}
    The caller (CF Worker or local adapter) is responsible for spawning workers.
    """
    _ensure_loaded()
    agents = _agents  # snapshot reference
    workflows = _workflows

    agent_id = _parse_agent_id_from_topic(topic)
    if agent_id == "supervisor":
        return {}

    if not response_topic or not correlation_data:
        log.warning("Request missing Response Topic or Correlation Data")
        if response_topic:
            _send_error(
                response_topic,
                correlation_data,
                "Request must include MQTT v5 Response Topic and Correlation Data",
                code=A2A_TRANSPORT_PROTOCOL_ERROR,
                a2a_error="transport_protocol_error",
            )
        return {}

    try:
        req = A2ARequest.from_json(payload)
    except Exception as e:
        log.error("Bad inbound JSON-RPC: %s", e)
        return {}

    workflow_id = ""
    if agent_id.startswith("workflow-"):
        workflow_id = agent_id.removeprefix("workflow-")
        agent_id = ""

    if agent_id:
        if agent_id not in agents:
            _send_error(
                response_topic,
                correlation_data,
                f"Unknown agent: {agent_id}",
                code=A2A_RESPONDER_UNAVAILABLE,
                a2a_error="responder_unavailable",
            )
            return {}
        session = create_session(
            req.session_id, req.text, agent_id=agent_id, text=req.text, agents=agents
        )
    elif workflow_id:
        workflow = workflows.get(workflow_id)
        if not workflow:
            _send_error(
                response_topic,
                correlation_data,
                f"Unknown workflow: {workflow_id}",
                code=A2A_RESPONDER_UNAVAILABLE,
                a2a_error="responder_unavailable",
            )
            return {}
        session = create_session(
            req.session_id,
            req.text,
            workflow=workflow,
            variables=req.variables,
            agents=agents,
        )
    else:
        default_agent = "skitter"
        if default_agent not in agents:
            _send_error(
                response_topic,
                correlation_data,
                "No default agent configured.",
            )
            return {}
        agent_id = default_agent
        session = create_session(
            req.session_id, req.text, agent_id=agent_id, text=req.text, agents=agents
        )

    session.caller_reply_topic = response_topic
    session.caller_correlation = correlation_data

    for task_name in session.tasks:
        session.task_dispatches[task_name] = build_dispatch_spec(
            session, task_name, agents
        )

    # Publish session as retained event via EMQX REST API
    emqx.publish(
        topic_session(session.session_id),
        session.to_json(),
        qos=1,
        retain=True,
    )

    # Mark all tasks as running and re-publish
    workers = []
    for task_name, st in session.tasks.items():
        st.status = "running"
        workers.append(
            {
                "agent": st.agent,
                "session_id": session.session_id,
                "task_id": st.task_id,
            }
        )

    emqx.publish(
        topic_session(session.session_id),
        session.to_json(),
        qos=1,
        retain=True,
    )

    label = f"agent '{agent_id}'" if agent_id else f"workflow '{workflow_id}'"
    log.info(
        "%s started for %s (%d tasks)",
        label.capitalize(),
        session.session_id,
        len(session.tasks),
    )

    return {"session_id": session.session_id, "workers": workers}


def handle_event(topic: str, payload: str) -> dict:
    """Handle event webhook (dead events for crash recovery).

    Returns spawn instructions for dead workers:
        {"agent": str, "session_id": str, "task_id": str}
    or empty dict if not a dead event.
    """
    if not topic.endswith("/dead"):
        return {}

    try:
        data = json.loads(payload)
    except Exception:
        return {}

    task_id = data.get("task_id", "")
    agent = data.get("agent", "")
    session_id = data.get("session_id", "")

    if not task_id or not agent or not session_id:
        log.warning("Dead event missing fields: %s", data)
        return {}

    log.warning("Worker dead for task %s — respawn needed", task_id)
    return {"agent": agent, "session_id": session_id, "task_id": task_id}


# --- WSGI app ---


def wsgi_app(environ: dict, start_response) -> list[bytes]:
    """Minimal WSGI app for EMQX webhook handler.

    Routes:
        POST /request  — handle agent/workflow request
        POST /event    — handle dead events
        POST /reload   — reload config and republish discovery
        GET  /health   — health check
    """
    method = environ.get("REQUEST_METHOD", "")
    path = environ.get("PATH_INFO", "")

    if method == "GET" and path == "/health":
        start_response("200 OK", [("Content-Type", "application/json")])
        return [json.dumps({"status": "ok"}).encode()]

    if method != "POST":
        start_response("405 Method Not Allowed", [])
        return [b""]

    try:
        content_length = int(environ.get("CONTENT_LENGTH", 0))
        body = environ["wsgi.input"].read(content_length)
        webhook = json.loads(body)
    except Exception as e:
        log.error("Bad webhook body: %s", e)
        start_response("400 Bad Request", [])
        return [b""]

    if path == "/request":
        topic = webhook.get("topic", "")
        payload = webhook.get("payload", "")
        props = webhook.get("pub_props", {})
        response_topic = props.get("Response-Topic", "")
        correlation_data = props.get("Correlation-Data", "")

        result = handle_request(topic, payload, response_topic, correlation_data)
        start_response("200 OK", [("Content-Type", "application/json")])
        return [json.dumps(result).encode()]

    elif path == "/event":
        topic = webhook.get("topic", "")
        payload = webhook.get("payload", "")
        result = handle_event(topic, payload)
        start_response("200 OK", [("Content-Type", "application/json")])
        return [json.dumps(result).encode()]

    elif path == "/reload":
        reload_config()
        publish_discovery()
        start_response("200 OK", [("Content-Type", "application/json")])
        return [json.dumps({"status": "reloaded"}).encode()]

    start_response("404 Not Found", [])
    return [b""]
