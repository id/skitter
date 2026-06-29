"""Runtime state query handler.

Registered as ``skitter`` -- handles structured queries and
returns typed result objects. JSON serialization happens at the
coordinator's reply boundary.

Queries:
    list apps           -> all apps with current version info
    get app {id}        -> app details + version history
    list sessions [id]  -> sessions, optionally filtered by app
    get session {id}    -> session with all task states
    cancel session {id} -> cancel a running session
    create app {json}   -> create a composed app from agent IDs + instructions
    delete app {id}     -> delete an app and all its versions/sessions/tasks
"""

from __future__ import annotations

import json
import logging
import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

import uuid

from skitter.a2a import TaskState
from skitter.config import AgentDef
from skitter.db import App, AppVersion, AsyncDB
from skitter.discovery import (
    build_card,
    extract_app_tasks,
    is_task_agent_card,
    task_needs,
)
from skitter.graph_gen import GraphValidationError, generate_graph
from skitter.llm import complete, strip_code_fence

if TYPE_CHECKING:
    from skitter.coordinator import DiscoveryRegistry

log = logging.getLogger("skitter.runtime_api")

AGENT_ID = "skitter"


# --- Typed result objects ---


class QueryResult(ABC):
    """Base for runtime query results."""

    @abstractmethod
    def to_dict(self) -> dict: ...


@dataclass
class DataResult(QueryResult):
    """Generic data result (list, get queries)."""

    data: dict

    def to_dict(self) -> dict:
        return self.data


@dataclass
class ErrorResult(QueryResult):
    """Query failed."""

    message: str

    def to_dict(self) -> dict:
        return {"error": self.message}


@dataclass
class MessageResult(QueryResult):
    """Plain user-facing chat result."""

    message: str

    def to_dict(self) -> dict:
        return {"message": self.message}


@dataclass
class CancelSessionResult(QueryResult):
    """Session canceled; coordinator should clean up."""

    session_id: str

    def to_dict(self) -> dict:
        return {"canceled": self.session_id}


@dataclass
class CreateAppResult(QueryResult):
    """App created; coordinator should register MQTT connection."""

    app_id: str
    version: int
    card_json: str
    message: str = ""

    def to_dict(self) -> dict:
        return {
            "created_app": {
                "app_id": self.app_id,
                "version": self.version,
                "card": json.loads(self.card_json),
            }
        }


@dataclass
class DeleteAppResult(QueryResult):
    """App deleted; coordinator should tear down MQTT connection."""

    app_id: str

    def to_dict(self) -> dict:
        return {"deleted_app": self.app_id}


@dataclass
class RunAppResult(QueryResult):
    """Run an existing app via the coordinator app execution path."""

    app_id: str
    prompt: str
    reason: str = ""

    def to_dict(self) -> dict:
        return {
            "run_app": {
                "app_id": self.app_id,
                "prompt": self.prompt,
                "reason": self.reason,
            }
        }


def coordinator_card() -> dict:
    """Build the discovery card for the skitter agent."""
    agent = AgentDef(
        id=AGENT_ID,
        name="Skitter",
        description="Create, run, and coordinate composed multi-agent workflows",
    )
    return build_card(agent)


async def handle_query(
    db: AsyncDB, text: str, registry: DiscoveryRegistry | None = None
) -> QueryResult:
    """Parse a query command and return a typed result."""
    parts = text.strip().split(None, 2)
    if not parts:
        return ErrorResult("Empty query")

    verb = parts[0].lower()
    noun = parts[1].lower() if len(parts) > 1 else ""
    arg = parts[2].strip() if len(parts) > 2 else ""

    if verb == "list" and noun in ("apps", "app"):
        return await _list_apps(db)
    if verb == "get" and noun == "app" and arg:
        return await _get_app(db, arg)
    if verb == "list" and noun in ("sessions", "session"):
        return await _list_sessions(db, app_id=arg or None)
    if verb == "get" and noun == "session" and arg:
        return await _get_session(db, arg)
    if verb == "cancel" and noun == "session" and arg:
        return await _cancel_session(db, arg)
    if verb == "create" and noun == "app" and arg:
        return await _handle_create_app(db, arg, registry)
    if verb == "delete" and noun == "app" and arg:
        return await _delete_app(db, arg)

    if registry:
        return await _handle_natural_language(db, text.strip(), registry)

    return ErrorResult(f"Unknown query: {text.strip()}")


async def _list_apps(db: AsyncDB) -> DataResult:
    apps = await db.list_apps()
    items = []
    for app in apps:
        current = await db.get_current_version(app.id)
        items.append(
            {
                "id": app.id,
                "name": app.name,
                "description": app.description,
                "current_version": current.version if current else None,
                "current_version_id": current.id if current else None,
            }
        )
    return DataResult(data={"apps": items})


async def _get_app(db: AsyncDB, app_id: str) -> QueryResult:
    app = await db.get_app(app_id)
    if not app:
        return ErrorResult(f"App not found: {app_id}")
    versions = await db.list_app_versions(app_id)
    return DataResult(
        data={
            "id": app.id,
            "name": app.name,
            "description": app.description,
            "versions": [
                {"id": v.id, "version": v.version, "created_at": v.created_at}
                for v in versions
            ],
        }
    )


async def _list_sessions(db: AsyncDB, app_id: str | None = None) -> DataResult:
    sessions = await db.list_sessions(app_id=app_id)
    return DataResult(
        data={
            "sessions": [
                {
                    "id": s.id,
                    "app_version_id": s.app_version_id,
                    "state": s.state,
                    "created_at": s.created_at,
                    "completed_at": s.completed_at,
                }
                for s in sessions
            ],
        }
    )


async def _resolve_session(db: AsyncDB, ref: str):
    """Look up a session by internal ID or request_task_id."""
    return await db.get_session(ref) or await db.get_session_by_request_task_id(ref)


async def _get_session(db: AsyncDB, session_id: str) -> QueryResult:
    session = await _resolve_session(db, session_id)
    if not session:
        return ErrorResult(f"Session not found: {session_id}")
    tasks = await db.list_tasks(session.id)
    return DataResult(
        data={
            "id": session.id,
            "app_version_id": session.app_version_id,
            "state": session.state,
            "created_at": session.created_at,
            "completed_at": session.completed_at,
            "tasks": [
                {
                    "node_id": t.node_id,
                    "agent": t.agent,
                    "state": t.state,
                    "result": t.result,
                    "error": t.error,
                    "started_at": t.started_at,
                    "completed_at": t.completed_at,
                }
                for t in tasks
            ],
        }
    )


async def _cancel_session(db: AsyncDB, session_id: str) -> QueryResult:
    session = await _resolve_session(db, session_id)
    if not session:
        return ErrorResult(f"Session not found: {session_id}")
    if session.state != "running":
        return ErrorResult(f"Session not running (state={session.state})")
    await db.update_session_state(session.id, TaskState.CANCELED)
    return CancelSessionResult(session_id=session.id)


async def _delete_app(db: AsyncDB, app_id: str) -> QueryResult:
    app = await db.get_app(app_id)
    if not app:
        return ErrorResult(f"App not found: {app_id}")
    running = [s for s in await db.list_sessions(app_id=app_id) if s.state == "running"]
    if running:
        return ErrorResult(
            f"App has {len(running)} running session(s); cancel them first"
        )
    await db.delete_app(app_id)
    return DeleteAppResult(app_id=app_id)


_NATURAL_LANGUAGE_SYSTEM = """\
You are Skitter, an MQTT A2A workflow coordinator.

The user talks to you in natural language. Decide whether to answer, create a
workflow app, or run an existing workflow app.

Return only one JSON object with one of these shapes:

{"action":"answer","message":"short user-facing answer"}

{"action":"create_app","name":"workflow name","description":"short description","instructions":"natural language orchestration instructions","agents":["agent-id"]}

{"action":"run_app","app_id":"existing-app-id","prompt":"what the user wants this app to do","reason":"short reason"}

Rules:
- Use only agent IDs listed in available_agents.
- For create_app, choose all relevant agents. If the request is broad, include all available agents.
- Do not invent business-specific workflows. Use the user's words as the source of truth.
- If the user asks to enable, start, open, run, or execute an existing workflow, use run_app.
- If the user includes concrete runtime details, copy them verbatim into run_app.prompt.
- If no existing app clearly matches a run request, answer that the workflow needs to be created first.
- Keep answers concise and conversational.
"""


async def _handle_natural_language(
    db: AsyncDB, text: str, registry: DiscoveryRegistry
) -> QueryResult:
    """Turn natural language into runtime actions without UI-side rules."""
    app_summaries = await _list_app_summaries(db)
    agent_summaries = _list_agent_summaries(registry)

    if not agent_summaries and not app_summaries:
        return MessageResult(
            "I do not see any available agents or workflows yet. Start agents first, "
            "then ask me to create a workflow."
        )

    prompt = json.dumps(
        {
            "user_message": text,
            "existing_apps": app_summaries,
            "available_agents": agent_summaries,
        },
        ensure_ascii=False,
        indent=2,
    )

    try:
        raw = await complete(prompt, system=_NATURAL_LANGUAGE_SYSTEM)
        decision = _parse_llm_json(raw)
    except Exception:
        log.exception("Runtime planner failed")
        return MessageResult(
            "I could not plan that request yet. Please rephrase and try again."
        )

    action = str(decision.get("action", "")).strip().lower()
    if action == "answer":
        message = str(decision.get("message", "")).strip()
        return MessageResult(
            message or "Tell me what workflow you want to create or run."
        )

    if action == "create_app":
        return await _create_app_from_plan(db, decision, registry, agent_summaries)

    if action == "run_app":
        return _run_app_from_plan(decision, app_summaries, fallback_prompt=text)

    return MessageResult(
        "I can create workflows from your request, or run an existing workflow."
    )


async def _list_app_summaries(db: AsyncDB) -> list[dict]:
    apps = await db.list_apps()
    summaries = []
    for app in apps:
        current = await db.get_current_version(app.id)
        tasks = []
        if app.card_json:
            try:
                card = json.loads(app.card_json)
                tasks = extract_app_tasks(card)
            except Exception:
                tasks = []
        summaries.append(
            {
                "id": app.id,
                "name": app.name,
                "description": app.description,
                "current_version": current.version if current else None,
                "tasks": tasks,
            }
        )
    return summaries


def _list_agent_summaries(registry: DiscoveryRegistry) -> list[dict]:
    summaries = []
    for agent_id in registry.list_agents():
        if agent_id == AGENT_ID:
            continue
        card = registry.get(agent_id) or {}
        if is_task_agent_card(card):
            continue
        summaries.append(
            {
                "id": agent_id,
                "name": card.get("name", agent_id),
                "description": card.get("description", ""),
                "status": registry.status(agent_id),
                "skills": [
                    {
                        "id": skill.get("id", ""),
                        "name": skill.get("name", ""),
                        "description": skill.get("description", ""),
                    }
                    for skill in card.get("skills", [])
                    if isinstance(skill, dict)
                ],
            }
        )
    return summaries


def _parse_llm_json(raw: str) -> dict:
    data = json.loads(strip_code_fence(raw))
    if not isinstance(data, dict):
        raise ValueError("planner returned a non-object JSON value")
    return data


async def _create_app_from_plan(
    db: AsyncDB,
    decision: dict,
    registry: DiscoveryRegistry,
    agent_summaries: list[dict],
) -> QueryResult:
    name = str(decision.get("name", "")).strip()
    instructions = str(decision.get("instructions", "")).strip()
    if not name or not instructions:
        return MessageResult(
            "Tell me the workflow name and what the agents should coordinate."
        )

    available_agent_ids = {str(agent["id"]) for agent in agent_summaries}
    requested_agents = [
        str(agent_id)
        for agent_id in decision.get("agents", [])
        if str(agent_id) in available_agent_ids
    ]
    agent_ids = requested_agents or sorted(available_agent_ids)
    if not agent_ids:
        return MessageResult(
            "I do not see any available agents to build that workflow."
        )

    spec = json.dumps(
        {
            "id": _stable_app_id(name),
            "name": name,
            "description": str(decision.get("description", "")).strip(),
            "instructions": instructions,
            "agents": agent_ids,
        },
        ensure_ascii=False,
    )
    result = await _handle_create_app(db, spec, registry)
    if isinstance(result, CreateAppResult):
        result.message = (
            f"Created workflow '{name}' with {len(agent_ids)} agent"
            f"{'' if len(agent_ids) == 1 else 's'}. Ask me to run it when you are ready."
        )
    return result


def _run_app_from_plan(
    decision: dict, app_summaries: list[dict], *, fallback_prompt: str
) -> QueryResult:
    app_ref = str(decision.get("app_id", "")).strip()
    app = _resolve_app_ref(app_ref, app_summaries)
    if not app:
        return MessageResult(
            "I do not see a matching workflow yet. Ask me to create it first."
        )

    prompt = str(decision.get("prompt", "")).strip() or fallback_prompt
    return RunAppResult(
        app_id=str(app["id"]),
        prompt=prompt,
        reason=str(decision.get("reason", "")).strip(),
    )


def _resolve_app_ref(ref: str, app_summaries: list[dict]) -> dict | None:
    normalized = ref.casefold().strip()
    if not normalized:
        return None
    for app in app_summaries:
        if str(app["id"]).casefold() == normalized:
            return app
    for app in app_summaries:
        if str(app["name"]).casefold() == normalized:
            return app
    # Fuzzy fallback: only an unambiguous substring match on the name. Never
    # match the free-text description, and never guess when several names match.
    name_matches = [
        app for app in app_summaries if normalized in str(app["name"]).casefold()
    ]
    return name_matches[0] if len(name_matches) == 1 else None


def _stable_app_id(name: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", name.strip().lower()).strip("-")
    return slug[:48] or uuid.uuid4().hex[:12]


async def create_app(
    db: AsyncDB,
    *,
    app_id: str = "",
    name: str,
    description: str = "",
    source_cards: list[dict] | None = None,
    instructions: str = "",
    graph: dict | None = None,
) -> tuple[App, AppVersion, str]:
    """Create or update a composed app. Returns (app, version, card_json)."""
    app_id = app_id or uuid.uuid4().hex[:12]
    source_cards = source_cards or []

    existing = await db.get_app(app_id)
    if existing:
        current = await db.get_current_version(app_id)
        next_version = (current.version + 1) if current else 1
    else:
        await db.create_app(App(id=app_id, name=name, description=description))
        next_version = 1

    version_id = f"{app_id}-v{next_version}"
    graph_json = json.dumps(graph) if graph else "{}"

    av = AppVersion(
        id=version_id,
        app_id=app_id,
        version=next_version,
        source_cards=json.dumps(source_cards),
        instructions=instructions,
        graph_json=graph_json,
    )
    await db.create_app_version(av)

    # Build and persist discovery card
    agent_def = AgentDef(id=app_id, name=name, description=description)
    tasks = graph.get("tasks", []) if graph else []
    metadata = {
        "variables": graph.get("variables", []) if graph else [],
        "tasks": [
            {
                "id": t["id"],
                "agent": t.get("agent", ""),
                "description": t.get("description", ""),
                "needs": task_needs(t),
                "terminal": bool(t.get("terminal", False)),
            }
            for t in tasks
        ],
    }
    card = build_card(agent_def, metadata=metadata)
    card_json = json.dumps(card)

    await db.update_app_card(app_id, card_json)
    app = await db.get_app(app_id)

    log.info("Created app %s v%d (%d tasks)", app_id, next_version, len(tasks))
    return app, av, card_json


async def _handle_create_app(
    db: AsyncDB, arg: str, registry: DiscoveryRegistry | None
) -> QueryResult:
    """Create a composed app from agent IDs + natural language instructions."""
    try:
        spec = json.loads(arg)
    except json.JSONDecodeError as e:
        return ErrorResult(f"Invalid JSON: {e}")

    name = spec.get("name", "")
    if not name:
        return ErrorResult("Missing 'name' in create app spec")

    instructions = spec.get("instructions", "")
    if not instructions:
        return ErrorResult("Missing 'instructions' in create app spec")

    agent_ids = spec.get("agents", [])
    if not agent_ids:
        return ErrorResult("Missing 'agents' in create app spec")

    if not registry:
        return ErrorResult("No discovery registry available")

    app_id = spec.get("id", None)

    # Look up agent cards from registry.
    # NOTE: registry presence doesn't guarantee the agent is online;
    # discovery cards are retained on the broker after disconnect.
    # Online/offline status is tracked broker-side via LWT.
    cards: dict[str, dict] = {}
    missing = []
    for aid in agent_ids:
        card = registry.get(aid)
        if card:
            cards[aid] = card
        else:
            missing.append(aid)

    if missing:
        return ErrorResult(f"Agents not found: {', '.join(missing)}")

    # Generate orchestration graph via LLM
    try:
        graph = await generate_graph(instructions, cards, required_agent_ids=set(cards))
    except GraphValidationError as e:
        return ErrorResult(f"Graph generation failed: {e}")
    except Exception as e:
        log.exception("Unexpected error generating graph")
        return ErrorResult(f"Graph generation failed: {e}")

    description = spec.get("description", "")
    app, version, card_json = await create_app(
        db,
        app_id=app_id,
        name=name,
        description=description,
        source_cards=list(cards.values()),
        instructions=instructions,
        graph=graph,
    )

    return CreateAppResult(
        app_id=app.id,
        version=version.version,
        card_json=card_json,
    )
