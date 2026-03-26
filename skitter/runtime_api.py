"""Runtime state query handler.

Registered as ``skitter`` — handles structured queries and
returns typed result objects. JSON serialization happens at the
coordinator's reply boundary.

Queries:
    list apps           → all apps with current version info
    get app {id}        → app details + version history
    list sessions [id]  → sessions, optionally filtered by app
    get session {id}    → session with all task states
    cancel session {id} → cancel a running session
    create app {json}   → create a composed app from agent IDs + instructions
    delete app {id}     → delete an app and all its versions/sessions/tasks
"""

from __future__ import annotations

import json
import logging
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import TYPE_CHECKING

import uuid

from skitter.config import AgentDef
from skitter.db import App, AppVersion, DB
from skitter.discovery import build_card
from skitter.graph_gen import GraphValidationError, generate_graph

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


def runtime_card() -> dict:
    """Build the discovery card for the skitter agent."""
    agent = AgentDef(
        id=AGENT_ID,
        name="Skitter Runtime",
        description="Query and manage Skitter runtime state",
    )
    return build_card(agent)


async def handle_query(
    db: DB, text: str, registry: DiscoveryRegistry | None = None
) -> QueryResult:
    """Parse a query command and return a typed result."""
    parts = text.strip().split(None, 2)
    if not parts:
        return ErrorResult("Empty query")

    verb = parts[0].lower()
    noun = parts[1].lower() if len(parts) > 1 else ""
    arg = parts[2].strip() if len(parts) > 2 else ""

    if verb == "list" and noun in ("apps", "app"):
        return _list_apps(db)
    if verb == "get" and noun == "app" and arg:
        return _get_app(db, arg)
    if verb == "list" and noun in ("sessions", "session"):
        return _list_sessions(db, app_id=arg or None)
    if verb == "get" and noun == "session" and arg:
        return _get_session(db, arg)
    if verb == "cancel" and noun == "session" and arg:
        return _cancel_session(db, arg)
    if verb == "create" and noun == "app" and arg:
        return await _handle_create_app(db, arg, registry)
    if verb == "delete" and noun == "app" and arg:
        return _delete_app(db, arg)

    return ErrorResult(f"Unknown query: {text.strip()}")


def _list_apps(db: DB) -> DataResult:
    apps = db.list_apps()
    items = []
    for app in apps:
        current = db.get_current_version(app.id)
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


def _get_app(db: DB, app_id: str) -> QueryResult:
    app = db.get_app(app_id)
    if not app:
        return ErrorResult(f"App not found: {app_id}")
    versions = db.list_app_versions(app_id)
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


def _list_sessions(db: DB, app_id: str | None = None) -> DataResult:
    sessions = db.list_sessions(app_id=app_id)
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


def _resolve_session(db: DB, ref: str):
    """Look up a session by internal ID or request_task_id."""
    return db.get_session(ref) or db.get_session_by_request_task_id(ref)


def _get_session(db: DB, session_id: str) -> QueryResult:
    session = _resolve_session(db, session_id)
    if not session:
        return ErrorResult(f"Session not found: {session_id}")
    tasks = db.list_tasks(session.id)
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


def _cancel_session(db: DB, session_id: str) -> QueryResult:
    session = _resolve_session(db, session_id)
    if not session:
        return ErrorResult(f"Session not found: {session_id}")
    if session.state != "running":
        return ErrorResult(f"Session not running (state={session.state})")
    db.update_session_state(session.id, "canceled")
    return CancelSessionResult(session_id=session.id)


def _delete_app(db: DB, app_id: str) -> QueryResult:
    app = db.get_app(app_id)
    if not app:
        return ErrorResult(f"App not found: {app_id}")
    running = [s for s in db.list_sessions(app_id=app_id) if s.state == "running"]
    if running:
        return ErrorResult(
            f"App has {len(running)} running session(s); cancel them first"
        )
    db.delete_app(app_id)
    return DeleteAppResult(app_id=app_id)


def create_app(
    db: DB,
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

    existing = db.get_app(app_id)
    if existing:
        current = db.get_current_version(app_id)
        next_version = (current.version + 1) if current else 1
    else:
        db.create_app(App(id=app_id, name=name, description=description))
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
    db.create_app_version(av)

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
            }
            for t in tasks
        ],
    }
    card = build_card(agent_def, metadata=metadata)
    card_json = json.dumps(card)

    db.update_app_card(app_id, card_json)
    app = db.get_app(app_id)

    log.info("Created app %s v%d (%d tasks)", app_id, next_version, len(tasks))
    return app, av, card_json


async def _handle_create_app(
    db: DB, arg: str, registry: DiscoveryRegistry | None
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
        graph = await generate_graph(instructions, cards)
    except GraphValidationError as e:
        return ErrorResult(f"Graph generation failed: {e}")
    except Exception:
        log.exception("Unexpected error generating graph")
        return ErrorResult("Graph generation failed unexpectedly")

    description = spec.get("description", "")
    app, version, card_json = create_app(
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
