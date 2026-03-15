"""Runtime state query handler.

Registered as ``skitter-runtime`` — handles structured text queries and
returns JSON results in standard A2A TaskStatusUpdateEvent replies.

Queries:
    list apps           → all apps with current version info
    get app {id}        → app details + version history
    list sessions [id]  → sessions, optionally filtered by app
    get session {id}    → session with all task states
    cancel session {id} → cancel a running session
"""

import json
import logging

from skitter.config import AgentDef
from skitter.db import DB
from skitter.discovery import build_card

log = logging.getLogger("skitter.runtime_api")

AGENT_ID = "skitter-runtime"
CANCEL_KEY = "cancelled"


def runtime_card() -> dict:
    """Build the discovery card for the skitter-runtime agent."""
    agent = AgentDef(
        id=AGENT_ID,
        name="Skitter Runtime",
        description="Query and manage Skitter runtime state",
    )
    return build_card(agent)


def handle_query(db: DB, text: str) -> str:
    """Parse a query command and return a JSON result string."""
    parts = text.strip().split(None, 2)
    if not parts:
        return json.dumps({"error": "Empty query"})

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

    return json.dumps({"error": f"Unknown query: {text.strip()}"})


def _list_apps(db: DB) -> str:
    apps = db.list_apps()
    result = []
    for app in apps:
        current = db.get_current_version(app.id)
        result.append(
            {
                "id": app.id,
                "name": app.name,
                "description": app.description,
                "current_version": current.version if current else None,
                "current_version_id": current.id if current else None,
            }
        )
    return json.dumps({"apps": result})


def _get_app(db: DB, app_id: str) -> str:
    app = db.get_app(app_id)
    if not app:
        return json.dumps({"error": f"App not found: {app_id}"})
    versions = db.list_app_versions(app_id)
    return json.dumps(
        {
            "id": app.id,
            "name": app.name,
            "description": app.description,
            "versions": [
                {"id": v.id, "version": v.version, "created_at": v.created_at}
                for v in versions
            ],
        }
    )


def _list_sessions(db: DB, app_id: str | None = None) -> str:
    sessions = db.list_sessions(app_id=app_id)
    return json.dumps(
        {
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


def _get_session(db: DB, session_id: str) -> str:
    session = db.get_session(session_id)
    if not session:
        return json.dumps({"error": f"Session not found: {session_id}"})
    tasks = db.list_tasks(session_id)
    return json.dumps(
        {
            "id": session.id,
            "app_version_id": session.app_version_id,
            "state": session.state,
            "created_at": session.created_at,
            "completed_at": session.completed_at,
            "tasks": [
                {
                    "task_id": t.task_id,
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


def _cancel_session(db: DB, session_id: str) -> str:
    session = db.get_session(session_id)
    if not session:
        return json.dumps({"error": f"Session not found: {session_id}"})
    if session.state != "running":
        return json.dumps({"error": f"Session not running (state={session.state})"})
    db.update_session_state(session_id, "cancelled")
    return json.dumps({CANCEL_KEY: session_id})
