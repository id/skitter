"""App creation and versioning.

Handles requests from NexHub to create/update composed apps.
Input: selected A2A cards + wiring instructions.
Output: app ID + discovery card.
"""

import json
import logging
import uuid

from skitter.config import AgentDef
from skitter.db import App, AppVersion, DB
from skitter.discovery import build_card

log = logging.getLogger("skitter.apps")


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
    """Create or update a composed app.

    Returns (app, version, card_json).
    """
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

    # Build discovery card for the composed app
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
