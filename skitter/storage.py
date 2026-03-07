"""Config loading backends — filesystem (default)."""

import json
import os

from skitter.config import (
    CARDS_DIR,
    AgentDef,
    WorkflowDef,
    load_agents as _load_fs,
    load_workflows as _load_fs_workflows,
)

STORAGE_MODE = os.environ.get("SKITTER_STORAGE", "filesystem")


def load_agents() -> dict[str, AgentDef]:
    if STORAGE_MODE == "filesystem":
        return _load_fs()
    raise ValueError(f"Unknown storage mode: {STORAGE_MODE}")


def load_workflows() -> dict[str, WorkflowDef]:
    if STORAGE_MODE == "filesystem":
        return _load_fs_workflows()
    raise ValueError(f"Unknown storage mode: {STORAGE_MODE}")


def load_cards() -> dict[str, str]:
    """Load pre-built agent card JSON files from ~/.skitter/cards/."""
    cards: dict[str, str] = {}
    if not CARDS_DIR.is_dir():
        return cards
    for path in sorted(CARDS_DIR.glob("*.json")):
        try:
            card_data = json.loads(path.read_text())
            cards[path.stem] = json.dumps(card_data)
        except Exception:
            pass
    return cards
