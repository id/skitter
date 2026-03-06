"""Config loading backends — filesystem (default) or R2 (future)."""

import os

from skitter.config import (
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
