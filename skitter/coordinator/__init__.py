"""Coordinator package.

Re-exports public API so ``from skitter.coordinator import X`` continues to work.
"""

from skitter.coordinator.models import (
    SessionState,
    SessionTask,
    build_context,
    compute_ready,
    find_terminal_tasks,
    is_graph_task_terminal,
    propagate_failure,
)
from skitter.coordinator.registry import DiscoveryRegistry
from skitter.coordinator.service import Coordinator, main, _parse_agent_id_from_topic

__all__ = [
    "Coordinator",
    "DiscoveryRegistry",
    "SessionState",
    "SessionTask",
    "_parse_agent_id_from_topic",
    "build_context",
    "compute_ready",
    "find_terminal_tasks",
    "is_graph_task_terminal",
    "main",
    "propagate_failure",
]
