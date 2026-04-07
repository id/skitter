"""In-memory session state for dependency resolution."""

from dataclasses import dataclass, field

from skitter.a2a import TaskState, TaskTarget


@dataclass
class SessionTask:
    """Per-task state within a session."""

    agent: str
    description: str
    needs: list[str] = field(default_factory=list)
    terminal: bool = False
    target: TaskTarget | None = None
    dispatch_correlation: str = ""  # MQTT Correlation Data sent with dispatch
    dispatch_task_id: str = ""  # A2A Task.id sent to agent; used for CancelTask
    reply_topic: str = ""  # MQTT reply topic for unsubscribe after completion


@dataclass
class SessionState:
    """In-memory state for an active session."""

    session_id: str  # internal; coordinator-generated UUID
    request_task_id: str  # incoming A2A Task.id; used for dedup and wire replies
    app_version_id: str
    app_id: str = ""
    context_id: str = ""
    conversation_history: str = ""
    caller_reply_topic: str = ""
    caller_correlation: str = ""
    graph: dict[str, SessionTask] = field(default_factory=dict)
    results: dict[str, str] = field(default_factory=dict)
    pending: set[str] = field(default_factory=set)
    inflight: set[str] = field(default_factory=set)
    failed: set[str] = field(default_factory=set)
    variables: dict[str, str] = field(default_factory=dict)

    @property
    def a2a_state(self) -> TaskState:
        """Derive A2A task state from session progress."""
        if self.pending or self.inflight:
            return TaskState.WORKING
        if self.failed:
            return TaskState.FAILED
        return TaskState.COMPLETED


def compute_ready(state: SessionState) -> list[str]:
    """Return node_ids that are pending and have all needs satisfied."""
    ready = []
    for tid in list(state.pending):
        task = state.graph[tid]
        if all(n in state.results or n in state.failed for n in task.needs):
            if any(n in state.failed for n in task.needs):
                continue
            ready.append(tid)
    return ready


def propagate_failure(state: SessionState, failed_tid: str) -> list[str]:
    """Mark all transitively dependent tasks as failed. Returns newly failed node_ids."""
    newly_failed = []
    queue = [failed_tid]
    while queue:
        tid = queue.pop(0)
        for other_tid, task in state.graph.items():
            if other_tid in state.failed:
                continue
            if tid in task.needs:
                state.failed.add(other_tid)
                state.pending.discard(other_tid)
                newly_failed.append(other_tid)
                queue.append(other_tid)
    return newly_failed


def build_context(state: SessionState, task: SessionTask) -> str:
    """Build context string from upstream results for a join task."""
    if not task.needs:
        return ""
    parts = []
    for need_id in task.needs:
        if need_id in state.results:
            parts.append(f"## Input from task '{need_id}'\n{state.results[need_id]}")
    return "\n\n".join(parts)


def is_graph_task_terminal(t: dict) -> bool:
    """True if the graph task dict has ``terminal: true``."""
    return bool(t.get("terminal", False))


def find_terminal_tasks(state: SessionState) -> list[str]:
    """Find terminal tasks in a session."""
    return [tid for tid, task in state.graph.items() if task.terminal]
