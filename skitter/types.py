from dataclasses import asdict, dataclass, field
import json
import time


# --- A2A error codes (§ Mandatory Binding-Specific Error Mapping) ---

A2A_RESPONDER_UNAVAILABLE = -32004
A2A_TRANSPORT_PROTOCOL_ERROR = -32005


@dataclass
class A2AResponse:
    """JSON-RPC 2.0 response wrapper for A2A messages."""

    id: str
    result: dict | None = None
    error: dict | None = None

    def to_json(self) -> str:
        d: dict = {"jsonrpc": "2.0", "id": self.id}
        if self.error is not None:
            d["error"] = self.error
        else:
            d["result"] = self.result or {}
        return json.dumps(d)


def make_status_event(
    request_id: str,
    task_id: str,
    state: str,
    message: str = "",
    message_type: str = "",
    artifact_text: str = "",
    task_name: str = "",
) -> str:
    """Build a TaskStatusUpdateEvent JSON-RPC response (A2A streaming reply).

    Used for both streaming updates (state="working") and terminal results
    (state="completed"/"failed"/"cancelled").

    task_id is the A2A Task.id (= session_id). task_name is the internal
    workflow task name, carried in metadata for dashboard routing.
    """
    metadata: dict = {}
    if message_type:
        metadata["type"] = message_type
    if task_name:
        metadata["task_name"] = task_name
    status: dict = {"state": state}
    if message:
        status["message"] = message
    if metadata:
        status["metadata"] = metadata
    result: dict = {
        "type": "TaskStatusUpdateEvent",
        "taskId": task_id,
        "status": status,
    }
    if artifact_text:
        result["artifact"] = {
            "parts": [{"type": "text", "text": artifact_text}],
        }
    return json.dumps({"jsonrpc": "2.0", "id": request_id, "result": result})


# --- Reply classification (used by all consumers) ---

REPLY_SUBMITTED = "submitted"
REPLY_TEXT = "text"
REPLY_TOOL = "tool_use"
REPLY_TERMINAL = "terminal"
REPLY_ERROR = "error"


def classify_reply(data: dict) -> tuple[str, str]:
    """Classify an A2A reply message. Returns (kind, content).

    kind is one of: REPLY_TEXT, REPLY_TOOL, REPLY_TERMINAL, REPLY_ERROR,
    or "" for unrecognized messages.
    """
    if "error" in data:
        err = data["error"]
        return REPLY_ERROR, err.get("message", str(err))

    result = data.get("result", {})
    if result.get("type") != "TaskStatusUpdateEvent":
        return "", ""

    status = result.get("status", {})
    state = status.get("state", "")

    if state == "submitted":
        task_id = result.get("taskId", "")
        return REPLY_SUBMITTED, task_id

    if state == "working":
        message = status.get("message", "")
        metadata = status.get("metadata", {})
        msg_type = metadata.get("type", "text")
        if msg_type == "tool_use":
            return REPLY_TOOL, message
        return REPLY_TEXT, message

    if state in ("completed", "failed", "cancelled"):
        artifact = result.get("artifact", {})
        parts = artifact.get("parts", [])
        artifact_text = parts[0].get("text", "") if parts else ""
        return REPLY_TERMINAL, artifact_text

    return "", ""


# --- Core messaging types ---


@dataclass
class A2ARequest:
    """JSON-RPC 2.0 request for A2A tasks/send."""

    text: str
    request_id: str
    sender: str = ""
    variables: dict[str, str] = field(default_factory=dict)

    def to_json(self) -> str:
        metadata: dict = {}
        if self.sender:
            metadata["sender"] = self.sender
        if self.variables:
            metadata["variables"] = self.variables
        params: dict = {
            "message": {
                "role": "user",
                "parts": [{"type": "text", "text": self.text}],
            },
        }
        if metadata:
            params["metadata"] = metadata
        return json.dumps(
            {
                "jsonrpc": "2.0",
                "id": self.request_id,
                "method": "tasks/send",
                "params": params,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "A2ARequest":
        d = json.loads(data)
        params = d.get("params", {})
        message = params.get("message", {})
        metadata = params.get("metadata", {})
        parts = message.get("parts", [])
        text = parts[0].get("text", "") if parts else ""
        return cls(
            text=text,
            request_id=d.get("id", f"req-{time.time_ns()}"),
            sender=metadata.get("sender", ""),
            variables=metadata.get("variables", {}),
        )


@dataclass
class SessionTask:
    """Task record — single source of truth for task data."""

    id: str
    agent: str
    description: str
    model: str = ""
    runtime: str = "claude"
    status: str = "pending"
    next: str = ""
    needs: list[str] = field(default_factory=list)
    workspace: str = ""  # persistent workspace path (slug or slug/task_id)


@dataclass
class Session:
    session_id: str
    workflow_id: str = ""
    agent_id: str = ""
    label: str = ""
    variables: dict[str, str] = field(default_factory=dict)
    caller_reply_topic: str = ""
    caller_correlation: str = ""
    tasks: dict[str, SessionTask] = field(default_factory=dict)
    created_at: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(asdict(self))

    @classmethod
    def from_json(cls, data: str) -> "Session":
        d = json.loads(data)
        tasks = {k: SessionTask(**v) for k, v in d.get("tasks", {}).items()}
        d["tasks"] = tasks
        return cls(**d)
