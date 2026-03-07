from dataclasses import dataclass, field
import json
import time


# --- A2A error codes (§ Mandatory Binding-Specific Error Mapping) ---

A2A_INVALID_PARAMS = -32602
A2A_REQUEST_EXPIRED = -32003
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

    @classmethod
    def from_json(cls, data: str) -> "A2AResponse":
        d = json.loads(data)
        return cls(
            id=d["id"],
            result=d.get("result"),
            error=d.get("error"),
        )


@dataclass
class TaskStatusUpdate:
    """Terminal status update published on the Response Topic (QoS 1)."""

    task_id: str
    state: str  # "completed", "failed", "cancelled"
    result: str = ""
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(
            {
                "task_id": self.task_id,
                "state": self.state,
                "result": self.result,
                "timestamp": self.timestamp,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "TaskStatusUpdate":
        d = json.loads(data)
        return cls(
            task_id=d["task_id"],
            state=d["state"],
            result=d.get("result", ""),
            timestamp=d.get("timestamp", time.time()),
        )


@dataclass
class StreamItem:
    """A single streaming item published to the Response Topic (QoS 0)."""

    task_id: str
    seq: int
    type: str  # "text", "tool_use", "tool_result", "thinking", "error"
    content: str
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(
            {
                "task_id": self.task_id,
                "seq": self.seq,
                "type": self.type,
                "content": self.content,
                "timestamp": self.timestamp,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "StreamItem":
        d = json.loads(data)
        return cls(
            task_id=d["task_id"],
            seq=d["seq"],
            type=d["type"],
            content=d["content"],
            timestamp=d.get("timestamp", time.time()),
        )


# --- Core messaging types (kept from pre-A2A) ---


@dataclass
class InboundMessage:
    text: str
    sender: str
    session_id: str
    timestamp: float = field(default_factory=time.time)
    workflow_id: str = ""
    workflow_vars: dict[str, str] = field(default_factory=dict)
    agent_id: str = ""

    def to_json(self) -> str:
        d: dict = {
            "text": self.text,
            "sender": self.sender,
            "session_id": self.session_id,
            "timestamp": self.timestamp,
        }
        if self.workflow_id:
            d["workflow_id"] = self.workflow_id
        if self.workflow_vars:
            d["workflow_vars"] = self.workflow_vars
        if self.agent_id:
            d["agent_id"] = self.agent_id
        return json.dumps(d)

    @classmethod
    def from_json(cls, data: str) -> "InboundMessage":
        d = json.loads(data)
        return cls(
            text=d["text"],
            sender=d["sender"],
            session_id=d["session_id"],
            timestamp=d.get("timestamp", time.time()),
            workflow_id=d.get("workflow_id", ""),
            workflow_vars=d.get("workflow_vars", {}),
            agent_id=d.get("agent_id", ""),
        )


@dataclass
class AgentMessage:
    task_id: str
    session_id: str
    description: str
    agent: str = ""
    context: str = ""
    model: str = ""
    runtime: str = "claude"
    next: str = ""
    caller_reply_topic: str = ""
    caller_correlation: str = ""
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(
            {
                "task_id": self.task_id,
                "session_id": self.session_id,
                "description": self.description,
                "agent": self.agent,
                "context": self.context,
                "model": self.model,
                "runtime": self.runtime,
                "next": self.next,
                "caller_reply_topic": self.caller_reply_topic,
                "caller_correlation": self.caller_correlation,
                "timestamp": self.timestamp,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "AgentMessage":
        d = json.loads(data)
        return cls(
            task_id=d["task_id"],
            session_id=d["session_id"],
            description=d["description"],
            agent=d.get("agent", ""),
            context=d.get("context", ""),
            model=d.get("model", ""),
            runtime=d.get("runtime", "claude"),
            next=d.get("next", ""),
            caller_reply_topic=d.get("caller_reply_topic", ""),
            caller_correlation=d.get("caller_correlation", ""),
            timestamp=d.get("timestamp", time.time()),
        )


@dataclass
class SessionTask:
    """Lightweight task record for status tracking and dashboard display."""

    id: str
    task_id: str
    agent: str
    description: str
    model: str = ""
    status: str = "pending"
    next: str = ""
    needs: list[str] = field(default_factory=list)

    def to_dict(self) -> dict:
        return {
            "id": self.id,
            "task_id": self.task_id,
            "agent": self.agent,
            "description": self.description,
            "model": self.model,
            "status": self.status,
            "next": self.next,
            "needs": self.needs,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "SessionTask":
        return cls(
            id=d["id"],
            task_id=d["task_id"],
            agent=d["agent"],
            description=d["description"],
            model=d.get("model", ""),
            status=d.get("status", "pending"),
            next=d.get("next", ""),
            needs=d.get("needs", []),
        )


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
    task_dispatches: dict[str, dict] = field(default_factory=dict)
    result: str = ""

    def to_json(self) -> str:
        return json.dumps(
            {
                "session_id": self.session_id,
                "workflow_id": self.workflow_id,
                "agent_id": self.agent_id,
                "label": self.label,
                "variables": self.variables,
                "caller_reply_topic": self.caller_reply_topic,
                "caller_correlation": self.caller_correlation,
                "tasks": {k: v.to_dict() for k, v in self.tasks.items()},
                "task_dispatches": self.task_dispatches,
                "result": self.result,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "Session":
        d = json.loads(data)
        tasks = {k: SessionTask.from_dict(v) for k, v in d.get("tasks", {}).items()}
        return cls(
            session_id=d["session_id"],
            workflow_id=d.get("workflow_id", ""),
            agent_id=d.get("agent_id", ""),
            label=d.get("label", ""),
            variables=d.get("variables", {}),
            caller_reply_topic=d.get("caller_reply_topic", ""),
            caller_correlation=d.get("caller_correlation", ""),
            tasks=tasks,
            task_dispatches=d.get("task_dispatches", {}),
            result=d.get("result", ""),
        )
