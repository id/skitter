from dataclasses import dataclass, field
import json
import time


# --- A2A types ---


@dataclass
class AgentCard:
    """A2A Agent Card published as retained discovery message."""

    agent_id: str
    name: str
    description: str = ""
    capabilities: list[str] = field(default_factory=list)
    model: str = ""
    max_turns: int = 10

    def to_json(self) -> str:
        return json.dumps(
            {
                "agent_id": self.agent_id,
                "name": self.name,
                "description": self.description,
                "capabilities": self.capabilities,
                "model": self.model,
                "max_turns": self.max_turns,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "AgentCard":
        d = json.loads(data)
        return cls(
            agent_id=d["agent_id"],
            name=d["name"],
            description=d.get("description", ""),
            capabilities=d.get("capabilities", []),
            model=d.get("model", ""),
            max_turns=d.get("max_turns", 10),
        )


@dataclass
class A2ARequest:
    """JSON-RPC 2.0 request wrapper for A2A messages."""

    method: str
    params: dict
    id: str

    def to_json(self) -> str:
        return json.dumps(
            {
                "jsonrpc": "2.0",
                "method": self.method,
                "params": self.params,
                "id": self.id,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "A2ARequest":
        d = json.loads(data)
        return cls(
            method=d["method"],
            params=d.get("params", {}),
            id=d["id"],
        )


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
    pipeline_id: str = ""
    pipeline_vars: dict[str, str] = field(default_factory=dict)
    agent_id: str = ""

    def to_json(self) -> str:
        d: dict = {
            "text": self.text,
            "sender": self.sender,
            "session_id": self.session_id,
            "timestamp": self.timestamp,
        }
        if self.pipeline_id:
            d["pipeline_id"] = self.pipeline_id
        if self.pipeline_vars:
            d["pipeline_vars"] = self.pipeline_vars
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
            pipeline_id=d.get("pipeline_id", ""),
            pipeline_vars=d.get("pipeline_vars", {}),
            agent_id=d.get("agent_id", ""),
        )


@dataclass
class OutboundMessage:
    text: str
    session_id: str

    def to_json(self) -> str:
        return json.dumps({"text": self.text, "session_id": self.session_id})

    @classmethod
    def from_json(cls, data: str) -> "OutboundMessage":
        d = json.loads(data)
        return cls(text=d["text"], session_id=d["session_id"])


@dataclass
class AgentMessage:
    task_id: str
    session_id: str
    description: str
    soul: str
    skills: str
    context: str = ""
    max_turns: int = 10
    model: str = ""
    runtime: str = "claude"
    next: str = ""
    next_needs: list[str] = field(default_factory=list)
    caller_reply_topic: str = ""
    caller_correlation: str = ""
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(
            {
                "task_id": self.task_id,
                "session_id": self.session_id,
                "description": self.description,
                "soul": self.soul,
                "skills": self.skills,
                "context": self.context,
                "max_turns": self.max_turns,
                "model": self.model,
                "runtime": self.runtime,
                "next": self.next,
                "next_needs": self.next_needs,
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
            soul=d["soul"],
            skills=d["skills"],
            context=d.get("context", ""),
            max_turns=d.get("max_turns", 10),
            model=d.get("model", ""),
            runtime=d.get("runtime", "claude"),
            next=d.get("next", ""),
            next_needs=d.get("next_needs", []),
            caller_reply_topic=d.get("caller_reply_topic", ""),
            caller_correlation=d.get("caller_correlation", ""),
            timestamp=d.get("timestamp", time.time()),
        )


@dataclass
class CancelSignal:
    task_id: str
    session_id: str
    reason: str
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(
            {
                "task_id": self.task_id,
                "session_id": self.session_id,
                "reason": self.reason,
                "timestamp": self.timestamp,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "CancelSignal":
        d = json.loads(data)
        return cls(
            task_id=d["task_id"],
            session_id=d["session_id"],
            reason=d["reason"],
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
    pipeline_id: str = ""
    agent_id: str = ""
    label: str = ""
    variables: dict[str, str] = field(default_factory=dict)
    caller_reply_topic: str = ""
    caller_correlation: str = ""
    tasks: dict[str, SessionTask] = field(default_factory=dict)
    spawn_request_id: str = ""

    def to_json(self) -> str:
        return json.dumps(
            {
                "session_id": self.session_id,
                "pipeline_id": self.pipeline_id,
                "agent_id": self.agent_id,
                "label": self.label,
                "variables": self.variables,
                "caller_reply_topic": self.caller_reply_topic,
                "caller_correlation": self.caller_correlation,
                "tasks": {k: v.to_dict() for k, v in self.tasks.items()},
                "spawn_request_id": self.spawn_request_id,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "Session":
        d = json.loads(data)
        tasks = {k: SessionTask.from_dict(v) for k, v in d.get("tasks", {}).items()}
        return cls(
            session_id=d["session_id"],
            pipeline_id=d.get("pipeline_id", ""),
            agent_id=d.get("agent_id", ""),
            label=d.get("label", ""),
            variables=d.get("variables", {}),
            caller_reply_topic=d.get("caller_reply_topic", ""),
            caller_correlation=d.get("caller_correlation", ""),
            tasks=tasks,
            spawn_request_id=d.get("spawn_request_id", ""),
        )
