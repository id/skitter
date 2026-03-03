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
    chat_id: str
    timestamp: float = field(default_factory=time.time)
    pipeline_id: str = ""
    pipeline_vars: dict[str, str] = field(default_factory=dict)
    agent_id: str = ""

    def to_json(self) -> str:
        d: dict = {
            "text": self.text,
            "sender": self.sender,
            "chat_id": self.chat_id,
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
            chat_id=d["chat_id"],
            timestamp=d.get("timestamp", time.time()),
            pipeline_id=d.get("pipeline_id", ""),
            pipeline_vars=d.get("pipeline_vars", {}),
            agent_id=d.get("agent_id", ""),
        )


@dataclass
class OutboundMessage:
    text: str
    chat_id: str

    def to_json(self) -> str:
        return json.dumps({"text": self.text, "chat_id": self.chat_id})

    @classmethod
    def from_json(cls, data: str) -> "OutboundMessage":
        d = json.loads(data)
        return cls(text=d["text"], chat_id=d["chat_id"])


@dataclass
class TaskMessage:
    task_id: str
    chat_id: str
    description: str
    soul: str
    skills: str
    context: str = ""
    max_turns: int = 10
    model: str = ""
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(
            {
                "task_id": self.task_id,
                "chat_id": self.chat_id,
                "description": self.description,
                "soul": self.soul,
                "skills": self.skills,
                "context": self.context,
                "max_turns": self.max_turns,
                "model": self.model,
                "timestamp": self.timestamp,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "TaskMessage":
        d = json.loads(data)
        return cls(
            task_id=d["task_id"],
            chat_id=d["chat_id"],
            description=d["description"],
            soul=d["soul"],
            skills=d["skills"],
            context=d.get("context", ""),
            max_turns=d.get("max_turns", 10),
            model=d.get("model", ""),
            timestamp=d.get("timestamp", time.time()),
        )


@dataclass
class CancelSignal:
    task_id: str
    chat_id: str
    reason: str
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(
            {
                "task_id": self.task_id,
                "chat_id": self.chat_id,
                "reason": self.reason,
                "timestamp": self.timestamp,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "CancelSignal":
        d = json.loads(data)
        return cls(
            task_id=d["task_id"],
            chat_id=d["chat_id"],
            reason=d["reason"],
            timestamp=d.get("timestamp", time.time()),
        )


@dataclass
class JobTask:
    logical_id: str
    task_id: str
    agent: str
    description: str
    soul: str
    skills: str
    depends_on: list[str] = field(default_factory=list)
    status: str = "pending"
    max_turns: int = 10
    model: str = ""

    def to_dict(self) -> dict:
        return {
            "logical_id": self.logical_id,
            "task_id": self.task_id,
            "agent": self.agent,
            "description": self.description,
            "soul": self.soul,
            "skills": self.skills,
            "depends_on": self.depends_on,
            "status": self.status,
            "max_turns": self.max_turns,
            "model": self.model,
        }

    @classmethod
    def from_dict(cls, d: dict) -> "JobTask":
        return cls(
            logical_id=d["logical_id"],
            task_id=d["task_id"],
            agent=d["agent"],
            description=d["description"],
            soul=d["soul"],
            skills=d["skills"],
            depends_on=d.get("depends_on", []),
            status=d.get("status", "pending"),
            max_turns=d.get("max_turns", 10),
            model=d.get("model", ""),
        )


@dataclass
class JobSpec:
    chat_id: str
    original_text: str
    tasks: dict[str, JobTask] = field(default_factory=dict)
    results: dict[str, str] = field(default_factory=dict)

    def to_json(self) -> str:
        return json.dumps(
            {
                "chat_id": self.chat_id,
                "original_text": self.original_text,
                "tasks": {k: v.to_dict() for k, v in self.tasks.items()},
                "results": self.results,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "JobSpec":
        d = json.loads(data)
        tasks = {k: JobTask.from_dict(v) for k, v in d.get("tasks", {}).items()}
        return cls(
            chat_id=d["chat_id"],
            original_text=d["original_text"],
            tasks=tasks,
            results=d.get("results", {}),
        )
