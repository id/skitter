from dataclasses import dataclass, field
import json
import time


@dataclass
class InboundMessage:
    text: str
    sender: str
    chat_id: str
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(
            {
                "text": self.text,
                "sender": self.sender,
                "chat_id": self.chat_id,
                "timestamp": self.timestamp,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "InboundMessage":
        d = json.loads(data)
        return cls(
            text=d["text"],
            sender=d["sender"],
            chat_id=d["chat_id"],
            timestamp=d.get("timestamp", time.time()),
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
class TaskResultMessage:
    task_id: str
    chat_id: str
    result: str
    timestamp: float = field(default_factory=time.time)

    def to_json(self) -> str:
        return json.dumps(
            {
                "task_id": self.task_id,
                "chat_id": self.chat_id,
                "result": self.result,
                "timestamp": self.timestamp,
            }
        )

    @classmethod
    def from_json(cls, data: str) -> "TaskResultMessage":
        d = json.loads(data)
        return cls(
            task_id=d["task_id"],
            chat_id=d["chat_id"],
            result=d["result"],
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
