"""Tests for skitter chain-based supervisor model."""

import json
from unittest.mock import patch

import pytest

from skitter.config import (
    AgentDef,
    PipelineDef,
    PipelineTask,
)
from skitter.coordinator import (
    build_context_for_join,
    create_session,
    find_id_by_task_id,
    find_task_by_task_id,
    get_entry_tasks,
)
from skitter.mqtt import (
    topic_chain_result,
    topic_control_reload,
    topic_event,
    topic_event_wildcard,
    topic_state_dispatch,
)
from skitter.types import (
    AgentMessage,
    Session,
    SessionTask,
)


# --- Foundation types ---


class TestAgentMessage:
    def test_roundtrip(self):
        msg = AgentMessage(
            task_id="t1",
            session_id="c1",
            description="do stuff",
            soul="be good",
            skills="search",
            context="prior result",
            max_turns=5,
            model="sonnet",
            runtime="claude",
            next="step2",
            next_needs=["step1a", "step1b"],
            caller_reply_topic="reply/topic",
            caller_correlation="corr123",
        )
        json_str = msg.to_json()
        restored = AgentMessage.from_json(json_str)
        assert restored.task_id == "t1"
        assert restored.runtime == "claude"
        assert restored.next == "step2"
        assert restored.next_needs == ["step1a", "step1b"]
        assert restored.caller_reply_topic == "reply/topic"
        assert restored.caller_correlation == "corr123"

    def test_defaults(self):
        msg = AgentMessage.from_json(
            json.dumps(
                {
                    "task_id": "t",
                    "session_id": "c",
                    "description": "d",
                    "soul": "",
                    "skills": "",
                }
            )
        )
        assert msg.runtime == "claude"
        assert msg.next == ""
        assert msg.next_needs == []
        assert msg.caller_reply_topic == ""


class TestSessionTask:
    def test_roundtrip(self):
        task = SessionTask(
            id="research",
            task_id="abc123",
            agent="researcher",
            description="do research",
            next="review",
            needs=["prep"],
        )
        d = task.to_dict()
        assert d["id"] == "research"
        assert d["next"] == "review"
        assert d["needs"] == ["prep"]

        restored = SessionTask.from_dict(d)
        assert restored.id == "research"
        assert restored.next == "review"
        assert restored.needs == ["prep"]


class TestSession:
    def test_caller_fields(self):
        session = Session(
            session_id="c1",
            label="test",
            caller_reply_topic="reply/t",
            caller_correlation="corr",
        )
        json_str = session.to_json()
        restored = Session.from_json(json_str)
        assert restored.caller_reply_topic == "reply/t"
        assert restored.caller_correlation == "corr"


# --- Topic builders ---


class TestTopics:
    def test_event_topics(self):
        t = topic_event("researcher", "alive")
        assert "/event/" in t
        assert "/researcher/alive" in t

    def test_event_wildcard(self):
        t = topic_event_wildcard()
        assert t.endswith("/+/+")

    def test_chain_result(self):
        t = topic_chain_result("session1", "task1")
        assert "/chain/session1/task1" in t

    def test_state_dispatch(self):
        t = topic_state_dispatch("task1")
        assert "/dispatch/task1" in t

    def test_control_reload(self):
        t = topic_control_reload()
        assert "/control/" in t
        assert "/reload" in t


# --- Session building ---


class TestCreateSession:
    def setup_method(self):
        self.agents = {
            "researcher": AgentDef(
                id="researcher",
                name="Researcher",
                soul="research soul",
                skills="search",
                model="sonnet",
                max_turns=15,
            ),
            "writer": AgentDef(
                id="writer",
                name="Writer",
                soul="writer soul",
                model="haiku",
            ),
            "codex_agent": AgentDef(
                id="codex_agent",
                name="Codex Agent",
                runtime="codex",
                model="o3-mini",
            ),
        }
        self.models = {"haiku": "fast", "sonnet": "balanced", "opus": "best"}
        self.pipeline = PipelineDef(
            id="test",
            name="Test Pipeline",
            variables=["topic"],
            tasks=[
                PipelineTask(
                    id="research",
                    agent="researcher",
                    description="Research '{topic}'",
                    next="fact_check",
                    needs=[],
                ),
                PipelineTask(
                    id="analyze",
                    agent="researcher",
                    description="Analyze '{topic}'",
                    next="fact_check",
                    needs=[],
                ),
                PipelineTask(
                    id="fact_check",
                    agent="writer",
                    description="Check '{topic}'",
                    next="output",
                    needs=["research", "analyze"],
                ),
            ],
        )

    def test_pipeline_session(self):
        session = create_session(
            "c1",
            "test",
            pipeline=self.pipeline,
            variables={"topic": "AI"},
            models=self.models,
            agents=self.agents,
        )
        assert "research" in session.tasks
        assert "analyze" in session.tasks
        assert "fact_check" in session.tasks
        assert session.tasks["research"].next == "fact_check"
        assert session.tasks["fact_check"].needs == ["research", "analyze"]

    def test_agent_session_sets_output(self):
        session = create_session(
            "c1",
            "test",
            agent_id="researcher",
            text="test",
            agents=self.agents,
            models=self.models,
        )
        assert "researcher" in session.tasks
        assert session.tasks["researcher"].next == "output"

    def test_codex_agent_model(self):
        # o3-mini is not in models dict, so it falls back to default (haiku)
        session = create_session(
            "c1",
            "code this",
            agent_id="codex_agent",
            text="code this",
            agents=self.agents,
            models=self.models,
        )
        assert session.tasks["codex_agent"].model == "haiku"

    def test_codex_agent_model_when_known(self):
        # When agent model is in models dict, it's used
        models_with_codex = {**self.models, "o3-mini": "codex model"}
        session = create_session(
            "c1",
            "code this",
            agent_id="codex_agent",
            text="code this",
            agents=self.agents,
            models=models_with_codex,
        )
        assert session.tasks["codex_agent"].model == "o3-mini"

    def test_variable_interpolation(self):
        session = create_session(
            "c1",
            "test",
            pipeline=self.pipeline,
            variables={"topic": "quantum"},
            models=self.models,
            agents=self.agents,
        )
        assert "quantum" in session.tasks["research"].description


# --- Entry task detection ---


class TestEntryTasks:
    def test_finds_entry_tasks(self):
        session = Session(session_id="c1", label="test")
        session.tasks["a"] = SessionTask(
            id="a",
            task_id="t1",
            agent="r",
            description="",
            needs=[],
            next="c",
        )
        session.tasks["b"] = SessionTask(
            id="b",
            task_id="t2",
            agent="r",
            description="",
            needs=[],
            next="c",
        )
        session.tasks["c"] = SessionTask(
            id="c",
            task_id="t3",
            agent="w",
            description="",
            needs=["a", "b"],
            next="output",
        )
        entry = get_entry_tasks(session)
        assert len(entry) == 2
        entry_ids = {t.id for t in entry}
        assert entry_ids == {"a", "b"}

    def test_skips_running(self):
        session = Session(session_id="c1", label="test")
        session.tasks["a"] = SessionTask(
            id="a",
            task_id="t1",
            agent="r",
            description="",
            needs=[],
            status="running",
        )
        entry = get_entry_tasks(session)
        assert len(entry) == 0


# --- Join context building ---


class TestJoinContext:
    def test_build_context(self):
        session = Session(session_id="c1", label="test")
        session.tasks["a"] = SessionTask(
            id="a",
            task_id="tid_a",
            agent="r",
            description="",
        )
        session.tasks["b"] = SessionTask(
            id="b",
            task_id="tid_b",
            agent="r",
            description="",
        )
        join_inputs = {"tid_a": "result A", "tid_b": "result B"}
        context = build_context_for_join(join_inputs, ["a", "b"], session)
        assert "result A" in context
        assert "result B" in context
        assert "Result from 'a'" in context
        assert "Result from 'b'" in context


# --- Pipeline loading: auto-infer next ---


class TestPipelineLoading:
    def test_auto_infer_next(self, tmp_path):
        """Pipeline without explicit next gets it auto-inferred from needs."""
        pipeline_yaml = tmp_path / "test.yaml"
        pipeline_yaml.write_text(
            """
name: Test
tasks:
  - id: step1
    agent: worker
    description: first
    needs: []
  - id: step2
    agent: worker
    description: second
    needs: [step1]
"""
        )
        import yaml

        data = yaml.safe_load(pipeline_yaml.read_text())
        tasks = []
        for t in data["tasks"]:
            tasks.append(
                PipelineTask(
                    id=t.get("id", ""),
                    agent=t.get("agent", "worker"),
                    description=t.get("description", ""),
                    next=t.get("next", ""),
                    needs=t.get("needs", []),
                )
            )
        # Auto-infer next
        for t in tasks:
            if not t.next:
                dependents = [other.id for other in tasks if t.id in other.needs]
                if len(dependents) == 1:
                    t.next = dependents[0]
                elif len(dependents) == 0:
                    t.next = "output"

        assert tasks[0].id == "step1"
        assert tasks[0].next == "step2"
        assert tasks[1].id == "step2"
        assert tasks[1].next == "output"


# --- Codex runtime dispatch ---


class TestCodexDispatch:
    @pytest.mark.asyncio
    async def test_codex_not_installed(self):
        """run_codex_agent handles missing codex CLI."""
        from skitter.worker import run_codex_agent

        task = AgentMessage(
            task_id="t1",
            session_id="c1",
            description="code something",
            soul="",
            skills="",
            model="o3-mini",
            runtime="codex",
        )

        async def noop_publish(item_type, content, seq):
            pass

        with patch("asyncio.create_subprocess_exec", side_effect=FileNotFoundError):
            result, usage, cost = await run_codex_agent(task, "/tmp", noop_publish)
            assert "codex CLI not installed" in result
            assert usage is None


# --- Reload signal ---


class TestReload:
    def test_reload_module_exists(self):
        """skitter.reload module is importable."""
        import skitter.reload

        assert hasattr(skitter.reload, "main")


# --- Task helpers ---


class TestTaskHelpers:
    def test_find_task_by_task_id(self):
        session = Session(session_id="c1", label="test")
        session.tasks["a"] = SessionTask(
            id="a", task_id="tid1", agent="r", description=""
        )
        assert find_task_by_task_id(session, "tid1") is not None
        assert find_task_by_task_id(session, "nope") is None

    def test_find_id_by_task_id(self):
        session = Session(session_id="c1", label="test")
        session.tasks["a"] = SessionTask(
            id="a", task_id="tid1", agent="r", description=""
        )
        assert find_id_by_task_id(session, "tid1") == "a"
        assert find_id_by_task_id(session, "nope") is None
