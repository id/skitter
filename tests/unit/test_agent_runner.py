"""Agent runner unit tests."""

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from skitter.a2a import A2ARequest
from skitter.config import AgentDef


# --- A2A compliance: agent runner handle_request ---


class TestAgentRunnerCompliance:
    """Verify agent_runner.handle_request echoes req.task_id in all status events."""

    @pytest.mark.asyncio
    async def test_status_events_use_task_id(self):
        """All status events (submitted, working, completed) must carry req.task_id."""

        from skitter.agent_runner import handle_request

        agent = AgentDef(id="test", name="Test")
        task_id = "550e8400-e29b-41d4-a716-446655440000"
        req = A2ARequest(
            text="hello", request_id="rpc-1", task_id=task_id, sender="test"
        )

        mock_client = MagicMock()
        published: list[str] = []

        async def capture_publish(topic, payload, **kwargs):
            published.append(payload)

        mock_client.publish = AsyncMock(side_effect=capture_publish)
        semaphore = asyncio.Semaphore(1)

        with patch(
            "skitter.agent_runner._run_cli",
            new=AsyncMock(return_value="result text"),
        ):
            await handle_request(
                mock_client, agent, req, "reply/t", "corr-1", {}, semaphore
            )

        # Every published event must have taskId = req.task_id
        for raw in published:
            data = json.loads(raw)
            result = data.get("result", {})
            su = result.get("statusUpdate")
            if su:
                assert su["taskId"] == task_id

    @pytest.mark.asyncio
    async def test_graceful_disconnect_publishes_offline_status(self):
        """Graceful shutdown must republish Agent Card with a2a-status=offline."""
        from unittest.mock import patch

        from skitter.agent_runner import run_with_def

        agent = AgentDef(id="test-offline", name="Test Offline")

        mock_client = MagicMock()
        published: list[tuple[str, dict]] = []

        async def capture_publish(topic, payload, qos=0, retain=False, **kwargs):
            props = kwargs.get("properties")
            user_props = getattr(props, "UserProperty", None) if props else None
            published.append(
                (str(topic), {"qos": qos, "retain": retain, "user_props": user_props})
            )

        mock_client.publish = AsyncMock(side_effect=capture_publish)
        mock_client.subscribe = AsyncMock()

        # Messages iterator that blocks until cancelled
        async def blocking_messages():
            await asyncio.sleep(999)
            yield  # never reached

        type(mock_client).messages = property(lambda self: blocking_messages())

        with patch("aiomqtt.Client") as MockClient:
            MockClient.return_value.__aenter__ = AsyncMock(return_value=mock_client)
            MockClient.return_value.__aexit__ = AsyncMock(return_value=False)

            task = asyncio.create_task(run_with_def(agent))
            await asyncio.sleep(0.05)  # let it start and publish online card
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        # Find the offline publish (last publish to the discovery topic with retain=True)
        discovery_publishes = [
            (t, info) for t, info in published if "discovery" in t and info["retain"]
        ]
        assert len(discovery_publishes) >= 2  # online + offline

        last_topic, last_info = discovery_publishes[-1]
        assert last_info["user_props"] is not None
        user_props_dict = dict(last_info["user_props"])
        assert user_props_dict.get("a2a-status") == "offline"
        assert user_props_dict.get("a2a-status-source") == "agent"

    @pytest.mark.asyncio
    async def test_stream_qos_is_1(self):
        """Streaming updates must use QoS 1 per spec."""

        from skitter.agent_runner import handle_request

        agent = AgentDef(id="test", name="Test")
        req = A2ARequest(text="hello", request_id="rpc-1", sender="test")

        mock_client = MagicMock()
        qos_values: list[int] = []

        async def capture_publish(topic, payload, qos=0, **kwargs):
            qos_values.append(qos)

        mock_client.publish = AsyncMock(side_effect=capture_publish)
        semaphore = asyncio.Semaphore(1)

        async def streaming_cli(agent, prompt, publish_stream, env):
            await publish_stream("text", "chunk")
            return "done"

        with patch("skitter.agent_runner._run_cli", new=streaming_cli):
            await handle_request(
                mock_client, agent, req, "reply/t", "corr-1", {}, semaphore
            )

        # All publishes (submitted ack, stream chunk, terminal) should be QoS 1
        assert all(q == 1 for q in qos_values)


# --- Agent runner ---


class TestAgentRunnerCli:
    def test_build_claude_cmd(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(
            id="researcher",
            name="Researcher",
            runtime="claude",
            model="sonnet",
            instructions="You are a researcher.",
        )
        cmd = _build_cli_cmd(agent, "test prompt")
        assert cmd[0] == "claude"
        assert "-p" in cmd
        # Instructions should be prepended to prompt
        prompt_idx = cmd.index("-p") + 1
        assert "You are a researcher." in cmd[prompt_idx]
        assert "test prompt" in cmd[prompt_idx]
        assert "--agent" not in cmd
        assert "--model" in cmd
        assert "sonnet" in cmd
        # Use --permission-mode bypassPermissions: --dangerously-skip-permissions
        # triggers a Claude CLI hang on large + stream-json inputs.
        assert "--dangerously-skip-permissions" not in cmd
        assert cmd[cmd.index("--permission-mode") + 1] == "bypassPermissions"

    def test_build_codex_cmd(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(
            id="coder",
            name="Coder",
            runtime="codex",
            model="gpt-5-nano",
        )
        cmd = _build_cli_cmd(agent, "code something")
        assert cmd[0] == "codex"
        assert cmd[-1] == "code something"  # prompt must be last (positional)
        assert "--model" in cmd
        assert "gpt-5-nano" in cmd
        assert "--ephemeral" not in cmd
        assert cmd[cmd.index("--color") + 1] == "never"
        assert "--dangerously-bypass-approvals-and-sandbox" in cmd

    def test_build_codex_cmd_with_instructions(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(
            id="coder",
            name="Coder",
            runtime="codex",
            instructions="You are a senior developer.",
        )
        cmd = _build_cli_cmd(agent, "write tests")
        assert "-c" in cmd
        # Find the -c arg that sets developer_instructions
        c_indices = [i for i, v in enumerate(cmd) if v == "-c"]
        dev_instr_args = [
            cmd[i + 1] for i in c_indices if "developer_instructions=" in cmd[i + 1]
        ]
        assert len(dev_instr_args) == 1
        assert dev_instr_args[0] == "developer_instructions=You are a senior developer."

    def test_build_codex_cmd_no_instructions(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(id="coder", name="Coder", runtime="codex")
        cmd = _build_cli_cmd(agent, "write tests")
        c_indices = [i for i, v in enumerate(cmd) if v == "-c"]
        dev_instr_args = [
            cmd[i + 1] for i in c_indices if "developer_instructions=" in cmd[i + 1]
        ]
        assert len(dev_instr_args) == 0

    def test_build_claude_cmd_no_instructions(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(id="researcher", name="Researcher", runtime="claude")
        cmd = _build_cli_cmd(agent, "test")
        # Without instructions, prompt is passed directly
        prompt_idx = cmd.index("-p") + 1
        assert cmd[prompt_idx] == "test"
        assert "--agent" not in cmd

    def test_build_claude_cmd_with_resume(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(id="researcher", name="Researcher", runtime="claude")
        ctx = "550e8400-e29b-41d4-a716-446655440000"
        cmd = _build_cli_cmd(agent, "test", resume_id=ctx)
        assert "--resume" in cmd
        idx = cmd.index("--resume")
        assert cmd[idx + 1] == ctx

    def test_build_claude_cmd_no_resume_without_context(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(id="researcher", name="Researcher", runtime="claude")
        cmd = _build_cli_cmd(agent, "test")
        assert "--resume" not in cmd

    def test_build_codex_cmd_with_context_uses_resume(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(id="coder", name="Coder", runtime="codex")
        ctx = "550e8400-e29b-41d4-a716-446655440000"
        cmd = _build_cli_cmd(agent, "test", resume_id=ctx)
        # Codex resume: codex exec resume [flags] <session_id> <prompt>
        assert cmd[:3] == ["codex", "exec", "resume"]
        assert cmd[-2] == ctx
        assert cmd[-1] == "test"  # prompt is last

    def test_build_codex_cmd_no_context(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(id="coder", name="Coder", runtime="codex")
        cmd = _build_cli_cmd(agent, "test")
        # Without context_id, uses regular exec (no resume subcommand)
        assert cmd[0] == "codex"
        assert cmd[1] == "exec"
        assert "resume" not in cmd

    @pytest.mark.asyncio
    async def test_run_cli_missing_binary(self):
        from skitter.agent_runner import _run_cli

        agent = AgentDef(id="test", name="Test", runtime="claude")

        async def noop(t, c):
            pass

        with patch("asyncio.create_subprocess_exec", side_effect=FileNotFoundError):
            result, session_id = await _run_cli(agent, "test", noop, {})
            assert "claude" in result.lower()
            assert "not found" in result.lower()
            assert session_id == ""


class TestLoadAgent:
    def test_load_claude_agent(self, tmp_path):
        (tmp_path / "researcher.md").write_text(
            "---\n"
            "name: researcher\n"
            "description: Deep research\n"
            "model: sonnet\n"
            "---\n"
            "You are a researcher.\n"
        )
        from skitter.agent_runner import load_agent

        agent, _ = load_agent(str(tmp_path / "researcher.md"))
        assert agent.id == "researcher"
        assert agent.name == "researcher"
        assert agent.description == "Deep research"
        assert agent.model == "sonnet"
        assert agent.runtime == "claude"
        assert agent.instructions == "You are a researcher."

    def test_load_claude_agent_minimal(self, tmp_path):
        (tmp_path / "simple.md").write_text("---\nname: simple\n---\nBe brief.\n")
        from skitter.agent_runner import load_agent

        agent, _ = load_agent(str(tmp_path / "simple.md"))
        assert agent.id == "simple"
        assert agent.description == ""
        assert agent.model == ""
        assert agent.runtime == "claude"
        assert agent.instructions == "Be brief."

    def test_load_claude_agent_name_differs_from_filename(self, tmp_path):
        """Agent ID should use frontmatter name, not filename stem."""
        (tmp_path / "my-copy.md").write_text(
            "---\nname: researcher\ndescription: Research\n---\nDo research.\n"
        )
        from skitter.agent_runner import load_agent

        agent, _ = load_agent(str(tmp_path / "my-copy.md"))
        assert agent.id == "researcher"
        assert agent.instructions == "Do research."

    def test_load_codex_agent(self, tmp_path):
        (tmp_path / "coder.toml").write_text(
            'model = "gpt-5.1-codex-mini"\n'
            'developer_instructions = "You are a senior developer."\n'
        )
        from skitter.agent_runner import load_agent

        agent, _ = load_agent(str(tmp_path / "coder.toml"))
        assert agent.id == "coder"
        assert agent.runtime == "codex"
        assert agent.model == "gpt-5.1-codex-mini"
        assert agent.description == "You are a senior developer."
        assert agent.instructions == "You are a senior developer."


# --- Skill support ---


class TestSkillLoading:
    """Tests for _load_skills and skill frontmatter parsing."""

    def test_load_skills(self, tmp_path):
        """Skills are loaded from ~/.skitter/skills/<name>/SKILL.md."""
        from skitter.agent_runner import _load_skills

        skills_dir = tmp_path / "skills"
        (skills_dir / "web-search").mkdir(parents=True)
        (skills_dir / "web-search" / "SKILL.md").write_text(
            "---\n"
            "name: web-search\n"
            "description: Search the web for current information\n"
            "---\n"
            "Use search engines to find answers.\n"
        )
        (skills_dir / "summarize").mkdir()
        (skills_dir / "summarize" / "SKILL.md").write_text(
            "---\n"
            "name: summarize\n"
            "description: Summarize long text\n"
            "---\n"
            "Create concise summaries.\n"
        )

        with patch("skitter.config.skills_dir", return_value=skills_dir):
            result = _load_skills(["web-search", "summarize"])

        assert len(result) == 2
        assert result[0].id == "web-search"
        assert result[0].name == "web-search"
        assert result[0].description == "Search the web for current information"
        assert result[1].id == "summarize"

    def test_load_skills_missing(self, tmp_path):
        """Missing skill names are skipped with a warning."""
        from skitter.agent_runner import _load_skills

        skills_dir = tmp_path / "skills"
        skills_dir.mkdir()

        with patch("skitter.config.skills_dir", return_value=skills_dir):
            result = _load_skills(["nonexistent"])

        assert result == []

    def test_load_skills_bad_frontmatter(self, tmp_path):
        """Skill with missing frontmatter is skipped."""
        from skitter.agent_runner import _load_skills

        skills_dir = tmp_path / "skills"
        (skills_dir / "bad").mkdir(parents=True)
        (skills_dir / "bad" / "SKILL.md").write_text("No frontmatter here.\n")

        with patch("skitter.config.skills_dir", return_value=skills_dir):
            result = _load_skills(["bad"])

        assert result == []


class TestSkillLinks:
    """Tests for _setup_skill_links symlink creation."""

    def test_setup_skill_links_claude(self, tmp_path):
        """Claude agent gets .claude/skills/<name> symlinks."""
        from skitter.agent_runner import _setup_skill_links

        skills_dir = tmp_path / "skills"
        (skills_dir / "web-search").mkdir(parents=True)
        (skills_dir / "web-search" / "SKILL.md").write_text(
            "---\nname: web-search\n---\n"
        )

        resource_dir = tmp_path / "resource"
        resource_dir.mkdir()

        agent = AgentDef(
            id="test",
            name="test",
            runtime="claude",
            skill_refs=["web-search"],
        )

        with patch("skitter.config.skills_dir", return_value=skills_dir):
            _setup_skill_links(agent, resource_dir)

        link = resource_dir / ".claude" / "skills" / "web-search"
        assert link.is_symlink()
        assert link.resolve() == (skills_dir / "web-search").resolve()

    def test_setup_skill_links_codex(self, tmp_path):
        """Codex agent gets .agents/skills/<name> symlinks."""
        from skitter.agent_runner import _setup_skill_links

        skills_dir = tmp_path / "skills"
        (skills_dir / "debug").mkdir(parents=True)
        (skills_dir / "debug" / "SKILL.md").write_text("---\nname: debug\n---\n")

        resource_dir = tmp_path / "resource"
        resource_dir.mkdir()

        agent = AgentDef(
            id="test",
            name="test",
            runtime="codex",
            skill_refs=["debug"],
        )

        with patch("skitter.config.skills_dir", return_value=skills_dir):
            _setup_skill_links(agent, resource_dir)

        link = resource_dir / ".agents" / "skills" / "debug"
        assert link.is_symlink()
        assert link.resolve() == (skills_dir / "debug").resolve()

    def test_setup_skill_links_idempotent(self, tmp_path):
        """Running setup twice does not error."""
        from skitter.agent_runner import _setup_skill_links

        skills_dir = tmp_path / "skills"
        (skills_dir / "s1").mkdir(parents=True)
        (skills_dir / "s1" / "SKILL.md").write_text("---\nname: s1\n---\n")

        resource_dir = tmp_path / "resource"
        resource_dir.mkdir()

        agent = AgentDef(
            id="test",
            name="test",
            runtime="claude",
            skill_refs=["s1"],
        )

        with patch("skitter.config.skills_dir", return_value=skills_dir):
            _setup_skill_links(agent, resource_dir)
            _setup_skill_links(agent, resource_dir)

        link = resource_dir / ".claude" / "skills" / "s1"
        assert link.is_symlink()

    def test_setup_skill_links_stale_removed(self, tmp_path):
        """Stale symlinks for removed skills are cleaned up."""
        from skitter.agent_runner import _setup_skill_links

        skills_dir = tmp_path / "skills"
        (skills_dir / "old-skill").mkdir(parents=True)
        (skills_dir / "old-skill" / "SKILL.md").write_text("---\nname: old\n---\n")
        (skills_dir / "new-skill").mkdir(parents=True)
        (skills_dir / "new-skill" / "SKILL.md").write_text("---\nname: new\n---\n")

        resource_dir = tmp_path / "resource"
        resource_dir.mkdir()

        # First: link old-skill
        agent_v1 = AgentDef(
            id="test",
            name="test",
            runtime="claude",
            skill_refs=["old-skill"],
        )
        with patch("skitter.config.skills_dir", return_value=skills_dir):
            _setup_skill_links(agent_v1, resource_dir)

        assert (resource_dir / ".claude" / "skills" / "old-skill").is_symlink()

        # Second: only new-skill referenced
        agent_v2 = AgentDef(
            id="test",
            name="test",
            runtime="claude",
            skill_refs=["new-skill"],
        )
        with patch("skitter.config.skills_dir", return_value=skills_dir):
            _setup_skill_links(agent_v2, resource_dir)

        assert not (resource_dir / ".claude" / "skills" / "old-skill").exists()
        assert (resource_dir / ".claude" / "skills" / "new-skill").is_symlink()


class TestBuildCardWithSkills:
    """Tests for discovery card skill population."""

    def test_card_with_skills(self):
        from skitter.config import SkillDef
        from skitter.discovery import build_card

        agent = AgentDef(
            id="researcher",
            name="Researcher",
            description="Research agent",
            skills=[
                SkillDef(
                    id="web-search", name="Web Search", description="Search the web"
                ),
                SkillDef(
                    id="summarize", name="Summarize", description="Summarize text"
                ),
            ],
        )
        card = build_card(agent, url="mqtt://test:1883")
        assert len(card["skills"]) == 2
        assert card["skills"][0]["id"] == "web-search"
        assert card["skills"][0]["name"] == "Web Search"
        assert card["skills"][0]["description"] == "Search the web"
        assert card["skills"][1]["id"] == "summarize"

    def test_card_without_skills_has_default(self):
        from skitter.discovery import build_card

        agent = AgentDef(id="writer", name="Writer", description="Writes")
        card = build_card(agent, url="mqtt://test:1883")
        assert len(card["skills"]) == 1
        assert card["skills"][0]["id"] == "default"
        assert card["skills"][0]["name"] == "Writer"


class TestAgentSkillRefs:
    """Tests for skill_refs parsing from agent definitions."""

    def test_md_agent_with_skills(self, tmp_path):
        from skitter.agent_runner import _load_md_agent

        agent_file = tmp_path / "test.md"
        agent_file.write_text(
            "---\n"
            "name: test\n"
            "description: Test agent\n"
            "runtime: claude\n"
            "skills: [web-search, summarize]\n"
            "---\n"
            "Instructions here.\n"
        )
        agent = _load_md_agent(agent_file)
        assert agent.skill_refs == ["web-search", "summarize"]

    def test_md_agent_without_skills(self, tmp_path):
        from skitter.agent_runner import _load_md_agent

        agent_file = tmp_path / "test.md"
        agent_file.write_text(
            "---\nname: test\ndescription: Test agent\n---\nInstructions here.\n"
        )
        agent = _load_md_agent(agent_file)
        assert agent.skill_refs == []

    def test_toml_agent_with_skills(self, tmp_path):
        from skitter.agent_runner import _load_toml_agent

        agent_file = tmp_path / "test.toml"
        agent_file.write_text(
            'name = "test"\n'
            'description = "Test agent"\n'
            'runtime = "codex"\n'
            'skills = ["debug", "test-runner"]\n'
            'developer_instructions = "Instructions."\n'
        )
        agent = _load_toml_agent(agent_file)
        assert agent.skill_refs == ["debug", "test-runner"]


class TestBuildCliCmd:
    """Tests for _build_cli_cmd with maxTurns and tools."""

    def test_claude_max_turns(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(id="test", name="test", runtime="claude", max_turns=5)
        cmd = _build_cli_cmd(agent, "hello")
        assert "--max-turns" in cmd
        idx = cmd.index("--max-turns")
        assert cmd[idx + 1] == "5"

    def test_claude_tools(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(
            id="test",
            name="test",
            runtime="claude",
            tools=["Read", "Grep", "Bash"],
        )
        cmd = _build_cli_cmd(agent, "hello")
        assert "--allowedTools" in cmd
        idx = cmd.index("--allowedTools")
        assert cmd[idx + 1] == "Read,Grep,Bash"

    def test_claude_no_max_turns_when_zero(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(id="test", name="test", runtime="claude", max_turns=0)
        cmd = _build_cli_cmd(agent, "hello")
        assert "--max-turns" not in cmd

    def test_claude_no_tools_when_empty(self):
        from skitter.agent_runner import _build_cli_cmd

        agent = AgentDef(id="test", name="test", runtime="claude")
        cmd = _build_cli_cmd(agent, "hello")
        assert "--allowedTools" not in cmd

    def test_md_agent_reads_max_turns_and_tools(self, tmp_path):
        from skitter.agent_runner import _load_md_agent

        agent_file = tmp_path / "test.md"
        agent_file.write_text(
            "---\n"
            "name: test\n"
            "runtime: claude\n"
            "maxTurns: 10\n"
            "tools: Read, Grep, Bash\n"
            "---\n"
            "Instructions.\n"
        )
        agent = _load_md_agent(agent_file)
        assert agent.max_turns == 10
        assert agent.tools == ["Read", "Grep", "Bash"]
