"""Acceptance tests: simulate real user journey with actual services.

Exercises the full onboarding and usage flow in an isolated environment:
setup -> doctor -> agent setup -> ask -> multi-turn context.

Uses a temporary HOME directory for config isolation, Docker EMQX for the
MQTT broker, and a mock CLI binary for agent execution.

Requirements:
    - Docker running, EMQX on localhost:1883 (docker compose up -d --wait)

Usage:
    docker compose up -d --wait
    uv run pytest tests/test_acceptance.py -v -s
    docker compose down
"""

from __future__ import annotations

import asyncio
import contextlib
import json
import os
import signal
import subprocess
import sys
import textwrap
import time
import uuid
from pathlib import Path

import aiomqtt
import pytest
import yaml

from skitter.a2a import a2a_org, a2a_unit, topic_discovery
from skitter.mqtt import get_user_property, mqtt_client_kwargs
from tests.conftest import broker_reachable, run_skitter, PROJECT_ROOT

needs_mqtt = pytest.mark.skipif(
    not broker_reachable(),
    reason="MQTT broker not available on localhost:1883 (run: docker compose up -d --wait)",
)

pytestmark = needs_mqtt


@contextlib.contextmanager
def _agent_runner(agent_path: Path, env: dict[str, str]):
    """Start an agent-runner subprocess, wait for readiness, yield, then stop."""
    proc = subprocess.Popen(
        [sys.executable, "-m", "skitter", "agent-runner", str(agent_path)],
        env=env,
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
        cwd=str(PROJECT_ROOT),
    )
    try:
        time.sleep(3)
        assert proc.poll() is None, f"agent-runner exited early (rc={proc.returncode})"
        yield proc
    finally:
        proc.send_signal(signal.SIGINT)
        try:
            proc.wait(timeout=10)
        except subprocess.TimeoutExpired:
            proc.kill()
            proc.wait()


def _extract_context_id(output: str) -> str:
    """Parse context_id from ``skitter request`` output."""
    for line in output.splitlines():
        if "context_id:" in line:
            return line.split("context_id:")[1].strip()
    return ""


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture(scope="session")
def tmp_home(tmp_path_factory):
    """Isolated HOME directory; all skitter config goes under $HOME/.skitter/."""
    return tmp_path_factory.mktemp("acceptance_home")


@pytest.fixture(scope="session")
def mock_claude_bin(tmp_home):
    """Create a mock ``claude`` binary that outputs stream-json."""
    bin_dir = tmp_home / "bin"
    bin_dir.mkdir()
    script = bin_dir / "claude"
    script.write_text(
        textwrap.dedent("""\
        #!/usr/bin/env python3
        import json, sys

        import hashlib

        prompt = ""
        resume_id = ""
        args = sys.argv[1:]
        for i, arg in enumerate(args):
            if arg == "-p" and i + 1 < len(args):
                prompt = args[i + 1]
            if arg == "--resume" and i + 1 < len(args):
                resume_id = args[i + 1]

        # Derive a stable session_id from the resume_id or a hash of the prompt.
        # This lets the agent-runner map context_id -> CLI session_id for resume.
        sid = resume_id or hashlib.sha256(prompt.encode()).hexdigest()[:16]

        text = f"Echo: {prompt}"
        if resume_id:
            text += f" [resumed:{resume_id}]"

        event = {
            "type": "assistant",
            "session_id": sid,
            "message": {
                "content": [{"type": "text", "text": text}]
            },
        }
        print(json.dumps(event), flush=True)
    """)
    )
    script.chmod(0o755)
    return bin_dir


@pytest.fixture(scope="session")
def mock_codex_bin(tmp_home):
    """Create a mock ``codex`` binary that outputs stream-json."""
    bin_dir = tmp_home / "bin"
    bin_dir.mkdir(exist_ok=True)
    script = bin_dir / "codex"
    script.write_text(
        textwrap.dedent("""\
        #!/usr/bin/env python3
        import json, sys

        prompt = sys.argv[-1] if len(sys.argv) > 1 else ""

        event = {
            "type": "item.completed",
            "item": {
                "type": "agent_message",
                "text": f"Codex: {prompt}"
            },
        }
        print(json.dumps(event), flush=True)
    """)
    )
    script.chmod(0o755)
    return bin_dir


@pytest.fixture(scope="session")
def skitter_env(tmp_home, mock_claude_bin, mock_codex_bin):
    """Subprocess environment with isolated HOME and mock CLIs on PATH."""
    env = os.environ.copy()
    env["HOME"] = str(tmp_home)
    env["PATH"] = f"{mock_claude_bin}:{mock_codex_bin}:{env.get('PATH', '')}"
    env["NO_COLOR"] = "1"
    env["TERM"] = "dumb"
    env["COLUMNS"] = "200"
    real_docker_cfg = Path.home() / ".docker"
    if real_docker_cfg.is_dir():
        env["DOCKER_CONFIG"] = str(real_docker_cfg)
    return env


@pytest.fixture(scope="session")
def onboarded(skitter_env, tmp_home):
    """Run ``skitter setup --non-interactive`` once for the session."""
    result = run_skitter(["setup", "--non-interactive"], skitter_env)
    assert result.returncode == 0, (
        f"setup --non-interactive failed (rc={result.returncode}):\n"
        f"{result.stdout}\n{result.stderr}"
    )
    config_path = tmp_home / ".skitter" / "config.yaml"
    assert config_path.is_file(), "config.yaml was not created"
    return config_path


@pytest.fixture(scope="session")
def onboard_config(onboarded):
    """Parsed config.yaml from the setup session."""
    return yaml.safe_load(onboarded.read_text())


@pytest.fixture()
def echo_agent(onboarded, tmp_home):
    """Write a minimal agent definition that uses the mock claude binary."""
    agents_dir = tmp_home / ".skitter" / "agents"
    agents_dir.mkdir(parents=True, exist_ok=True)
    agent_file = agents_dir / "echo-test.md"
    agent_file.write_text(
        "---\n"
        "name: echo-test\n"
        "description: Echo agent for acceptance testing\n"
        "runtime: claude\n"
        "---\n"
        "You are an echo agent. Repeat the user's message.\n"
    )
    return agent_file


@pytest.fixture()
def codex_agent(onboarded, tmp_home):
    """Write a minimal Codex agent definition that uses the mock codex binary."""
    agents_dir = tmp_home / ".skitter" / "agents"
    agents_dir.mkdir(parents=True, exist_ok=True)
    agent_file = agents_dir / "codex-test.toml"
    agent_file.write_text(
        'name = "codex-test"\n'
        'description = "Codex agent for acceptance testing"\n'
        'runtime = "codex"\n'
        'developer_instructions = "You are a test agent."\n'
    )
    return agent_file


# ---------------------------------------------------------------------------
# Tests: Setup
# ---------------------------------------------------------------------------


class TestSetup:
    """skitter setup --non-interactive."""

    def test_config_created(self, onboarded):
        assert onboarded.is_file()

    def test_config_has_broker(self, onboard_config):
        assert "broker" in onboard_config

    def test_broker_tier_is_docker(self, onboard_config):
        assert onboard_config["broker"]["tier"] == "docker"
        assert onboard_config["broker"]["url"] == "mqtt://localhost:1883"

    def test_agents_dir_created(self, tmp_home, onboarded):
        assert (tmp_home / ".skitter" / "agents").is_dir()

    def test_idempotent(self, skitter_env):
        """Running setup twice in non-interactive mode succeeds."""
        r = run_skitter(["setup", "--non-interactive"], skitter_env)
        assert r.returncode == 0


# ---------------------------------------------------------------------------
# Tests: SKITTER_HOME
# ---------------------------------------------------------------------------


class TestSkitterHome:
    def test_custom_skitter_home_via_env(self, tmp_path, skitter_env):
        """setup with SKITTER_HOME writes config under custom path."""
        custom_env = {**skitter_env, "SKITTER_HOME": str(tmp_path)}
        r = run_skitter(["setup", "--non-interactive"], custom_env)
        assert r.returncode == 0, f"setup failed:\n{r.stdout}\n{r.stderr}"
        assert (tmp_path / "config.yaml").is_file()

    def test_skitter_home_cli_flag(self, tmp_path, skitter_env):
        """--skitter-home flag overrides SKITTER_HOME."""
        custom_dir = tmp_path / "flag_home"
        custom_env = {**skitter_env}
        custom_env.pop("SKITTER_HOME", None)
        r = run_skitter(
            ["--skitter-home", str(custom_dir), "setup", "--non-interactive"],
            custom_env,
        )
        assert r.returncode == 0, f"setup failed:\n{r.stdout}\n{r.stderr}"
        assert (custom_dir / "config.yaml").is_file()


# ---------------------------------------------------------------------------
# Tests: Doctor
# ---------------------------------------------------------------------------


class TestDoctor:
    def test_config_and_broker(self, onboarded, skitter_env):
        """Doctor passes config and broker checks."""
        r = run_skitter(["doctor"], skitter_env, timeout=30)
        assert "found and valid" in r.stdout
        assert "reachable" in r.stdout
        assert "round-trip" in r.stdout


# ---------------------------------------------------------------------------
# Tests: Ask (parameterized across runtimes)
# ---------------------------------------------------------------------------


class TestAsk:
    @pytest.mark.parametrize(
        "agent_fixture,agent_name,query",
        [
            ("echo_agent", "echo-test", "hello world"),
            ("codex_agent", "codex-test", "hello from codex"),
        ],
    )
    def test_ask_one_shot(self, request, skitter_env, agent_fixture, agent_name, query):
        """One-shot ask returns output from mock agent."""
        agent = request.getfixturevalue(agent_fixture)
        with _agent_runner(agent, skitter_env):
            r = run_skitter(
                ["ask", agent_name, query],
                skitter_env,
                timeout=30,
            )
            assert r.returncode == 0, (
                f"ask failed (rc={r.returncode}):\n{r.stdout}\n{r.stderr}"
            )
            assert query in r.stdout
            assert "context_id:" in r.stdout

    def test_ask_with_context(self, skitter_env, echo_agent):
        """Two asks with same context_id succeed (multi-turn)."""
        with _agent_runner(echo_agent, skitter_env):
            r1 = run_skitter(
                ["ask", "echo-test", "first message"],
                skitter_env,
                timeout=30,
            )
            assert r1.returncode == 0

            ctx = _extract_context_id(r1.stdout)
            assert ctx, f"No context_id in output:\n{r1.stdout}"

            r2 = run_skitter(
                ["ask", "echo-test", "second message", "--context", ctx],
                skitter_env,
                timeout=30,
            )
            assert r2.returncode == 0
            assert "second message" in r2.stdout

    def test_agent_discovery_card(self, skitter_env, echo_agent):
        """Agent publishes a retained discovery card with a2a-status=online."""
        with _agent_runner(echo_agent, skitter_env):

            async def _check_card():
                topic = topic_discovery("echo-test")
                async with aiomqtt.Client(
                    **mqtt_client_kwargs(
                        identifier=f"{a2a_org()}/{a2a_unit()}/accept-test-{uuid.uuid4().hex[:6]}",
                    ),
                ) as client:
                    await client.subscribe(topic, qos=1)
                    async with asyncio.timeout(10):
                        async for msg in client.messages:
                            if msg.payload:
                                card = json.loads(msg.payload)
                                assert card["name"] == "echo-test"
                                assert get_user_property(msg, "a2a-status") == "online"
                                return

            asyncio.run(_check_card())


# ---------------------------------------------------------------------------
# Tests: Status
# ---------------------------------------------------------------------------


class TestStatus:
    def test_status_shows_config(self, onboarded, skitter_env):
        r = run_skitter(["status"], skitter_env, timeout=15)
        assert r.returncode == 0
        assert "config.yaml" in r.stdout

    def test_status_no_config(self, tmp_path, skitter_env):
        """status without config suggests setup."""
        empty_env = {**skitter_env, "SKITTER_HOME": str(tmp_path / "empty")}
        r = run_skitter(["status"], empty_env, timeout=15)
        assert r.returncode == 0
        assert "setup" in r.stdout.lower()


# ---------------------------------------------------------------------------
# Tests: Services (non-destructive)
# ---------------------------------------------------------------------------


class TestServices:
    def test_down_noop(self, onboarded, skitter_env):
        """``skitter down`` is safe when nothing is running."""
        r = run_skitter(["down"], skitter_env, timeout=15)
        assert r.returncode == 0


# ---------------------------------------------------------------------------
# Tests: Restart persistence
# ---------------------------------------------------------------------------


class TestRestartPersistence:
    def test_context_survives_restart(self, skitter_env, echo_agent):
        """Stop and restart agent-runner; second ask with same context_id
        passes --resume to the CLI, proving the context is wired through."""
        with _agent_runner(echo_agent, skitter_env):
            r1 = run_skitter(
                ["ask", "echo-test", "turn one"],
                skitter_env,
                timeout=30,
            )
            assert r1.returncode == 0
            ctx = _extract_context_id(r1.stdout)
            assert ctx, f"No context_id in output:\n{r1.stdout}"

        with _agent_runner(echo_agent, skitter_env):
            r2 = run_skitter(
                ["ask", "echo-test", "turn two", "--context", ctx],
                skitter_env,
                timeout=30,
            )
            assert r2.returncode == 0
            assert "turn two" in r2.stdout
            assert "[resumed:" in r2.stdout


# ---------------------------------------------------------------------------
# Tests: Full journey with custom home
# ---------------------------------------------------------------------------


class TestJourney:
    def test_full_journey_with_custom_home(self, tmp_path, skitter_env):
        """Golden path with SKITTER_HOME set to temp dir."""
        custom_home = tmp_path / "journey_home"
        custom_env = {**skitter_env, "SKITTER_HOME": str(custom_home)}

        r = run_skitter(["setup", "--non-interactive"], custom_env)
        assert r.returncode == 0

        agents_dir = custom_home / "agents"
        agents_dir.mkdir(parents=True, exist_ok=True)
        agent_file = agents_dir / "echo-journey.md"
        agent_file.write_text(
            "---\n"
            "name: echo-journey\n"
            "description: Echo agent for journey test\n"
            "runtime: claude\n"
            "---\n"
            "You are an echo agent.\n"
        )

        with _agent_runner(agent_file, custom_env):
            r = run_skitter(
                ["ask", "echo-journey", "custom home test"],
                custom_env,
                timeout=30,
            )
            assert r.returncode == 0
            assert "custom home test" in r.stdout


# ---------------------------------------------------------------------------
# Tests: Non-default broker tier
# ---------------------------------------------------------------------------


class TestPublicBrokerTier:
    def test_setup_public_tier(self, tmp_path, skitter_env):
        """Public broker tier config is valid for status."""
        custom_env = {
            **skitter_env,
            "SKITTER_HOME": str(tmp_path),
            "MQTT_BROKER_URL": "mqtt://broker.emqx.io:1883",
        }
        tmp_path.mkdir(exist_ok=True)
        (tmp_path / "agents").mkdir(exist_ok=True)
        (tmp_path / "config.yaml").write_text(
            yaml.dump(
                {
                    "broker": {
                        "tier": "public",
                        "url": "mqtt://broker.emqx.io:1883",
                        "org_prefix": "skitter-test-abc123",
                    },
                    "org": "skitter-test-abc123",
                },
                default_flow_style=False,
            )
        )
        r = run_skitter(["status"], custom_env, timeout=15)
        assert r.returncode == 0
        assert "public" in r.stdout.lower() or "broker.emqx.io" in r.stdout

    def test_doctor_validates_public_broker(self, tmp_path, skitter_env):
        """doctor can reach the public broker."""
        custom_env = {
            **skitter_env,
            "SKITTER_HOME": str(tmp_path),
            "MQTT_BROKER_URL": "mqtt://broker.emqx.io:1883",
        }
        tmp_path.mkdir(exist_ok=True)
        (tmp_path / "agents").mkdir(exist_ok=True)
        (tmp_path / "config.yaml").write_text(
            yaml.dump(
                {
                    "broker": {
                        "tier": "public",
                        "url": "mqtt://broker.emqx.io:1883",
                    },
                },
                default_flow_style=False,
            )
        )
        r = run_skitter(["doctor"], custom_env, timeout=30)
        assert "reachable" in r.stdout
        assert "round-trip" in r.stdout
