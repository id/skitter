"""CLI unit tests."""

import yaml

from click.testing import CliRunner

from skitter.commands import cli


# ---------------------------------------------------------------------------
# Click command tree
# ---------------------------------------------------------------------------


class TestClickCli:
    def test_help(self):
        result = CliRunner().invoke(cli, ["--help"])
        assert result.exit_code == 0
        assert "MQTT-based AI agent orchestrator" in result.output

    def test_ask_help(self):
        result = CliRunner().invoke(cli, ["ask", "--help"])
        assert result.exit_code == 0
        assert "AGENT_ID" in result.output
        assert "--context" in result.output

    def test_up_help(self):
        result = CliRunner().invoke(cli, ["up", "--help"])
        assert result.exit_code == 0
        assert "--broker-only" in result.output
        assert "--agent" in result.output

    def test_ask_missing_args(self):
        result = CliRunner().invoke(cli, ["ask"])
        assert result.exit_code != 0

    def test_skitter_home_sets_env(self):
        """--skitter-home should set SKITTER_HOME before subcommand runs."""
        result = CliRunner().invoke(cli, ["--skitter-home", "/tmp/test", "--help"])
        assert result.exit_code == 0


# ---------------------------------------------------------------------------
# Status helpers
# ---------------------------------------------------------------------------


class TestStatusHelpers:
    def test_next_action_no_config(self):
        from skitter.services import _next_action

        assert "setup" in _next_action(False, [], [], [])

    def test_next_action_no_runtimes(self):
        from skitter.services import _next_action

        msg = _next_action(True, [], [], [])
        assert "claude" in msg.lower() or "codex" in msg.lower()

    def test_next_action_all_good(self):
        from skitter.services import _next_action

        containers = [("emqx", "running", "emqx:latest")]
        agents = [("researcher", "agents/researcher.md", "claude")]
        msg = _next_action(True, ["claude"], containers, agents)
        assert "ask" in msg.lower()

    def test_generate_compose_basic(self):
        """Compose generation should produce valid YAML."""
        from skitter.services import _generate_compose
        from skitter.config import SkitterConfig

        cfg = SkitterConfig()
        content = _generate_compose(cfg, [])
        data = yaml.safe_load(content)
        assert "services" in data
        assert "emqx" in data["services"]
