"""CLI unit tests."""

import yaml
from unittest.mock import patch


# ---------------------------------------------------------------------------
# M4: ask arg parsing
# ---------------------------------------------------------------------------


class TestAskArgParsing:
    def test_basic_args(self):
        from skitter.__main__ import _parse_request_args

        agent, prompt, ctx = _parse_request_args(["my-agent", "hello", "world"])
        assert agent == "my-agent"
        assert prompt == "hello world"
        assert ctx == ""

    def test_with_context(self):
        from skitter.__main__ import _parse_request_args

        agent, prompt, ctx = _parse_request_args(
            ["my-agent", "hello", "--context", "ctx-123"]
        )
        assert agent == "my-agent"
        assert prompt == "hello"
        assert ctx == "ctx-123"

    def test_context_before_prompt(self):
        from skitter.__main__ import _parse_request_args

        agent, prompt, ctx = _parse_request_args(
            ["my-agent", "--context", "ctx-123", "hello"]
        )
        assert agent == "my-agent"
        assert prompt == "hello"
        assert ctx == "ctx-123"

    def test_missing_args(self):
        from skitter.__main__ import _parse_request_args

        agent, prompt, ctx = _parse_request_args(["only-agent"])
        assert agent == ""
        assert prompt == ""


# ---------------------------------------------------------------------------
# M5: status helpers
# ---------------------------------------------------------------------------


class TestStatusHelpers:
    def test_next_action_no_config(self):
        from skitter.services import _next_action

        assert "setup" in _next_action(False, [], [], [])

    def test_next_action_no_runtimes(self):
        from skitter.services import _next_action

        assert "Install" in _next_action(True, [], [], [])

    def test_next_action_not_running(self):
        from skitter.services import _next_action

        result = _next_action(True, ["claude"], [], [])
        assert "skitter up" in result

    def test_next_action_no_agents(self):
        from skitter.services import _next_action

        containers = [("skitter-emqx", "Up 5 minutes", "emqx")]
        result = _next_action(True, ["claude"], containers, [])
        assert "create-agent" in result

    def test_next_action_all_good(self):
        from skitter.services import _next_action

        containers = [("skitter-emqx", "Up 5 minutes", "emqx")]
        agents = [("my-agent", "my-agent.md", "claude")]
        result = _next_action(True, ["claude"], containers, agents)
        assert "skitter ask my-agent" in result

    def test_runtime_detection(self):
        from skitter.config import detect_runtimes

        runtimes = detect_runtimes()
        assert "claude" in runtimes
        assert "codex" in runtimes
        # Values are either a path string or None
        for v in runtimes.values():
            assert v is None or isinstance(v, str)

    def test_compose_coordinator_bind_mounts_skitter_home(self, tmp_path):
        """Regression: coordinator must bind-mount the caller's SKITTER_HOME,
        not a named Docker volume."""
        from skitter.config import SkitterConfig, BrokerConfig
        from skitter.services import _generate_compose

        cfg = SkitterConfig(broker=BrokerConfig(tier="docker"))
        with patch.dict("os.environ", {"SKITTER_HOME": str(tmp_path)}, clear=False):
            result = yaml.safe_load(_generate_compose(cfg, []))
        coord_vols = result["services"]["coordinator"]["volumes"]
        home_mount = [v for v in coord_vols if "/home/skitter/.skitter" in v]
        assert home_mount, "Expected a bind mount for .skitter"
        assert home_mount[0].startswith(str(tmp_path) + ":")
