"""Services and container management unit tests."""

import os
from unittest.mock import patch

import yaml


# --- Services (container management) ---


class TestServices:
    def test_resolve_env_docker_tier(self):
        from skitter.config import SkitterConfig, BrokerConfig, LLMConfig

        cfg = SkitterConfig(
            llm=LLMConfig(model="claude-sonnet-4-6", api="anthropic"),
            broker=BrokerConfig(tier="docker"),
            org="myorg",
            unit="myunit",
        )
        with patch.dict("os.environ", {"SKITTER_LLM_API_KEY": "sk-test"}):
            from skitter.services import _resolve_env

            env = _resolve_env(cfg)
        assert env["MQTT_BROKER_URL"] == "mqtt://skitter-emqx:1883"
        assert env["SKITTER_LLM_API_KEY"] == "sk-test"
        assert env["SKITTER_LLM_MODEL"] == "claude-sonnet-4-6"
        assert env["SKITTER_A2A_ORG"] == "myorg"
        assert env["SKITTER_A2A_UNIT"] == "myunit"

    def test_resolve_env_external_tier(self):
        from skitter.config import SkitterConfig, BrokerConfig, LLMConfig

        cfg = SkitterConfig(
            llm=LLMConfig(model="gpt-5.4-mini", api="openai"),
            broker=BrokerConfig(tier="serverless", url="mqtts://broker.emqx.io:8883"),
            org="org",
            unit="unit",
        )
        with patch.dict("os.environ", {"SKITTER_LLM_API_KEY": "sk-openai"}):
            from skitter.services import _resolve_env

            env = _resolve_env(cfg)
        assert env["MQTT_BROKER_URL"] == "mqtts://broker.emqx.io:8883"

    def test_generate_compose_includes_broker(self):
        from skitter.config import SkitterConfig, BrokerConfig
        from skitter.services import _generate_compose, BROKER_IMAGE

        cfg = SkitterConfig(broker=BrokerConfig(tier="docker"))
        result = yaml.safe_load(_generate_compose(cfg, []))
        assert "emqx" in result["services"]
        assert result["services"]["emqx"]["image"] == BROKER_IMAGE

    def test_agent_container_name(self):
        from skitter.services import _agent_container_name

        assert _agent_container_name("researcher") == "skitter-agent-researcher"

    def test_discover_agents(self, tmp_path):
        from skitter.services import _discover_agents

        agents_dir = tmp_path / "agents"
        agents_dir.mkdir()
        (agents_dir / "researcher.md").write_text(
            "---\nname: researcher\ndescription: test\nruntime: claude\n---\nInstructions"
        )
        (agents_dir / "coder.toml").write_text(
            'name = "coder"\nruntime = "codex"\ndescription = "a coder"'
        )
        with patch.dict("os.environ", {"SKITTER_HOME": str(tmp_path)}, clear=False):
            agents = _discover_agents()
        assert len(agents) == 2
        ids = {a[0] for a in agents}
        assert "researcher" in ids
        assert "coder" in ids


# ---------------------------------------------------------------------------
# M3: SKITTER_HOME resolution
# ---------------------------------------------------------------------------


class TestSkitterHomeResolution:
    def test_default_path(self):
        from skitter.config import skitter_home

        with patch.dict("os.environ", {}, clear=False):
            os.environ.pop("SKITTER_HOME", None)
            result = skitter_home()
        from pathlib import Path

        assert result == Path.home() / ".skitter"

    def test_env_override(self, tmp_path):
        from skitter.config import skitter_home

        with patch.dict("os.environ", {"SKITTER_HOME": str(tmp_path)}, clear=False):
            result = skitter_home()
        assert result == tmp_path

    def test_config_file_derives_from_home(self, tmp_path):
        from skitter.config import config_file

        with patch.dict("os.environ", {"SKITTER_HOME": str(tmp_path)}, clear=False):
            result = config_file()
        assert result == tmp_path / "config.yaml"

    def test_load_config_uses_skitter_home(self, tmp_path):
        from skitter.config import load_config

        cfg_file = tmp_path / "config.yaml"
        cfg_file.write_text("broker:\n  tier: public\n  url: mqtt://example.com:1883\n")
        with patch.dict(
            "os.environ",
            {"SKITTER_HOME": str(tmp_path), "SKITTER_LLM_MODEL": ""},
            clear=False,
        ):
            cfg = load_config()
        assert cfg.broker.tier == "public"
        assert cfg.broker.url == "mqtt://example.com:1883"
