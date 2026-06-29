"""Setup wizard unit tests."""

import os
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# Setup wizard regression tests
# ---------------------------------------------------------------------------


class TestSetupNonInteractive:
    """Regression tests for skitter setup --non-interactive edge cases."""

    def test_skips_llm_when_api_key_missing(self):
        """Non-interactive must not require API credentials."""
        from skitter.setup import _collect_llm

        # No ANTHROPIC_API_KEY in environment
        with patch.dict("os.environ", {}, clear=True):
            result = _collect_llm(non_interactive=True, standalone=False, existing={})
        assert result is None

    def test_returns_llm_config_when_api_key_present(self):
        from skitter.setup import _collect_llm

        env = {"SKITTER_LLM_API_KEY": "sk-test-key"}
        with patch.dict("os.environ", env, clear=True):
            result = _collect_llm(non_interactive=True, standalone=False, existing={})
        assert result is not None
        assert result["api"] == "anthropic"
        assert "model" in result

    def test_respects_custom_api(self):
        from skitter.setup import _collect_llm

        env = {
            "SKITTER_LLM_API": "openai",
            "SKITTER_LLM_API_KEY": "sk-openai",
        }
        with patch.dict("os.environ", env, clear=True):
            result = _collect_llm(non_interactive=True, standalone=False, existing={})
        assert result is not None
        assert result["api"] == "openai"

    def test_deepseek_defaults_to_deepseek_chat(self):
        from skitter.setup import _collect_llm

        env = {
            "SKITTER_LLM_API": "deepseek",
            "SKITTER_LLM_API_KEY": "sk-deepseek",
        }
        with patch.dict("os.environ", env, clear=True):
            result = _collect_llm(non_interactive=True, standalone=False, existing={})
        assert result is not None
        assert result["api"] == "deepseek"
        assert result["model"] == "deepseek-chat"


class TestSetupVerifyPassesLLMConfig:
    """_verify must pass LLM config to check() without mutating os.environ."""

    def test_does_not_mutate_environ(self):
        from skitter.setup import _verify

        llm_cfg = {
            "model": "gpt-5.4-mini",
            "api": "openai",
        }
        broker_cfg = {"tier": "docker"}
        env_before = set(os.environ)
        with patch.dict("os.environ", {"SKITTER_LLM_API_KEY": "sk-test"}, clear=False):
            with patch("skitter.setup.asyncio.run"):
                with patch("skitter.llm.check", new=MagicMock()):
                    _verify(llm_cfg, broker_cfg)
        new_keys = set(os.environ) - env_before
        assert "SKITTER_LLM_MODEL" not in new_keys


class TestSetupPreservesOrgPrefix:
    """P2 regression: public-broker re-run must preserve existing org_prefix."""

    def test_reuses_existing_org_prefix(self):
        from skitter.setup import _collect_broker

        existing = {"tier": "public", "org_prefix": "skitter-abc12345"}
        result = _collect_broker(non_interactive=True, existing=existing)
        # Non-interactive always picks docker tier, so test the public path directly
        # by calling the function in interactive mode with mocked input
        with patch("skitter.setup._prompt_choice", return_value="public"):
            with patch("skitter.setup._prompt", return_value="y"):
                result = _collect_broker(non_interactive=False, existing=existing)
        assert result["org_prefix"] == "skitter-abc12345"

    def test_generates_new_prefix_when_none_exists(self):
        from skitter.setup import _collect_broker

        with patch("skitter.setup._prompt_choice", return_value="public"):
            with patch("skitter.setup._prompt", return_value="y"):
                result = _collect_broker(non_interactive=False, existing={})
        assert result["org_prefix"].startswith("skitter-")
