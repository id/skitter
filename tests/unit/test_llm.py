"""LLM client unit tests."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest


class TestLLMComplete:
    def setup_method(self):
        import skitter.llm

        skitter.llm._client_cache.clear()

    def _mock_anthropic_response(self, content="test response"):

        mock_block = MagicMock()
        mock_block.text = content
        mock_resp = MagicMock()
        mock_resp.content = [mock_block] if content is not None else []
        mock_create = AsyncMock(return_value=mock_resp)
        mock_client = MagicMock()
        mock_client.messages.create = mock_create
        return mock_client, mock_create

    def _mock_openai_responses(self, content="test response"):
        """Mock for OpenAI Responses API (native OpenAI models)."""

        mock_resp = MagicMock()
        mock_resp.output_text = content
        mock_create = AsyncMock(return_value=mock_resp)
        mock_client = MagicMock()
        mock_client.responses.create = mock_create
        return mock_client, mock_create

    def _mock_openai_chat(self, content="test response"):
        """Mock for OpenAI Chat Completions API (3rd-party compatible)."""

        mock_message = MagicMock()
        mock_message.content = content
        mock_choice = MagicMock()
        mock_choice.message = mock_message
        mock_resp = MagicMock()
        mock_resp.choices = [mock_choice] if content is not None else []
        mock_create = AsyncMock(return_value=mock_resp)
        mock_client = MagicMock()
        mock_client.chat.completions.create = mock_create
        return mock_client, mock_create

    @pytest.mark.asyncio
    async def test_complete_calls_anthropic(self):
        from skitter.config import LLMConfig
        from skitter.llm import complete

        cfg = LLMConfig(model="claude-sonnet-4-6", api="anthropic", api_key="test-key")
        mock_client, mock_create = self._mock_anthropic_response("test response")
        with (
            patch("anthropic.AsyncAnthropic", return_value=mock_client),
            patch("skitter.llm.load_config", return_value=MagicMock(llm=cfg)),
        ):
            result = await complete("hello", model="claude-sonnet-4-6")

        assert result == "test response"
        mock_create.assert_called_once()
        assert mock_create.call_args.kwargs["model"] == "claude-sonnet-4-6"
        msgs = mock_create.call_args.kwargs["messages"]
        assert len(msgs) == 1
        assert msgs[0]["role"] == "user"

    @pytest.mark.asyncio
    async def test_complete_with_system(self):
        from skitter.config import LLMConfig
        from skitter.llm import complete

        cfg = LLMConfig(model="claude-sonnet-4-6", api="anthropic", api_key="test-key")
        mock_client, mock_create = self._mock_anthropic_response("ok")
        with (
            patch("anthropic.AsyncAnthropic", return_value=mock_client),
            patch("skitter.llm.load_config", return_value=MagicMock(llm=cfg)),
        ):
            await complete("hello", system="be helpful", model="claude-sonnet-4-6")

        assert mock_create.call_args.kwargs["system"] == "be helpful"
        msgs = mock_create.call_args.kwargs["messages"]
        assert len(msgs) == 1
        assert msgs[0]["role"] == "user"

    @pytest.mark.asyncio
    async def test_complete_openai_uses_responses_api(self):
        from skitter.config import LLMConfig
        from skitter.llm import complete

        mock_client, mock_create = self._mock_openai_responses("openai response")
        cfg = LLMConfig(model="gpt-5.4-mini", api="openai", api_key="test-key")
        with (
            patch("openai.AsyncOpenAI", return_value=mock_client),
            patch("skitter.llm.load_config", return_value=MagicMock(llm=cfg)),
        ):
            result = await complete("hello", model="gpt-5.4-mini")

        assert result == "openai response"
        mock_create.assert_called_once()
        assert mock_create.call_args.kwargs["model"] == "gpt-5.4-mini"
        assert mock_create.call_args.kwargs["input"] == "hello"

    @pytest.mark.asyncio
    async def test_complete_openai_with_system_uses_instructions(self):
        from skitter.config import LLMConfig
        from skitter.llm import complete

        mock_client, mock_create = self._mock_openai_responses("ok")
        cfg = LLMConfig(model="gpt-5.4-mini", api="openai", api_key="test-key")
        with (
            patch("openai.AsyncOpenAI", return_value=mock_client),
            patch("skitter.llm.load_config", return_value=MagicMock(llm=cfg)),
        ):
            await complete("hello", system="be helpful", model="gpt-5.4-mini")

        assert mock_create.call_args.kwargs["instructions"] == "be helpful"
        assert mock_create.call_args.kwargs["input"] == "hello"

    @pytest.mark.asyncio
    async def test_complete_openai_with_base_url(self):
        """base_url is passed to the OpenAI client."""
        from skitter.config import LLMConfig
        from skitter.llm import complete

        mock_client, mock_create = self._mock_openai_responses("custom response")
        cfg = LLMConfig(
            model="custom-model",
            api="openai",
            base_url="https://api.example.com/v1",
            api_key="test-key",
        )
        with (
            patch("openai.AsyncOpenAI", return_value=mock_client) as mock_cls,
            patch("skitter.llm.load_config", return_value=MagicMock(llm=cfg)),
        ):
            result = await complete("hello", model="custom-model")

        assert result == "custom response"
        assert mock_cls.call_args.kwargs["base_url"] == "https://api.example.com/v1"

    @pytest.mark.asyncio
    async def test_complete_no_model_raises(self):
        from skitter.llm import complete

        with (
            patch(
                "skitter.llm.load_config",
                return_value=MagicMock(llm=MagicMock(model="")),
            ),
            patch.dict("os.environ", {"SKITTER_LLM_MODEL": ""}),
        ):
            with pytest.raises(ValueError, match="No LLM model configured"):
                await complete("hello")

    @pytest.mark.asyncio
    async def test_complete_openai_completions_uses_chat_api(self):
        from skitter.config import LLMConfig
        from skitter.llm import complete

        mock_client, mock_create = self._mock_openai_chat("chat response")
        cfg = LLMConfig(
            model="moonshot-v1-8k", api="openai-completions", api_key="test-key"
        )
        with (
            patch("openai.AsyncOpenAI", return_value=mock_client),
            patch("skitter.llm.load_config", return_value=MagicMock(llm=cfg)),
        ):
            result = await complete("hello", model="moonshot-v1-8k")

        assert result == "chat response"
        mock_create.assert_called_once()
        messages = mock_create.call_args.kwargs["messages"]
        assert len(messages) == 1
        assert messages[0] == {"role": "user", "content": "hello"}

    @pytest.mark.asyncio
    async def test_complete_openai_completions_with_system(self):
        from skitter.config import LLMConfig
        from skitter.llm import complete

        mock_client, mock_create = self._mock_openai_chat("ok")
        cfg = LLMConfig(
            model="moonshot-v1-8k", api="openai-completions", api_key="test-key"
        )
        with (
            patch("openai.AsyncOpenAI", return_value=mock_client),
            patch("skitter.llm.load_config", return_value=MagicMock(llm=cfg)),
        ):
            await complete("hello", system="be helpful", model="moonshot-v1-8k")

        messages = mock_create.call_args.kwargs["messages"]
        assert len(messages) == 2
        assert messages[0] == {"role": "system", "content": "be helpful"}
        assert messages[1] == {"role": "user", "content": "hello"}

    @pytest.mark.asyncio
    async def test_complete_none_content_raises(self):
        from skitter.config import LLMConfig
        from skitter.llm import complete

        cfg = LLMConfig(model="claude-sonnet-4-6", api="anthropic", api_key="test-key")
        mock_client, _ = self._mock_anthropic_response(None)
        with (
            patch("anthropic.AsyncAnthropic", return_value=mock_client),
            patch("skitter.llm.load_config", return_value=MagicMock(llm=cfg)),
        ):
            with pytest.raises(ValueError, match="no text content"):
                await complete("hello", model="claude-sonnet-4-6")
