"""LLM API wrapper using direct Anthropic and OpenAI SDKs.

Provider is selected by ``llm.api`` in config (default: ``anthropic``).
Optional ``llm.base_url`` overrides the endpoint for either provider.
"""

import logging
import os

from skitter.config import LLMConfig, load_config

log = logging.getLogger("skitter.llm")

# env var name → cached SDK client
_client_cache: dict[str, object] = {}

_API_KEY_ENV = "SKITTER_LLM_API_KEY"


def _resolve_model(cfg: LLMConfig) -> str:
    if not cfg.model:
        raise ValueError(
            "No LLM model configured. Set llm.model in ~/.skitter/config.yaml "
            "or SKITTER_LLM_MODEL env var."
        )
    return cfg.model


def _api_key() -> str:
    key = os.environ.get(_API_KEY_ENV, "")
    if not key:
        raise ValueError(
            f"API key not set. Set the {_API_KEY_ENV} environment variable."
        )
    return key


def _anthropic_client(cfg: LLMConfig):
    from anthropic import AsyncAnthropic

    cache_key = f"anthropic:{cfg.base_url}"
    if cache_key in _client_cache:
        return _client_cache[cache_key]
    kwargs: dict = {"api_key": _api_key()}
    if cfg.base_url:
        kwargs["base_url"] = cfg.base_url
    client = AsyncAnthropic(**kwargs)
    _client_cache[cache_key] = client
    return client


def _openai_client(cfg: LLMConfig):
    from openai import AsyncOpenAI

    cache_key = f"openai:{cfg.base_url}"
    if cache_key in _client_cache:
        return _client_cache[cache_key]
    kwargs: dict = {"api_key": _api_key()}
    if cfg.base_url:
        kwargs["base_url"] = cfg.base_url
    client = AsyncOpenAI(**kwargs)
    _client_cache[cache_key] = client
    return client


# --- Public API ---


async def check(cfg: LLMConfig | None = None) -> None:
    """Verify LLM connectivity with a minimal API call. Raises on failure."""
    if cfg is None:
        cfg = load_config().llm
    model = _resolve_model(cfg)
    log.info("Checking LLM connectivity (api=%s, model=%s) ...", cfg.api, model)

    if cfg.api == "openai":
        client = _openai_client(cfg)
        response = await client.responses.create(model=model, input="ping")
        if not response.output_text:
            raise ValueError("LLM returned no content")
    else:
        client = _anthropic_client(cfg)
        response = await client.messages.create(
            model=model, max_tokens=5, messages=[{"role": "user", "content": "ping"}]
        )
        if not response.content:
            raise ValueError("LLM returned no content")

    log.info("LLM OK")


async def complete(
    prompt: str,
    *,
    system: str = "",
    model: str = "",
) -> str:
    """Call LLM API, return text response."""
    cfg = load_config().llm
    model = model or _resolve_model(cfg)

    log.debug(
        "LLM request (api=%s, model=%s, prompt=%d chars)", cfg.api, model, len(prompt)
    )
    try:
        if cfg.api == "openai":
            return await _complete_openai(prompt, system=system, model=model, cfg=cfg)
        return await _complete_anthropic(prompt, system=system, model=model, cfg=cfg)
    except Exception:
        log.exception("LLM call failed (api=%s, model=%s)", cfg.api, model)
        raise


async def _complete_anthropic(
    prompt: str, *, system: str, model: str, cfg: LLMConfig
) -> str:
    client = _anthropic_client(cfg)
    kwargs: dict = {
        "model": model,
        "messages": [{"role": "user", "content": prompt}],
        "max_tokens": 4096,
    }
    if system:
        kwargs["system"] = system
    response = await client.messages.create(**kwargs)
    text = "".join(block.text for block in response.content if hasattr(block, "text"))
    if not text:
        raise ValueError("LLM returned no text content")
    log.debug("LLM response (%d chars)", len(text))
    return text


async def _complete_openai(
    prompt: str, *, system: str, model: str, cfg: LLMConfig
) -> str:
    client = _openai_client(cfg)
    kwargs: dict = {"model": model, "input": prompt}
    if system:
        kwargs["instructions"] = system
    response = await client.responses.create(**kwargs)
    text = response.output_text
    if not text:
        raise ValueError("LLM returned no text content")
    log.debug("LLM response (%d chars)", len(text))
    return text
