"""LLM API wrapper using litellm.

litellm is imported lazily inside complete() to avoid its 3-4s import
penalty at module load time.
"""

import logging
import os

from skitter.config import load_llm_config

log = logging.getLogger("skitter.llm")


def _resolve_model(override: str = "") -> str:
    """Return the LLM model name from override, config, or env. Raises if unset."""
    model = (
        override or load_llm_config().model or os.environ.get("SKITTER_LLM_MODEL", "")
    )
    if not model:
        raise ValueError(
            "No LLM model configured. Set llm.model in ~/.skitter/config.yaml "
            "or SKITTER_LLM_MODEL env var."
        )
    return model


async def check() -> None:
    """Verify LLM connectivity with a minimal API call. Raises on failure."""
    from litellm import acompletion

    model = _resolve_model()
    log.info("Checking LLM connectivity (model=%s) ...", model)
    response = await acompletion(
        model=model,
        messages=[{"role": "user", "content": "ping"}],
        max_tokens=5,
    )
    content = response.choices[0].message.content
    if content is None:
        raise ValueError("LLM returned no content")
    log.info("LLM OK")


async def complete(
    prompt: str,
    *,
    system: str = "",
    model: str = "",
) -> str:
    """Call LLM API, return text response."""
    from litellm import acompletion

    model = _resolve_model(model)

    messages = []
    if system:
        messages.append({"role": "system", "content": system})
    messages.append({"role": "user", "content": prompt})

    try:
        response = await acompletion(model=model, messages=messages)
    except Exception:
        log.exception("LLM call failed (model=%s)", model)
        raise

    content = response.choices[0].message.content
    if content is None:
        raise ValueError("LLM returned no text content")
    return content
