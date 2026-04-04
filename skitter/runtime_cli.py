"""Shared runtime CLI helpers: stream-json text extraction and output cleanup."""

import json


def extract_claude_text(event: dict) -> str:
    """Extract text from a Claude stream-json event."""
    if event.get("type") == "assistant":
        return "".join(
            block.get("text", "")
            for block in event.get("message", {}).get("content", [])
            if block.get("type") == "text"
        )
    return ""


def extract_codex_text(event: dict) -> str:
    """Extract text from a Codex stream-json event."""
    if event.get("type") == "item.completed":
        item = event.get("item", {})
        if item.get("type") == "agent_message":
            return item.get("text", "")
    return ""


def extract_text(event: dict, runtime: str) -> str:
    """Extract text from a stream-json event for the given runtime."""
    if runtime == "codex":
        return extract_codex_text(event)
    return extract_claude_text(event)


def extract_session_id(event: dict) -> str:
    """Extract CLI-native session identifier from a stream-json event."""
    return event.get("session_id") or event.get("thread_id") or ""


def parse_stream_output(stdout: str, runtime: str) -> list[str]:
    """Parse all text fragments from stream-json output lines."""
    texts: list[str] = []
    for line in stdout.splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            event = json.loads(line)
        except json.JSONDecodeError:
            continue
        text = extract_text(event, runtime)
        if text:
            texts.append(text)
    return texts


def clean_output(text: str, fmt: str = "md") -> str:
    """Strip markdown fences and preamble from LLM output.

    *fmt* is "md" (YAML frontmatter) or "toml".
    """
    # Remove markdown fences
    lines = [ln for ln in text.splitlines(True) if not ln.startswith("```")]
    text = "".join(lines)
    # Strip preamble before actual file content
    if fmt == "md" or text.lstrip().startswith("---"):
        idx = text.find("---")
        if idx >= 0:
            text = text[idx:]
    elif fmt == "toml":
        for marker in ("name", "model", "description", "developer_instructions"):
            idx = text.find(marker)
            if idx >= 0:
                text = text[idx:]
                break
    return text.strip()
