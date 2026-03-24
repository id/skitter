"""Skitter configuration — data types and DB config."""

import logging
import string
from dataclasses import dataclass, field
from pathlib import Path

import yaml

log = logging.getLogger("skitter.config")

SKITTER_DIR = Path.home() / ".skitter"


@dataclass
class BrokerConfig:
    host: str = ""
    port: int = 0


@dataclass
class AgentDef:
    id: str
    name: str
    description: str = ""
    runtime: str = ""  # "claude" or "codex"
    model: str = ""  # optional model override
    agent_file: str = ""  # runtime-specific prompt file (e.g. researcher.md)
    system_prompt: str = ""  # codex only: passed via -c developer_instructions
    broker: BrokerConfig | None = None
    capabilities: dict[str, bool] = field(default_factory=dict)
    input_modes: list[str] = field(default_factory=lambda: ["text/plain"])
    output_modes: list[str] = field(default_factory=lambda: ["text/plain"])
    tags: list[str] = field(default_factory=list)


@dataclass
class DBConfig:
    backend: str = "sqlite"
    sqlite_path: str = ""
    postgres_dsn: str = ""


@dataclass
class LLMConfig:
    model: str = ""


class SafeFormatter(string.Formatter):
    """Formatter that leaves unknown {placeholders} untouched."""

    def vformat(self, format_string: str, args: tuple, kwargs: dict) -> str:
        result: list[str] = []
        for literal_text, field_name, format_spec, conversion in self.parse(
            format_string
        ):
            result.append(literal_text)
            if field_name is not None:
                if field_name in kwargs:
                    value = kwargs[field_name]
                    if conversion == "s":
                        value = str(value)
                    elif conversion == "r":
                        value = repr(value)
                    if format_spec:
                        value = format(value, format_spec)
                    result.append(str(value))
                else:
                    placeholder = "{" + field_name
                    if conversion:
                        placeholder += "!" + conversion
                    if format_spec:
                        placeholder += ":" + format_spec
                    placeholder += "}"
                    result.append(placeholder)
        return "".join(result)


_safe_fmt = SafeFormatter()


def safe_format(template: str, variables: dict[str, str]) -> str:
    """Interpolate variables into template, leaving unknown {placeholders} intact."""
    return _safe_fmt.vformat(template, (), variables)


CONFIG_FILE = SKITTER_DIR / "config.yaml"


def _load_global_config() -> dict:
    """Read ~/.skitter/config.yaml as a dict. Returns {} on failure."""
    if CONFIG_FILE.is_file():
        try:
            data = yaml.safe_load(CONFIG_FILE.read_text())
            if isinstance(data, dict):
                return data
        except Exception as e:
            log.warning("Failed to read %s: %s", CONFIG_FILE, e)
    return {}


def load_db_config() -> DBConfig:
    """Read database config from ~/.skitter/config.yaml."""
    data = _load_global_config().get("db", {})
    if not isinstance(data, dict):
        return DBConfig(sqlite_path=str(SKITTER_DIR / "skitter.db"))
    return DBConfig(
        backend=data.get("backend", "sqlite"),
        sqlite_path=data.get("sqlite_path", str(SKITTER_DIR / "skitter.db")),
        postgres_dsn=data.get("postgres_dsn", ""),
    )


def load_llm_config() -> LLMConfig:
    """Read LLM config from ~/.skitter/config.yaml."""
    data = _load_global_config().get("llm", {})
    if not isinstance(data, dict):
        return LLMConfig()
    return LLMConfig(model=data.get("model", ""))
