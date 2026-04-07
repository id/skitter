"""Skitter configuration: data types, config loading, and shared utilities."""

import logging
import os
import string
from dataclasses import dataclass, field
from pathlib import Path

import yaml

log = logging.getLogger("skitter.config")


def configure_logging() -> None:
    """Set up root logger from SKITTER_LOG_LEVEL (default INFO)."""
    level_name = os.environ.get("SKITTER_LOG_LEVEL", "INFO").upper()
    level = getattr(logging, level_name, logging.INFO)
    logging.basicConfig(
        level=level, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
    )


def detect_runtimes() -> dict[str, str | None]:
    """Check which agent runtimes are available on PATH."""
    import shutil

    return {name: shutil.which(name) for name in ("claude", "codex")}


def skitter_home() -> Path:
    """Return the skitter home directory (SKITTER_HOME or ~/.skitter)."""
    if os.environ.get("SKITTER_HOME"):
        return Path(os.environ["SKITTER_HOME"])
    return Path.home() / ".skitter"


def config_file() -> Path:
    """Return the path to config.yaml."""
    return skitter_home() / "config.yaml"


def skills_dir() -> Path:
    """Return the shared skills library path (~/.skitter/skills/)."""
    return skitter_home() / "skills"


def write_env_file(env_path: Path, env_vars: dict[str, str]) -> None:
    """Write a .env file (KEY=value per line).

    Values containing ``$`` are single-quoted so docker compose does not
    try to interpolate them as variable references.
    """
    lines: list[str] = []
    for k, v in env_vars.items():
        if not v:
            continue
        if "$" in v:
            # Single quotes prevent variable interpolation in docker compose
            v = f"'{v}'"
        lines.append(f"{k}={v}")
    env_path.write_text("\n".join(lines) + "\n" if lines else "")


# --- Agent definition ---


@dataclass
class SkillDef:
    """Skill metadata loaded from SKILL.md frontmatter."""

    id: str  # kebab-case directory name
    name: str
    description: str = ""


@dataclass
class AgentDef:
    id: str
    name: str
    description: str = ""
    runtime: str = "claude"  # claude or codex
    model: str = ""
    instructions: str = ""
    max_turns: int = 0  # 0 = runtime default
    tools: list[str] = field(default_factory=list)  # allowed tools (Claude only)
    skill_refs: list[str] = field(default_factory=list)  # skill names from frontmatter
    skills: list[SkillDef] = field(default_factory=list)  # resolved skill metadata
    capabilities: dict[str, bool] = field(default_factory=dict)
    input_modes: list[str] = field(default_factory=lambda: ["text/plain"])
    output_modes: list[str] = field(default_factory=lambda: ["text/plain"])
    tags: list[str] = field(default_factory=list)


# --- Section configs ---


@dataclass
class DBConfig:
    backend: str = "sqlite"
    sqlite_path: str = ""
    postgres_dsn: str = ""


@dataclass
class LLMConfig:
    model: str = ""
    api: str = "anthropic"  # anthropic, openai, or openai-completions
    base_url: str = ""
    api_key: str = ""


# --- Unified config ---


@dataclass
class BrokerConfig:
    tier: str = "docker"  # docker, public, serverless, custom
    url: str = "mqtt://localhost:1883"
    username: str = ""
    password: str = ""
    ca_cert: str = ""

    def client_kwargs(self, **overrides) -> dict:
        """Connection kwargs for aiomqtt.Client.

        Returns a dict with hostname, port, protocol, tls_context (if mqtts),
        and username/password. Caller can pass additional overrides
        (e.g. identifier, will).
        """
        import ssl
        from urllib.parse import urlparse

        import aiomqtt

        parsed = urlparse(self.url)
        host = parsed.hostname or "localhost"
        tls = parsed.scheme == "mqtts"
        port = parsed.port or (8883 if tls else 1883)

        kwargs: dict = {
            "hostname": host,
            "port": port,
            "protocol": aiomqtt.ProtocolVersion.V5,
        }
        if tls:
            ctx = ssl.create_default_context()
            if self.ca_cert:
                ctx.load_verify_locations(self.ca_cert)
            kwargs["tls_context"] = ctx
        if self.username:
            kwargs["username"] = self.username
            kwargs["password"] = self.password
        kwargs.update(overrides)
        return kwargs


@dataclass
class SkitterConfig:
    """Unified config resolved from env vars > ~/.skitter/config.yaml."""

    llm: LLMConfig = field(default_factory=LLMConfig)
    db: DBConfig = field(default_factory=DBConfig)
    broker: BrokerConfig = field(default_factory=BrokerConfig)
    org: str = "skitter"
    unit: str = "default"


# --- Config loading ---


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


def load_raw_config(*, strict: bool = False) -> dict:
    """Read config.yaml as a dict.

    Returns ``{}`` on failure by default. When *strict* is True, raises
    on parse errors or invalid content (used by ``skitter doctor``).
    """
    cfg = config_file()
    if not cfg.is_file():
        if strict:
            raise FileNotFoundError(f"{cfg} not found")
        return {}
    try:
        data = yaml.safe_load(cfg.read_text())
    except yaml.YAMLError as e:
        if strict:
            raise
        log.warning("Failed to read %s: %s", cfg, e)
        return {}
    if not isinstance(data, dict):
        if strict:
            raise ValueError(f"{cfg} is not a valid YAML mapping")
        return {}
    return data


def _env_or(key: str, fallback: str, *, file_only: bool) -> str:
    if file_only:
        return fallback
    return os.environ.get(key, "") or fallback


def load_config(*, file_only: bool = False) -> SkitterConfig:
    """Load unified config from ~/.skitter/config.yaml.

    By default, env vars override config file values. Pass ``file_only=True``
    to ignore env vars (used when generating container env, so the host shell
    doesn't leak into container config).
    """
    data = load_raw_config()

    llm_data = data.get("llm", {}) or {}
    llm = LLMConfig(
        model=_env_or(
            "SKITTER_LLM_MODEL", llm_data.get("model", ""), file_only=file_only
        ),
        api=_env_or(
            "SKITTER_LLM_API", llm_data.get("api", "anthropic"), file_only=file_only
        ),
        base_url=_env_or(
            "SKITTER_LLM_BASE_URL", llm_data.get("base_url", ""), file_only=file_only
        ),
        api_key=_env_or(
            "SKITTER_LLM_API_KEY", llm_data.get("api_key", ""), file_only=file_only
        ),
    )

    db_data = data.get("db", {}) or {}
    db = DBConfig(
        backend=db_data.get("backend", "sqlite"),
        sqlite_path=db_data.get("sqlite_path", str(skitter_home() / "skitter.db")),
        postgres_dsn=db_data.get("postgres_dsn", ""),
    )

    broker_data = data.get("broker", {}) or {}
    broker = BrokerConfig(
        tier=broker_data.get("tier", "docker"),
        url=_env_or(
            "MQTT_BROKER_URL",
            broker_data.get("url", "mqtt://localhost:1883"),
            file_only=file_only,
        ),
        username=_env_or(
            "MQTT_USERNAME", broker_data.get("username", ""), file_only=file_only
        ),
        password=_env_or(
            "MQTT_PASSWORD", broker_data.get("password", ""), file_only=file_only
        ),
        ca_cert=_env_or(
            "MQTT_CA_CERT", broker_data.get("ca_cert", ""), file_only=file_only
        ),
    )

    org = _env_or("SKITTER_A2A_ORG", data.get("org", "skitter"), file_only=file_only)
    unit = _env_or("SKITTER_A2A_UNIT", data.get("unit", "default"), file_only=file_only)

    return SkitterConfig(llm=llm, db=db, broker=broker, org=org, unit=unit)
