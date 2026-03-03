"""Configuration loader for ~/.skitter/ agents and pipelines."""

import logging
import string
from dataclasses import dataclass, field
from pathlib import Path

import yaml

from skitter.types import AgentCard

log = logging.getLogger("skitter.config")

SKITTER_DIR = Path.home() / ".skitter"
AGENTS_DIR = SKITTER_DIR / "agents"
PIPELINES_DIR = SKITTER_DIR / "pipelines"


@dataclass
class AgentDef:
    id: str
    name: str
    description: str = ""
    soul: str = ""
    skills: str = ""
    model: str = ""
    max_turns: int = 10


@dataclass
class PipelineTask:
    logical_id: str
    agent: str
    description: str
    depends_on: list[str] = field(default_factory=list)
    # Per-task overrides (empty = use agent default)
    soul: str = ""
    skills: str = ""
    model: str = ""
    max_turns: int = 0  # 0 = use agent default


@dataclass
class PipelineDef:
    id: str
    name: str
    description: str = ""
    variables: list[str] = field(default_factory=list)
    tasks: list[PipelineTask] = field(default_factory=list)


class SafeFormatter(string.Formatter):
    """Formatter that leaves unknown {placeholders} untouched."""

    def vformat(self, format_string: str, args: tuple, kwargs: dict) -> str:
        # Override to handle missing keys gracefully
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
                    # Reconstruct the original placeholder
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


def ensure_dirs() -> None:
    AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    PIPELINES_DIR.mkdir(parents=True, exist_ok=True)


def load_agents() -> dict[str, AgentDef]:
    agents: dict[str, AgentDef] = {}
    if not AGENTS_DIR.is_dir():
        return agents
    for path in sorted(AGENTS_DIR.glob("*.yaml")):
        try:
            data = yaml.safe_load(path.read_text())
            if not isinstance(data, dict):
                continue
            agent_id = path.stem
            agents[agent_id] = AgentDef(
                id=agent_id,
                name=data.get("name", agent_id),
                description=data.get("description", ""),
                soul=data.get("soul", ""),
                skills=data.get("skills", ""),
                model=data.get("model", ""),
                max_turns=data.get("max_turns", 10),
            )
        except Exception as e:
            log.warning("Failed to load agent %s: %s", path.name, e)
    return agents


def load_pipelines() -> dict[str, PipelineDef]:
    pipelines: dict[str, PipelineDef] = {}
    if not PIPELINES_DIR.is_dir():
        return pipelines
    for path in sorted(PIPELINES_DIR.glob("*.yaml")):
        try:
            data = yaml.safe_load(path.read_text())
            if not isinstance(data, dict):
                continue
            pipeline_id = path.stem
            tasks = []
            for t in data.get("tasks", []):
                tasks.append(
                    PipelineTask(
                        logical_id=t["logical_id"],
                        agent=t.get("agent", "worker"),
                        description=t.get("description", ""),
                        depends_on=t.get("depends_on", []),
                        soul=t.get("soul", ""),
                        skills=t.get("skills", ""),
                        model=t.get("model", ""),
                        max_turns=t.get("max_turns", 0),
                    )
                )
            pipelines[pipeline_id] = PipelineDef(
                id=pipeline_id,
                name=data.get("name", pipeline_id),
                description=data.get("description", ""),
                variables=data.get("variables", []),
                tasks=tasks,
            )
        except Exception as e:
            log.warning("Failed to load pipeline %s: %s", path.name, e)
    return pipelines


# --- Example files for `skitter init` ---

_EXAMPLES_DIR = Path(__file__).parent / "examples"


def _load_examples(subdir: str) -> dict[str, str]:
    """Load example YAML files from skitter/examples/{subdir}/."""
    examples: dict[str, str] = {}
    d = _EXAMPLES_DIR / subdir
    if d.is_dir():
        for path in sorted(d.glob("*.yaml")):
            examples[path.stem] = path.read_text()
    return examples


EXAMPLE_AGENTS = _load_examples("agents")
EXAMPLE_PIPELINES = _load_examples("pipelines")


WORKSPACES_DIR = SKITTER_DIR / "workspaces"


def agent_def_to_card(agent: "AgentDef") -> AgentCard:
    """Convert an AgentDef to an A2A Agent Card for discovery publishing."""
    capabilities: list[str] = []
    if agent.max_turns > 0:
        capabilities.append("tool_use")
    return AgentCard(
        agent_id=agent.id,
        name=agent.name,
        description=agent.description,
        capabilities=capabilities,
        model=agent.model,
        max_turns=agent.max_turns,
    )


def write_examples() -> tuple[list[str], list[str]]:
    """Write example agent and pipeline files. Returns (agents_written, pipelines_written)."""
    ensure_dirs()
    agents_written: list[str] = []
    pipelines_written: list[str] = []
    for name, content in EXAMPLE_AGENTS.items():
        path = AGENTS_DIR / f"{name}.yaml"
        if not path.exists():
            path.write_text(content)
            agents_written.append(name)
    for name, content in EXAMPLE_PIPELINES.items():
        path = PIPELINES_DIR / f"{name}.yaml"
        if not path.exists():
            path.write_text(content)
            pipelines_written.append(name)
    return agents_written, pipelines_written
