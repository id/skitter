"""Configuration loader for ~/.skitter/ agents and workflows."""

import json
import logging
import string
from dataclasses import dataclass, field
from pathlib import Path

import yaml

log = logging.getLogger("skitter.config")

SKITTER_DIR = Path.home() / ".skitter"
AGENTS_DIR = SKITTER_DIR / "agents"
WORKFLOWS_DIR = SKITTER_DIR / "workflows"


@dataclass
class AgentDef:
    id: str
    name: str
    description: str = ""
    runtime: str = ""  # "claude" or "codex"; empty = use default_runtime
    workspace: str = ""  # custom cwd for worker


@dataclass
class WorkflowTask:
    id: str
    agent: str
    description: str
    next: str = ""  # target task id, or "output" for terminal
    needs: list[str] = field(default_factory=list)
    # Per-task overrides (empty = use sub-agent default)
    model: str = ""


@dataclass
class WorkflowDef:
    id: str
    name: str
    description: str = ""
    variables: list[str] = field(default_factory=list)
    tasks: list[WorkflowTask] = field(default_factory=list)
    workspace: str = ""  # persistent workspace slug


@dataclass
class WorkspaceConfig:
    remote: str = ""  # rclone remote name
    local_mount: str = ""  # local mount for subprocess mode (e.g. Google Drive)
    base_path: str = "skitter/workspaces"


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


def load_default_runtime() -> str:
    """Read default_runtime from ~/.skitter/config.yaml. Falls back to 'claude'."""
    return _load_global_config().get("default_runtime", "claude")


def load_workspace_config() -> WorkspaceConfig:
    """Read workspace config from ~/.skitter/config.yaml."""
    data = _load_global_config().get("workspace", {})
    if not isinstance(data, dict):
        return WorkspaceConfig()
    return WorkspaceConfig(
        remote=data.get("remote", ""),
        local_mount=data.get("local_mount", ""),
        base_path=data.get("base_path", "skitter/workspaces"),
    )


def ensure_dirs() -> None:
    AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    WORKFLOWS_DIR.mkdir(parents=True, exist_ok=True)


def load_agents(
    default_runtime: str = "", agents_dir: Path | None = None
) -> dict[str, AgentDef]:
    default_runtime = default_runtime or load_default_runtime()
    agents: dict[str, AgentDef] = {}
    d = agents_dir or AGENTS_DIR
    if not d.is_dir():
        return agents
    for path in sorted(d.glob("*.yaml")):
        try:
            data = yaml.safe_load(path.read_text())
            if not isinstance(data, dict):
                continue
            agent_id = path.stem
            agents[agent_id] = AgentDef(
                id=agent_id,
                name=data.get("name", agent_id),
                description=data.get("description", ""),
                runtime=data.get("runtime", "") or default_runtime,
                workspace=data.get("workspace", ""),
            )
        except Exception as e:
            log.warning("Failed to load agent %s: %s", path.name, e)
    return agents


def infer_task_next(tasks: list[WorkflowTask]) -> None:
    """Auto-infer `next` from reverse dependency graph if absent."""
    for t in tasks:
        if not t.next:
            dependents = [other.id for other in tasks if t.id in other.needs]
            if len(dependents) == 1:
                t.next = dependents[0]
            elif len(dependents) == 0:
                t.next = "output"


def load_workflows(workflows_dir: Path | None = None) -> dict[str, WorkflowDef]:
    workflows: dict[str, WorkflowDef] = {}
    d = workflows_dir or WORKFLOWS_DIR
    if not d.is_dir():
        return workflows
    for path in sorted(d.glob("*.yaml")):
        try:
            data = yaml.safe_load(path.read_text())
            if not isinstance(data, dict):
                continue
            workflow_id = path.stem
            tasks = []
            for t in data.get("tasks", []):
                task_id = t.get("id", "")
                needs = t.get("needs", [])
                tasks.append(
                    WorkflowTask(
                        id=task_id,
                        agent=t.get("agent", "worker"),
                        description=t.get("description", ""),
                        next=t.get("next", ""),
                        needs=needs,
                        model=t.get("model", ""),
                    )
                )
            infer_task_next(tasks)
            workflows[workflow_id] = WorkflowDef(
                id=workflow_id,
                name=data.get("name", workflow_id),
                description=data.get("description", ""),
                variables=data.get("variables", []),
                tasks=tasks,
                workspace=data.get("workspace", ""),
            )
        except Exception as e:
            log.warning("Failed to load workflow %s: %s", path.name, e)
    return workflows


# --- Example files for `skitter init` ---

_EXAMPLES_DIR = Path(__file__).parent / "examples"


def _load_examples(subdir: str, ext: str = "*.yaml") -> dict[str, str]:
    """Load example files from skitter/examples/{subdir}/."""
    examples: dict[str, str] = {}
    d = _EXAMPLES_DIR / subdir
    if d.is_dir():
        for path in sorted(d.glob(ext)):
            examples[path.stem] = path.read_text()
    return examples


EXAMPLE_AGENTS = _load_examples("agents")
EXAMPLE_WORKFLOWS = _load_examples("workflows")
EXAMPLE_CLAUDE_AGENTS = _load_examples("claude-agents", "*.md")
EXAMPLE_CONFIG = (_EXAMPLES_DIR / "config.yaml").read_text()


WORKSPACES_DIR = SKITTER_DIR / "workspaces"
CARDS_DIR = SKITTER_DIR / "cards"
DOCKER_CLAUDE_DIR = SKITTER_DIR / "docker-claude"
CLAUDE_AGENTS_DIR = Path.home() / ".claude" / "agents"


def load_cards() -> dict[str, str]:
    """Load pre-built agent card JSON files from ~/.skitter/cards/."""
    cards: dict[str, str] = {}
    if not CARDS_DIR.is_dir():
        return cards
    for path in sorted(CARDS_DIR.glob("*.json")):
        try:
            raw = path.read_text()
            json.loads(raw)  # validate
            cards[path.stem] = raw
        except Exception:
            pass
    return cards


def write_examples() -> tuple[list[str], list[str]]:
    """Write example agent and workflow files. Returns (agents_written, workflows_written)."""
    ensure_dirs()
    CLAUDE_AGENTS_DIR.mkdir(parents=True, exist_ok=True)
    agents_written: list[str] = []
    workflows_written: list[str] = []
    for name, content in EXAMPLE_AGENTS.items():
        path = AGENTS_DIR / f"{name}.yaml"
        if not path.exists():
            path.write_text(content)
            agents_written.append(name)
    for name, content in EXAMPLE_WORKFLOWS.items():
        path = WORKFLOWS_DIR / f"{name}.yaml"
        if not path.exists():
            path.write_text(content)
            workflows_written.append(name)
    # Claude sub-agent definitions
    for name, content in EXAMPLE_CLAUDE_AGENTS.items():
        path = CLAUDE_AGENTS_DIR / f"{name}.md"
        if not path.exists():
            path.write_text(content)
    # Global config
    if not CONFIG_FILE.exists():
        CONFIG_FILE.write_text(EXAMPLE_CONFIG)
    return agents_written, workflows_written
