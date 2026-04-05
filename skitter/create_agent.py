"""Create agent definitions and skills via runtime-backed generation.

    skitter create-agent <name> <prompt> [options]

Generates an agent definition file in ~/.skitter/agents/ by expanding a
short prompt into a complete definition using the selected runtime
(claude or codex). Optionally creates skill files too.
"""

import argparse
import logging
import os
import re
import subprocess
import sys
from pathlib import Path

from skitter.runtime_cli import clean_output, parse_stream_output

log = logging.getLogger("skitter.create_agent")

KEBAB_RE = re.compile(r"^[a-z][a-z0-9]*(-[a-z0-9]+)*$")

# Maps runtime -> file extension
RUNTIME_EXT: dict[str, str] = {
    "claude": ".md",
    "codex": ".toml",
}

# Auth env var per runtime, in priority order
_RUNTIME_AUTH_VARS: dict[str, list[str]] = {
    "claude": ["CLAUDE_CODE_OAUTH_TOKEN", "ANTHROPIC_API_KEY"],
    "codex": ["OPENAI_API_KEY"],
}

_AUTH_HINTS: dict[str, str] = {
    "CLAUDE_CODE_OAUTH_TOKEN": "generate via: claude setup-token",
    "ANTHROPIC_API_KEY": "fallback if no OAuth token",
    "OPENAI_API_KEY": "",
}


MD_FORMAT = """\
File format: YAML frontmatter between --- delimiters, followed by system instructions.
The runtime field MUST be included in the frontmatter.

Frontmatter fields:
- name (required): the agent name
- runtime (required): the runtime that executes this agent
- description (required): one-line description (under 80 chars)
- model (optional): model name or variant
- skills (optional): list of skill names from ~/.skitter/skills/ to attach to this agent
- maxTurns (optional): max conversation turns (default 3 for simple tasks, up to 15 for complex ones)
- tools (optional): comma-separated list of allowed tools. \
Available: Read, Grep, Glob, Bash, WebSearch, WebFetch, Write, Edit, NotebookEdit. \
Only include tools the agent actually needs."""

TOML_FORMAT = """\
File format: TOML.

Fields:
- name (required): agent name
- runtime (required): the runtime that executes this agent
- description (required): one-line description
- model (optional): model name (e.g. "gpt-5.4")
- developer_instructions (required): multi-line string with system instructions"""

SKILL_FORMAT = """\
File format: YAML frontmatter between --- delimiters, followed by detailed skill instructions.

Frontmatter fields:
- name (required): the skill name
- description (required): one-line description of when this skill should trigger"""

SUFFIX = (
    "\n\nOutput ONLY the raw file contents. "
    "No markdown fences, no commentary, no questions, no explanation before or after."
)


_RUNTIME_AUTH_FILES: dict[str, Path] = {
    "codex": Path.home() / ".codex" / "auth.json",
}


def _prompt_auth(runtime: str) -> dict[str, str]:
    """Prompt for runtime auth credentials. Returns {VAR: value} for non-empty entries.

    Skips prompting if file-based auth (e.g. codex auth.json) already exists.
    """
    auth_file = _RUNTIME_AUTH_FILES.get(runtime)
    if auth_file and auth_file.is_file():
        print(f"\nUsing existing {auth_file} for {runtime} auth.", file=sys.stderr)
        return {}

    candidates = _RUNTIME_AUTH_VARS.get(runtime, [])
    if not candidates:
        return {}

    print(f"\n--- {runtime} auth ---\n", file=sys.stderr)
    result: dict[str, str] = {}
    for var in candidates:
        default = os.environ.get(var, "")
        hint = _AUTH_HINTS.get(var, "")
        label = f"{var}"
        if hint:
            label += f" ({hint})"
        prompt_str = f"{label} [{default[:8]}...]" if default else label
        value = input(f"{prompt_str}: ").strip() or default
        if value:
            result[var] = value
            break  # use the first available credential
    return result


def _build_agent_prompt(
    name: str,
    prompt: str,
    runtime: str,
    model: str,
    skills: list[tuple[str, str]],
) -> str:
    parts = [
        "Generate an agent definition file.",
        f"\nAgent name: {name}",
        f"What it does: {prompt}",
    ]
    if model:
        parts.append(f"The agent should use model: {model}")

    if skills:
        parts.append(
            "\nThe agent has the following skills "
            "(each is a separate SKILL.md file loaded on demand):"
        )
        for sname, sdesc in skills:
            parts.append(f"- {sname}: {sdesc}")
        parts.append(
            "\nThe agent definition should be a lean base persona that references these skills. "
            "Keep the agent instructions short: describe the role and how to pick the right skill. "
            "Do NOT include the full skill instructions in the agent definition."
        )

    ext = RUNTIME_EXT[runtime]
    format_spec = TOML_FORMAT if ext == ".toml" else MD_FORMAT
    parts.append(f"\nRuntime value for this agent: {runtime}")
    parts.append(f"\n{format_spec}")
    parts.append(
        "\nWrite clear, concise system instructions covering: "
        "role, approach, output format (if any), and constraints."
    )
    parts.append(SUFFIX)
    return "\n".join(parts)


def _build_skill_prompt(
    agent_name: str,
    agent_desc: str,
    skill_name: str,
    skill_desc: str,
) -> str:
    return (
        f"Generate a SKILL.md file for an agent skill.\n\n"
        f"Agent: {agent_name} ({agent_desc})\n"
        f"Skill name: {skill_name}\n"
        f"Skill description: {skill_desc}\n\n"
        f"{SKILL_FORMAT}\n\n"
        f"After the frontmatter, write detailed instructions for the agent to follow "
        f"when this skill is invoked. "
        f"Cover: procedure, tools to use, output format, edge cases, and constraints."
        f"{SUFFIX}"
    )


def _inject_skill_refs(content: str, skill_names: list[str], fmt: str) -> str:
    """Ensure the agent definition includes a ``skills`` field listing skill names.

    For md: adds ``skills: [a, b]`` to the YAML frontmatter if missing.
    For toml: adds ``skills = ["a", "b"]`` if missing.
    """
    if fmt == "md":
        idx = content.find("\n---", 3)
        if idx >= 0:
            frontmatter = content[:idx]
            if "\nskills:" not in frontmatter and not frontmatter.startswith("skills:"):
                names = ", ".join(skill_names)
                content = frontmatter + f"\nskills: [{names}]" + content[idx:]
    elif fmt == "toml":
        if not re.search(r"^\s*skills\s*=", content, re.MULTILINE):
            quoted = ", ".join(f'"{n}"' for n in skill_names)
            content += f"\nskills = [{quoted}]\n"
    return content


def _parse_skill(value: str) -> tuple[str, str]:
    """Parse 'name: description' into (name, description). Raises on bad format."""
    if ":" not in value:
        raise argparse.ArgumentTypeError(
            f"skill must be 'name: description', got: {value}"
        )
    name, desc = value.split(":", 1)
    name = name.strip()
    desc = desc.strip()
    if not KEBAB_RE.match(name):
        raise argparse.ArgumentTypeError(f"skill name must be kebab-case, got: {name}")
    if not desc:
        raise argparse.ArgumentTypeError(f"skill '{name}' has empty description")
    return name, desc


def _build_generate_cmd(prompt: str, runtime: str) -> list[str]:
    """Build a CLI command for one-shot generation."""
    if runtime == "codex":
        return [
            "codex",
            "exec",
            "--json",
            "--full-auto",
            "--ephemeral",
            "--color",
            "never",
            "--skip-git-repo-check",
            "-c",
            "approval_policy=never",
            prompt,
        ]
    return [
        "claude",
        "-p",
        prompt,
        "--output-format",
        "stream-json",
        "--permission-mode",
        "auto",
        "--verbose",
    ]


def _generate(prompt: str, runtime: str) -> str:
    """Generate text using the selected runtime CLI."""
    cmd = _build_generate_cmd(prompt, runtime)
    binary = cmd[0]
    try:
        proc = subprocess.run(cmd, capture_output=True, text=True, timeout=120)
    except FileNotFoundError:
        print(f"Error: '{binary}' CLI not found on PATH.", file=sys.stderr)
        sys.exit(1)

    texts = parse_stream_output(proc.stdout, runtime)

    if not texts:
        if proc.returncode:
            print(
                f"Error: {binary} CLI exited with code {proc.returncode}",
                file=sys.stderr,
            )
        else:
            print(
                f"Error: {binary} CLI produced no parseable output",
                file=sys.stderr,
            )
        if proc.stderr:
            print(proc.stderr[:500], file=sys.stderr)
        sys.exit(1)

    return "\n".join(texts)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="skitter create-agent",
        description="Create an agent definition via runtime-backed generation.",
    )
    parser.add_argument("name", help="Agent name (kebab-case)")
    parser.add_argument("prompt", help="What the agent should do (plain English)")
    parser.add_argument(
        "--runtime",
        choices=list(RUNTIME_EXT),
        default="claude",
        help="Runtime (default: claude)",
    )
    parser.add_argument("--model", default="", help="Model the agent should use")
    parser.add_argument(
        "--skill",
        action="append",
        default=[],
        type=_parse_skill,
        metavar="name:description",
        help="Add a skill (repeatable)",
    )
    parser.add_argument(
        "--dry-run", action="store_true", help="Print generated files without writing"
    )
    parser.add_argument(
        "--edit", action="store_true", help="Open $EDITOR on the result before saving"
    )
    parser.add_argument("--force", action="store_true", help="Overwrite existing files")
    return parser


def run(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)

    if not KEBAB_RE.match(args.name):
        parser.error(f"name must be kebab-case (e.g. my-agent), got: {args.name}")

    from skitter.config import skitter_home

    agents_dir = skitter_home() / "agents"
    agents_dir.mkdir(parents=True, exist_ok=True)
    ext = RUNTIME_EXT[args.runtime]
    outfile = agents_dir / f"{args.name}{ext}"

    if outfile.exists() and not args.force and not args.dry_run:
        print(
            f"Error: {outfile} already exists. Use --force to overwrite.",
            file=sys.stderr,
        )
        sys.exit(1)

    # Build prompts
    agent_prompt = _build_agent_prompt(
        args.name, args.prompt, args.runtime, args.model, args.skill
    )
    skill_prompts = {
        sname: _build_skill_prompt(args.name, args.prompt, sname, sdesc)
        for sname, sdesc in args.skill
    }

    # Generate agent + skills sequentially
    print("Generating agent definition...", file=sys.stderr)
    agent_text = _generate(agent_prompt, args.runtime)
    skill_texts: dict[str, str] = {}
    for sname, sprompt in skill_prompts.items():
        print(f"Generating skill: {sname}...", file=sys.stderr)
        skill_texts[sname] = _generate(sprompt, args.runtime)

    # Clean up outputs
    fmt = "toml" if ext == ".toml" else "md"
    agent_content = clean_output(agent_text, fmt)
    skill_contents = {
        sname: clean_output(content, "md") for sname, content in skill_texts.items()
    }

    # Ensure the agent definition references its skills
    if skill_contents:
        agent_content = _inject_skill_refs(agent_content, list(skill_contents), fmt)

    from skitter.config import skills_dir as _skills_dir

    skills_base = _skills_dir()

    # Dry run
    if args.dry_run:
        print(f"=== {outfile} ===")
        print(agent_content)
        for sname, content in skill_contents.items():
            print(f"\n=== {skills_base / sname / 'SKILL.md'} ===")
            print(content)
        return

    # Open editor if requested (agent definition only)
    if args.edit:
        import tempfile

        with tempfile.NamedTemporaryFile(mode="w", suffix=ext, delete=False) as f:
            f.write(agent_content)
            tmppath = f.name
        try:
            editor = os.environ.get("EDITOR", "vi")
            os.system(f'{editor} "{tmppath}"')
            agent_content = Path(tmppath).read_text()
        finally:
            Path(tmppath).unlink(missing_ok=True)

    # Write agent definition
    outfile.write_text(agent_content + "\n")
    print(f"Created {outfile}", file=sys.stderr)

    # Write skills to shared library
    for sname, content in skill_contents.items():
        skill_dir = skills_base / sname
        skill_file = skill_dir / "SKILL.md"
        skill_dir.mkdir(parents=True, exist_ok=True)
        skill_file.write_text(content + "\n")
        print(f"Created {skill_file}", file=sys.stderr)

    # Prompt for runtime auth and write .env
    env_file = agents_dir / f"{args.name}.env"
    auth_vars = _prompt_auth(args.runtime)
    if auth_vars:
        from skitter.config import write_env_file

        write_env_file(env_file, auth_vars)
        print(f"Created {env_file}", file=sys.stderr)

    print(file=sys.stderr)
    print(agent_content, file=sys.stderr)
    print(f"\nStart with:  skitter up --agent {args.name}", file=sys.stderr)
    print(f"Or directly:  skitter agent-runner {outfile}", file=sys.stderr)


def main() -> None:
    """Entry point from CLI dispatch."""
    run(sys.argv[2:])
