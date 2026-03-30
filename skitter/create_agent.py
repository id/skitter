"""Create agent definitions and skills via LLM expansion.

    skitter create-agent <name> <prompt> [options]

Generates an agent definition file in ./agents/ by expanding a short
prompt into a complete definition via the configured LLM. Optionally
creates skill files too.
"""

import argparse
import asyncio
import logging
import os
import re
import sys
from pathlib import Path

log = logging.getLogger("skitter.create_agent")

KEBAB_RE = re.compile(r"^[a-z][a-z0-9]*(-[a-z0-9]+)*$")

# Maps runtime -> (file extension, agent directory for symlink)
RUNTIME_INFO: dict[str, tuple[str, str]] = {
    "claude": (".md", ".claude/agents"),
    "codex": (".toml", ".codex/agents"),
    "copilot": (".md", ".github/agents"),
    "gemini": (".md", ".gemini/agents"),
    "qwen": (".md", ".qwen/agents"),
}

MD_FORMAT = """\
File format: YAML frontmatter between --- delimiters, followed by system instructions.
The runtime field MUST be included in the frontmatter.

Frontmatter fields:
- name (required): the agent name
- runtime (required): the runtime that executes this agent
- description (required): one-line description (under 80 chars)
- model (optional): model name or variant
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

    ext, _ = RUNTIME_INFO[runtime]
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


def _clean_output(text: str, fmt: str = "md") -> str:
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


def _symlink(src: Path, dst: Path) -> None:
    """Create a symlink from dst -> src, overwriting if needed."""
    dst.parent.mkdir(parents=True, exist_ok=True)
    if dst.is_symlink() or dst.exists():
        dst.unlink()
    dst.symlink_to(src.resolve())


async def _generate(llm_prompt: str) -> str:
    from skitter.llm import complete

    return await complete(llm_prompt)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="skitter create-agent",
        description="Create an agent definition via LLM expansion.",
    )
    parser.add_argument("name", help="Agent name (kebab-case)")
    parser.add_argument("prompt", help="What the agent should do (plain English)")
    parser.add_argument(
        "--runtime",
        choices=list(RUNTIME_INFO),
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

    agents_dir = Path("agents")
    agents_dir.mkdir(exist_ok=True)
    ext, link_dir_str = RUNTIME_INFO[args.runtime]
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

    # Generate all in parallel
    _AGENT_KEY = object()  # sentinel to avoid collision with skill names

    async def _generate_all():
        tasks = {_AGENT_KEY: _generate(agent_prompt)}
        for sname, sprompt in skill_prompts.items():
            tasks[sname] = _generate(sprompt)

        print("Generating agent definition...", file=sys.stderr)
        if skill_prompts:
            skill_names = ", ".join(skill_prompts)
            print(f"Generating skills: {skill_names}...", file=sys.stderr)

        results = {}
        gathered = await asyncio.gather(*tasks.values(), return_exceptions=True)
        for key, result in zip(tasks, gathered):
            if isinstance(result, Exception):
                print(f"Error generating {key}: {result}", file=sys.stderr)
                sys.exit(1)
            results[key] = result
        return results

    results = asyncio.run(_generate_all())

    # Clean up outputs
    fmt = "toml" if ext == ".toml" else "md"
    agent_content = _clean_output(results.pop(_AGENT_KEY), fmt)
    skill_contents = {
        sname: _clean_output(content, "md") for sname, content in results.items()
    }

    # Dry run
    if args.dry_run:
        print(f"=== {outfile} ===")
        print(agent_content)
        for sname, content in skill_contents.items():
            print(f"\n=== .agents/skills/{sname}/SKILL.md ===")
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

    # Symlink into runtime's agent directory
    link_dir = Path(link_dir_str)
    _symlink(outfile, link_dir / outfile.name)

    print(f"Created {outfile} (symlinked to {link_dir}/)", file=sys.stderr)

    # Write skills
    for sname, content in skill_contents.items():
        skill_dir = Path(f".agents/skills/{sname}")
        skill_file = skill_dir / "SKILL.md"
        skill_dir.mkdir(parents=True, exist_ok=True)
        skill_file.write_text(content + "\n")

        # Symlink to .claude/skills/ for Claude Code discovery
        claude_skill = Path(f".claude/skills/{sname}/SKILL.md")
        _symlink(skill_file, claude_skill)

        print(
            f"Created {skill_file} (symlinked to .claude/skills/{sname}/)",
            file=sys.stderr,
        )

    print(file=sys.stderr)
    print(agent_content, file=sys.stderr)
    print(f"\nRun:  uv run python -m skitter agent-runner {outfile}", file=sys.stderr)


def main() -> None:
    """Entry point from CLI dispatch."""
    run(sys.argv[2:])
