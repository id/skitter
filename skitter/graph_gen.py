"""Generate and validate orchestration graphs from agent cards + instructions."""

import json
import logging

from skitter.llm import complete

log = logging.getLogger("skitter.graph_gen")

_SYSTEM = """\
You are an app orchestrator. Given a set of agent capabilities and user \
instructions, generate an orchestration graph as JSON.

Output format (no markdown, no explanation — just the JSON object):
{
  "tasks": [
    {
      "id": "short-kebab-id",
      "agent": "agent-id-from-the-list",
      "description": "what this task should do",
      "needs": ["id-of-upstream-task"],
      "terminal": true
    }
  ]
}

Rules:
- Every agent reference must match an agent ID from the provided list.
- The graph must be a DAG (no cycles).
- Task IDs must be unique.
- "needs" lists upstream dependencies (tasks whose results this task requires).
- Set "terminal": true on tasks whose output should be returned to the caller.
- At least one task must be terminal.
- A terminal task must not be listed in any other task's "needs".
- Every non-terminal task must be listed in at least one other task's "needs".
- Omit "terminal" (or set it to false) for non-terminal tasks.
- Use each agent at most once unless the instructions explicitly require multiple uses.
"""


def _build_prompt(instructions: str, agent_cards: dict[str, dict]) -> str:
    """Build the user prompt from instructions and agent cards."""
    agents_desc = []
    for agent_id, card in agent_cards.items():
        name = card.get("name", "")
        desc = card.get("description", "")
        agents_desc.append(f"- **{agent_id}** ({name}): {desc}")

    agents_block = "\n".join(agents_desc)
    return f"Available agents:\n{agents_block}\n\nInstructions: {instructions}"


class GraphValidationError(Exception):
    """Raised when a generated graph fails validation."""


def validate_graph(graph: dict, valid_agent_ids: set[str]) -> None:
    """Validate an orchestration graph. Raises GraphValidationError on failure."""
    tasks = graph.get("tasks")
    if not tasks or not isinstance(tasks, list):
        raise GraphValidationError("Graph must have a non-empty 'tasks' list")

    # Collect all task IDs upfront for reference validation
    all_ids = {t.get("id", "") for t in tasks}
    all_ids.discard("")

    seen: set[str] = set()
    for t in tasks:
        tid = t.get("id", "")
        if not tid:
            raise GraphValidationError("Task missing 'id'")
        if tid in seen:
            raise GraphValidationError(f"Duplicate task ID: {tid}")
        seen.add(tid)

        agent = t.get("agent", "")
        if agent not in valid_agent_ids:
            raise GraphValidationError(
                f"Task '{tid}' references unknown agent '{agent}'"
            )

        for need in t.get("needs", []):
            if need not in all_ids:
                raise GraphValidationError(f"Task '{tid}' needs unknown task '{need}'")

    # Check for cycles using DFS on dependency edges
    adj: dict[str, list[str]] = {t.get("id"): t.get("needs", []) for t in tasks}
    visited: set[str] = set()
    in_stack: set[str] = set()

    def _dfs(node: str) -> None:
        if node in in_stack:
            raise GraphValidationError(f"Cycle detected involving task '{node}'")
        if node in visited:
            return
        in_stack.add(node)
        for dep in adj.get(node, []):
            _dfs(dep)
        in_stack.remove(node)
        visited.add(node)

    for tid in seen:
        _dfs(tid)

    # At least one terminal task
    terminal_ids = {t["id"] for t in tasks if t.get("terminal")}
    if not terminal_ids:
        raise GraphValidationError("No terminal task (set 'terminal': true)")

    # Terminal tasks must not have downstream dependents
    depended_on = {need for t in tasks for need in t.get("needs", [])}
    bad = terminal_ids & depended_on
    if bad:
        raise GraphValidationError(
            f"Terminal tasks must not have dependents: {', '.join(sorted(bad))}"
        )

    # Non-terminal tasks must be depended on by at least one other task
    non_terminal_sinks = (all_ids - terminal_ids) - depended_on
    if non_terminal_sinks:
        raise GraphValidationError(
            f"Non-terminal tasks with no dependents (dead ends): "
            f"{', '.join(sorted(non_terminal_sinks))}"
        )


async def generate_graph(
    instructions: str,
    agent_cards: dict[str, dict],
    *,
    model: str = "",
) -> dict:
    """Use LLM to generate an orchestration graph from instructions + agent cards.

    Validates the result and retries once on validation failure.
    """
    valid_ids = set(agent_cards.keys())

    prompt = _build_prompt(instructions, agent_cards)

    for attempt in range(2):
        raw = await complete(prompt, system=_SYSTEM, model=model)

        # Strip markdown fences if present
        text = raw.strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1] if "\n" in text else text[3:]
            if text.endswith("```"):
                text = text[:-3]
            text = text.strip()

        try:
            graph = json.loads(text)
        except json.JSONDecodeError as e:
            if attempt == 0:
                prompt = (
                    f"{prompt}\n\nYour previous response was not valid JSON: {e}. "
                    f"Please output ONLY a JSON object."
                )
                continue
            raise GraphValidationError(f"LLM returned invalid JSON: {e}") from e

        try:
            validate_graph(graph, valid_ids)
            return graph
        except GraphValidationError as e:
            if attempt == 0:
                prompt = (
                    f"{prompt}\n\nYour previous graph had an error: {e}. Please fix it."
                )
                continue
            raise

    # Unreachable, but satisfies type checker
    raise GraphValidationError("Graph generation failed after retries")
