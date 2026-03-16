# Skitter

~2,700 lines of Python. MQTT-based personal AI assistant. Coordinator + A2A-over-MQTT agents + MQTT broker as infrastructure backbone.

## Quick Orientation

| What | Where |
|---|---|
| Coordinator (A2A orchestrator, session management, DAG dispatch) | `skitter/coordinator.py` |
| Agent runner (CLI-to-A2A convenience wrapper) | `skitter/agent_runner.py` |
| Discovery (build + parse A2A agent/workflow cards) | `skitter/discovery.py` |
| LLM client (litellm wrapper) | `skitter/llm.py` |
| Graph generation + validation | `skitter/graph_gen.py` |
| Runtime API + app creation | `skitter/runtime_api.py` |
| Pull agent cards from broker | `skitter/pull.py` |
| MQTT settings, A2A topic builders, v5 helpers | `skitter/mqtt.py` |
| Config loading (~/.skitter/), dataclasses | `skitter/config.py` |
| DB interface (SQLite/PostgreSQL) | `skitter/db.py` |
| Message types | `skitter/types.py` |
| Chat client | `skitter/cli.py` |
| CLI dispatch | `skitter/__main__.py` |
| Dashboard (single-file, MQTT-connected) | `dashboard.html` |

## Docs

| Doc | Content |
|---|---|
| `docs/architecture.md` | Design principles, topic scheme, execution flows, recovery model |
| `docs/fly-deployment.md` | EMQX Serverless + Fly.io setup guide (always-on coordinator, deploy, testing) |
| `CONTRIBUTING.md` | Project structure, config reference, env vars, testing, lint |
| `README.md` | User-facing quickstart, deploy, how-it-works |

## Architecture in One Paragraph

Clients publish JSON-RPC requests to `$a2a/v1/request/{org}/{unit}/{agent_id}`. Any A2A-over-MQTT compliant agent can handle requests; skitter ships an agent-runner as a convenience for wrapping CLI tools, but it's not required. For composed apps, the coordinator subscribes to the app's request topic, creates a DB-backed session, and dispatches A2A requests to individual agents. The coordinator only sends A2A requests and collects replies and doesn't care how agents are implemented. Locally: agents + Docker EMQX. On Fly: always-on coordinator + EMQX Serverless + independent agent machines.

## Key Concepts

- **Immutable sessions.** Persisted once by coordinator in DB, never mutated. Per-task status tracked separately.
- **Namespace separation.** `$a2a/v1/...` for client-facing A2A protocol (request, reply, discovery, events).
- **Native sub-agents.** Agent identity owned by `~/.claude/agents/*.md` / `~/.codex/agents/*.toml`, not skitter. No separate skitter agent config.
- **Independent agents.** Agents are any A2A-over-MQTT compliant process. The coordinator doesn't spawn or manage them.

## Writing Style

- **Avoid em-dashes.** Use colons, semicolons, periods, commas, or parentheses instead. Only use em-dashes where no other punctuation works naturally (very rare). Never use double-dashes.

## Planning and Implementation Process

For non-trivial requests (new features, architectural changes, multi-file refactors):

### 1. Planning Phase
- **Use a team of agents** for planning. Delegate research and analysis to subagents.
- **Evaluate fit.** Research whether the request aligns with skitter's goals (minimal MQTT-based coordinator, independent agents, small codebase). Push back if a request conflicts with core principles or adds unnecessary complexity.
- **Persist the plan.** Write a markdown file under `docs/` with timestamp: `docs/YYYY-mm-DD-HH-MM-SS-<slug>.md`.

### 2. Implementation Phase
- **Coding persona.** Professional senior Python developer. Idiomatic, neat Python. No boilerplate or unnecessary abstractions.
- **Tests.** Cover new/changed functionality with focused tests. Test edge cases, not obvious behavior.
- **No backward compatibility.** Rewrite and drop old code freely. No shims, re-exports, or deprecation warnings.

### 3. Quality Phase
1. **`/simplify`**: run the simplify skill. Fix all findings.
2. **Staff-engineer review**: run the `staff-engineer` agent. Fix all findings.
3. **Lint and format**: `uvx ruff format` and `uvx ruff check` on changed files.
4. **Unit tests**: `uv run python -m pytest tests/test_unit.py -q`.
5. **Live tests**: `tests/test_live.py` (`--runtime claude` / `--runtime codex`). Confirm with user first.
6. **Dashboard**: verify `dashboard.html` still works if session state or topics changed.
7. **Docs and env files**: update `CLAUDE.md`, `README.md`, `CONTRIBUTING.md`, `docs/architecture.md`, `.env.example`, and `.env.cloud.example` if behavior, config, env vars, or CLI usage changed.

## Limitations

- Agent runners use `dangerouslySkipPermissions`
- No built-in authentication (rely on broker auth)
- Single coordinator per broker (enforced via retained MQTT lock)
- No timeout for normal dispatched tasks (only recovery tasks get 120s timeout)
- Codex `.toml` agent definitions: only `model` is applied at runtime; `sandbox_mode` and other fields are not passed to `codex exec` (always uses `--full-auto`)

## Roadmap

- Telegram bridge
- Per-chat conversation history
- Task dispatch timeouts and exponential backoff
