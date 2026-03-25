# Skitter

MQTT-based personal AI assistant. Coordinator + A2A-over-MQTT agents + MQTT broker as infrastructure backbone.

## Quick Orientation

| What | Where |
|---|---|
| Coordinator (A2A orchestrator, session management, DAG dispatch) | `skitter/coordinator.py` |
| Agent runner (CLI-to-A2A convenience wrapper) | `skitter/agent_runner.py` |
| Discovery (build + parse A2A agent/workflow cards) | `skitter/discovery.py` |
| LLM client (litellm wrapper) | `skitter/llm.py` |
| Graph generation + validation | `skitter/graph_gen.py` |
| Runtime API + app creation | `skitter/runtime_api.py` |
| Pull discovery cards from broker (JSON) | `skitter/pull.py` |
| A2A protocol (message types, topics, validation, requester helper) | `skitter/a2a.py` |
| MQTT v5 transport (connection, properties) | `skitter/mqtt.py` |
| Config loading (~/.skitter/), dataclasses | `skitter/config.py` |
| DB interface (SQLite/PostgreSQL) | `skitter/db.py` |
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
- **Native sub-agents.** Agent identity owned by the external runtime (Claude Code or Codex). Skitter reads metadata from native definition files (`~/.claude/agents/*.md`, `~/.codex/agents/*.toml`) but the files are resolved and executed by the respective CLI tools. No separate skitter agent config.
- **Independent agents.** Agents are any A2A-over-MQTT compliant process. The coordinator doesn't spawn or manage them.
- **A2A protocol compliance.** All protocol-facing code must conform to A2A v1.0.0 and the A2A-over-MQTT v0.1 binding. Local copies of the specs live in `docs/spec/`. Use `/a2a-compliance` to validate after protocol changes.

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
5. **E2E tests**: `docker compose up -d --wait && uv run python -m pytest tests/test_e2e.py -v -s`. Always run E2E tests together with unit tests; do not skip them.
6. **A2A compliance**: if protocol-facing code changed, run `/a2a-compliance`.
7. **Dashboard**: verify `dashboard.html` still works if session state or topics changed.
8. **Docs and env files**: update `CLAUDE.md`, `README.md`, `CONTRIBUTING.md`, `docs/architecture.md`, `.env.example`, and `.env.cloud.example` if behavior, config, env vars, or CLI usage changed.

## Limitations

- Agent runners use `--permission-mode auto` with filesystem sandbox (write restricted to `/tmp`)
- No built-in authentication (rely on broker auth)
- Single coordinator per broker (enforced via retained MQTT lock)
- No timeout for coordinator-dispatched tasks (only recovery tasks get 120s timeout); requester-side `send_and_wait` has retry/timeout profile
- Codex `.toml` agent definitions: `model` and `developer_instructions` are applied at runtime via CLI flags; other fields (`sandbox_mode`, etc.) are not passed to `codex exec` (always uses `--full-auto`)
- A2A-over-MQTT: Core Conformance only; Extended Conformance features (shared pool dispatch, task handover, binary artifacts, UBSP, broker-managed status, OAuth) are not implemented
