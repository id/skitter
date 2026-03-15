# Skitter

~2,700 lines of Python. MQTT-based personal AI assistant. Coordinator + independent agent runners + MQTT broker as infrastructure backbone.

## Quick Orientation

| What | Where |
|---|---|
| Coordinator (A2A orchestrator, session management, DAG dispatch) | `skitter/coordinator.py` |
| Agent runner (standalone A2A agent process) | `skitter/agent_runner.py` |
| Discovery (build + publish agent/workflow cards) | `skitter/discovery.py` |
| LLM client (litellm wrapper) | `skitter/llm.py` |
| Graph generation + validation | `skitter/graph_gen.py` |
| Runtime API + app creation | `skitter/runtime_api.py` |
| Fly Machines API client | `skitter/fly.py` |
| Deploy to Fly | `skitter/deploy_fly.py` |
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
| `docs/landscape.md` | Competitive landscape research (OpenClaw, Nanobot, etc.) and library analysis (pi-mono, litellm) |
| `CONTRIBUTING.md` | Project structure, config reference, env vars, testing, lint |
| `README.md` | User-facing quickstart, deploy, how-it-works |

## Architecture in One Paragraph

Clients publish JSON-RPC requests to `$a2a/v1/request/{org}/{unit}/{agent_id}`. For standalone agents, the agent-runner handles the request directly. For composed apps, the coordinator subscribes to the app's request topic, creates a DB-backed session, and dispatches A2A requests to individual agents. Agents are independent processes — the coordinator only sends A2A requests and collects replies. The `$a2a` namespace is purely client-facing; `skitter/` topics handle internal coordination. Locally: subprocess agent-runners + Docker EMQX. On Fly: always-on coordinator + EMQX Serverless + independent agent machines.

## Key Concepts

- **Immutable sessions** — persisted once by coordinator in DB, never mutated. Per-task status tracked separately.
- **Result routing** — task results published as retained on `skitter/result/{app_version}/{task}/{sid}` for observability.
- **Namespace separation** — `$a2a/v1/...` for client-facing A2A protocol (request, reply, discovery); `skitter/...` for internal coordination.
- **Native sub-agents** — agent identity owned by `~/.claude/agents/*.md` / `~/.codex/agents/*.toml`, not skitter. Skitter YAML stubs (`~/.skitter/agents/*.yaml`) contain only orchestration metadata.
- **Independent agents** — agent-runners are standalone processes. The coordinator doesn't spawn or manage them.

## Planning and Implementation Process

For non-trivial requests (new features, architectural changes, multi-file refactors):

### 1. Planning Phase
- **Use a team of agents** for planning — delegate research and analysis to subagents.
- **Evaluate fit** — research whether the request aligns with skitter's goals (minimal MQTT-based coordinator, independent agents, small codebase). Push back if a request conflicts with core principles or adds unnecessary complexity.
- **Persist the plan** — write a markdown file under `docs/` with timestamp: `docs/YYYY-mm-DD-HH-MM-SS-<slug>.md`.

### 2. Implementation Phase
- **Coding persona** — professional senior Python developer. Idiomatic, neat Python. No boilerplate or unnecessary abstractions.
- **Tests** — cover new/changed functionality with focused tests. Test edge cases, not obvious behavior.
- **No backward compatibility** — rewrite and drop old code freely. No shims, re-exports, or deprecation warnings.

### 3. Quality Phase
1. **`/simplify`** — run the simplify skill. Fix all findings.
2. **Staff-engineer review** — run the `staff-engineer` agent. Fix all findings.
3. **Lint and format** — `uvx ruff format` and `uvx ruff check` on changed files.
4. **Unit tests** — `uv run python -m pytest tests/test_unit.py -q`.
5. **Live tests** — `tests/test_live_claude.py` and `tests/test_live_codex.py`. Confirm with user first.
6. **Dashboard** — verify `dashboard.html` still works if session state or topics changed.

## Limitations

- Agent runners use `dangerouslySkipPermissions`
- Agent errors passed as normal results
- No built-in authentication (rely on broker auth)

## Roadmap

- Telegram bridge
- Per-chat conversation history
- Task timeouts and exponential backoff
