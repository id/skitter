# Skitter

~3,200 lines of Python. MQTT-based personal AI assistant. Stateless supervisor + self-coordinating workers + MQTT broker as infrastructure backbone.

## Quick Orientation

| What | Where |
|---|---|
| Supervisor (session creation, worker spawning) | `skitter/supervisor.py` |
| Worker (reads session, runs agent CLI, publishes results) | `skitter/worker.py` |
| Spawn backends (subprocess, docker, fly) | `skitter/spawn.py` |
| Fly Machines API client | `skitter/fly.py` |
| Deploy to Fly | `skitter/deploy_fly.py` |
| MQTT settings, A2A topic builders, v5 helpers | `skitter/mqtt.py` |
| Config loading (~/.skitter/), dataclasses | `skitter/config.py` |
| Crash recovery (LWT dead events) | `skitter/supervisor.py` (`handle_dead_event`) |
| Message types | `skitter/types.py` |
| Chat client | `skitter/cli.py` |
| CLI dispatch | `skitter/__main__.py` |
| Dashboard (single-file, MQTT-connected) | `dashboard.html` |

## Docs

| Doc | Content |
|---|---|
| `docs/architecture.md` | Design principles, topic scheme, execution flows, recovery model |
| `docs/fly-deployment.md` | EMQX Serverless + Fly.io setup guide (always-on supervisor, deploy, testing) |
| `docs/landscape.md` | Competitive landscape research (OpenClaw, Nanobot, etc.) and library analysis (pi-mono, litellm) |
| `CONTRIBUTING.md` | Project structure, config reference, env vars, testing, lint |
| `README.md` | User-facing quickstart, deploy, how-it-works |

## Architecture in One Paragraph

Clients publish JSON-RPC requests to `$a2a/v1/request/{org}/{unit}/{agent_id}`. The supervisor intercepts via wildcard subscription, creates a session (retained MQTT message), and spawns workers. Workers read the session, wait for upstream chain results if they have `needs`, run `claude --agent <name>` or `codex` as a subprocess, and publish results. Terminal tasks reply directly to the caller. The broker handles routing, fan-out, and state. Locally: subprocess workers + Docker EMQX. On Fly: always-on supervisor + EMQX Serverless + ephemeral worker machines.

## Key Concepts

- **Immutable sessions** — published once by supervisor, never mutated. Per-task status on separate retained topics.
- **Chain routing** — non-terminal workers publish retained chain results; downstream workers subscribe and wait.
- **A2A-over-MQTT** — all topics follow `$a2a/v1/{method}/{org}/{unit}/{agent_id}/{suffix}`.
- **Native sub-agents** — agent identity owned by `~/.claude/agents/*.md` / `~/.codex/agents/*.toml`, not skitter. Skitter YAML stubs (`~/.skitter/agents/*.yaml`) contain only orchestration metadata.
- **Spawn modes** — `subprocess` (local), `docker` (containerized), `fly` (Fly Machines API).

## Planning and Implementation Process

For non-trivial requests (new features, architectural changes, multi-file refactors):

### 1. Planning Phase
- **Use a team of agents** for planning — delegate research and analysis to subagents.
- **Evaluate fit** — research whether the request aligns with skitter's goals (minimal MQTT-based supervisor, self-coordinating workers, small codebase). Push back if a request conflicts with core principles or adds unnecessary complexity.
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

- Workers run with `dangerouslySkipPermissions`
- Worker errors passed as normal results
- Sessions live in retained MQTT only (not persisted to disk)
- No built-in authentication (rely on broker auth)

## Roadmap

- Telegram bridge
- Per-chat conversation history
- Worker timeouts and exponential backoff
- Persist sessions to disk
