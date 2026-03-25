# Contributing

## Project Structure

```
skitter/
  coordinator.py   A2A orchestrator: session management, DAG dispatch, runtime API
  agent_runner.py  CLI-to-A2A convenience wrapper (claude/codex)
  runtime_api.py   Runtime state queries + app creation
  graph_gen.py     LLM-based graph generation + validation
  llm.py           LLM API wrapper (litellm)
  discovery.py     Build + parse A2A discovery cards
  db.py            Database interface (SQLite/PostgreSQL)
  pull.py          Pull discovery cards from broker, save as JSON
  a2a.py           A2A protocol: message types, topics, validation, requester helper
  mqtt.py          MQTT v5 transport: connection, properties, extraction
  config.py        ~/.skitter/ management, YAML loading, dataclasses
  cli.py           Chat client
  __main__.py      CLI dispatch
```

Key docs: `docs/architecture.md` (detailed design), `docs/spec/` (A2A and A2A-over-MQTT specs).

## Development Setup

```bash
uv sync
docker compose up -d   # local EMQX broker
uv run python -m skitter   # start coordinator
```

## Agent Runner

Skitter works with any A2A-over-MQTT compliant agent. The built-in agent-runner is a convenience that wraps CLI tools (Claude Code, Codex). It reads native CLI agent definitions for metadata, then delegates execution to the respective CLI tool.

**Claude agents** are references to registered Claude Code agent names. The runner reads metadata (`name`, `description`, `model`) from the `.md` frontmatter and passes the agent name to `claude --agent <name>`. Claude Code resolves and executes the agent from its own registry (`~/.claude/agents/`).

```markdown
---
name: researcher
description: Deep research with source citation
model: sonnet
---
You are a research specialist. Be thorough, cite sources.
```

**Codex agents** carry their instructions inline. The runner reads `model` and `developer_instructions` from the `.toml` file and passes them to `codex exec` via CLI flags. The first 100 chars of `developer_instructions` are used as the agent description for the discovery card. Other `.toml` fields (e.g. `sandbox_mode`) are not applied; the runner always uses `--full-auto`.

```toml
model = "gpt-5.1-codex-mini"
developer_instructions = "You are a senior developer."
```

Start an agent-runner by pointing it at the file:

```bash
uv run python -m skitter agent-runner ~/.claude/agents/researcher.md
uv run python -m skitter agent-runner ~/.codex/agents/coder.toml
```

Runtime is inferred from file extension (`.md` = Claude, `.toml` = Codex).

## Configuration

```yaml
# ~/.skitter/config.yaml
db:
  backend: sqlite              # or "postgres"
  sqlite_path: ~/.skitter/skitter.db
  postgres_dsn: postgresql://...

llm:
  model: anthropic/claude-haiku-4-5  # for graph generation (litellm format)
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `MQTT_HOST` | `localhost` | Broker hostname |
| `MQTT_PORT` | `1883` | Broker port |
| `MQTT_TLS` | (empty) | Set to `1` for TLS |
| `MQTT_USER` / `MQTT_PASS` | (empty) | Broker auth |
| `SKITTER_A2A_ORG` | `skitter` | A2A topic org segment |
| `SKITTER_A2A_UNIT` | `default` | A2A topic unit segment |
| `SKITTER_LLM_MODEL` | (empty) | LLM model for graph generation (`provider/model`, e.g. `anthropic/claude-haiku-4-5`; see [litellm providers](https://docs.litellm.ai/docs/providers)) |
| `ANTHROPIC_API_KEY` | (empty) | Anthropic API key (for Claude models) |
| `OPENAI_API_KEY` | (empty) | OpenAI API key (for OpenAI models) |
| `CLAUDE_CODE_OAUTH_TOKEN` | (empty) | Claude auth for agent-runners |
| `SKITTER_REPLY_FIRST_TIMEOUT` | `15.0` | Seconds to wait for first reply before retry |
| `SKITTER_STREAM_IDLE_TIMEOUT` | `30.0` | Seconds between stream messages before timeout |
| `SKITTER_MAX_ATTEMPTS` | `3` | Max send attempts (1 initial + retries) |
| `SKITTER_AGENT_MAX_CONCURRENT` | `4` | Max concurrent requests per agent runner |

## Topic Scheme

All topics use the `$a2a` namespace following the A2A-over-MQTT scheme (see `docs/spec/a2a-over-mqtt-transport.md`).

### A2A topics

```
$a2a/v1/
  discovery/{org}/{unit}/{agent_id}          # Retained Agent/App Cards
  request/{org}/{unit}/{agent_id}            # Requests
  reply/{org}/{unit}/{agent_id}/{suffix}     # Replies
  event/{org}/{unit}/{agent_id}              # Session lifecycle + agent LWT (alive/dead)
```

## Testing

```bash
# Unit tests (no broker needed)
uv run python -m pytest tests/test_unit.py -q

# E2E tests (needs EMQX on localhost, no Docker/LLM API required)
docker compose up -d   # start local EMQX
uv run python -m pytest tests/test_e2e.py -v -s
```

E2E tests run the coordinator and agent-runners in-process with mocked `_run_cli` (no real CLI subprocess) and mocked `generate_graph` (no LLM API). Real MQTT messages flow through EMQX on localhost. Tests cover: agent discovery, direct queries, streaming, composed app pipelines (linear + fan-out/fan-in), session cancellation, and failure propagation/cascading.

## Lint and Format

```bash
uvx ruff format skitter/
uvx ruff check skitter/
```
