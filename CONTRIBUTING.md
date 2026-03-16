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
  fly.py           Fly Machines API client
  deploy_fly.py    Deploy to Fly (build image, set secrets)
  mqtt.py          MQTT connection settings, A2A topic builders, v5 property helpers
  config.py        ~/.skitter/ management, YAML loading, dataclasses
  types.py         Message type definitions
  cli.py           Chat client
  __main__.py      CLI dispatch
```

Key docs: `docs/architecture.md` (detailed design), `docs/fly-deployment.md` (cloud setup).

## Development Setup

```bash
uv sync
docker compose up -d   # local EMQX broker
uv run python -m skitter   # start coordinator
```

## Agent Runner

Skitter works with any A2A-over-MQTT compliant agent. The built-in agent-runner is a convenience that wraps CLI tools (Claude Code, Codex). It reads native CLI agent definitions directly, no extra config layer.

**Claude** (`~/.claude/agents/researcher.md`):
```markdown
---
name: researcher
description: Deep research with source citation
model: sonnet
memory: user
tools: Read, Grep, Glob, Bash, WebSearch, WebFetch
---
You are a research specialist. Be thorough, cite sources.
```

**Codex** (`~/.codex/agents/coder.toml`):
```toml
model = "gpt-5.1-codex-mini"
sandbox_mode = "workspace-write"
developer_instructions = "You are a senior developer."
```

Start an agent-runner by pointing it at the file:

```bash
uv run python -m skitter agent-runner ~/.claude/agents/researcher.md
uv run python -m skitter agent-runner ~/.codex/agents/coder.toml
```

Runtime is inferred from file extension (`.md` → claude, `.toml` → codex).

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

For cloud deployment, see `.env.cloud.example` and `docs/fly-deployment.md`.

## Topic Scheme

Two namespaces: `$a2a` follows [A2A-over-MQTT](https://www.emqx.com/mqtt-for-ai/a2a-over-mqtt/) (client-facing), `skitter` is internal.

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

# Live tests (Docker + broker + LLM API key + runtime credentials)
docker compose up -d
docker build -f Dockerfile.agent -t skitter-agent:latest .
export ANTHROPIC_API_KEY='your-key'
export SKITTER_LLM_MODEL=anthropic/claude-haiku-4-5

# Claude
export CLAUDE_CODE_OAUTH_TOKEN='your-token'
uv run python -m pytest tests/test_live.py -v -s --runtime claude

# Codex (requires ~/.codex/auth.json)
uv run python -m pytest tests/test_live.py -v -s --runtime codex
```

Live tests run both agent-runners and the coordinator in Docker containers. They never touch local agent files or config. Credentials are loaded from `.env.test`.

## Lint and Format

```bash
uvx ruff format skitter/
uvx ruff check skitter/
```
