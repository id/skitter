# Contributing

## Project Structure

```
skitter/
  coordinator.py   A2A orchestrator: session management, DAG dispatch, runtime API
  agent_runner.py  CLI-to-A2A convenience wrapper (claude/codex)
  runtime_api.py   Runtime state queries + app creation
  graph_gen.py     LLM-based graph generation + validation
  llm.py           LLM API wrapper (Anthropic + OpenAI SDKs)
  discovery.py     Build + parse A2A discovery cards
  db.py            Database interface (SQLite/PostgreSQL)
  pull.py          Pull discovery cards from broker, save as JSON
  a2a.py           A2A protocol: message types, topics, validation, requester helper
  mqtt.py          MQTT v5 transport: connection, properties, extraction
  config.py        ~/.skitter/ management, YAML loading, dataclasses
  request.py       One-shot A2A request
  cli.py           Interactive A2A session client
  manage.py        App/session management (coordinator wrappers)
  __main__.py      CLI dispatch
```

Key docs: `docs/architecture.md` (detailed design), `docs/spec/` (A2A and A2A-over-MQTT specs).

## Development Setup

```bash
git clone https://github.com/id/skitter.git
cd skitter
uv sync
uv run skitter setup --non-interactive
docker compose up -d   # local EMQX broker
uv run skitter         # start coordinator
```

All development examples use `uv run skitter` (runs from source).

## Agent Runner

Skitter works with any A2A-over-MQTT compliant agent. The built-in agent-runner is a convenience that wraps CLI tools (Claude Code, Codex). It reads native CLI agent definitions for metadata, then delegates execution to the respective CLI tool.

**Claude agents** carry their system instructions in the `.md` body after the YAML frontmatter. The runner reads metadata (`name`, `description`, `model`) from the frontmatter and passes the system instructions directly to `claude -p` along with the user prompt.

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
uv run skitter agent-runner ~/.skitter/agents/researcher.md
uv run skitter agent-runner ~/.skitter/agents/coder.toml
```

Runtime is inferred from file extension (`.md` = Claude, `.toml` = Codex).

## Skills

Skills are reusable instruction modules stored under `~/.skitter/skills/`:

```
~/.skitter/skills/
  web-search/SKILL.md
  summarize/SKILL.md
```

Each `SKILL.md` follows the standard format (YAML frontmatter + instructions):

```markdown
---
name: web-search
description: Use when the user needs current information from the web
---

Search instructions here...
```

Agent definitions reference skills by name in frontmatter:

```yaml
---
name: researcher
description: Deep research agent
runtime: claude
skills: [web-search, summarize]
---
```

At startup, the agent-runner symlinks referenced skills into the runtime-native path:
- Claude Code: `<resource_dir>/.claude/skills/<name>/`
- Codex: `<resource_dir>/.agents/skills/<name>/`

Both runtimes discover and auto-trigger skills based on the `description` field. No prompt injection; the runtime handles skill invocation natively.

Create an agent with skills:

```bash
uv run skitter create-agent researcher "research with citations" \
  --skill "web-search: find current information from the web"
```

This generates both the skill file in `~/.skitter/skills/` and the agent definition with the skill reference.

## Configuration

```yaml
# ~/.skitter/config.yaml
db:
  backend: sqlite              # or "postgres"
  sqlite_path: ~/.skitter/skitter.db
  postgres_dsn: postgresql://...

llm:
  model: anthropic/claude-haiku-4-5  # for graph generation
```

## Environment Variables

| Variable | Default | Description |
|---|---|---|
| `SKITTER_HOME` | `~/.skitter` | Override config and agents directory |
| `SKITTER_LOG_LEVEL` | `INFO` | Log level (`DEBUG`, `INFO`, `WARNING`, `ERROR`). `DEBUG` logs all MQTT messages. |
| `MQTT_BROKER_URL` | `mqtt://localhost:1883` | Broker URL (`mqtt://` or `mqtts://`) |
| `MQTT_USERNAME` / `MQTT_PASSWORD` | (empty) | Broker auth |
| `MQTT_CA_CERT` | (empty) | Path to custom CA certificate for `mqtts://` |
| `SKITTER_A2A_ORG` | `skitter` | A2A topic org segment |
| `SKITTER_A2A_UNIT` | `default` | A2A topic unit segment |
| `SKITTER_LLM_MODEL` | (empty) | LLM model for graph generation (`provider/model`, e.g. `anthropic/claude-haiku-4-5`) |
| `SKITTER_LLM_API_KEY` | (empty) | API key for coordinator LLM (Anthropic or OpenAI) |
| `SKITTER_LLM_API` | `anthropic` | Coordinator LLM provider (`anthropic` or `openai`) |
| `SKITTER_LLM_BASE_URL` | (empty) | Custom endpoint URL for coordinator LLM |
| `ANTHROPIC_API_KEY` | (empty) | Anthropic API key (for Claude Code agent-runners; separate from coordinator) |
| `OPENAI_API_KEY` | (empty) | OpenAI API key (for Codex agent-runners; separate from coordinator) |
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

Four test tiers, from fast/free to slow/real:

```bash
# Unit tests (no broker needed)
uv run pytest tests/test_unit.py -q

# E2E tests (needs EMQX on localhost)
docker compose up -d --wait
uv run pytest tests/test_e2e.py -v -s

# Acceptance tests (needs EMQX on localhost; full CLI user journey with mock agents)
uv run pytest tests/test_acceptance.py -v -s

# Docker E2E tests (needs real auth tokens; exercises real Claude/Codex CLIs in Docker)
docker compose --env-file .env.test -f docker-compose.test.yml up -d --wait --build
uv run pytest tests/test_docker_e2e.py -v -s
```

**E2E tests** run coordinator and agent-runners in-process with mocked `_run_cli` (no real CLI) and mocked `generate_graph` (no LLM API). Real MQTT messages flow through EMQX.

**Docker E2E tests** exercise real Claude Code and Codex CLIs in Docker containers against a real EMQX broker. Auth tokens are loaded from `.env.test` (not committed). Tests skip gracefully when a token is absent. `docker-compose.test.yml` includes the base `docker-compose.yml` via `include`, so only one `-f` flag is needed. Test agent definitions live in `tests/fixtures/agents/`.

## Lint and Format

```bash
uvx ruff format skitter/
uvx ruff check skitter/
```
