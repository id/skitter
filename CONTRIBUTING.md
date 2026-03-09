# Contributing

## Project Structure

```
skitter/
  supervisor.py    Stateless supervisor: wildcard MQTT listener, session creator, worker spawner
  worker.py        Self-coordinating worker: reads session from MQTT, runs agent CLI, publishes results
  discovery.py     Build + publish discovery cards from agent/workflow YAML definitions
  spawn.py         Worker spawn backends: subprocess, docker, fly
  fly.py           Fly Machines API client
  deploy_fly.py    Deploy to Fly (build image, set secrets)
  mqtt.py          MQTT connection settings, A2A topic builders, v5 property helpers
  config.py        ~/.skitter/ management, YAML loading, dataclasses
  types.py         Message type definitions
  cli.py           Chat client
  agents_cli.py    `skitter agents` subcommands
  workflow_cli.py  `skitter workflow` subcommands
  docker_cli.py    `skitter docker` subcommands
  __main__.py      CLI dispatch
  reload.py        Publish reload signal to supervisor
```

Key docs: `docs/architecture.md` (detailed design), `docs/fly-deployment.md` (cloud setup).

## Development Setup

```bash
uv sync
docker compose up -d   # local EMQX broker
uv run python -m skitter   # start supervisor
```

## Agent Configuration

Skitter delegates agent identity to native CLI sub-agent systems. Each agent has two files:

**Orchestration stub** (`~/.skitter/agents/researcher.yaml`):
```yaml
name: Research Specialist
description: Deep research with source citation
runtime: claude    # "claude" or "codex"
workspace: ""
```

**Native sub-agent** (`~/.claude/agents/researcher.md`):
```markdown
---
name: researcher
model: sonnet
maxTurns: 15
memory: user
tools: Read, Grep, Glob, Bash, WebSearch, WebFetch
---
You are a research specialist. Be thorough, cite sources.
```

The YAML filename stem must match the native sub-agent name. Agent personality, model, tools, and memory are owned by the native CLI system, not skitter.

### Codex sub-agents

```toml
# ~/.codex/agents/coder.toml
model = "gpt-5.1-codex-mini"
sandbox_mode = "workspace-write"
developer_instructions = "You are a senior developer."
```

### Global config

```yaml
# ~/.skitter/config.yaml
default_runtime: claude
```

## Workflow Templates

```yaml
# ~/.skitter/workflows/deep-research.yaml
name: Deep Research
description: Multi-source research with fact-checking
variables:
  - topic
tasks:
  - id: research_web
    agent: researcher
    description: "Research '{topic}' using web sources."
    next: fact_check
    needs: []
  - id: research_academic
    agent: researcher
    description: "Research '{topic}' focusing on academic papers."
    next: fact_check
    needs: []
  - id: fact_check
    agent: reviewer
    description: "Cross-reference findings about '{topic}'."
    next: synthesize
    needs: [research_web, research_academic]
  - id: synthesize
    agent: writer
    description: "Combine all findings about '{topic}'."
    next: output
    needs: [fact_check]
```

Each task specifies `next` (another task id or `"output"` for terminal) and `needs` (upstream dependencies). `next` is auto-inferred from the reverse dependency graph if omitted. Tasks can override `model` per-task.

## Docker Workers

Workers can run in Docker containers for sandboxing:

```bash
uv run python -m skitter docker build
uv run python -m skitter docker login    # one-time OAuth flow
uv run python -m skitter docker sync     # copy agent definitions
SKITTER_SPAWN_MODE=docker uv run python -m skitter
```

## Environment Variables

Copy `.env.example` to `.env` for local development:

| Variable | Default | Description |
|---|---|---|
| `MQTT_HOST` | `localhost` | Broker hostname |
| `MQTT_PORT` | `1883` | Broker port |
| `MQTT_TLS` | (empty) | Set to `1` for TLS |
| `MQTT_USER` / `MQTT_PASS` | (empty) | Broker auth |
| `SKITTER_A2A_ORG` | `skitter` | A2A topic org segment |
| `SKITTER_A2A_UNIT` | `default` | A2A topic unit segment |
| `SKITTER_SPAWN_MODE` | `subprocess` | `subprocess`, `docker`, or `fly` |
| `SKITTER_WORKER_IMAGE` | `skitter-worker:latest` | Docker image for workers |
| `SKITTER_DOCKER_NETWORK` | `skitter` | Docker network for workers |

For cloud deployment, see `.env.cloud.example` and `docs/fly-deployment.md`.

## Topic Scheme

Skitter uses two namespaces. `$a2a` follows the [A2A-over-MQTT](https://www.emqx.com/mqtt-for-ai/a2a-over-mqtt/) standard (client-facing). `skitter` is internal coordination.

### A2A topics (client-facing)

```
$a2a/v1/
  discovery/{org}/{unit}/{agent_id}          # Retained Agent Cards
  request/{org}/{unit}/{agent_id}            # Requests
  request/{org}/{unit}/{agent_id}/cancel     # Cancel signals
  reply/{org}/{unit}/{agent_id}/{suffix}     # Replies
```

### Skitter internal topics

```
skitter/
  session/{session_id}                       # Retained session spec (immutable)
  result/{workflow_id}/{task}/{session_id}   # Retained inter-worker results
  status/{workflow_id}/{task}/{session_id}   # Retained per-task status
  usage/{workflow_id}/{task}/{session_id}    # Usage tracking
  event/{agent}/{type}                       # alive/dead (LWT)
  control/reload                             # Reload agents/workflows signal
```

`workflow_id` equals `agent_id` for single-agent sessions. No `{org}/{unit}` in the `skitter` namespace.

## Spec Deviations

| Area | A2A Spec | Skitter |
|---|---|---|
| Streaming | Separate `TaskStatusUpdateEvent` + `TaskArtifactUpdateEvent` | `TaskStatusUpdateEvent` for both streaming and terminal |
| Internal state | Part of `event/` topic tree | Separate `skitter/` namespace |
| Retry / timeout | Exponential backoff, timeouts | Not implemented |

## Testing

```bash
# Unit tests (no broker needed)
uv run python -m pytest tests/test_unit.py -q

# Integration tests (needs local broker)
uv run python -m pytest tests/test_e2e.py -v

# Live tests (runs actual Claude/Codex calls)
unset CLAUDECODE && uv run python -m pytest tests/test_live_claude.py -v -s
uv run python -m pytest tests/test_live_codex.py -v -s
```

## Lint and Format

```bash
uvx ruff format skitter/
uvx ruff check skitter/
```
