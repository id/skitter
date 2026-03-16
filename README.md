# Skitter

MQTT-based AI orchestrator. Independent agent processes coordinate via an MQTT broker. A coordinator handles composed multi-agent apps: creating orchestration graphs from natural language via LLM, dispatching A2A requests, and resolving dependencies.

~3,500 lines of Python. Supports Claude Code and Codex CLI as agent runtimes.

## Quickstart (Local)

You need Docker, Python 3.10+, and [uv](https://docs.astral.sh/uv/).

The coordinator uses [litellm](https://docs.litellm.ai/) for LLM calls (graph generation for composed apps). Set the API key for your provider and the model to use:

```bash
# Pick your provider (see https://docs.litellm.ai/docs/providers)
export ANTHROPIC_API_KEY=sk-ant-...    # for Claude models
# or: export OPENAI_API_KEY=sk-...    # for OpenAI models
# or: export GEMINI_API_KEY=...       # for Gemini models

export SKITTER_LLM_MODEL=claude-haiku-4-5-20251001  # any litellm model string
```

Agent runners need [Claude Code](https://docs.anthropic.com/en/docs/claude-code) or [Codex](https://github.com/openai/codex) logged in (depending on which runtime the agent uses).

```bash
# Start MQTT broker
docker compose up -d

# Install
uv sync

# Start coordinator (terminal 1)
uv run python -m skitter

# Start an agent (terminal 2, see "Agents" section for setup)
uv run python -m skitter agent-runner ~/.claude/agents/researcher.md

# Send a request (terminal 3)
uv run python -m skitter run "What are the key features of MQTT v5?"
```

Open `dashboard.html` in a browser to watch jobs execute in real time (connects directly to the broker via WebSocket).

## How It Works

```
Any MQTT v5 Client          MQTT Broker           Agent Runners
(CLI, dashboard,          (local or EMQX)      (claude / codex CLI)
 Telegram bot, etc.)
                      ┌────────────────────┐
                      │                    │
 Standalone agent     │                    │    ┌───────────┐
─────────────────────>│  request topic  ───────>│ Agent A   │
 (direct to agent)    │                    │    │ (sonnet)  │──> result
                      │                    │    └───────────┘
                      │                    │
 Composed app         │  ┌─────────────┐   │    ┌───────────┐
─────────────────────>│  │ Coordinator │───────>│ Agent A   │
 (via coordinator)    │  │ (DAG, DB)   │   │    └───────────┘
                      │  └─────────────┘   │    ┌───────────┐
                      │   Resolves deps,   │    │ Agent B   │
 result               │   dispatches A2A ──────>│ (haiku)   │
<─────────────────────│                    │    └───────────┘
                      │                    │
                      └────────────────────┘
```

**Standalone agents** handle requests directly, no coordinator involved. Clients publish to the agent's request topic, the agent-runner processes it and replies.

**Composed apps** are multi-agent workflows. The coordinator subscribes to each app's request topic, creates a DB-backed session, dispatches A2A requests to individual agents following the dependency graph, and sends the final result back to the caller.

**Creating composed apps:** send a `create app` request to the coordinator's `skitter` topic with agent IDs and natural language instructions. The coordinator looks up agent capabilities from their discovery cards, calls an LLM to generate an orchestration graph (validated for cycles, missing refs, and next/needs consistency), persists the app, and publishes its discovery card.

## Agents

Agents use native CLI definitions directly, no extra config layer:

```markdown
# ~/.claude/agents/researcher.md
---
name: researcher
description: Deep research with source citation
model: sonnet
memory: user
tools: Read, Grep, Glob, Bash, WebSearch, WebFetch
---
You are a research specialist. Be thorough, cite sources.
```

Start an agent-runner by pointing it at the agent file:

```bash
uv run python -m skitter agent-runner ~/.claude/agents/researcher.md
uv run python -m skitter agent-runner ~/.codex/agents/coder.toml
```

Each agent-runner publishes a discovery card, handles A2A requests independently, and maintains its own MQTT connection for liveness tracking.

## Composed Apps

Create a multi-agent app by sending agent IDs and instructions to the coordinator:

```bash
uv run python -m skitter run 'create app {
  "name": "Deep Research",
  "instructions": "Research the topic using web sources, then fact-check the findings",
  "agents": ["researcher", "reviewer"]
}'
```

The coordinator generates and validates an orchestration graph via LLM, persists the app with versioning, and starts accepting requests on the new app's topic.

## Deploy to Fly.io

Always-on coordinator (~$2/mo) with EMQX Serverless as the broker.

```bash
fly apps create skitter

cp .env.cloud.example .env.cloud
# Fill in EMQX Serverless + Fly + API credentials

set -a && source .env.cloud && set +a
uv run python -m skitter deploy --target fly
```

See [docs/fly-deployment.md](docs/fly-deployment.md) for the full setup guide.

## Why MQTT?

Instead of a monolithic orchestrator, skitter pushes routing and fan-out into the MQTT broker.

- **Zero-code integrations.** Connect Telegram, Slack, or anything else with a ~100-line bridge script that publishes requests and subscribes to replies.
- **Run agents anywhere.** Local processes, Docker containers, or cloud machines. As long as they can reach the broker, they work.
- **Free monitoring.** Subscribe to `$a2a/v1/#` and `skitter/#` with any MQTT client and watch every request, result, and event in real time.
- **Cheap cloud deploy.** Always-on coordinator (~$2/mo) on Fly.io, agents billed per-second.

Topics follow the [A2A-over-MQTT](https://www.emqx.com/mqtt-for-ai/a2a-over-mqtt/) scheme.

## Testing

```bash
# Unit tests (no broker needed)
uv run python -m pytest tests/test_unit.py -q

# Live tests (requires Docker + MQTT broker + LLM API key + runtime credentials)
docker compose up -d                          # start EMQX broker
docker build -f Dockerfile.agent -t skitter-agent:latest .  # build agent image
export ANTHROPIC_API_KEY='your-key'           # LLM API key for coordinator
export SKITTER_LLM_MODEL=claude-haiku-4-5-20251001

# Claude (requires OAuth token)
export CLAUDE_CODE_OAUTH_TOKEN='your-token'
uv run python -m pytest tests/test_live.py -v -s --runtime claude

# Codex (requires ~/.codex/auth.json)
uv run python -m pytest tests/test_live.py -v -s --runtime codex
```

Live tests run agent-runners in Docker containers for full isolation. They never touch your local agent files. The coordinator runs as a local subprocess.

## Limitations

- Agent runners use `dangerouslySkipPermissions`, only run in trusted environments
- Agent errors are passed as normal results to downstream tasks
- No built-in authentication (rely on MQTT broker auth)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for project structure, configuration, environment variables, testing, and the full topic scheme reference.
