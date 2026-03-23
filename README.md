# Skitter

MQTT-based AI orchestrator. Independent agent processes coordinate via an MQTT broker. A coordinator handles composed multi-agent apps: creating orchestration graphs from natural language via LLM, dispatching A2A requests, and resolving dependencies.

~3,900 lines of Python. Works with any A2A-over-MQTT agent. Ships with a convenience wrapper for Claude Code and Codex CLI.

## Quickstart (Local)

You need Docker, Python 3.11+, and [uv](https://docs.astral.sh/uv/).

The coordinator uses [litellm](https://docs.litellm.ai/) for LLM calls (graph generation for composed apps). Set the API key for your provider and the model to use:

```bash
# Pick your provider (see https://docs.litellm.ai/docs/providers)
export ANTHROPIC_API_KEY=sk-ant-...    # for Claude models
# or: export OPENAI_API_KEY=sk-...    # for OpenAI models
# or: export GEMINI_API_KEY=...       # for Gemini models

export SKITTER_LLM_MODEL=anthropic/claude-haiku-4-5  # any litellm model string
```

The built-in agent-runner needs [Claude Code](https://docs.anthropic.com/en/docs/claude-code) or [Codex](https://github.com/openai/codex) logged in (depending on which runtime the agent uses). You can also run any A2A-over-MQTT compliant agent instead.

```bash
# Start MQTT broker
docker compose up -d

# Install
uv sync

# Start coordinator (terminal 1)
uv run python -m skitter

# Start an agent (terminal 2, see "Agents" section for setup)
uv run python -m skitter agent-runner ~/.claude/agents/researcher.md

# One-shot request (terminal 3; see "run" section below)
uv run python -m skitter run "list apps"
```

The `run` command sends a one-shot A2A request. Without an agent ID it targets the coordinator's runtime API; with one it targets that agent directly:

```bash
# Runtime API query
uv run python -m skitter run "list apps"

# Direct agent request
uv run python -m skitter run researcher "summarize the latest MQTT 5.0 features"
```

Use `chat` for interactive conversations with agents:

```bash
uv run python -m skitter chat
```

Open `dashboard.html` in a browser to watch jobs execute in real time (connects directly to the broker via WebSocket).

## How It Works

```
Any MQTT v5 Client          MQTT Broker           A2A Agents
(CLI, dashboard,          (local or EMQX)      (any implementation)
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

**Standalone agents** handle requests directly, no coordinator involved. Clients publish to the agent's request topic, the agent processes it and replies. Any A2A-over-MQTT compliant process works; skitter's agent-runner is just a convenience for wrapping CLI tools.

**Composed apps** are multi-agent workflows. The coordinator subscribes to each app's request topic, creates a DB-backed session, dispatches A2A requests to individual agents following the dependency graph, and sends the final result back to the caller.

**Creating composed apps:** send a `create app` request to the coordinator's `skitter` topic with agent IDs and natural language instructions. The coordinator looks up agent capabilities from their discovery cards, calls an LLM to generate an orchestration graph (validated for cycles, missing refs, and next/needs consistency), persists the app, and publishes its discovery card.

## Agent Runner

Skitter's built-in agent-runner wraps CLI tools (Claude Code, Codex) as A2A-over-MQTT agents. Agents use native CLI definitions directly, no extra config layer:

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

Each agent-runner publishes a retained discovery card on startup and handles A2A requests independently.

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

Always-on coordinator (~$2/mo) with EMQX Serverless as the broker. See [docs/fly-deployment.md](docs/fly-deployment.md) for the full setup guide.

## Why MQTT?

Instead of a monolithic orchestrator, skitter pushes routing and fan-out into the MQTT broker.

- **Zero-code integrations.** Connect Telegram, Slack, or anything else with a ~100-line bridge script that publishes requests and subscribes to replies.
- **Run agents anywhere.** Local processes, Docker containers, or cloud machines. As long as they can reach the broker, they work.
- **Free monitoring.** Subscribe to `$a2a/v1/#` with any MQTT client and watch every request, result, and event in real time.
- **Cheap cloud deploy.** Always-on coordinator (~$2/mo) on Fly.io, agents billed per-second.

Topics follow the [A2A-over-MQTT](https://www.emqx.com/mqtt-for-ai/a2a-over-mqtt/) scheme.

## Testing

```bash
# Unit tests (no broker needed)
uv run python -m pytest tests/test_unit.py -q

# E2E tests (needs EMQX on localhost, no Docker/LLM API required)
docker compose up -d   # start local EMQX
uv run python -m pytest tests/test_e2e.py -v -s
```

E2E tests run the coordinator and agent-runners in-process with mocked CLI and graph generation. Real MQTT messages flow through the local broker.

## Limitations

- The built-in agent-runner uses `dangerouslySkipPermissions`; only run in trusted environments
- No built-in authentication (rely on MQTT broker auth)
- Single coordinator instance per broker (enforced via retained MQTT lock)
- Codex `.toml` agent definitions: only `model` is applied at runtime (`sandbox_mode` and other fields are ignored)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for project structure, configuration, environment variables, testing, and the full topic scheme reference.
