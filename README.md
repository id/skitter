# Skitter

Personal AI assistant built on MQTT. Define agents and workflows in YAML, run them locally or on Fly.io. Workers self-coordinate via retained MQTT messages, the supervisor just creates sessions and spawns workers. Supports both Claude Code and Codex CLI as runtimes.

Small Python codebase (~3,000 lines).

## Quickstart (Local)

You need Docker, Python 3.10+, [uv](https://docs.astral.sh/uv/), and [Claude Code](https://docs.anthropic.com/en/docs/claude-code) logged in.

```bash
# Start MQTT broker
docker compose up -d

# Install and run
uv sync
uv run python -m skitter

# Initialize predefined agents and workflows (in another terminal)
uv run python -m skitter init
```

That's it. The supervisor is listening. Try it:

```bash
# Run an agent
uv run python -m skitter agents run researcher "What are the key features of MQTT v5?"

# Run a multi-agent workflow
uv run python -m skitter workflow run deep-research --var topic="MQTT v5"

# Interactive chat
uv run python -m skitter chat
```

Open `dashboard.html` in a browser to watch jobs execute in real time (connects directly to the broker via WebSocket, no backend needed).

## Deploy to Fly.io

For a fully serverless setup where nothing runs when idle:

```bash
# Create Fly app (one-time)
fly apps create skitter

# Configure credentials
cp .env.cloud.example .env.cloud
# Fill in EMQX Serverless + Fly + Anthropic credentials

# Deploy
set -a && source .env.cloud && set +a
uv run python -m skitter deploy --target fly
```

Then configure the EMQX rule engine to intercept requests and trigger Fly machine creation.

See [docs/fly-deployment.md](docs/fly-deployment.md) for the full setup guide including EMQX Serverless configuration, rule engine setup, and end-to-end testing.

## How It Works

```
Any MQTT v5 Client          MQTT Broker              Workers
(CLI, dashboard,          (local or EMQX)      (claude / codex CLI)
 Telegram bot, etc.)
                      ┌────────────────────┐
 JSON request         │                    │
────────────────────> │  request topic     │
 (v5 Response Topic   │       |            │
  + Correlation Data) │       v            │
                      │  ┌───────────┐     │    ┌───────────┐
                      │  │Supervisor │     │    │ Worker A  │
                      │  │ (no LLM)  │─────────>│ (sonnet)  │──┐
                      │  └───────────┘     │    └───────────┘  │
                      │  Publishes session │    ┌───────────┐  │
                      │  + spawns workers  │    │ Worker B  │  │
                      │                    │    │ (haiku)   │──┤
                      │                    │    └───────────┘  │
                      │                    │                   │
 result               │                    │                   │
<──────────────────────────────────────────────────────────────┘
 (direct to caller)   │                    │
                      └────────────────────┘
```

1. Any MQTT v5 client publishes a request to an agent's topic
2. Supervisor intercepts via wildcard subscription, creates a session, spawns workers
3. Workers read the session from retained MQTT and self-coordinate
4. Workers with dependencies wait for upstream results before starting
5. Results stream directly back to the caller — supervisor is not in the data path

The broker handles routing, fan-out, and state (retained messages). The supervisor is stateless. Workers are ephemeral.

## Agents and Workflows

Agents are defined in two places: a slim orchestration stub for skitter, and a native sub-agent definition for the CLI runtime.

```yaml
# ~/.skitter/agents/researcher.yaml
name: Research Specialist
description: Deep research with source citation
runtime: claude
```

```markdown
# ~/.claude/agents/researcher.md
---
name: researcher
model: sonnet
memory: user
tools: Read, Grep, Glob, Bash, WebSearch, WebFetch
---
You are a research specialist. Be thorough, cite sources.
```

Workflows chain multiple agents with fan-out, join, and dependency routing:

```yaml
# ~/.skitter/workflows/deep-research.yaml
tasks:
  - id: research_web
    agent: researcher
    description: "Research '{topic}' using web sources."
    next: fact_check
  - id: research_academic
    agent: researcher
    description: "Research '{topic}' focusing on academic papers."
    next: fact_check
  - id: fact_check
    agent: reviewer
    needs: [research_web, research_academic]
    next: synthesize
  - id: synthesize
    agent: writer
    needs: [fact_check]
    next: output
```

## Why MQTT?

Instead of a monolithic orchestrator that handles routing, retries, fan-out, and load balancing on top of AI reasoning, skitter pushes all that infrastructure into the MQTT broker.

- **Zero-code integrations.** Connect Telegram, Slack, or anything else with a ~100-line bridge script that publishes requests and subscribes to replies. The supervisor is invisible.
- **Run workers anywhere.** Local processes, Docker containers, or Fly Machines. As long as they can reach the broker, they work.
- **Crash recovery.** Retained messages = durable state. Workers set LWT for crash detection. The supervisor respawns dead workers; the new worker picks up from the retained session.
- **Free monitoring.** Subscribe to `$a2a/v1/#` with any MQTT client and watch every request, result, and chain execution in real time.
- **Serverless.** The supervisor is stateless — it can run as an ephemeral Fly Machine that processes one request and exits.

Topics follow the [A2A-over-MQTT](https://www.emqx.com/mqtt-for-ai/a2a-over-mqtt/) scheme.

## Limitations

- Agents run with `dangerouslySkipPermissions` — only run in trusted environments
- Worker errors are passed as normal results to downstream tasks
- Sessions live in retained MQTT messages only (not persisted to disk)
- No built-in authentication (rely on MQTT broker auth)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for project structure, configuration details, environment variables, testing, and the full topic scheme reference.
