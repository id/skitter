# Skitter

Personal AI assistant built on MQTT. Define agents and workflows in YAML, run them locally or on Fly.io. Workers self-coordinate via retained MQTT messages, the supervisor just creates sessions and spawns workers. Supports both Claude Code and Codex CLI as runtimes.

Small Python codebase (~2,700 lines).

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
# Publish discovery cards (agents + workflows visible in dashboard)
uv run python -m skitter publish

# Run an agent
uv run python -m skitter agents run researcher "What are the key features of MQTT v5?"

# Run a multi-agent workflow
uv run python -m skitter workflow run deep-research --var topic="MQTT v5"

# Interactive chat
uv run python -m skitter chat
```

Open `dashboard.html` in a browser to watch jobs execute in real time (connects directly to the broker via WebSocket, no backend needed).

## Deploy to Fly.io

Always-on supervisor (~$2/mo) with ephemeral worker machines. Workers auto-destroy after completing their task.

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

See [docs/fly-deployment.md](docs/fly-deployment.md) for the full setup guide.

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
                      │  │Supervisor │     │    │ Worker A  │──┐
                      │  │ (no LLM)  │─────────>│ (sonnet)  │  │
                      │  └───────────┘     │    └───────────┘  │
                      │  Publishes session │    ┌───────────┐  │  ┌──────────┐
                      │  + spawns workers  │    │ Worker B  │  │  │  Google  │
                      │                    │    │ (haiku)   │──┤<>│  Drive   │
                      │                    │    └───────────┘  │  └──────────┘
                      │                    │                   │   (via rclone
 result               │                    │                   │    or local
<──────────────────────────────────────────────────────────────┘    mount)
 (direct to caller)   │                    │
                      └────────────────────┘
```

1. Any MQTT v5 client publishes a request to an agent's topic
2. Supervisor intercepts via wildcard subscription, creates a session, spawns workers
3. Workers read the session from retained MQTT and self-coordinate
4. Workers with dependencies wait for upstream results before starting
5. Workers with persistent workspaces sync files from Google Drive (or any rclone remote) before running, and sync back after
6. Results stream directly back to the caller -- supervisor is not in the data path

The broker handles routing, fan-out, and state (retained messages). The supervisor is stateless. Workers are ephemeral.

## What You Can Build

Skitter turns multi-agent workflows into something you can describe in a few lines of YAML and trigger from anywhere (CLI, dashboard, Telegram, cron). Some examples:

- **Deep research pipelines.** Fan out to multiple researcher agents in parallel (web, academic, industry sources), join results through a fact-checker, and synthesize into a final report.
- **Recurring market intelligence.** A scheduled workflow that discovers prospects, finds contacts, verifies data, and compiles a sales-ready report. Persistent workspaces on Google Drive let each run build on previous results -- excluding known accounts and avoiding duplicate leads.
- **Code review and refactoring.** Chain a code analysis agent with a reviewer and a writer to produce structured reviews or migration plans across large codebases.
- **Content pipelines.** Research a topic, draft content, review for accuracy and tone, then produce final copy -- each step handled by a specialized agent with the right tools.
- **Data processing with memory.** Workflows that accumulate results across runs: competitive monitoring, weekly summaries, customer feedback analysis. Google Drive workspaces persist files between executions so agents can reference historical data.
- **Multi-channel access.** The same workflows are accessible from the CLI, the browser dashboard, a Telegram bot, or any MQTT client. A ~100-line bridge script connects any new channel.

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

### Persistent Workspaces

Workflows can declare a persistent workspace backed by Google Drive (or any rclone remote). Workers sync files down before running and sync back after. Each task gets its own subdirectory but can read shared files and sibling task outputs.

```yaml
# ~/.skitter/workflows/weekly-report.yaml
workspace: weekly-report   # synced to Google Drive
tasks:
  - id: gather
    agent: researcher
    description: "Read previous reports from the workspace, then research new developments."
    next: compile
  - id: compile
    agent: writer
    needs: [gather]
    next: output
```

Locally, the Google Drive desktop app provides direct filesystem access with no sync overhead. On Fly.io, rclone handles the transfer automatically.

## Why MQTT?

Instead of a monolithic orchestrator that handles routing, retries, fan-out, and load balancing on top of AI reasoning, skitter pushes all that infrastructure into the MQTT broker.

- **Zero-code integrations.** Connect Telegram, Slack, or anything else with a ~100-line bridge script that publishes requests and subscribes to replies. The supervisor is invisible.
- **Run workers anywhere.** Local processes, Docker containers, or Fly Machines. As long as they can reach the broker, they work.
- **Crash recovery.** Retained messages = durable state. Workers set LWT for crash detection. The supervisor respawns dead workers; the new worker picks up from the retained session.
- **Free monitoring.** Subscribe to `$a2a/v1/#` and `skitter/#` with any MQTT client and watch every request, result, and internal coordination message in real time.
- **Cheap cloud deploy.** Always-on supervisor (~$2/mo) on Fly.io with ephemeral worker machines billed per-second.

Topics follow the [A2A-over-MQTT](https://www.emqx.com/mqtt-for-ai/a2a-over-mqtt/) scheme.

## Limitations

- Agents run with `dangerouslySkipPermissions` -- only run in trusted environments
- Worker errors are passed as normal results to downstream tasks
- Sessions live in retained MQTT messages only (not persisted to disk)
- No built-in authentication (rely on MQTT broker auth)

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for project structure, configuration details, environment variables, testing, and the full topic scheme reference.
