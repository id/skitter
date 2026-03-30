# Skitter

AI agent orchestrator using A2A-over-MQTT.

Two modes:

- **Standalone agents**: direct A2A-over-MQTT requests to individual agents, no coordinator needed.
- **Composed apps**: multi-agent workflows orchestrated by a coordinator that generates a dependency graph and dispatches work.

Works with any A2A-over-MQTT agent. Ships with a built-in agent-runner that wraps Claude Code and Codex CLI.

## Quickstart (Local)

**Prerequisites:** Python 3.11+, [uv](https://docs.astral.sh/uv/), Docker (only needed to run the local EMQX broker), and at least one agent CLI: [Claude Code](https://docs.anthropic.com/en/docs/claude-code) or [Codex](https://github.com/openai/codex), installed and authenticated.

```bash
# Start MQTT broker (Docker required for this step)
docker compose up -d

# Install
uv sync

# Configure LLM provider (used by create-agent and coordinator)
cp .env.example .env
# Edit .env: set your API key and model
```

### Run a standalone agent

No coordinator needed. Create an agent definition, start an agent-runner, and send it a request.

```bash
# Create an agent definition
uv run skitter create-agent random-x "returns a random number as JSON"

# Terminal 1: start the agent
uv run skitter agent-runner agents/random-x.md

# Terminal 2: send a request
uv run skitter request random-x "go"
# => {"x": 73}
```

The agent-runner reads metadata from the definition file, publishes a discovery card, and handles A2A requests independently. It supports Claude Code (`.md`) and Codex (`.toml`) definitions. Use `uv run skitter create-agent` to generate definitions, or write them by hand. Any A2A-over-MQTT compliant process works too.

### Create a composed app

Composed apps need a coordinator, which uses an LLM to generate orchestration graphs (configured via `.env` above).

Create two more agents alongside random-x:

```bash
uv run skitter create-agent random-y "returns a random number as JSON"
uv run skitter create-agent sum "extracts numbers from input and returns their sum as JSON"
```

Start the agents, coordinator, then create and run the app:

```bash
# Terminal 1: start agents
uv run skitter agent-runner agents/random-x.md
uv run skitter agent-runner agents/random-y.md
uv run skitter agent-runner agents/sum.md

# Terminal 2: start coordinator
uv run skitter

# Terminal 3: create an app and run it
uv run skitter create-app "Add Numbers" \
  "Generate two random numbers in parallel, then sum them" \
  --agents random-x,random-y,sum --id add-numbers

uv run skitter request add-numbers "go"
# => {"y": 73} ... {"x": 47} ... {"sum": 120}
```

The coordinator generates a fan-out/fan-in graph: random-x and random-y run in parallel, then sum receives both results.

Open `dashboard.html` in a browser to watch requests execute in real time (connects to the broker via WebSocket).

Use `chat` for interactive sessions with any agent:

```bash
uv run skitter chat random-x
uv run skitter chat add-numbers
```

## How It Works

```
                      ┌────────────┐
  ┌────────┐          │            │        ┌─────────┐
  │ Client │<────────>│            │<──────>│ Agent A │
  └────────┘          │    MQTT    │        └─────────┘
                      │   Broker   │        ┌─────────┐
  ┌─────────────┐     │            │<──────>│ Agent B │
  │ Coordinator │<───>│            │        └─────────┘
  └─────────────┘     │            │        ┌─────────┐
   app cards          │            │<──────>│ Agent C │
                      └────────────┘        └─────────┘
                                             agent cards
```

All participants connect to the broker via MQTT pub/sub.

**Standalone agents** handle requests directly. Clients publish to the agent's request topic; the agent processes and replies. Each agent publishes its own discovery card. Any A2A-over-MQTT compliant process works.

**Composed apps** are multi-agent workflows. The coordinator subscribes to each app's request topic, creates a DB-backed session, dispatches A2A requests following the dependency graph, and returns the final result to the caller. The coordinator publishes an app card for each composed workflow.

Topics follow the [A2A-over-MQTT](docs/spec/a2a-over-mqtt-transport.md) scheme.

## Agent Runner

The built-in agent-runner wraps Claude Code and Codex CLI as A2A-over-MQTT agents. It reads metadata from definition files (`.md` for Claude, `.toml` for Codex), publishes a discovery card, and delegates execution to the respective CLI tool. Runtime is inferred from the file extension.

Use `skitter create-agent` to generate definitions, or write them by hand:

```bash
# Generate via LLM
uv run skitter create-agent coder "writes clean, idiomatic code" --runtime codex

# Or write by hand
uv run skitter agent-runner agents/coder.toml
```

Agent definitions live in `agents/` and are symlinked into `.claude/agents/` or `.codex/agents/` so the respective CLI discovers them by name.

### Permissions and isolation

**Claude agents** run with `--permission-mode auto` and a filesystem sandbox: writes are restricted to `/tmp`.

**Codex agents** run with `--full-auto`, `--ephemeral`, and `approval_policy=never`. The session is ephemeral and workspace-write sandboxed.

## Running Agents in Docker

`Dockerfile.agent` packages the agent-runner with both Claude Code and Codex binaries. Use it to run agents in isolated containers.

```bash
# Build
docker build -f Dockerfile.agent -t skitter-agent .

# Run a Claude agent
# Auth: CLAUDE_CODE_OAUTH_TOKEN (from "claude setup-token") or ANTHROPIC_API_KEY
docker run --rm \
  -e MQTT_BROKER_URL=mqtt://your-broker:1883 \
  -e CLAUDE_CODE_OAUTH_TOKEN=... \
  -v ./agents/researcher.md:/app/agents/researcher.md:ro \
  skitter-agent /app/agents/researcher.md

# Run a Codex agent
# Auth: CODEX_API_KEY, or mount ~/.codex/auth.json
docker run --rm \
  -e MQTT_BROKER_URL=mqtt://your-broker:1883 \
  -e CODEX_API_KEY=... \
  -v ./agents/coder.toml:/app/agents/coder.toml:ro \
  skitter-agent /app/agents/coder.toml
```

The container runs as a non-root user. The entrypoint is `python -m skitter.agent_runner`.

## Managing Apps and Sessions

```bash
uv run skitter list-apps
uv run skitter get-app add-numbers
uv run skitter delete-app add-numbers
uv run skitter list-sessions
uv run skitter list-sessions add-numbers    # filter by app
uv run skitter get-session <session-id>
uv run skitter cancel-session <session-id>
```

## Why MQTT?

Instead of a monolithic orchestrator, skitter pushes routing and fan-out into the broker.

- **Zero-code integrations.** Connect Telegram, Slack, or anything else with a ~100-line bridge that publishes requests and subscribes to replies.
- **Run agents anywhere.** Local processes, Docker containers, or cloud machines. As long as they reach the broker, they work.
- **Free monitoring.** Subscribe to `$a2a/v1/#` with any MQTT client to watch every request, result, and event in real time.


## Testing

```bash
# Unit tests (no broker needed)
uv run pytest tests/test_unit.py -q

# E2E tests (needs EMQX on localhost; no Docker or LLM API required beyond the broker)
docker compose up -d
uv run pytest tests/test_e2e.py -v -s
```

E2E tests run the coordinator and agent-runners in-process with mocked CLI and graph generation. Real MQTT messages flow through the local broker.

## Limitations

- No built-in authentication (rely on MQTT broker auth)
- Single coordinator instance per broker (enforced via retained MQTT lock)
- Codex `.toml` agent definitions: `model` and `developer_instructions` are applied at runtime via CLI flags; other fields (`sandbox_mode`, etc.) are ignored
- A2A Core Conformance only; Extended Conformance features (shared pool dispatch, task handover, binary artifacts, OAuth) are not implemented

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for project structure, configuration, environment variables, testing, and the full topic scheme reference.
