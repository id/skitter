# Skitter

Skitter turns Claude Code and Codex into MQTT-addressable AI agents.

## What You Can Do in 2 Minutes

Prerequisites: Python 3.11+, [uv](https://docs.astral.sh/uv/), and at least one agent CLI ([Claude Code](https://docs.anthropic.com/en/docs/claude-code) or [Codex](https://github.com/openai/codex)) installed and authenticated.

```bash
# Install
pip install skitter
# or: uv tool install skitter
# or from source: pip install git+https://github.com/id/skitter.git

# Configure broker and (optionally) LLM provider
skitter setup

# Create an agent and start services
skitter create-agent random-x "returns a random number as JSON"
skitter up

# Ask it something
skitter ask random-x "go"
# => {"x": 73}
```

All `skitter` examples below assume a package install. If running from source, use `uv run skitter` instead.

## Under the Hood

- **Agent runner** wraps Claude Code or Codex CLI as an A2A-over-MQTT agent. It reads a definition file from `~/.skitter/agents/`, publishes a discovery card, and handles requests independently.
- **Definitions** live in `~/.skitter/agents/` (or `$SKITTER_HOME/agents/`). Claude agents use `.md` (YAML frontmatter + system instructions); Codex agents use `.toml`.
- **MQTT** carries all discovery, requests, and replies. Subscribe to `$a2a/v1/#` with any MQTT client to watch everything in real time.
- **Coordinator** is only needed for composed (multi-agent) apps. It uses an LLM to generate a dependency graph and dispatches work to agents via MQTT.
- **Runtime auth** is separate from coordinator config. Claude Code and Codex authenticate with their own credentials; the coordinator's LLM config is only for graph generation.

## Support Matrix

| Runtime | Auth | Multi-turn | Streaming |
|---------|------|------------|-----------|
| Claude Code (`.md`) | Claude OAuth or API key | Yes (via `context_id`) | Yes |
| Codex (`.toml`) | OpenAI API key | Yes (via `context_id`) | No |

Any A2A-over-MQTT compliant process can also serve as an agent; the built-in runner is a convenience.

## Service Management

`skitter up` starts the broker (Docker tier), coordinator, and all agents from `~/.skitter/agents/`.

```bash
skitter up                    # start everything
skitter up --broker-only      # just the broker (run coordinator from source)
skitter up --agent random-x   # start a single agent
skitter status                # readiness overview
skitter logs emqx             # view broker logs
skitter logs coordinator      # view coordinator logs
skitter down                  # stop everything
skitter down --agent random-x # stop a single agent
skitter doctor                # check config, broker, agents, LLM
```

`skitter status` shows: config path, available runtimes, broker tier, container state, agent count, and a recommended next action.

## Composed Apps

Composed apps need a coordinator, which uses an LLM to generate orchestration graphs.

Create agents and a multi-agent workflow:

```bash
skitter create-agent random-x "returns a random number as JSON"
skitter create-agent random-y "returns a random number as JSON"
skitter create-agent sum "extracts numbers from input and returns their sum as JSON"

skitter up

skitter create-app "Add Numbers" \
  "Generate two random numbers in parallel, then sum them" \
  --agents random-x,random-y,sum --id add-numbers

skitter ask add-numbers "go"
# => {"y": 73} ... {"x": 47} ... {"sum": 120}
```

Use `chat` for interactive sessions:

```bash
skitter chat random-x
skitter chat add-numbers
```

Open `dashboard.html` in a browser to watch requests in real time (connects via WebSocket).

## Multi-turn Conversations

Both Claude Code and Codex agents support multi-turn via A2A `context_id`. The agent-runner captures the CLI's native session ID on the first request and uses it to resume on subsequent requests with the same `context_id`.

```bash
skitter ask researcher "analyze this data"
# context_id: abc123

skitter ask researcher "now summarize" --context abc123
```

## Architecture

```
                      +------------+
  +--------+          |            |        +---------+
  | Client |<-------->|            |<------>| Agent A |
  +--------+          |    MQTT    |        +---------+
                      |   Broker   |        +---------+
  +-------------+     |            |<------>| Agent B |
  | Coordinator |<--->|            |        +---------+
  +-------------+     |            |        +---------+
   app cards          |            |<------>| Agent C |
                      +------------+        +---------+
                                             agent cards
```

All participants connect via MQTT pub/sub. Topics follow the [A2A-over-MQTT](docs/spec/a2a-over-mqtt-transport.md) scheme:

```
$a2a/v1/discovery/{org}/{unit}/{agent_id}   # Retained discovery cards
$a2a/v1/request/{org}/{unit}/{agent_id}     # Requests
$a2a/v1/reply/{org}/{unit}/{agent_id}/...   # Replies
$a2a/v1/event/{org}/{unit}/{agent_id}       # Lifecycle events
```

**Standalone agents** handle requests directly. **Composed apps** are orchestrated by the coordinator, which creates a DB-backed session, dispatches A2A requests following a dependency graph, and returns the final result.

## LLM Provider Configuration

The coordinator uses an LLM for graph generation in composed apps. Configure during `skitter setup` or via environment variables. Not needed for standalone agents.

Set `SKITTER_LLM_API_KEY` with your API key, then configure the provider:

- **Anthropic** (default): `llm.api: anthropic`
- **OpenAI**: `llm.api: openai`
- **Custom endpoint**: set `llm.base_url` and declare the protocol family via `llm.api`

Runtime auth (Claude Code, Codex) is separate: agents use their own credentials (`ANTHROPIC_API_KEY`, `OPENAI_API_KEY`, or OAuth).

## SKITTER_HOME

By default, skitter stores config and agent definitions in `~/.skitter/`. Override with:

```bash
export SKITTER_HOME=/path/to/my/skitter
skitter setup
```

Or per-command:

```bash
skitter --skitter-home /path/to/my/skitter status
```

## Managing Apps and Sessions

```bash
skitter list-apps
skitter get-app add-numbers
skitter delete-app add-numbers
skitter list-sessions
skitter list-sessions add-numbers    # filter by app
skitter get-session <session-id>
skitter cancel-session <session-id>
```

## Running Agents in Docker

The `ghcr.io/id/skitter/agent` image packages both Claude Code and Codex CLIs. An agent container only needs to reach an MQTT broker; no other infrastructure is required.

### Minimal example (Claude agent)

```bash
docker run --rm \
  -e MQTT_BROKER_URL=mqtt://broker.emqx.io:1883 \
  -e CLAUDE_CODE_OAUTH_TOKEN="$CLAUDE_CODE_OAUTH_TOKEN" \
  -e SKITTER_AGENT_PERMISSION_MODE=bypassPermissions \
  -v ~/.skitter:/home/skitter/.skitter \
  ghcr.io/id/skitter/agent my-agent
```

### Minimal example (Codex agent)

```bash
docker run --rm \
  -e MQTT_BROKER_URL=mqtt://broker.emqx.io:1883 \
  -e SKITTER_AGENT_PERMISSION_MODE=bypassPermissions \
  -v ~/.skitter:/home/skitter/.skitter \
  -v ~/.codex/auth.json:/home/skitter/.codex/auth.json:ro \
  ghcr.io/id/skitter/agent my-codex-agent
```

The agent name is resolved from `SKITTER_HOME/agents/` (trying `<name>.md` then `<name>.toml`). You can also pass a full file path.

### Auth

| Runtime | How to provide |
|---------|---------------|
| Claude Code | `CLAUDE_CODE_OAUTH_TOKEN` env var |
| Codex | Bind-mount `~/.codex/auth.json` to `/home/skitter/.codex/auth.json` |

### Permission mode

By default, agents sandbox file writes (`--permission-mode auto` for Claude, `--full-auto` for Codex). In Docker containers (already isolated), set `SKITTER_AGENT_PERMISSION_MODE=bypassPermissions` to enable full tool access (file generation, shell commands, etc.).

### Volumes

Without bind-mounts, agent-generated files and memory are lost when the container is removed. Mount these to persist them on the host:

| Mount target | Purpose |
|-------------|---------|
| `/home/skitter/.claude` | Claude session state, auto-memory, and conversation history |
| `/tmp/workspace` | Agent-generated files (reports, code, data) |

See `docker-compose.test.yml` for a multi-agent example.

## Why MQTT?

Instead of a monolithic orchestrator, skitter pushes routing and fan-out into the broker.

- **Zero-code integrations.** Connect Telegram, Slack, or anything else with a ~100-line bridge that publishes requests and subscribes to replies.
- **Run agents anywhere.** Local processes, Docker containers, or cloud machines. As long as they reach the broker, they work.
- **Free monitoring.** Subscribe to `$a2a/v1/#` with any MQTT client to watch every request, result, and event in real time.

## Testing

```bash
# Unit tests (no broker needed)
uv run pytest tests/test_unit.py -q

# E2E tests (needs EMQX on localhost)
docker compose up -d --wait
uv run pytest tests/test_e2e.py -v -s

# Acceptance tests (needs EMQX on localhost; exercises full user journey)
uv run pytest tests/test_acceptance.py -v -s

# Docker E2E tests (real CLIs in Docker; needs auth tokens in .env.test)
docker compose --env-file .env.test -f docker-compose.test.yml up -d --wait --build
uv run pytest tests/test_docker_e2e.py -v -s
```

## Limitations

- No built-in authentication (rely on MQTT broker auth)
- Single coordinator instance per broker (enforced via retained MQTT lock)
- Codex `.toml` agents: `model` and `developer_instructions` are applied via CLI flags; other fields are ignored
- A2A Core Conformance only; Extended Conformance features (shared pool dispatch, task handover, binary artifacts, OAuth) are not implemented

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for project structure, development setup, configuration reference, and testing.
