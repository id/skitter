# Skitter Usage Guide

This guide keeps the advanced day-to-day usage out of the main README.

This guide assumes you are running Skitter from a source checkout with `uv run skitter`. If you install Skitter separately as a tool, omit the `uv run` prefix.

## Service Management

`uv run skitter up` starts the broker, coordinator, and all local agents from `~/.skitter/agents/`.

```bash
uv run skitter up
uv run skitter up --broker-only
uv run skitter up --agent random-x

uv run skitter status
uv run skitter logs emqx
uv run skitter logs coordinator

uv run skitter down
uv run skitter down --agent random-x
uv run skitter doctor
```

## Working with Agents

Create an agent, send a one-shot request, or open an interactive session:

```bash
uv run skitter create-agent random-x "returns a random number as JSON"
uv run skitter list-agents
uv run skitter ask random-x "go"
uv run skitter chat random-x
```

Agent definitions live in `~/.skitter/agents/` by default:

- Claude Code agents use `.md` files with YAML frontmatter plus system instructions
- Codex agents use `.toml` files with inline instructions

You can override the home directory with `SKITTER_HOME` or `uv run skitter --skitter-home ...`.

## Composed Apps

Composed apps use the coordinator to generate and run a dependency graph across multiple agents.

Before you create an app, get an API key for a supported LLM provider, export it as `SKITTER_LLM_API_KEY`, and configure the coordinator LLM in `uv run skitter setup`.

```bash
uv run skitter create-agent random-x "returns a random number as JSON"
uv run skitter create-agent random-y "returns a random number as JSON"
uv run skitter create-agent sum "extracts numbers from input and returns their sum as JSON"

uv run skitter up

uv run skitter create-app "Add Numbers" \
  "Generate two random numbers in parallel, then sum them" \
  --agents random-x,random-y,sum --id add-numbers

uv run skitter ask add-numbers "go"
uv run skitter chat add-numbers
```

Useful inspection commands:

```bash
uv run skitter list-apps
uv run skitter get-app add-numbers
uv run skitter delete-app add-numbers

uv run skitter list-sessions
uv run skitter list-sessions add-numbers
uv run skitter get-session <session-id>
uv run skitter cancel-session <session-id>
```

Coordinator notes:

- Standalone agents do not need coordinator LLM configuration
- Composed apps do need coordinator LLM configuration and a provider API key exposed as `SKITTER_LLM_API_KEY` for graph generation
- Runtime auth for Claude Code or Codex is separate from coordinator LLM config

## Multi-turn Conversations

Both Claude Code and Codex agents support multi-turn conversations through A2A `context_id`.

```bash
uv run skitter ask researcher "analyze this data"
# context_id: abc123

uv run skitter ask researcher "now summarize" --context abc123
```

## Config and Storage

`uv run skitter setup` configures the broker and, when needed, the coordinator LLM. The defaults live under `~/.skitter/`.

Important locations:

- `~/.skitter/config.yaml`: broker and coordinator config
- `~/.skitter/agents/`: local agent definitions
- `~/.skitter/skitter.db`: default SQLite database

Useful environment variables:

- `SKITTER_HOME`: move config, agents, and the local database
- `SKITTER_LLM_API_KEY`: coordinator LLM API key
- `SKITTER_LLM_API`: coordinator provider, such as `anthropic` or `openai`
- `SKITTER_LLM_BASE_URL`: custom coordinator endpoint
- `MQTT_BROKER_URL`: override the broker URL

For the full configuration reference, see [CONTRIBUTING.md](../CONTRIBUTING.md).

## Docker Agents

The `ghcr.io/id/skitter/agent` image packages both Claude Code and Codex CLIs. The container only needs network access to an MQTT broker plus the right auth material.

Minimal Claude agent example:

```bash
docker run --rm \
  -e MQTT_BROKER_URL=mqtt://broker.emqx.io:1883 \
  -e CLAUDE_CODE_OAUTH_TOKEN="$CLAUDE_CODE_OAUTH_TOKEN" \
  -e SKITTER_AGENT_PERMISSION_MODE=bypassPermissions \
  -v ~/.skitter:/home/skitter/.skitter \
  ghcr.io/id/skitter/agent my-agent
```

Minimal Codex agent example:

```bash
docker run --rm \
  -e MQTT_BROKER_URL=mqtt://broker.emqx.io:1883 \
  -e SKITTER_AGENT_PERMISSION_MODE=bypassPermissions \
  -v ~/.skitter:/home/skitter/.skitter \
  -v ~/.codex/auth.json:/home/skitter/.codex/auth.json:ro \
  ghcr.io/id/skitter/agent my-codex-agent
```

Notes:

- The agent name resolves against `SKITTER_HOME/agents/`
- Without bind mounts, agent state and generated files are lost when the container exits
- In Docker, `SKITTER_AGENT_PERMISSION_MODE=bypassPermissions` enables full tool access inside the container
- See `docker-compose.test.yml` for a multi-agent example

## Observability

- Subscribe to `$a2a/v1/#` with any MQTT client to watch discovery, requests, replies, and events
- Open `dashboard.html` in a browser to watch traffic over WebSocket

## Limitations

- No built-in authentication; rely on MQTT broker auth
- One coordinator instance per broker
- Codex `.toml` agents currently apply only `model` and `developer_instructions`
- A2A Core Conformance only; Extended Conformance features are not implemented
