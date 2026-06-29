# Skitter

AI agent orchestrator using A2A-over-MQTT.

Skitter has two jobs:

- bundle individual agents into multi-step "apps"
- provide a harness for creating and running A2A-over-MQTT agents

The built-in runner wraps Claude Code and Codex, but any A2A-over-MQTT compliant process can participate.

## Why A2A-over-MQTT

Most agent orchestration stacks keep transport, scheduling, and runtime control inside one framework process. That makes them harder to inspect, harder to distribute, and harder to integrate with systems outside that stack.

A2A-over-MQTT is a better boundary:

- routing and fan-out live in the broker, not in bespoke in-process orchestration code
- agents can run anywhere, as long as they can reach MQTT
- agents and the coordinator only need egress access to the broker, not public internet ingress
- discovery, requests, replies, and lifecycle events are all visible on the wire
- interoperable agents can participate without being rewritten around one SDK

Skitter packages that model into a usable CLI: create agents, run them locally or in Docker, and compose them into apps when one agent is not enough.

## What Skitter Gives You

- An agent harness for Claude Code `.md` agents and Codex `.toml` agents
- An optional coordinator that turns multiple agents into a composed app
- A CLI for setup, service lifecycle, chat, and app/session inspection
- A local-broker workflow driven by `skitter up`

## Quick Start

Prerequisites:

- Python 3.11+
- `git`
- `uv`
- Docker, if you want the default local broker flow
- At least one runtime installed and authenticated: [Claude Code](https://docs.anthropic.com/en/docs/claude-code) or [Codex](https://github.com/openai/codex)
- An API key for a supported LLM provider, if you want the orchestrator to compose agents into apps

```bash
git clone https://github.com/id/skitter.git
cd skitter
uv sync

uv run skitter setup
uv run skitter create-agent random-x "returns a random number as JSON"
uv run skitter up
uv run skitter ask random-x "go"
```

What happens here:

1. `uv sync` installs Skitter and its dependencies into a local virtual environment
2. `uv run skitter setup` writes config under `~/.skitter/`
3. `uv run skitter create-agent` creates an agent definition under `~/.skitter/agents/`
4. `uv run skitter up` starts the broker, coordinator, and local agents
5. `uv run skitter ask` sends an A2A request over MQTT and prints the reply

This quick start uses a single standalone agent, so no orchestrator API key is required. The provider key is only needed for composed apps created with `skitter create-app`.

## Mental Model

- Agents are independent services that publish discovery cards and handle requests over MQTT.
- Skitter can run a single agent directly, or bundle multiple agents into an app with the coordinator.
- The built-in harness reads Claude Code `.md` agents and Codex `.toml` agents from `~/.skitter/agents/`.

## Web Dashboard

`web/` contains an optional React dashboard that connects straight to the broker over MQTT (WebSocket): browse discovered agents, send requests, drive composed apps through chat, and follow workflow runs live. It is a static single-page app with no backend of its own.

```bash
cd web
pnpm install
pnpm dev
```

The dev server runs at http://localhost:18084/skitter/. Set the broker URL, organization, and unit from the in-app settings (persisted in `localStorage`). See [web/README.md](web/README.md) for details.

## Next Steps

- [Usage Guide](docs/usage.md): service management, multi-agent apps, chat, Docker, storage, and limitations
- [Architecture](docs/architecture.md): design, execution flow, recovery, and coordinator details
- [A2A-over-MQTT Transport](docs/spec/a2a-over-mqtt-transport.md): topic scheme and protocol details
- [Contributing](CONTRIBUTING.md): development setup, testing, and config reference

## Further Reading

- [MQTT.AI](https://mqtt.ai/): broader MQTT-native AI ecosystem work, including [MCP over MQTT](https://mqtt.ai/docs/mcp-over-mqtt/)
