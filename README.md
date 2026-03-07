# Skitter

> **Warning: Experimental Proof-of-Concept**
> Skitter currently has no authentication, no TLS, and agents run with bypass permissions. Please only run this on localhost or inside a trusted, firewalled environment.

Skitter is a personal AI assistant built on MQTT.

Instead of a monolithic agent process that tries to handle orchestration, LLM calls, and chat I/O all in one place, Skitter completely decouples the stack. A stateless supervisor intercepts agent requests via wildcard MQTT subscriptions, creates sessions, pre-materializes dispatch specs, and spawns all workers upfront. Workers are self-coordinating: they read session specs from retained MQTT, wait for upstream results (joins), and route chain results themselves. Agent identity (personality, model, tools, memory) is delegated to native CLI sub-agent systems. Both Claude Code CLI and OpenAI's Codex CLI are supported as runtimes.

Small Python codebase.

## Prerequisites

* Docker (for the MQTT broker)
* Python 3.10+
* [uv](https://docs.astral.sh/uv/)
* Claude Code logged in (any plan supporting Claude Code)
* (Optional) [Codex CLI](https://github.com/openai/codex) logged in (`codex login`) for Codex-runtime agents

## Quickstart

1. **Start the MQTT broker**
   ```bash
   docker compose up -d
   ```

2. **Install dependencies and start the supervisor**
   ```bash
   uv sync
   uv run python -m skitter
   ```

3. **Initialize predefined agents and workflows**
   ```bash
   uv run python -m skitter init
   ```
   Creates:
   - `~/.skitter/agents/*.yaml` – orchestration stubs (name, description, runtime)
   - `~/.skitter/workflows/*.yaml` – workflow templates
   - `~/.skitter/config.yaml` – global config (default runtime)
   - `~/.claude/agents/*.md` – Claude sub-agent definitions (personality, model, tools, memory)

   Safe to re-run; won't overwrite existing files.

4. **Run an agent directly** (in another terminal)
   ```bash
   uv run python -m skitter agents run researcher "What are the key features of MQTT v5?"
   ```

   Or **run a workflow** for multi-step chain execution:
   ```bash
   uv run python -m skitter workflow run deep-research --var topic="MQTT v5"
   ```

   Or use the **interactive chat client** for both:
   ```bash
   uv run python -m skitter chat
   ```
   Type `/agent researcher What is MQTT v5?` or `/workflow deep-research --var topic=MQTT`, then `/send`. Use `/drop` to discard.

5. **Watch it work.** Open `dashboard.html` in a browser to see jobs, tasks, and chain execution in real time. Connects to the broker's WebSocket endpoint (`ws://localhost:8083/mqtt`) using MQTT v5, no backend required.

*You can also use any MQTT v5 client directly (`mqttx`, `mosquitto_pub`/`mosquitto_sub`, custom Telegram/Slack bots, etc). Publish JSON requests to an agent's request topic with MQTT v5 Response Topic and Correlation Data properties. The supervisor intercepts via wildcard subscription — clients never need to know it exists.*

## Agent Configuration

Skitter delegates agent identity to native CLI sub-agent systems. Each agent has a slim orchestration stub in `~/.skitter/agents/` plus a native sub-agent definition.

### Skitter orchestration stub

```yaml
# ~/.skitter/agents/researcher.yaml
name: Research Specialist
description: Deep research with source citation
runtime: claude    # "claude" or "codex" (omit to use default_runtime)
workspace: ""      # custom cwd (default: ~/.skitter/workspaces/{task_id})
```

The YAML filename stem must match the native sub-agent name.

### Claude sub-agents

```markdown
# ~/.claude/agents/researcher.md
---
name: researcher
description: Deep research with source citation
model: sonnet
maxTurns: 15
memory: user
tools: Read, Grep, Glob, Bash, WebSearch, WebFetch
---

You are a research specialist. Be thorough, cite sources,
and distinguish fact from speculation.

Search broadly before going deep. Cite URLs for every claim.
```

Features from Claude Code's sub-agent system:
- **Persistent memory**: `memory: user` gives each agent auto-managed memory at `~/.claude/agent-memory/<name>/MEMORY.md`
- **Per-agent tools**: restrict which tools each agent can use
- **Per-agent MCP servers**: give specific agents access to specific services
- **Per-agent model**: each agent can use a different model

These are standard Claude Code sub-agents. You can use them directly without skitter:

```bash
# From the terminal
claude --agent researcher -p "What are the key features of MQTT v5?"

# Or within a Claude Code session, ask Claude to delegate:
# "Use the researcher agent to look into MQTT v5"
```

Skitter adds workflow orchestration on top: fan-out, join, chain routing, and crash recovery across multiple agents.

### Codex sub-agents

```toml
# ~/.codex/agents/coder.toml
model = "gpt-5.1-codex-mini"
model_reasoning_effort = "high"
sandbox_mode = "workspace-write"
developer_instructions = """
You are a senior developer. Write clean, idiomatic code.
"""
```

Register in `~/.codex/config.toml`:
```toml
[agents.coder]
description = "Implementation and code changes"
config_file = "agents/coder.toml"
```

### Global config

```yaml
# ~/.skitter/config.yaml
default_runtime: claude    # or "codex"
```

Agents without explicit `runtime` in their YAML use this default.

### CLI commands

```bash
uv run python -m skitter agents list           # table of loaded agents
uv run python -m skitter agents show researcher # YAML dump
uv run python -m skitter agents run researcher "What is MQTT v5?"
```

## Workflows

Workflows are chain-based task templates in `~/.skitter/workflows/`:

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
    description: "Cross-reference findings about '{topic}'. Flag contradictions."
    next: synthesize
    needs: [research_web, research_academic]
  - id: synthesize
    agent: writer
    description: "Combine all research findings about '{topic}' into a clear, coherent response."
    next: output
    needs: [fact_check]
```

Each task specifies its `next` target (another task id, or `"output"` for terminal) and `needs` (upstream dependencies whose results are passed as context). If `next` is omitted, it's auto-inferred from the reverse dependency graph. Workflow tasks can override `model` per-task (passed as CLI flag, overriding the sub-agent's default).

Run workflows from the CLI:

```bash
uv run python -m skitter workflow list
uv run python -m skitter workflow run deep-research --var topic="quantum computing"
```

## Docker Workers

Workers can run in Docker containers instead of local subprocesses. This provides sandboxing and makes it possible to run workers on remote machines.

### Setup

```bash
# Build the worker image
uv run python -m skitter docker build

# Authenticate Claude inside a container (one-time)
uv run python -m skitter docker login
# Run /login inside the container, then exit

# Sync agent definitions into the docker dir
uv run python -m skitter docker sync
```

The login flow saves OAuth credentials to `~/.skitter/docker-claude/`. The sync step copies `~/.claude/agents/*.md` and `~/.claude/agent-memory/` into the same directory. Worker containers mount this directory read-only.

### Running

```bash
# Start the supervisor in Docker mode
SKITTER_SPAWN_MODE=docker uv run python -m skitter
```

Workers connect to the MQTT broker via the `skitter` Docker network. Re-run `skitter docker sync` after modifying agent definitions.

## Why MQTT?

In a typical HTTP-based AI system, the orchestrator handles routing, retries, fan-out, and load balancing on top of the actual AI reasoning. By leaning on MQTT, we push all that infrastructure into the broker.

```
    ┌─────────────────────────────────────────────────────────────────────┐
    │                        TYPICAL HTTP STACK                           │
    │                                                                     │
    │   Telegram ──┐                              ┌── Agent A             │
    │              ▼                              │                       │
    │   Slack ───▶ Orchestrator ◀── routing ──────┼── Agent B             │
    │              ▲   │  ▲   retries, fan-out,   │                       │
    │   Web UI ───┘    │  │   load balancing,     └── Agent C             │
    │                  │  │   state management                            │
    │                  │  │                                               │
    │              Everything goes through the orchestrator.              │
    │              It's the bottleneck AND the brain.                     │
    └─────────────────────────────────────────────────────────────────────┘

    ┌─────────────────────────────────────────────────────────────────────┐
    │                        SKITTER (MQTT)                               │
    │                                                                     │
    │   Telegram ──┐     ┌───────────┐             ┌── Worker A ──┐       │
    │              ├────▶│           │◀───────────▶│  (claude)    │       │
    │   Slack ─────┤     │   MQTT    │             └──────────────┘       │
    │              ├────▶│  Broker   │◀───────────▶┌── Worker B ──┐       │
    │   Web UI ────┤     │           │             │  (codex)     │       │
    │              │     │  routing  │             └──────────────┘       │
    │  Supervisor ─┘     │  fan-out  │◀───────────▶┌── Worker C ──┐       │
    │  (stateless,       │  state    │             │  (claude)    │       │
    │   just spawns)     │  LWT      │             └──────────────┘       │
    │                    └───────────┘                                    │
    │              The broker IS the infrastructure.                      │
    │              Workers self-coordinate. Supervisor just spawns.       │
    └─────────────────────────────────────────────────────────────────────┘
```

What this means in practice:

* **Zero-code UI integrations:** To connect Telegram or WhatsApp, you just write a tiny ~100-line bridge script that publishes requests to the agent's request topic and subscribes to a reply topic. The supervisor is invisible.
* **Run workers anywhere:** Workers can be local processes, Docker containers, or cloud functions. As long as they can reach the broker, they work.
* **Serverless supervisor:** The supervisor is a stateless session creator. It pre-materializes dispatch specs and spawns workers upfront. Workers self-coordinate from there. The supervisor can run on AWS Lambda or Cloud Run.
* **Free monitoring:** Subscribe to `$a2a/v1/#` using any MQTT client and watch every job spec, task assignment, and result flow in real-time.

## How It Works

Skitter uses the [A2A-over-MQTT](https://www.emqx.com/mqtt-for-ai/a2a-over-mqtt/) topic scheme for agent communication. Every message (requests, results, streaming, liveness, coordination state) flows through MQTT v5 topics with application-defined suffixes after the agent ID:

```
$a2a/v1/
├── discovery/{org}/{unit}/{agent_id}                              # Retained Agent/Workflow Cards
├── request/{org}/{unit}/{agent_id}                                # Requests (clients address agents directly)
├── request/{org}/{unit}/{agent_id}/cancel                         # Cancel signals
├── request/{org}/{unit}/supervisor/reload                         # Reload agents/workflows signal
├── reply/{org}/{unit}/{agent_id}/{suffix}                         # Replies (per-caller session)
├── event/{org}/{unit}/{agent_id}/{event_type}                     # Agent events (alive/done/dead)
├── event/{org}/{unit}/supervisor/session/{session_id}             # Retained session specs (immutable)
├── event/{org}/{unit}/{agent_id}/task-status/{sid}/{tid}          # Per-task status (retained)
├── event/{org}/{unit}/{agent_id}/chain-result/{sid}/{tid}         # Retained chain results (for joins)
└── event/{org}/{unit}/{agent_id}/usage/{sid}/{tid}                # Usage tracking
```

The supervisor never calls an LLM. It listens on wildcard topics (`request/{o}/{u}/+` and `event/{o}/{u}/+/+`), creates sessions, pre-materializes dispatch specs as retained messages, and spawns all workers upfront. Clients address agents directly — the supervisor is invisible infrastructure.

### Chain-Based Execution

```text
  Any MQTT v5 Client                MQTT Broker                     Workers
 (CLI, dashboard,                (Docker, port 1883)        (claude CLI / codex CLI)
  Telegram bot, etc.)
                            ┌──────────────────────────┐
   JSON request             │                          │
  ──────────────────────────▶  request/.../agent_id    │
   (v5 Response Topic +     │          │               │
    Correlation Data)       │          ▼               │
                            │   ┌─────────────┐        │
                            │   │ Supervisor  │        │     ┌──────────────┐
                            │   │  (no LLM)   │        │     │  Worker A    │
                            │   └──────┬──────┘        │  ┌──│  (sonnet)    │──┐
                            │          │               │  │  └──────────────┘  │
                            │  Publish session spec    │  │  ┌──────────────┐  │
                            │  + dispatch specs        │  ├──│  Worker B    │──┤
                            │  (all retained)          │  │  │  (haiku)     │  │
                            │          │               │  │  └──────────────┘  │
                            │  Spawn ALL workers       │  │                    │
                            │          ├──────────────────┘   Workers read     │
                            │          │               │      retained specs   │
                            │          │               │      & self-coordinate│
        stream items        │          │               │     ┌────────────┐    │
  ◀─────────────────────────────────── │ ◀───────────────────│  Worker    │    │
        (direct to caller)  │          │               │     │  streams   │    │
                            │          │               │     └────────────┘    │
                            │          │               │                       │
                            │          │  chain result │   Workers publish     │
                            │          │  (retained) ◀─────  retained results  │
                            │          │               │   & wait for upstream │
        terminal result     │          │               │                       │
  ◀────────────────────────────────────────────────────────────────────────────┘
        (direct to caller)  │          │               │
                            │          │               │
                            └──────────────────────────┘
```

1. **Request:** Any MQTT v5 client publishes a JSON request to `$a2a/v1/request/{org}/{unit}/{agent_id}` (or `workflow-{id}` for workflows) with a `Response Topic` (where to send the answer) and `Correlation Data` (to match replies). The supervisor intercepts via wildcard subscription.
2. **Build session:** For agents: the supervisor creates a single-task session. For workflows: it resolves the workflow template and interpolates variables. Every task is a regular agent (research, review, synthesis, anything).
3. **Pre-materialize and spawn:** The supervisor publishes the session spec as a retained event message, then spawns all workers upfront. Workers read the session from retained MQTT on startup. Workers with upstream dependencies (joins) wait for chain results to arrive before executing.
4. **Direct-to-caller streaming:** Workers stream text deltas and tool events directly to the caller's reply topic at QoS 0. The supervisor is not in the streaming path.
5. **Self-coordinating chain routing:** When a worker finishes, it publishes a retained chain result to a suffixed event topic. Downstream workers subscribed to their upstream dependencies detect when all inputs arrive and begin execution. Terminal tasks publish their result directly to the caller.
6. **Multi-runtime:** Workers invoke the `claude` binary (Claude Code CLI) or `codex` binary (OpenAI Codex CLI) as subprocesses, parsing JSONL stdout from both. Workers use `--agent <name>` to invoke the appropriate native sub-agent.

### Agent Discovery

On startup, the supervisor publishes pre-built **Agent Cards** (retained) from `~/.skitter/cards/*.json`:

```
$a2a/v1/discovery/skitter/default/researcher  →  {"agent_id":"researcher","name":"Research Specialist",...}
```

Any MQTT client can discover available agents by subscribing to `$a2a/v1/discovery/skitter/default/+`.

### Crash Recovery

Session specs are published as **retained messages**. Since the session spec is immutable after creation and workers self-coordinate, the supervisor is crash-safe: restarting it has no effect on in-flight sessions. Workers set LWT messages for crash detection. When the TCP connection drops, the broker fires the will message and the supervisor can respawn the worker.

### A2A-over-MQTT Conformance

Skitter uses the A2A-over-MQTT topic scheme (`$a2a/v1/discovery/`, `$a2a/v1/request/`, `$a2a/v1/reply/`, `$a2a/v1/event/`) and MQTT v5 `Response Topic` + `Correlation Data` for request/reply correlation. Differences from the [spec](https://www.emqx.com/mqtt-for-ai/a2a-over-mqtt/):

- **Request payloads are plain JSON**, not JSON-RPC 2.0. Requests carry `{text, sender, session_id, agent_id, workflow_id}` instead of `{jsonrpc, method, params, id}`.
- **Error replies use JSON-RPC error objects** (`{jsonrpc, id, error: {code, message, data}}`), matching the spec. A2A error codes: `-32004` (responder unavailable), `-32005` (transport protocol error).
- **No `Task.id` in responses.** The spec requires responders to return a server-generated task ID. Skitter uses session-scoped task IDs embedded in the session spec instead.
- **Suffixed event topics** (`event/{org}/{unit}/{agent_id}/chain-result/{sid}/{tid}`, etc.) for coordination state are application-defined extensions using the spec's suffix mechanism.
- **No retry/timeout profile.** Clients don't implement the spec's `reply_first_timeout_ms`, `stream_idle_timeout_ms`, or exponential backoff.
- **No auth/TLS.** The spec requires TLS for bearer tokens and supports OAuth 2.0 via User Properties. Skitter currently runs unauthenticated on localhost.

## Configuration

Copy `.env.example` to `.env` to override defaults:

| Variable | Default | Description |
|---|---|---|
| `MQTT_HOST` | `localhost` | Broker hostname |
| `MQTT_PORT` | `1883` | Broker port |
| `SKITTER_A2A_ORG` | `skitter` | A2A topic organisation segment |
| `SKITTER_A2A_UNIT` | `default` | A2A topic unit segment |
| `SKITTER_SPAWN_MODE` | `subprocess` | Worker execution mode: `subprocess` or `docker` |
| `SKITTER_WORKER_IMAGE` | `skitter-worker:latest` | Docker image for workers (when mode=docker) |
| `SKITTER_DOCKER_NETWORK` | `skitter` | Docker network workers join |
| `SKITTER_DOCKER_MQTT_HOST` | `emqx` | MQTT host workers use inside Docker |

## Roadmap & Known Limitations

**Currently working on:**
- [ ] **Telegram bridge** – standalone script connecting a bot to Skitter.
- [ ] **Serverless deployment** – EMQX Cloud + Cloudflare Workers/Containers.

**Things to watch out for:**
- **Cost:** Each workflow run triggers one LLM API call per task. Keep an eye on your usage limits!
- **State overwrites:** Concurrent messages with the same `session_id` currently overwrite each other.
- **Error handling:** Worker errors (API failures, quota hits) are currently passed back as normal results to downstream tasks.
- **Incomplete crash recovery:** Supervisor crashes are safe (workers self-coordinate), but worker crashes require respawning. Accumulated stream data is lost on worker re-run.
