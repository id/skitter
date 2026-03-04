# Skitter

> **Warning: Experimental Proof-of-Concept**
> Skitter currently has no authentication, no TLS, and agents run with bypass permissions. Please only run this on localhost or inside a trusted, firewalled environment.

Skitter is a personal AI assistant built on MQTT.

Instead of a monolithic agent process that tries to handle orchestration, LLM calls, and chat I/O all in one place, Skitter completely decouples the stack. A lightweight supervisor spawns workers and handles joins via an MQTT broker, while independent workers route directly to the next task in the chain. Workers support both `claude-agent-sdk` and OpenAI's Codex CLI as runtimes.

The whole system is under 3k lines of Python.

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

3. **Initialize predefined agents and pipelines**
   ```bash
   uv run python -m skitter init
   ```
   Creates `~/.skitter/agents/` and `~/.skitter/pipelines/` with starter YAML files. Safe to re-run.

4. **Run an agent directly** (in another terminal)
   ```bash
   uv run python -m skitter agents run researcher "What are the key features of MQTT v5?"
   ```

   Or **run a pipeline** for multi-step chain execution:
   ```bash
   uv run python -m skitter pipeline run deep-research --var topic="MQTT v5"
   ```

   Or use the **interactive chat client** for both:
   ```bash
   uv run python -m skitter chat
   ```
   Type `/agent researcher What is MQTT v5?` or `/pipeline deep-research --var topic=MQTT`, then `/send`. Use `/drop` to discard. Use `--session-id` to set a specific session ID:
   ```bash
   uv run python -m skitter chat --session-id my-session
   ```

5. **Watch it work** — open `dashboard.html` in a browser to see jobs, tasks, and chain execution in real time. Connects to the broker's WebSocket endpoint (`ws://localhost:8083/mqtt`) using MQTT v5, no backend required.

*You can also use any MQTT v5 client directly (`mqttx`, `mosquitto_pub`/`mosquitto_sub`, custom Telegram/Slack bots, etc). Publish A2A requests to the supervisor's request topic with Response Topic and Correlation Data properties.*

## Predefined Agents

Define reusable, tunable agent profiles in `~/.skitter/agents/`:

```yaml
# ~/.skitter/agents/researcher.yaml
name: Research Specialist
description: Deep research with source citation
soul: |
  You are a research specialist. Be thorough, cite sources,
  and distinguish fact from speculation.
skills: |
  Search broadly before going deep. Cite URLs for every claim.
model: sonnet
max_turns: 15
runtime: claude    # "claude" (default) or "codex"
workspace: ""      # custom cwd (default: ~/.skitter/workspaces/{task_id})
```

Pipeline tasks reference agents by ID (e.g., `agent: researcher`). The agent's defaults (model, max_turns, runtime, etc.) are applied automatically. Pipeline tasks can still override any field. On startup, the supervisor publishes Agent Cards as retained MQTT messages for A2A discovery.

Manage and run agents with the CLI:

```bash
uv run python -m skitter agents list           # table of loaded agents
uv run python -m skitter agents show researcher # full YAML dump
uv run python -m skitter agents run researcher "What is MQTT v5?"  # run directly
```

## Pipelines

Pipelines are chain-based task templates in `~/.skitter/pipelines/`:

```yaml
# ~/.skitter/pipelines/deep-research.yaml
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

Each task specifies its `next` target (another task id, or `"output"` for terminal) and `needs` (upstream dependencies whose results are passed as context). If `next` is omitted, it's auto-inferred from the reverse dependency graph.

Run pipelines from the CLI:

```bash
uv run python -m skitter pipeline list
uv run python -m skitter pipeline run deep-research --var topic="quantum computing"
```

Variable interpolation uses a safe formatter that leaves unknown `{...}` patterns untouched (won't break JSON in descriptions). Pipeline tasks reference agent IDs and inherit their defaults.

## Why MQTT?

In a typical HTTP-based AI system, the orchestrator handles routing, retries, fan-out, and load balancing on top of the actual AI reasoning. By leaning on MQTT, we push all that infrastructure into the broker.

[![HTTP vs MQTT architecture comparison](http-vs-mqtt.svg)](http-vs-mqtt.svg)

What this means in practice:

* **Zero-code UI integrations:** To connect Telegram or WhatsApp, you just write a tiny ~100-line bridge script that publishes A2A requests to the supervisor's request topic and subscribes to a reply topic. The core supervisor never changes.
* **Run workers anywhere:** Workers can be local processes, k8s containers, or Lambda functions. As long as they can reach the broker, they work.
* **Serverless supervisor:** The supervisor is a stateless event handler (spawn entries, handle joins, respawn on crash). It can easily run on AWS Lambda or Cloud Run.
* **Free monitoring:** Subscribe to `$a2a/v1/#` using any MQTT client and watch every job spec, task assignment, and result flow in real-time.

## How It Works

Skitter implements [A2A-over-MQTT](https://www.emqx.com/mqtt-for-ai/a2a-over-mqtt/) for all agent communication. Every message in the system — requests, results, streaming, liveness, state — flows through a standard MQTT v5 topic scheme:

```
$a2a/v1/
├── discovery/{org}/{unit}/{agent_id}            # Retained Agent Cards (who can do what)
├── request/{org}/{unit}/{agent_id}              # A2A requests (JSON-RPC 2.0)
├── request/{org}/{unit}/{agent_id}/cancel       # Cancel signals
├── reply/{org}/{unit}/{agent_id}/{session}      # Replies (per-caller session)
├── event/{org}/{unit}/{agent_id}/{event_type}   # Agent events (alive/done/dead)
├── event/{org}/{unit}/supervisor/spawn          # Spawn requests from workers
├── state/{org}/{unit}/sessions/{session_id}            # Retained session specs
├── state/{org}/{unit}/chain/{session_id}/{task_id}    # Retained chain results (for joins)
├── state/{org}/{unit}/usage/{session_id}/{task_id}    # Usage tracking
└── control/{org}/{unit}/reload                  # Reload agents/pipelines signal
```

The supervisor never calls an LLM. It spawns workers for entry tasks, handles joins (multi-input tasks), and respawns on crash. Workers route directly to the next task in the chain.

### Chain-Based Execution

```text
  Any MQTT v5 Client                MQTT Broker                     Workers
 (CLI, dashboard,                (Docker, port 1883)       (claude-agent-sdk / codex)
  Telegram bot, etc.)
                            ┌──────────────────────────┐
   A2A JSON-RPC request     │                          │
  ──────────────────────────▶  request/.../coordinator │
   (v5 Response Topic +     │          │               │
    Correlation Data)       │          ▼               │
                            │   ┌─────────────┐        │
                            │   │ Supervisor  │        │     ┌──────────────┐
                            │   │  (no LLM)   │        │     │  Worker A    │
                            │   └──────┬──────┘        │  ┌─▶│  (sonnet)    │──┐
                            │          │               │  │  └──────────────┘  │
                            │  Spawn entry tasks       │  │  ┌──────────────┐  │
                            │          │               │  └─▶│  Worker B    │──┤
                            │          │  alive ◀──────────── │  (haiku)     │  │
                            │          │  (handshake)  │     └──────────────┘  │
                            │          ├──────────────────▶ dispatch task      │
                            │          │               │   (v5 properties)     │
                            │          │               │                       │
        stream items        │          │               │     ┌────────────┐    │
  ◀─────────────────────────────────── │ ◀─────────────────── │  Worker   │    │
        (direct to caller)  │          │               │     │  streams   │    │
                            │          │               │     └────────────┘    │
                            │          │  spawn next ◀─────────────────────────┤
                            │          ├──────────────────▶ next worker        │
                            │          │               │                       │
        terminal result     │          │               │                       │
  ◀────────────────────────────────────────────────────────────────────────────┘
        (direct to caller)  │          │               │
                            │          │               │
                            └──────────────────────────┘
```

1. **Request (A2A JSON-RPC):** Any MQTT v5 client publishes a request to `$a2a/v1/request/.../coordinator` with a `Response Topic` (where to send the answer) and `Correlation Data` (to match replies). The request includes either `agent_id` (direct call) or `pipeline_id` (chain execution).
2. **Build session:** For `agent_id`: supervisor creates a single-task session with the agent's defaults. For `pipeline_id`: it resolves the pipeline template and interpolates variables. Every task is a regular agent — research, review, synthesis, anything.
3. **Alive-triggered dispatch:** The supervisor spawns workers for entry tasks (no dependencies) and queues the task. The worker connects to the broker, sets an LWT for crash detection, and publishes an alive event. Only then does the supervisor dispatch the task.
4. **Direct-to-caller streaming:** Workers stream text deltas and tool events directly to the caller's reply topic at QoS 0 — the supervisor is not in the streaming path.
5. **Chain routing:** When a worker finishes, it routes based on `next`: for simple chains, it asks the supervisor to spawn the next worker (passing its result as context). For joins (multiple inputs), it publishes a retained chain result; the supervisor detects when all inputs arrive and spawns the join worker. Terminal tasks publish their result directly to the caller.
6. **Multi-runtime:** Workers support both `claude-agent-sdk` (Claude) and `codex exec` (OpenAI Codex CLI) runtimes, selected per-agent via YAML config.

### Agent Discovery

On startup, the supervisor publishes an **Agent Card** (retained) for each agent defined in `~/.skitter/agents/`:

```
$a2a/v1/discovery/skitter/default/researcher  →  {"agent_id":"researcher","name":"Research Specialist",...}
```

Any MQTT client can discover available agents by subscribing to `$a2a/v1/discovery/skitter/default/+`.

### Crash Recovery

Session specs and chain results are published as **retained messages**. If the supervisor crashes, it recovers in-flight sessions and accumulated chain results from the broker on restart, checks if any joins are satisfiable, and re-dispatches interrupted tasks. Worker crashes are detected via **LWT** — when the TCP connection drops, the broker fires the will message and the supervisor respawns the worker.

## Configuration

Copy `.env.example` to `.env` to override defaults:

| Variable | Default | Description |
|---|---|---|
| `MQTT_HOST` | `localhost` | Broker hostname |
| `MQTT_PORT` | `1883` | Broker port |
| `SKITTER_A2A_ORG` | `skitter` | A2A topic organisation segment |
| `SKITTER_A2A_UNIT` | `default` | A2A topic unit segment |
| `SKITTER_MODELS` | `haiku:...\|sonnet:...` | Available models for workers (see [docs/architecture.md](docs/architecture.md)) |
| `SKITTER_WORKER_MODE` | `subprocess` | Worker execution mode: `subprocess` or `docker` |
| `SKITTER_WORKER_IMAGE` | `skitter-worker:latest` | Docker image for workers (when mode=docker) |
| `SKITTER_DOCKER_NETWORK` | `skitter` | Docker network workers join |
| `SKITTER_DOCKER_MQTT_HOST` | `emqx` | MQTT host workers use inside Docker |

### Docker Worker Sandboxing

Workers can run in Docker containers instead of local subprocesses:

```bash
# Build the worker image
docker build -f Dockerfile.worker -t skitter-worker:latest .

# Enable Docker mode
export SKITTER_WORKER_MODE=docker
```

Workers connect to the MQTT broker via the `skitter` Docker network. The supervisor passes `ANTHROPIC_API_KEY` (and `OPENAI_API_KEY` for Codex-runtime agents) along with broker coordinates to each container. LWT crash detection works identically — a container crash drops the TCP connection, and the broker fires the will message.

## Roadmap & Known Limitations

**Currently working on:**
- [ ] **Telegram bridge** — standalone script connecting a bot to Skitter.
- [ ] **Conversation memory** — injecting per-chat history as context.
- [ ] **Worker timeouts & backoff** — handling hung or continually crashing workers.

**Things to watch out for:**
- **Cost:** Each pipeline run triggers one LLM API call per task. Keep an eye on your usage limits!
- **State overwrites:** Concurrent messages with the same `session_id` currently overwrite each other.
- **Error handling:** Worker errors (API failures, quota hits) are currently passed back as normal results to downstream tasks.
- **Incomplete crash recovery:** Restarting recovers running tasks from retained session specs, but accumulated stream data is lost (workers re-run).
