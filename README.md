# Skitter

MQTT-based multi-agent AI system. A stateless coordinator reads job specs from the broker, advances a task DAG, and spawns workers, making zero LLM calls itself. Workers use `claude-agent-sdk` for AI. The whole system is ~400 lines of Python.

## Architecture

```
  Any MQTT Client                    MQTT Broker                     Workers
 (mqttx, mosquitto,              (Docker, port 1883)            (claude-agent-sdk)
  Telegram bot, etc.)
                            ┌────────────────────────┐
        publish             │                        │
  ──────────────────────────▶  inbound/{chat_id}     │
                            │          │             │
                            │          ▼             │        ┌──────────────┐
                            │   ┌─────────────┐      │        │   Planner    │
                            │   │ Coordinator │──────────────▶│  (sonnet)    │
                            │   │  (no LLM)   │◀──────────────│  returns     │
                            │   └──────┬──────┘      │        │  JSON plan   │
                            │          │             │        └──────────────┘
                            │    builds DAG,         │
                            │    spawns workers      │        ┌──────────────┐
                            │          │             │   ┌───▶│  Worker A    │
                            │          ├─────────────────┤    │  (haiku)     │
                            │          │             │   └───▶│  Worker B    │
                            │          │             │        └──────────────┘
                            │          │             │              │
                            │          │  results    │◀─────────────┘
                            │          ▼             │
                            │   ┌─────────────┐      │        ┌──────────────┐
                            │   │ Coordinator │──────────────▶│  Synthesize  │
                            │   │ advances DAG│◀──────────────│  (haiku)     │
                            │   └──────┬──────┘      │        └──────────────┘
        subscribe           │          │             │
  ◀─────────────────────────── outbound/{chat_id}    │
                            │                        │
                            └────────────────────────┘
```

**Key property:** the coordinator is a pure graph executor. Planning and synthesis are worker tasks. If the coordinator crashes, it recovers in-flight jobs from retained MQTT messages on restart.

## Why MQTT

In a typical HTTP-based agent system, the orchestrator must handle routing, queuing, retries, fan-out, and load balancing itself — on top of the actual AI reasoning. With MQTT, all of that infrastructure moves into the broker:

<p align="center"><img src="http-vs-mqtt.svg" width="720" alt="HTTP vs MQTT architecture comparison"/></p>

This separation has practical consequences:

- **Chat app integration without code changes.** To connect Telegram, WhatsApp, Slack, or any other frontend, you write a single `{chat_app} <-> MQTT` bridge. Skitter itself doesn't change, it only sees `inbound/` and `outbound/` topics.
- **Workers can run anywhere.** Since workers communicate through the broker, they can be local processes, containers on Kubernetes, AWS Lambda functions, or machines in different regions with minimal modifications.
- **The coordinator can be serverless too.** It's a stateless event handler: message arrives, advance the DAG, publish, done. It doesn't need to run 24/7, it could be triggered by MQTT events on Lambda or Cloud Run.
- **Monitoring is free.** Any MQTT client (including browser-based ones via WebSocket) can subscribe to `skitter/#` and see every job spec, task assignment, result, and status change in real time, no dashboard code required.

## MQTT Topics

| Topic | Retain | Purpose |
|---|---|---|
| `skitter/inbound/{chat_id}` | No | User message → coordinator |
| `skitter/outbound/{chat_id}` | No | Final response → user |
| `skitter/jobs/{chat_id}` | Yes | Job spec (DAG + accumulated results) |
| `skitter/tasks/{agent}/{chat_id}/{task_id}` | Yes | Individual task for a worker |
| `skitter/results/{chat_id}/{task_id}` | No | Worker result → coordinator |
| `skitter/workers/{chat_id}/{task_id}/status` | No | Worker liveness (LWT) |

## Security Notice

**Skitter is an experimental proof-of-concept. It is not production-ready.**

- **No authentication.** Any client that can reach the MQTT broker can submit jobs, trigger Claude API calls, and execute code on the host.
- **No TLS.** All MQTT traffic is unencrypted (port 1883).
- **Workers run with `bypassPermissions`.** Claude agents can read/write any file and execute any command accessible to the process user.
- **No rate limiting or cost controls.** There is no cap on concurrent jobs, API calls, or spending. The planner LLM chooses the model per task — a crafted prompt can force every task to Opus.
- **No input validation.** Message fields (`chat_id`, `description`, etc.) are used unsanitized in topic strings and passed directly to workers.

**Do not expose the MQTT broker to untrusted networks. Run only on localhost or within a trusted, firewalled environment.**

Default EMQX dashboard credentials (`admin`/`public`) are not secure — change them before binding the broker to any non-loopback interface.

## Prerequisites

- Docker (for MQTT broker)
- Python 3.10+
- [uv](https://docs.astral.sh/uv/)
- Claude Code logged in (any plan that supports Claude Code)

## Quickstart

```bash
# 1. Start the MQTT broker
docker compose up -d

# 2. Install dependencies
uv sync

# 3. Start the coordinator
uv run python -m skitter

# 4. In another terminal — subscribe to responses, then send a message
mosquitto_sub -h localhost -t "skitter/outbound/my-chat" &
echo -n '{"text":"Hello!","sender":"me","chat_id":"my-chat"}' \
  | mosquitto_pub -h localhost -t "skitter/inbound/my-chat" -s
```

Any MQTT client works: `mqttx`, `mosquitto_pub`/`mosquitto_sub`, or a custom adapter (Telegram bot, Slack, etc.).

## Configuration

Copy `.env.example` to `.env` to override defaults:

| Variable | Default | Description |
|---|---|---|
| `MQTT_HOST` | `localhost` | MQTT broker hostname |
| `MQTT_PORT` | `1883` | MQTT broker port |
| `SKITTER_PLANNER_MODEL` | `sonnet` | Model for the planner worker |
| `SKITTER_MODELS` | `haiku:...\|sonnet:...\|opus:...` | Available models for workers (see [docs/architecture.md](docs/architecture.md)) |

EMQX dashboard (default broker): http://localhost:18083 (default login: `admin` / `public`).

## How It Works (short version)

1. User publishes to `skitter/inbound/{chat_id}`
2. Coordinator spawns a **planner** worker (zero tools, just JSON output)
3. Planner returns either `{"action":"respond","text":"..."}` or `{"action":"delegate","tasks":[...]}`
4. If delegate: coordinator builds a DAG, spawns workers for ready tasks (parallel where possible)
5. As results arrive, coordinator advances the DAG and spawns newly unblocked tasks
6. A **synthesize** task (auto-added, depends on all others) combines results into a final response
7. Final response published to `skitter/outbound/{chat_id}`

The planner picks which model to use per task (haiku for simple work, sonnet/opus for complex reasoning). See [docs/architecture.md](docs/architecture.md) for the full design.

## Known Limitations

**Reliability:**
- No task or job timeouts — a hung Claude agent runs indefinitely
- Dead worker respawn has no backoff or retry limit — a worker crashing on startup will be respawned in a tight loop
- Crash recovery restores running tasks but misses pending tasks whose dependencies completed before the crash

**Correctness:**
- Concurrent messages with the same `chat_id` silently overwrite each other — the first job's workers continue into the wrong job
- Worker errors (API failures, quota exhaustion, SDK crashes) are published as normal results — the synthesizer incorporates error strings into the user-facing response
- Invalid `depends_on` references from the planner crash the coordinator (unhandled `KeyError`)
- No dependency cycle detection — circular task graphs hang silently forever
- The planner occasionally ignores the JSON-only instruction and returns prose, causing a parse error that is forwarded to the user

**Missing features:**
- No conversation memory — each message is a fresh context
- No way to cancel an in-flight job
- No structured logging or metrics

**Cost:** Each inbound message triggers at least 2 Claude API calls (planner + synthesizer), plus one per delegated task. Monitor your usage.
