# Skitter

> **Warning: Experimental Proof-of-Concept**
> Skitter currently has no authentication, no TLS, and agents run with bypass permissions. Please only run this on localhost or inside a trusted, firewalled environment.

Skitter is a personal AI assistant built on MQTT.

Instead of a monolithic agent process that tries to handle orchestration, LLM calls, and chat I/O all in one place, Skitter completely decouples the stack. A stateless coordinator manages a task DAG via an MQTT broker, while independent workers handle the actual AI reasoning.

The whole system is ~1,000 lines of Python.

## Prerequisites

* Docker (for the MQTT broker)
* Python 3.10+
* [uv](https://docs.astral.sh/uv/)
* Claude Code logged in (any plan supporting Claude Code)

## Quickstart

1. **Start the MQTT broker**
   ```bash
   docker compose up -d
   ```

2. **Install dependencies and start the coordinator**
   ```bash
   uv sync
   uv run python -m skitter
   ```

3. **Chat with it** (in another terminal)
   ```bash
   uv run python -m skitter chat
   ```
   Type or paste a message, then enter `/send` to send. Use `/drop` to discard. Use `--chat-id` to set a specific session ID:
   ```bash
   uv run python -m skitter chat --chat-id my-session
   ```

4. **Watch it work** — open `dashboard.html` in a browser to see jobs, tasks, and DAG execution in real time. Connects to the broker's WebSocket endpoint (`ws://localhost:8083/mqtt`), no backend required.

*You can also use any MQTT client directly (`mqttx`, `mosquitto_pub`/`mosquitto_sub`, custom Telegram/Slack bots, etc).*

## Why MQTT?

In a typical HTTP-based AI system, the orchestrator handles routing, retries, fan-out, and load balancing on top of the actual AI reasoning. By leaning on MQTT, we push all that infrastructure into the broker.

[![HTTP vs MQTT architecture comparison](http-vs-mqtt.svg)](http-vs-mqtt.svg)

What this means in practice:

* **Zero-code UI integrations:** To connect Telegram or WhatsApp, you just write a tiny ~100-line bridge script translating chat APIs to `inbound/` and `outbound/` topics. The core AI coordinator never changes.
* **Run workers anywhere:** Workers can be local processes, k8s containers, or Lambda functions. As long as they can reach the broker, they work.
* **Serverless coordinator:** The coordinator is a stateless event handler (message arrives -> advance DAG -> publish). It can easily run on AWS Lambda or Cloud Run.
* **Free monitoring:** Subscribe to `skitter/#` using any MQTT client and watch every job spec, task assignment, and result flow in real-time.

## How It Works

Skitter relies on a pure graph executor. Planning and synthesis are just standard worker tasks. If the coordinator crashes, it simply recovers in-flight jobs from retained MQTT messages when it boots back up.

```text
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
                            │          │             │        └──────┬───────┘
                            │          │             │               │
                            │          │  results    │◀──────────────┘
                            │          ▼             │
                            │   ┌─────────────┐      │        ┌──────────────┐
                            │   │ Coordinator │──────────────▶│  QA Agent    │
                            │   │ checks qa   │◀──────────────│  pass/fail?  │
                            │   └──────┬──────┘      │        └──────────────┘
                            │          │             │
                            │     fail + retries     │
                            │     left? retry ───────────────▶ re-spawn worker
                            │          │             │
                            │     pass / exhausted   │        ┌──────────────┐
                            │          ├─────────────────────▶│  Synthesize  │
                            │          │             │◀───────│  (sonnet)    │
                            │          ▼             │        └──────────────┘
        subscribe           │          │             │
  ◀─────────────────────────── outbound/{chat_id}    │
                            │                        │
                            └────────────────────────┘
```

1. **Input:** User publishes to `skitter/inbound/{chat_id}`.
2. **Plan:** Coordinator spawns a planner worker. It returns either a direct response or a DAG (Directed Acyclic Graph) of tasks to execute.
3. **Execute:** Coordinator builds the DAG and spawns workers for ready tasks in parallel.
4. **Advance:** As results arrive, the coordinator advances the DAG, spawning newly unblocked tasks.
5. **QA & Retry:** If a task includes QA criteria, an ephemeral QA agent evaluates the result. Failures automatically retry with feedback.
6. **Synthesize:** A final synthesize task combines all results into a human-readable response.
7. **Output:** The response is published to `skitter/outbound/{chat_id}`.

## Configuration

Copy `.env.example` to `.env` to override defaults:

| Variable | Default | Description |
|---|---|---|
| `MQTT_HOST` | `localhost` | Broker hostname |
| `MQTT_PORT` | `1883` | Broker port |
| `SKITTER_PLANNER_MODEL` | `sonnet` | Model used for the planner worker |
| `SKITTER_MODELS` | `haiku:...|sonnet:...` | Available models for workers (see [docs/architecture.md](docs/architecture.md)) |

## Roadmap & Known Limitations

**Currently working on:**
- [ ] **Telegram bridge** — standalone script connecting a bot to Skitter.
- [ ] **Conversation memory** — injecting per-chat history as context.
- [ ] **Dependency cycle detection** — rejecting circular DAGs early.
- [ ] **Worker timeouts & backoff** — handling hung or continually crashing workers.

**Things to watch out for:**
- **Cost:** Each inbound message triggers at least 2 Claude API calls (planner + synthesizer), plus one per delegated task. Keep an eye on your usage limits!
- **State overwrites:** Concurrent messages with the same `chat_id` currently overwrite each other.
- **Error handling:** Worker errors (API failures, quota hits) are currently passed back as normal results, meaning the synthesizer might incorporate an error string directly into the user-facing chat.
- **Incomplete crash recovery:** Restarting recovers running tasks, but might drop pending tasks if their dependencies finished right before the crash.
