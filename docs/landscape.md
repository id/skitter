# Landscape: Personal AI Assistants

Research conducted March 2026. Covers the major open-source personal AI assistant projects and key libraries.

## OpenClaw

**What:** Self-hosted personal AI assistant platform. Hub-and-spoke architecture around a WebSocket Gateway. Multi-channel (WhatsApp, Telegram, Discord, Slack, Signal, iMessage, Teams, Matrix, etc.). 180k+ GitHub stars.

**Tech:** TypeScript/Node.js 22+. Uses pi-mono as its AI runtime (`pi-ai`, `pi-agent-core`, `pi-coding-agent`, `pi-tui`). SQLite for memory (hybrid vector + BM25 search). Docker sandboxing for tool execution.

**Key features:**
- Multi-channel routing with per-channel agent isolation
- Voice Wake + Talk Mode (ElevenLabs)
- Canvas + A2UI (agent-driven visual workspace)
- Plugin system (channels, memory, tools, providers)
- Session tools for agent-to-agent coordination
- Cron jobs and webhooks
- macOS/iOS/Android companion apps
- Security: pairing flow for unknown DMs, channel allowlists, Docker sandboxing

**Relationship to pi-mono:** Pi-mono IS OpenClaw's AI runtime. OpenClaw uses `createAgentSession()` from `pi-coding-agent` in-process (not as subprocess). The type system (`AgentMessage`, `AgentTool`, `Model<Api>`) permeates 40+ source files. Session persistence (JSONL with branching/compaction), token estimation, model discovery, and auth storage all come from pi-mono. Replacing pi-mono would mean rebuilding the core execution model.

**Relevant to skitter:** OpenClaw is a monolithic gateway; skitter is a decoupled coordinator. Different architectural philosophies — OpenClaw owns the full stack (channels, UI, tools, agent loop), skitter pushes infrastructure into the MQTT broker and keeps the coordinator minimal. OpenClaw's channel adapter pattern (normalizing inbound/outbound across platforms) is well-engineered and worth studying if skitter adds more integrations.

## Nanobot (HKUDS)

**What:** Ultra-lightweight personal AI assistant in ~4,000 lines of Python. Multi-provider, multi-channel. Emphasizes research accessibility.

**Tech:** Python 3.11+, async. JSON config. Docker deployment.

**Key features:**
- 22+ LLM providers via OpenRouter + direct APIs
- Channels: Telegram, Discord, WhatsApp, Slack, Email, QQ, Matrix, Feishu, DingTalk, etc.
- MCP integration for extensibility
- Memory system, web search (Brave API), task scheduling

**Relevant to skitter:** Closest in spirit to skitter — small Python codebase, multi-provider. But no DAG orchestration, no MQTT, no crash recovery. Single-agent architecture. Shows that multi-provider + multi-channel can be done in a small codebase without pi-mono or litellm.

## NanoClaw (qwibitai)

**What:** Lightweight alternative to OpenClaw. Single Node.js orchestrator with containerized Claude agents.

**Tech:** TypeScript/Node.js 20+. SQLite. Docker or Apple Container for isolation. Claude Agent SDK.

**Key features:**
- Channels: WhatsApp, Telegram, Discord, Slack, Signal
- Per-group container isolation (each group gets own sandboxed agent)
- Scheduled tasks, agent swarms
- Skill-based extensibility via Claude Code skills
- Filesystem-based IPC

**Design philosophy:** "Core functionality in a codebase small enough to understand: one process and a handful of files." Built because OpenClaw has "nearly half a million lines of code, 53 config files, and 70+ dependencies."

**Relevant to skitter:** Similar minimalist philosophy. Container-per-group isolation is interesting — skitter's worker-per-task model is analogous but more granular. NanoClaw uses Claude Agent SDK like skitter's workers do.

## ZeroClaw (zeroclaw-labs)

**What:** Runtime framework for agentic AI workflows, built entirely in Rust. "Build once, run anywhere."

**Tech:** 100% Rust. Single static binary. ARM/x86/RISC-V.

**Key features:**
- <5MB RAM, sub-10ms startup, ~8.8MB binary
- Trait-driven pluggable architecture (providers, channels, tools all swappable)
- Research phase (tool-gathering before response) to reduce hallucinations
- Security: sandboxing, explicit allowlists, workspace scoping

**Relevant to skitter:** Different niche entirely — edge/IoT deployment where resources are constrained. The trait-driven swappability pattern is well-designed. Not directly comparable to skitter's orchestration model.

## NullClaw (nullclaw)

**What:** "Smallest fully autonomous AI assistant infrastructure." Written in Zig.

**Tech:** 100% Zig. 678KB static binary, ~1MB peak RAM, <2ms boot on Apple Silicon. Zero runtime dependencies.

**Key features:**
- 22+ AI providers, 18 channels, vtable-based swappability
- SQLite with hybrid FTS5 + vector cosine similarity search
- Sandboxing: Landlock, Firejail, Bubblewrap, or Docker (auto-detected)
- WASM runtime support (wasmtime)
- Peripheral support: GPIO, serial, Arduino, Raspberry Pi, STM32
- Encrypted API keys (ChaCha20-Poly1305), pairing with OTP codes

**Relevant to skitter:** Extreme end of the minimalism spectrum. The hybrid memory search (FTS5 + vector) in SQLite is a good pattern to study for skitter's planned conversation memory. The vtable interface design achieves the same pluggability as ZeroClaw's traits but in Zig. Not directly comparable architecturally.

---

# Library Analysis

## pi-mono (badlogic/pi-mono)

TypeScript monorepo. 7 packages, 4 relevant to agent work.

**What's genuinely hard to replicate:**

1. **Multi-provider LLM abstraction (`pi-ai`)** — 9 backends (Anthropic, OpenAI, Google Gemini, Vertex AI, Azure, Bedrock, Codex, etc.) with production-grade streaming, tool calling, vision, and thinking/reasoning. Months of provider quirk handling: Mistral's 9-char tool IDs, Gemini thought signatures, OpenAI's 450+ char tool call IDs vs Anthropic's 64-char limit, Z.ai/Qwen `enable_thinking` boolean. Unified `reasoning` level ("minimal" through "xhigh") mapped to each provider's native mechanism.

2. **Cross-model conversation continuity (`transform-messages.ts`)** — switch models mid-conversation with proper handling of: encrypted thinking blocks, orphaned tool calls (synthetic result injection), tool call ID normalization across providers. Edge-case-heavy code that seems simple until you hit 50 bugs.

3. **Agent loop two-queue architecture (`agent-loop.ts`)** — steering messages (interrupt mid-tool-execution) + follow-up messages (post-completion queue). Abort/cleanup semantics for interrupted tool calls are non-trivial.

4. **Session management (`pi-coding-agent`)** — JSONL persistence with branching, two-tier context compaction (overflow recovery + proactive threshold), token estimation, extension hooks.

**What's thin / easy to replicate:**
- `Agent` class (state container with queues)
- `pi-tui` (terminal rendering)
- `pi-pods` (vLLM deployment CLI)
- `pi-web-ui` (chat components)
- Model catalog (auto-generated data, structurally simple)

**For skitter:** The most relevant piece is `pi-ai` for multi-provider workers. Skitter doesn't need pi-mono's session management (MQTT handles state) or compaction (workers are ephemeral). A TS worker using `pi-ai` would give provider-flexible workers while the coordinator stays Python/MQTT.

## litellm (BerriAI)

Python library. 100+ LLM providers. 37k+ GitHub stars. MIT license.

**Pros:**
- Broadest provider coverage available
- OpenAI-compatible `completion()`/`acompletion()` interface
- Built-in: cost tracking, rate limiting, fallback chains, load balancing, response caching
- Proxy/gateway mode (OpenAI-compatible HTTP server)
- Active maintenance, day-0 new model support

**Cons:**
- **3-4 second import time** per process (open issue #7605). Critical for skitter — every `subprocess.Popen` worker spawn pays this tax.
- Heavy dependency tree (13 direct deps including Rust-compiled `tiktoken`, `tokenizers`). Roughly triples skitter's current dep surface.
- Abstraction leaks: Bedrock doesn't support `tool_choice="none"`, schemas silently stripped for some providers, docs don't match code.
- Tool calling not truly uniform: documented breakages on o1 (#7292), Llama 4 Scout (#11047), Mistral schema params, Ollama prompt-injection fallback.
- ~21% per-request latency overhead vs direct SDK calls (#7764).
- Stability concerns: OOM after upgrades, memory leaks over time (#12685), perf regression v1.80->v1.81 (#19921).
- Multiple daily releases, breaking changes without migration guides, 5500-line main handler.

**Critical distinction:** litellm is a completion API, not an agent runtime. `claude-agent-sdk` provides the agent loop (multi-turn tool calling, file ops, command execution). They're not interchangeable. Realistic integration: use litellm for non-agentic simple tasks (single-turn, no tools) while keeping `claude-agent-sdk` for agentic workers.

**Alternative:** Thin adapter over native `anthropic` + `openai` + `google-genai` SDKs (~100-200 lines) avoids the dependency weight and import-time penalty but means maintaining your own quirk handling.

---

# Key Architectural Comparisons

| | Skitter | OpenClaw | NanoClaw | Nanobot | ZeroClaw | NullClaw |
|---|---|---|---|---|---|---|
| Language | Python | TypeScript | TypeScript | Python | Rust | Zig |
| LOC | ~3,200 | ~500,000 | ~5,000 | ~4,000 | N/A | ~110 files |
| Coordination | MQTT broker | WebSocket Gateway | Single process | Single process | N/A | Single binary |
| Agent runtime | Native CLI sub-agents | pi-mono embedded | Claude Agent SDK | Custom async | Trait-based | vtable-based |
| Multi-provider | Claude + Codex | Yes (via pi-ai) | No (Claude only) | Yes (22+) | Yes (pluggable) | Yes (22+) |
| Multi-channel | Any MQTT client | 10+ built-in | 5 built-in | 10+ built-in | Pluggable | 18 built-in |
| DAG orchestration | Yes | No | No | No | No | No |
| Crash recovery | Yes (MQTT retained + LWT) | Session persistence | Container restart | No | No | No |
| Parallel execution | Yes (DAG fan-out) | No (single agent) | Agent swarms | No | No | No |
| QA feedback loop | Yes | No | No | No | No | No |
| Memory | Per-agent (Claude native) | SQLite + vector | Per-group CLAUDE.md | Custom | N/A | SQLite FTS5 + vector |
| Edge deployment | No | No | No | No | Yes (<5MB RAM) | Yes (678KB binary) |

**Skitter's unique position:** Only project with broker-mediated DAG orchestration and crash recovery. The MQTT architecture is genuinely differentiated — no other project in this space uses a message broker as the coordination backbone. The trade-off is that skitter is currently Claude-only and has no channel integrations (by design — bridges are ~100-line scripts).

---

# MQTT as Agent Infrastructure

Source: "Why MQTT Is the Missing Infrastructure Layer for Agentic AI" (EMQX, 2026).

## The Actor Model Connection

MQTT is structurally an Actor Model runtime at the infrastructure layer:

- **Topics as mailboxes.** Each agent subscribes to its own topic — incoming messages queue like an actor's mailbox.
- **Retained messages as actor state.** Externally visible state published as retained messages. New/restarted agents pick up latest state immediately.
- **QoS as delivery guarantees.** At-most-once (QoS 0) through exactly-once (QoS 2) per interaction.
- **The broker as supervisor.** Tracks connection state, detects disconnects, publishes Last Will messages — structural analog to Akka-style supervision hierarchies.
- **Shared subscriptions as actor pools.** Distribute messages across competing consumers for load balancing.

This maps directly to skitter's architecture: workers are actors with topic-based mailboxes, retained sessions are shared state, LWT provides supervision, and the broker is the coordination fabric.

## HTTP vs MQTT for Agent Coordination

| Concern | HTTP Stack | MQTT |
|---|---|---|
| Agent discovery | External registry / well-known URLs | Retained messages |
| Task delegation | Request-response | Request-response over pub/sub |
| Load balancing | Service mesh / external LB | Shared subscriptions |
| Task queuing | Redis / RabbitMQ | MQTT Queues |
| Event streaming | Kafka | MQTT Streams |
| Health monitoring | Custom heartbeat logic | Built-in connection tracking |
| Authorization | Per-service auth layer | Topic-level ACLs |

The key argument: MQTT collapses discovery, messaging, task distribution, streaming, and health monitoring into a single protocol instead of stitching together HTTP APIs, WebSocket connections, message queues, and streaming platforms.

## EMQX Extensions (EIP-0033, Queues/Streams)

- **Agent registry in the broker** — Agent Cards validated against JSON schema, automatic online/offline status tracking, shared pool dispatch, OAuth2/JWKS security metadata.
- **Queue subscriptions** (`$queue/{name}/{topic}`) — persistent, exclusive-delivery message queuing. Replaces Redis/RabbitMQ for task distribution.
- **Stream subscriptions** (`$stream/{name}/{topic}`) — ordered, replayable message streams with consumer offsets. Kafka-like pattern for event replay.
- **MCP-over-MQTT** — service discovery via well-known topics, load balancing via shared subscriptions, authorization via topic-level ACLs. Relevant if skitter exposes tools via MCP.

---

# Multi-Agent Research Patterns

Source: "How we built our multi-agent research system" (Anthropic, June 2025).

Anthropic's Research feature uses an orchestrator-worker pattern: a lead agent plans, spawns parallel subagents for investigation, synthesizes results. Internal evals show multi-agent Claude Opus 4 + Sonnet 4 subagents outperformed single-agent Opus 4 by 90.2% on research tasks.

## Key Findings

- **Token usage explains 80% of performance variance** (BrowseComp eval). Number of tool calls and model choice explain the remaining 15%. Multi-agent architectures are fundamentally a way to scale token usage beyond single-context-window limits.
- **Multi-agent = ~15x token cost vs single chat.** Only viable when task value justifies the cost. Best fit: heavy parallelization, information exceeding single context windows, many complex tools.
- **Model upgrade > token budget.** Upgrading to Claude Sonnet 4 gave a larger performance gain than doubling the token budget on Sonnet 3.7.

## Patterns Relevant to Skitter

- **Teach the orchestrator how to delegate.** Each subagent needs: an objective, output format, tool/source guidance, and clear task boundaries to avoid duplicating work. Skitter's workflow YAML `description` field serves this purpose — make it specific.
- **Scale effort to query complexity.** Simple fact-finding: 1 agent, 3-10 tool calls. Complex research: 10+ subagents with clearly divided responsibilities. Skitter workflows should vary in fan-out based on task complexity.
- **Subagent output to filesystem to minimize "game of telephone."** Direct subagent outputs bypass the coordinator by writing to external systems and passing lightweight references. In skitter's model, chain results (retained MQTT messages) serve this role — workers publish results directly rather than routing through the supervisor.
- **Start wide, then narrow.** Search strategy should mirror expert research: short broad queries first, evaluate, then progressively narrow. Relevant for crafting workflow task descriptions.
- **Parallel tool calling transforms speed.** Lead agent spinning up 3-5 subagents in parallel + subagents using 3+ tools in parallel cut research time by up to 90%. Skitter's DAG fan-out already enables this at the workflow level.
