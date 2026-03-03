# Skitter

~1,500 lines of Python. MQTT-based personal AI assistant. Stateless coordinator manages task DAGs via an MQTT broker; independent workers handle AI reasoning via `claude-agent-sdk`.

Key files: `skitter/coordinator.py`, `skitter/worker.py`, `skitter/types.py`, `skitter/config.py`, `skitter/cli.py`, `SOUL.md`, `docs/architecture.md`.

## Architecture Essentials

- **Zero-LLM coordinator** — never calls an LLM. It is a pure DAG executor: build graph, dispatch tasks, collect results, advance.
- **A2A-over-MQTT** — all topics follow the A2A draft v0.1 scheme. MQTT v5 properties (Response Topic, Correlation Data) enable request/reply. Agents are discoverable via retained Agent Cards.
- **MQTT as backbone** — retained messages = durable state, LWT = crash detection, pub/sub = decoupled fan-out.
- **Stateless recovery** — coordinator rebuilds from retained MQTT messages on restart.
- **DAG execution** — pipeline templates define task graphs, coordinator spawns workers for ready tasks in parallel, advances graph as results arrive.
- **QA is a pipeline concern** — coordinator has no built-in QA logic. Add reviewer/fact-checker nodes to your pipeline YAML with dependencies on work tasks.
- **Predefined agents** — YAML definitions in `~/.skitter/agents/`. Pipeline tasks reference agents by ID; coordinator resolves defaults.
- **Pipeline templates** — YAML DAGs in `~/.skitter/pipelines/`. Every request must include `pipeline_id`. Variables interpolated with `SafeFormatter`.
- **Workers are subprocesses or Docker containers** — controlled by `SKITTER_WORKER_MODE` env var. Subprocess mode (default) uses `subprocess.Popen`; Docker mode runs containers on the `skitter` network.

## Key Modules

- `skitter/config.py` — `~/.skitter/` directory management, YAML loading, `AgentDef`/`PipelineDef`/`PipelineTask` dataclasses, `SafeFormatter`, `write_examples()` for `skitter init`.
- `skitter/agents_cli.py` — `skitter agents list/show` subcommands.
- `skitter/pipeline_cli.py` — `skitter pipeline list/show/run` subcommands.

## Current Limitations

- No auth/TLS (localhost-only PoC)
- Workers run with `bypassPermissions`
- Concurrent same-`chat_id` messages overwrite
- Worker errors passed as normal results
- No conversation memory across sessions
- No dependency cycle detection

## Roadmap

- Telegram bridge
- Per-chat conversation history
- Circular DAG detection
- Worker timeouts and exponential backoff

---

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
- Stability concerns: OOM after upgrades, memory leaks over time (#12685), perf regression v1.80→v1.81 (#19921).
- Multiple daily releases, breaking changes without migration guides, 5500-line main handler.

**Critical distinction:** litellm is a completion API, not an agent runtime. `claude-agent-sdk` provides the agent loop (multi-turn tool calling, file ops, command execution). They're not interchangeable. Realistic integration: use litellm for non-agentic simple tasks (single-turn, no tools) while keeping `claude-agent-sdk` for agentic workers.

**Alternative:** Thin adapter over native `anthropic` + `openai` + `google-genai` SDKs (~100-200 lines) avoids the dependency weight and import-time penalty but means maintaining your own quirk handling.

---

# Key Architectural Comparisons

| | Skitter | OpenClaw | NanoClaw | Nanobot | ZeroClaw | NullClaw |
|---|---|---|---|---|---|---|
| Language | Python | TypeScript | TypeScript | Python | Rust | Zig |
| LOC | ~1,000 | ~500,000 | ~5,000 | ~4,000 | N/A | ~110 files |
| Coordination | MQTT broker | WebSocket Gateway | Single process | Single process | N/A | Single binary |
| Agent runtime | claude-agent-sdk | pi-mono embedded | Claude Agent SDK | Custom async | Trait-based | vtable-based |
| Multi-provider | No (Claude only) | Yes (via pi-ai) | No (Claude only) | Yes (22+) | Yes (pluggable) | Yes (22+) |
| Multi-channel | Any MQTT client | 10+ built-in | 5 built-in | 10+ built-in | Pluggable | 18 built-in |
| DAG orchestration | Yes | No | No | No | No | No |
| Crash recovery | Yes (MQTT retained + LWT) | Session persistence | Container restart | No | No | No |
| Parallel execution | Yes (DAG fan-out) | No (single agent) | Agent swarms | No | No | No |
| QA feedback loop | Yes | No | No | No | No | No |
| Memory | Planned | SQLite + vector | Per-group CLAUDE.md | Custom | N/A | SQLite FTS5 + vector |
| Edge deployment | No | No | No | No | Yes (<5MB RAM) | Yes (678KB binary) |

**Skitter's unique position:** Only project with broker-mediated DAG orchestration and crash recovery. The MQTT architecture is genuinely differentiated — no other project in this space uses a message broker as the coordination backbone. The trade-off is that skitter is currently Claude-only and has no channel integrations (by design — bridges are ~100-line scripts).
