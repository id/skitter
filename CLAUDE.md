# Skitter

~2,600 lines of Python. MQTT-based personal AI assistant. A stateless gateway creates sessions and spawns all workers upfront; self-coordinating workers read their session spec from retained MQTT, wait for upstream results, invoke `claude` or `codex` CLI as subprocesses, and publish results.

Key files: `skitter/gateway.py`, `skitter/worker.py`, `skitter/spawn.py`, `skitter/storage.py`, `skitter/respawn.py`, `skitter/types.py`, `skitter/config.py`, `skitter/cli.py`, `skitter/mqtt.py`, `docs/architecture.md`.

## Planning and Implementation Process

For non-trivial requests (new features, architectural changes, multi-file refactors), follow this process:

### 1. Planning Phase
- **Use a team of agents** for planning — delegate research and analysis to subagents.
- **Evaluate fit** — research whether the request aligns with skitter's intended goals (minimal MQTT-based gateway, self-coordinating workers, small codebase). Push back on the user if a request conflicts with core architectural principles or adds unnecessary complexity.
- **Persist the plan** — write a markdown file under `docs/` with timestamp in the filename: `docs/YYYY-mm-DD-HH-MM-SS-<slug>.md`. Include: problem statement, proposed approach, affected files, risks, and open questions.

### 2. Implementation Phase
- **Coding persona** — write code as a professional senior Python developer. Prefer idiomatic, neat Python without boilerplate. No unnecessary abstractions or over-engineering.
- **Tests** — cover new and changed functionality with clever, focused tests. Don't add tests for trivial getters or obvious behavior; test the interesting edge cases and integration points.
- **No backward compatibility** — rewrite and drop old code freely. Don't add shims, re-exports, or deprecation warnings. If something is replaced, delete the old version.

### 3. Quality Phase
- **Lint and format** — always run `uvx ruff format` and `uvx ruff check` on changed files. Fix all issues.
- **Unit tests** — run `uv run python -m pytest tests/test_unit.py -q`.
- **Live tests** — run `tests/test_live_claude.py` and `tests/test_live_codex.py` for end-to-end verification (standalone agent + workflow). Use an MQTT spy to confirm message flow.
- **Dashboard** — verify `dashboard.html` still works with any changes to session state, discovery, or MQTT topics. Rewrite dashboard sections if the data model changed.

### 4. Review Phase
- **Correctness review** — review as a staff-level Python developer. Look for: correctness of async/MQTT interactions, edge cases in worker self-coordination and join waiting, state consistency across crash/recovery.
- **Simplification review** — separate pass focused on removing unnecessary logic and finding opportunities to simplify. Look for: dead code paths, redundant checks, over-abstracted helpers that are called once, conditionals that can't trigger, code that defends against impossible states, and any logic that exists "just in case." If two code paths do nearly the same thing, merge them. If a function wraps a single call, inline it. Prefer deleting code over explaining why it's needed.

## Architecture Essentials

- **Stateless gateway** — never calls an LLM. Creates sessions, pre-materializes dispatch specs for every task, publishes the session as a retained MQTT message, then spawns all workers upfront. Implemented in `skitter/gateway.py`.
- **Self-coordinating workers** — each worker reads its session spec from retained MQTT, waits for upstream results (join coordination via subscribing to chain result topics), runs the agent CLI, and publishes results. No central coordinator needed after spawn.
- **Immutable sessions** — the gateway publishes the session once; workers never mutate it. Per-task status is published to dedicated retained topics (`state/task/{session_id}/{task_id}`).
- **Chain-based routing** — workers publish retained chain results for non-terminal tasks. Join workers subscribe to upstream chain result topics and block until all inputs arrive.
- **A2A-over-MQTT** — all topics follow the A2A draft v0.1 scheme. Event topics: `event/{org}/{unit}/{agent_id}/{event_type}`. Task IDs in payload, not topic. Agents and workflows discoverable via retained discovery messages.
- **MQTT as backbone** — retained messages = durable state, LWT = crash detection, pub/sub = decoupled fan-out.
- **Crash recovery** — workers set MQTT LWT (Last Will and Testament). Gateway listens for dead events and respawns crashed workers. Retained session and chain results persist across crashes.
- **QA is a workflow concern** — gateway has no built-in QA logic. Add reviewer/fact-checker nodes to your workflow YAML with dependencies on work tasks.
- **Predefined agents** — YAML definitions in `~/.skitter/agents/`. Workflow tasks reference agents by ID; gateway resolves defaults. Supports `runtime: claude|codex` and custom `workspace`.
- **Default agent** — the `skitter` agent is the default. CLI queries without `/agent` or `/workflow` prefix route to it. It serves as both a general-purpose assistant and a configuration manager for agents/workflows.
- **Workflow templates** — YAML chains in `~/.skitter/workflows/`. Tasks have `id`, `next`, `needs` fields. Variables interpolated with `SafeFormatter`. Workflows are discoverable via retained MQTT discovery messages.
- **Multi-runtime workers** — `claude` (Claude CLI, default) or `codex` (OpenAI Codex CLI, authenticated via `codex login` or `OPENAI_API_KEY`). Both invoked as subprocesses with JSONL stdout parsing.
- **Workers are subprocesses or Docker containers** — controlled by `SKITTER_SPAWN_MODE` env var. Docker mode passes both `ANTHROPIC_API_KEY` and `OPENAI_API_KEY`.
- **Completed sessions persist** — sessions remain as retained MQTT messages after all tasks finish, with the terminal task's result published to the caller and to the per-task status topic. The dashboard renders results as markdown with copy/download options.

## Key Modules

- `skitter/gateway.py` — Stateless gateway: creates sessions, pre-materializes dispatch specs, publishes retained session, spawns all workers, listens for dead events to trigger respawn. Publishes agent/workflow discovery cards.
- `skitter/worker.py` — Self-coordinating worker: reads retained session from MQTT, waits for upstream chain results (join coordination), invokes `claude` or `codex` CLI as subprocess, parses JSONL stdout, publishes chain results or terminal results. Handles cancellation via a separate MQTT control connection.
- `skitter/spawn.py` — Worker spawn backends: subprocess (default) or Docker container, controlled by `SKITTER_SPAWN_MODE`.
- `skitter/storage.py` — Config loading backends: filesystem (default), delegates to `config.py`. Abstraction point for future R2/cloud storage.
- `skitter/respawn.py` — Handles LWT dead events by respawning crashed workers.
- `skitter/mqtt.py` — MQTT connection settings, topic builders, and MQTTv5 property helpers.
- `skitter/config.py` — `~/.skitter/` directory management, YAML loading, `AgentDef`/`WorkflowDef`/`WorkflowTask` dataclasses, `SafeFormatter`, `write_examples()` for `skitter init`.
- `skitter/agents_cli.py` — `skitter agents list/show/run` subcommands.
- `skitter/workflow_cli.py` — `skitter workflow list/show/run` subcommands.
- `skitter/__main__.py` — CLI dispatch. `skitter run "prompt"` and `skitter "prompt"` route to the default `skitter` agent.
- `skitter/reload.py` — Publishes reload signal to gateway via MQTT.
- `dashboard.html` — Single-file MQTT-connected dashboard. Agents/workflows clickable in sidebar; compose view in main area; session results rendered as markdown.

## Current Limitations

- No auth/TLS (localhost-only PoC)
- Workers run with `dangerouslySkipPermissions`
- Worker errors passed as normal results
- No conversation memory across sessions
- Sessions not persisted to disk (only MQTT retained messages)

## Roadmap

- Telegram bridge
- Per-chat conversation history
- Worker timeouts and exponential backoff
- Persist sessions to `~/.skitter/` for durability beyond MQTT broker restarts

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
