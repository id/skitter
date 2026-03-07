# Skitter Architecture

## Design Principles

1. **Stateless supervisor.** The supervisor makes no AI calls. It listens on wildcard topics (`request/{o}/{u}/+` and `event/{o}/{u}/+/+`), creates sessions with pre-materialized dispatch specs for every task, publishes the session as a retained MQTT message, and spawns all workers upfront. No dispatch loop, no join accumulation, no state tracking.

2. **Agents as A2A endpoints.** Clients address agents directly via `$a2a/v1/request/{org}/{unit}/{agent_id}`. The supervisor intercepts via wildcard subscription — invisible infrastructure.

3. **Self-coordinating workers.** Workers read their spec from the retained session message, wait for upstream chain results if they have `needs`, run the agent, and publish results. No supervisor dispatches tasks -- workers coordinate among themselves via retained MQTT messages.

4. **Chain-based routing.** Non-terminal workers publish retained chain results to suffixed event topics. Downstream workers with `needs` subscribe to upstream chain result topics and block until all inputs arrive. Terminal workers publish results directly to the caller.

5. **Immutable session spec.** The retained session message is written once by the supervisor and never modified. Per-task status is published to separate retained event topics (`event/{org}/{unit}/{agent_id}/task-status/{sid}/{tid}`). The dashboard merges per-task status over the session spec.

6. **MQTT v5 as the backbone.** The broker is the single source of truth. Retained messages = durable state, LWT = crash detection, pub/sub = decoupled fan-out.

7. **A2A-over-MQTT.** All topics follow the A2A draft v0.1 scheme with application-defined suffixes after the agent ID. Agents are discoverable via retained Agent Cards. Pre-built cards stored in `~/.skitter/cards/*.json`. Requests use v5 Response Topic + Correlation Data for reply routing.

8. **QA is a workflow concern.** The supervisor has no built-in QA logic. If you want fact-checking or review, add a reviewer task node to your workflow that depends on the work task.

9. **Multi-runtime workers.** Workers invoke `claude` or `codex` as CLI subprocesses, parsing JSONL stdout for streaming events. No SDK imports in the worker process.

## A2A Topic Scheme

```
$a2a/v1/
  discovery/{org}/{unit}/{agent_id}                              # Retained Agent/Workflow Cards
  request/{org}/{unit}/{agent_id}                                # Requests (clients address agents directly)
  request/{org}/{unit}/{agent_id}/cancel                         # Cancel signal for agent
  request/{org}/{unit}/supervisor/reload                         # Reload agents/workflows signal
  reply/{org}/{unit}/{agent_id}/{suffix}                         # Replies (Response Topic)
  event/{org}/{unit}/{agent_id}/{event_type}                     # Agent events (alive/done/dead)
  event/{org}/{unit}/supervisor/session/{session_id}             # Retained session spec (immutable)
  event/{org}/{unit}/{agent_id}/task-status/{sid}/{tid}          # Retained per-task status
  event/{org}/{unit}/{agent_id}/chain-result/{sid}/{tid}         # Retained chain results
  event/{org}/{unit}/{agent_id}/usage/{sid}/{tid}                # Usage tracking
```

Default `{org}` = `skitter`, `{unit}` = `default` (configurable via `SKITTER_A2A_ORG` / `SKITTER_A2A_UNIT`).

The supervisor subscribes to `request/{o}/{u}/+` (wildcard for all agent requests) and `event/{o}/{u}/+/+` (wildcard for all agent events). It also subscribes explicitly to `request/{o}/{u}/supervisor/reload` (since the suffixed topic doesn't match the single-level `+` wildcard).

## Chain-Based Execution

### Simple Chain (single agent)

```mermaid
sequenceDiagram
    participant C as Caller
    participant S as Supervisor
    participant B as MQTT Broker
    participant W as Worker

    C->>B: Request to agent's topic
    B->>S: Wildcard match (request/+)
    S->>S: Create session + dispatch specs
    S->>B: Retain session spec
    S->>W: Spawn process
    W->>B: Read retained session
    W->>W: Run agent (claude/codex CLI)
    W->>C: Stream items (QoS 0)
    W->>C: TaskStatusUpdate (terminal)
    W->>B: Retain per-task status (done)
```

### Linear Workflow (A -> B)

```mermaid
sequenceDiagram
    participant S as Supervisor
    participant B as MQTT Broker
    participant W1 as Worker A
    participant W2 as Worker B

    S->>B: Retain session (specs for A + B)
    S->>W1: Spawn A
    S->>W2: Spawn B
    W1->>B: Read retained session
    W2->>B: Read retained session
    W2->>B: Subscribe to chain result for A
    W1->>W1: Run agent
    W1->>B: Retain chain result (A)
    B->>W2: Deliver chain result (A)
    W2->>W2: Run agent (with A's result as context)
    W2->>Caller: TaskStatusUpdate (terminal)
    W2->>B: Retain per-task status (done)
```

### Fan-in Join (A + B -> Join)

```mermaid
sequenceDiagram
    participant S as Supervisor
    participant B as MQTT Broker
    participant WA as Worker A
    participant WB as Worker B
    participant WJ as Worker Join

    S->>B: Retain session (specs for A, B, Join)
    S->>WA: Spawn A
    S->>WB: Spawn B
    S->>WJ: Spawn Join
    WA->>B: Read session, run agent
    WB->>B: Read session, run agent
    WJ->>B: Read session, subscribe to A + B chain results
    WA->>B: Retain chain result (A)
    WB->>B: Retain chain result (B)
    B->>WJ: Deliver A + B results
    WJ->>WJ: Run agent (context = A + B results)
    WJ->>Caller: TaskStatusUpdate (terminal)
```

All workers are spawned immediately. Entry tasks (no `needs`) start work right away. Join tasks block on `wait_for_needs()`, subscribing to upstream chain result topics until all inputs arrive via MQTT retained messages.

### Direct-to-Caller Streaming

Workers stream `StreamItem` directly to the caller's Response Topic (QoS 0), bypassing the supervisor entirely.

## Supervisor

The supervisor (`skitter/supervisor.py`) is a long-lived MQTT subscriber that:

1. Subscribes to `$a2a/v1/request/{org}/{unit}/+` (wildcard for all agent requests)
2. Subscribes to `$a2a/v1/event/{org}/{unit}/+/+` (wildcard for all agent events)
3. Subscribes to `$a2a/v1/request/{org}/{unit}/supervisor/reload` (explicit)
4. On inbound request: extracts `agent_id` from the topic, creates a `Session` with `SessionTask` entries and pre-materialized `task_dispatches` dict
5. Publishes the session as a retained message on `event/{org}/{unit}/supervisor/session/{session_id}`
6. Spawns all workers (subprocess or Docker) -- every task gets a worker immediately
7. Listens for dead events (`event/+/+` ending in `/dead`) for crash detection and respawns dead workers

The supervisor holds no in-memory state about running sessions. It is restartable at any time.

## Worker Self-Coordination

Each worker (`skitter/worker.py`) runs as an independent process:

1. Connect to MQTT with LWT on `event/{org}/{unit}/{agent_id}/dead`
2. Read the retained session spec from `event/{org}/{unit}/supervisor/session/{session_id}`
3. Find own task by `task_id` in the session's `tasks` dict
4. Read dispatch spec from `session.task_dispatches[task_name]`
5. If task has `needs`: subscribe to upstream `event/{org}/{unit}/{upstream_agent}/chain-result/{sid}/{tid}` topics, block until all arrive
6. Build `AgentMessage` from spec + upstream context
7. Run agent as CLI subprocess (`claude` or `codex`), parse JSONL stdout
8. Publish result:
   - Non-terminal: retain chain result on `event/{org}/{unit}/{agent_id}/chain-result/{sid}/{tid}`
   - Terminal: publish `TaskStatusUpdate` to caller's Response Topic
9. Publish per-task status (retained) on `event/{org}/{unit}/{agent_id}/task-status/{sid}/{tid}`
10. Announce done, disconnect

## Workflow Templates

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
    description: "Cross-reference findings about '{topic}'."
    next: synthesize
    needs: [research_web, research_academic]
  - id: synthesize
    agent: writer
    description: "Combine all findings about '{topic}'."
    next: output
    needs: [fact_check]
```

`next` is auto-inferred from the reverse dependency graph if absent.

## Agent Definitions

```yaml
# ~/.skitter/agents/researcher.yaml
name: Research Specialist
description: Deep research with source citation
runtime: claude    # "claude" or "codex"
workspace: ""      # custom cwd (default: ~/.skitter/workspaces/{task_id})
```

Agent identity (personality, model, tools, memory) is owned by native CLI sub-agent systems (`~/.claude/agents/*.md` for Claude, `~/.codex/agents/*.toml` for Codex), not skitter. The YAML stub contains only orchestration metadata.

## CLI Runtimes

Workers invoke AI agents as CLI subprocesses:

**Claude** (`runtime: claude`):
- Spawns `claude -p "{description}" --output-format stream-json --verbose --max-turns {n} --dangerously-skip-permissions`
- Appends upstream context via `--append-system-prompt`
- Parses JSONL: `assistant` events for text/tool_use, `result` events for usage/cost

**Codex** (`runtime: codex`):
- Spawns `codex exec --json --full-auto --skip-git-repo-check "{prompt}"` with optional `--model`
- Auth via `OPENAI_API_KEY` env var
- Parses JSONL: `item.completed` for agent messages, `turn.completed` for usage

## Configuration Manager

The default `skitter` agent can create/modify agent and workflow YAML definitions at runtime:
- Works in `~/.skitter/` directory
- After writing files, runs `python -m skitter.reload` to notify the supervisor
- Supervisor re-reads all YAML files, reloads cards, and re-publishes discovery messages

## Recovery

**Supervisor crash:** The supervisor is stateless. On restart it re-publishes discovery cards and resumes listening. It does not need to recover sessions -- all session state lives in retained MQTT messages, and workers are self-coordinating.

**Worker crash:** LWT fires on `event/{org}/{unit}/{agent_id}/dead`. The supervisor receives the dead event and respawns the worker. The new worker reads the same retained session spec, waits for any upstream results (which may already be retained on the broker), and re-runs the task from scratch.

**Broker restart:** Sessions and chain results are lost (retained messages are in-memory by default). Planned: persist sessions to `~/.skitter/` for durability beyond broker restarts.

## Worker Execution Modes

Workers can run as local subprocesses (default) or Docker containers (`SKITTER_SPAWN_MODE=docker`). Docker mode passes both `ANTHROPIC_API_KEY` and `OPENAI_API_KEY` as env vars and connects to the `skitter` Docker network.

## Cancel via A2A

Cancel signals are published as JSON-RPC to `$a2a/v1/request/{org}/{unit}/{agent_id}/cancel`. Workers run a separate cancel listener that watches for cancel messages matching their `task_id` and terminates the agent subprocess.

## EMQX Rule Engine

Auxiliary concerns (logging, webhooks, dead-letter routing, metrics) are handled by EMQX rules rather than application code. See `docs/emqx-rules.md`.
