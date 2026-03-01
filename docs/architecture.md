# Skitter Architecture

## Design Principles

1. **Zero-LLM coordinator.** The coordinator makes no AI calls. Planning and synthesis are worker tasks in the DAG. `claude_agent_sdk` is only imported by the worker process.

2. **Stateless coordinator.** All state is derived from retained MQTT messages. If the coordinator crashes, it recovers jobs from the broker on restart and respawns interrupted workers.

3. **MQTT as the backbone.** The broker is the single source of truth. Retained messages act as durable storage for job specs and task assignments. LWT (Last Will and Testament) handles worker crash detection.

## Request Lifecycle

```mermaid
stateDiagram-v2
    [*] --> PlannerSpawned
    PlannerSpawned --> DirectResponse
    PlannerSpawned --> DAGBuilt
    DirectResponse --> [*]
    DAGBuilt --> WorkersRunning
    WorkersRunning --> WorkersRunning
    WorkersRunning --> QAReview
    QAReview --> WorkersRunning: fail + retries left
    QAReview --> WorkersRunning: pass
    WorkersRunning --> Synthesizing
    Synthesizing --> [*]
```

**States explained:**

- **PlannerSpawned** — User publishes to inbound topic, coordinator spawns planner worker
- **DirectResponse** — Planner returns a direct answer, coordinator forwards to outbound
- **DAGBuilt** — Planner returns a task graph, coordinator builds DAG with synthesize node
- **WorkersRunning** — Workers execute in parallel; as results arrive, newly unblocked tasks spawn
- **QAReview** — If a task has QA criteria, an ephemeral QA agent validates the output. On failure, the task retries with feedback
- **Synthesizing** — All workers done, synthesize task combines results into final response

## Coordinator Message Loop

The coordinator subscribes to three topic patterns and reacts to each.

```mermaid
flowchart TD
    subgraph Message Loop
        MSG[Incoming MQTT message]
        MSG -->|inbound| INBOUND
        MSG -->|results| RESULT
        MSG -->|worker status| LWT
    end

    subgraph Inbound Handler
        INBOUND[User message] --> PLAN[Create bootstrap job with planner task]
        PLAN --> PUB_JOB[Publish JobSpec retained]
        PUB_JOB --> PUB_TASK[Publish TaskMessage retained]
        PUB_TASK --> SPAWN_P[Spawn planner worker]
    end

    subgraph Result Handler
        RESULT[Worker result] --> IS_QA{QA result?}
        IS_QA -->|yes| QA_PARSE[Parse pass/fail JSON]
        QA_PARSE -->|pass| QA_ADVANCE[Clean up QA task, advance graph]
        QA_PARSE -->|fail + retries left| QA_RETRY[Reset task to pending, append feedback]
        QA_PARSE -->|fail + exhausted| QA_FORCE[Log warning, advance anyway]
        QA_RETRY --> SPAWN_READY
        QA_FORCE --> SPAWN_READY
        QA_ADVANCE --> SPAWN_READY
        IS_QA -->|no| IS_PLANNER{Planner task?}
        IS_PLANNER -->|respond| DIRECT[Publish to outbound and clear job]
        IS_PLANNER -->|delegate| BUILD[Build full DAG and add synthesize node]
        IS_PLANNER -->|other task| HAS_QA{Has qa field?}
        BUILD --> SPAWN_READY[Spawn workers for ready tasks]
        HAS_QA -->|yes| SPAWN_QA[Spawn ephemeral QA agent]
        HAS_QA -->|no| IS_SYNTH{Synthesize task?}
        IS_SYNTH -->|yes| FINAL[Publish to outbound and clear job]
        IS_SYNTH -->|no| ADVANCE[Find newly unblocked tasks]
        ADVANCE --> SPAWN_READY
    end

    subgraph LWT Handler
        LWT[Worker dead] --> RESPAWN[Respawn worker from retained task]
    end
```

## DAG Execution

A delegated request produces a task graph. Tasks with no dependencies run in parallel. The synthesize node is auto-added as a leaf that depends on all other tasks.

```mermaid
flowchart LR
    subgraph Parallel fan-out
        A[research - sonnet] --> S[synthesize - haiku]
        B[analyze_code - sonnet] --> S
        C[read_docs - haiku] --> S
    end
```

Tasks can also form chains when the planner specifies dependencies.

```mermaid
flowchart LR
    subgraph Sequential chain
        A[gather_data - haiku] --> B[analyze - sonnet]
        B --> S[synthesize - haiku]
    end
```

When a task has a `qa` field, an ephemeral QA agent gates its completion before downstream tasks can proceed.

```mermaid
flowchart LR
    subgraph QA-gated task
        A[research - sonnet] -.->|qa| QA[qa:research]
        QA -.->|pass| S[synthesize - haiku]
        QA -.->|fail| A
    end
```

The coordinator advances the graph with pure logic, with no LLM calls:

1. Mark completed task as "done", store its result
2. If the task has a `qa` field, spawn an ephemeral QA agent instead of advancing (see QA below)
3. Find tasks where all dependencies are "done" and status is "pending"
4. For each: build context from upstream results, publish retained task, spawn worker
5. Re-publish updated job spec

## QA Feedback Loop

The planner can attach a `"qa"` field to any task with criteria for validating the output. When a task with QA completes:

```mermaid
flowchart TD
    DONE[Worker completes task] --> HAS_QA{Has qa field?}
    HAS_QA -->|no| ADVANCE[Advance graph normally]
    HAS_QA -->|yes| SPAWN_QA[Spawn ephemeral QA agent]
    SPAWN_QA --> QA_EVAL[QA evaluates result against criteria]
    QA_EVAL --> PASS{Pass?}
    PASS -->|yes| ADVANCE
    PASS -->|no| RETRIES{Retries left?}
    RETRIES -->|yes| RESET[Reset task to pending, append feedback]
    RESET --> RESPAWN[Worker re-spawned with feedback in description]
    RESPAWN --> DONE
    RETRIES -->|no| FORCE[Log warning, advance with last result]
```

**Details:**

- QA tasks use logical ID `qa:{original_id}` and are ephemeral and don't appear in the synthesize `depends_on` list
- The QA agent gets: the original task description, the worker's output, and the QA criteria. It has no tools (`max_turns=0`) and must return `{"pass":true}` or `{"pass":false,"feedback":"..."}`
- On failure, the coordinator increments `retries`, appends feedback to the task description, resets the task to `"pending"`, and removes the old result. The normal `get_ready_tasks()` logic picks it up and re-spawns the worker
- Default `max_retries` is 2. The planner can override this per task
- If retries are exhausted, the coordinator keeps the last result and advances the graph anyway

### Planner Schema

```json
{
  "logical_id": "research",
  "agent": "researcher",
  "model": "sonnet",
  "description": "Research the topic thoroughly",
  "soul": "You are a research specialist. Cite sources.",
  "skills": "Search broadly, then write a structured summary.",
  "max_turns": 15,
  "qa": "Verify all claims have citations and sources are reputable",
  "max_retries": 3,
  "early_qa_interval": 10,
  "depends_on": []
}
```

Optional fields: `soul`, `skills`, `max_turns` (default 10), `qa`, `max_retries` (default 2), `early_qa_interval` (default 0, disabled). Tasks without `qa` behave exactly as before.

## Worker Lifecycle

```mermaid
sequenceDiagram
    participant C as Coordinator
    participant B as MQTT Broker
    participant W as Worker

    C->>B: Publish retained TaskMessage
    C->>W: Spawn process
    W->>B: Connect with LWT
    W->>B: Publish alive status
    W->>B: Subscribe to task topic
    B->>W: Deliver retained TaskMessage
    W->>W: Run claude_agent_sdk query
    W->>B: Publish result
    W->>B: Clear retained task
    W->>B: Publish done status
    W->>W: Exit

    Note over C,B: If worker crashes before completing
    B->>C: LWT fires with dead status
    C->>W: Respawn from retained task
```

## Recovery

On startup, the coordinator recovers from retained MQTT messages.

```mermaid
flowchart TD
    START[Coordinator starts] --> SUB[Subscribe to jobs topic]
    SUB --> DRAIN[Drain retained messages]
    DRAIN --> REBUILD[Rebuild in-memory jobs and task maps]
    REBUILD --> RESPAWN[Respawn workers for running tasks]
    RESPAWN --> UNSUB[Unsubscribe from jobs topic]
    UNSUB --> READY[Enter normal message loop]
```

If a worker completed while the coordinator was down and cleared its retained task, the respawned worker finds no task and exits harmlessly.

## Model Selection

The planner picks a model for each delegated task from a configurable list.

### Configuration

```bash
# Available models (pipe-separated name:description pairs)
SKITTER_MODELS="haiku:Fast and cheap|sonnet:Balanced|opus:Most capable"

# Model for the planner itself
SKITTER_PLANNER_MODEL=opus

# Models for QA and synthesizer
SKITTER_QA_MODEL=sonnet
SKITTER_SYNTH_MODEL=sonnet
```

The model list and descriptions are injected into the planner's system prompt. The planner includes a `"model"` field per task. The coordinator validates the choice against the known list (falls back to the first model if unknown) and passes it through to the worker.

### Typical Assignments

| Task Type | Typical Model | Why |
|-----------|--------------|-----|
| Planner | opus | Routing decisions shape everything downstream — worth the best model |
| Simple file read / summarize | haiku | Fast, cheap, sufficient for extraction |
| Code analysis / complex reasoning | sonnet or opus | Needs stronger capabilities |
| QA | sonnet | Evaluating output quality, moderate reasoning |
| Synthesize | sonnet | Combining and rewriting results coherently |

## Topic Scheme

```
skitter/
├── inbound/{chat_id}                          # User → coordinator
├── outbound/{chat_id}                         # Coordinator → user (retained)
├── jobs/{chat_id}                             # Retained job spec (DAG + results)
├── tasks/{agent}/{chat_id}/{task_id}          # Retained task for worker
├── results/{chat_id}/{task_id}                # Worker result → coordinator
├── workers/{chat_id}/{task_id}/status         # Worker liveness (LWT)
├── usage/{chat_id}/{task_id}                  # Token usage and cost per task
├── stream/{chat_id}/{task_id}                 # Live text/tool chunks from worker
├── stream/{chat_id}/{task_id}/snapshot        # Periodic progress snapshot (retained)
├── feedback/{chat_id}/{task_id}               # QA feedback injected mid-run (retained)
└── cancel/{chat_id}/{task_id}                 # Cancel signal for running worker (retained)
```

The coordinator subscribes at startup to: `skitter/inbound/+`, `skitter/results/+/+`, `skitter/workers/+/+/status`, `skitter/stream/+/+/snapshot`.
