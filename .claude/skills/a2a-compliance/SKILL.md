---
name: a2a-compliance
description: Validate A2A and A2A-over-MQTT protocol compliance. Use after changing protocol-facing code in skitter/a2a.py, skitter/coordinator.py, skitter/agent_runner.py, or dashboard.html.
allowed-tools: Read, Grep, Glob, Bash(uv:*, uvx:*)
---

# A2A Protocol Compliance Check

Validate that skitter's protocol layer conforms to the A2A v1.0.0 spec and the A2A-over-MQTT v0.1 binding.

## Authoritative Sources

- **A2A proto** (data structures): https://github.com/a2aproject/A2A/blob/main/specification/a2a.proto
- **A2A-over-MQTT spec** (MQTT transport binding): https://github.com/emqx/mqtt-for-ai/blob/main/a2a-over-mqtt/specification/0.1/basic/mqtt_transport.md

Fetch both specs via `gh api` or `WebFetch` before checking. Do not rely on memory; the specs evolve.

## Checks to Perform

### 1. Run the unit tests

```
uv run python -m pytest tests/test_unit.py -q
```

All tests must pass. The test suite includes spec-compliance tests (search for classes `TestStatusEvent`, `TestSpecDefaults`, `TestCoordinatorDispatchCompliance`, `TestAgentRunnerCompliance`).

### 2. Verify `TaskStatusUpdateEvent` structure (`skitter/a2a.py`)

Compare `make_status_event` output against the proto's `TaskStatusUpdateEvent`:

| Proto field | JSON key | Required | Check |
|---|---|---|---|
| `task_id` | `taskId` | YES | Always present |
| `context_id` | `contextId` | YES | Always present (even if empty) |
| `status` | `status` | YES | Contains `state` (required) and optional `message` (Message object) |
| `metadata` | `metadata` | no | Event-level, NOT inside `status` |

`status.message` MUST be a Message object (`{role, parts}`) when present, not a plain string.

There MUST NOT be an `artifact` field on `TaskStatusUpdateEvent`.

### 3. Verify `TaskArtifactUpdateEvent` structure (`skitter/a2a.py`)

Compare `make_artifact_event` output against the proto's `TaskArtifactUpdateEvent`:

| Proto field | JSON key | Required | Check |
|---|---|---|---|
| `task_id` | `taskId` | YES | Always present |
| `context_id` | `contextId` | YES | Always present |
| `artifact` | `artifact` | YES | Contains `artifactId` and `parts` |
| `last_chunk` | `lastChunk` | no | Present when set |
| `metadata` | `metadata` | no | Event-level |

### 4. Verify `classify_reply` handles all A2A states

Check that `classify_reply` in `skitter/a2a.py` handles:

- `submitted`, `working` (active states)
- `completed` (terminal)
- `failed`, `canceled`, `rejected` (terminal)
- `input-required`, `auth-required` (interrupted, stream-final but NOT task-terminal)
- `TaskArtifactUpdateEvent` (separate event type)

### 5. Verify A2A-over-MQTT transport compliance

Check these MUST requirements against the code:

**Responder behavior** (`skitter/agent_runner.py`, `skitter/coordinator.py`):
- Echoes `Correlation Data` on all replies (`make_properties(correlation_data=...)`)
- Publishes replies to the provided `Response Topic`
- Rejects requests with missing `Response Topic` or `Correlation Data` (`transport_protocol_error`, `-32005`)
- Rejects requests with missing `Task.id` (`transport_protocol_error`, `-32005`)
- Uses requester-provided `Task.id` for dedup; returns existing state on duplicate
- Echoes `context_id` in all responses

**Requester behavior** (`skitter/a2a.py` `send_and_wait`):
- Sets `Response Topic` and `Correlation Data` on requests
- Generates new `Correlation Data` per retry attempt, keeps same `Task.id`
- Default timeouts match spec: `reply_first_timeout=15s`, `stream_idle_timeout=30s`, `max_attempts=3`
- Backoff: exponential `1s, 2s, 4s` with `+/-20%` jitter

**Error codes** (`skitter/a2a.py`):
- `-32003` with `data.a2a_error = "request_expired"`
- `-32004` with `data.a2a_error = "responder_unavailable"`
- `-32005` with `data.a2a_error = "transport_protocol_error"`

### 6. Check consumers handle both event types

Verify that all reply consumers handle `TaskArtifactUpdateEvent` separately from `TaskStatusUpdateEvent`:

- `skitter/cli.py` (interactive chat)
- `skitter/__main__.py` (one-shot `run` command)
- `skitter/coordinator.py` (`handle_reply`)
- `dashboard.html` (JavaScript event parser)
- `tests/conftest.py` (`send_and_collect`)

### 7. Lint and format

```
uvx ruff format skitter/ tests/
uvx ruff check skitter/ tests/
```

## Output

Report:
- **PASS**: checks that conform to the spec
- **FAIL**: violations with file, line number, and what the spec requires
- **WARN**: areas where the spec says SHOULD but we don't (acceptable gaps)
