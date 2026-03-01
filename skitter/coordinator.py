import asyncio
import json
import logging
import subprocess
import sys
import uuid
from pathlib import Path

import os

import aiomqtt

from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    TOPIC_CANCEL,
    TOPIC_FEEDBACK,
    TOPIC_JOBS,
    TOPIC_OUTBOUND,
    TOPIC_RESULTS,
    TOPIC_STREAM_SNAPSHOT,
    TOPIC_TASKS,
)
from skitter.types import (
    CancelSignal,
    FeedbackSignal,
    InboundMessage,
    JobSpec,
    JobTask,
    OutboundMessage,
    StreamSnapshot,
    TaskMessage,
    TaskResultMessage,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.coordinator")

SOUL_PATH = Path("SOUL.md")

DEFAULT_MODELS = "haiku:Fast and cheap, good for simple tasks and summaries|sonnet:Balanced, good for research and analysis|opus:Most capable, use for complex reasoning and coding"
PLANNER_MODEL = os.environ.get("SKITTER_PLANNER_MODEL", "opus")
QA_MODEL = os.environ.get("SKITTER_QA_MODEL", "sonnet")
SYNTH_MODEL = os.environ.get("SKITTER_SYNTH_MODEL", "sonnet")
EARLY_QA_MODEL = "haiku"

EARLY_QA_SYSTEM = "You are a lightweight QA monitor. Evaluate worker progress. Return JSON only."
EARLY_QA_SKILLS = 'Output ONLY {"pass":true} or {"pass":false,"feedback":"one sentence"}'
EARLY_QA_TEMPLATE = """\
Task: {description}
QA criteria: {qa}

Worker status after {elapsed_s:.0f}s, {tool_calls} tool calls ({errors} errors):
Tools:
{tool_log}

Recent output (last 1KB):
{recent_text}

Is this worker on track? Return ONLY: {{"pass":true}} or {{"pass":false,"feedback":"one sentence"}}
"""


def load_models() -> dict[str, str]:
    """Load available models from SKITTER_MODELS env var.

    Format: "name:description|name:description|..."
    """
    raw = os.environ.get("SKITTER_MODELS", DEFAULT_MODELS)
    models = {}
    for entry in raw.split("|"):
        entry = entry.strip()
        if ":" in entry:
            name, desc = entry.split(":", 1)
            models[name.strip()] = desc.strip()
    return models


def models_prompt(models: dict[str, str]) -> str:
    lines = ['Available models (pick one per task via the "model" field):']
    for name, desc in models.items():
        lines.append(f"- {name}: {desc}")
    return "\n".join(lines)


PLANNING_INSTRUCTIONS = """\

## Planning Instructions

You are a routing coordinator. Analyze the user's message and return a JSON routing decision.

CRITICAL: Do NOT use any tools. Do NOT read files, run commands, or search. Just output JSON immediately.

Two possible JSON formats:

1. Direct response (for simple questions, greetings, chitchat):
{"action":"respond","text":"your response here"}

2. Delegate to sub-agents (for complex, multi-part, or research-heavy tasks):
{"action":"delegate","tasks":[{"logical_id":"unique_name","agent":"agent_type","model":"model_name","description":"specific task description","soul":"personality and approach for this worker","skills":"tool guidance and constraints for this worker","depends_on":[]}]}

Guidelines:
- Each task needs a unique "logical_id" (e.g. "research", "analyze_code", "review_docs")
- "agent" is the worker type (e.g. "researcher", "coder", "writer", "analyst")
- "model" picks which LLM to use for this task (see available models below)
- "depends_on" is a list of logical_ids this task needs results from before it can start
- Tasks with no depends_on run in parallel immediately
- You can create sequential chains: task B depends_on ["A"], task C depends_on ["A","B"]
- Do NOT include a final synthesis/summary task — one will be added automatically
- "soul" = worker persona (e.g. "You are a research specialist. Cite sources.")
- "skills" = tool guidance (e.g. "Read all .md files in the current directory. Summarize findings.")
- "max_turns" (optional, integer) = tool-use turn budget for this task. Default 10. The worker is told its budget and must write a summary before turns run out. Set higher (15-25) for deep research tasks, lower (3-5) for focused tasks.
- "qa" (optional) = quality criteria for reviewing this task's output (e.g. "Verify sources are cited and claims are factual")
- "early_qa_interval" (optional, integer) = check worker progress every N streaming chunks (e.g. 10). Use for expensive/long-running tasks on opus to catch bad trajectories early. Default 0 (disabled).
- Prefer direct response for simple questions — don't over-delegate
- Delegate to 2-5 sub-agents when parallelizable or sequential work exists
- Output ONLY the JSON object, nothing else
"""


def extract_json(text: str) -> dict:
    """Extract the first JSON object from LLM output, ignoring code fences and trailing text."""
    cleaned = text.strip()
    # Strip markdown code fences
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        lines = [line for line in lines[1:] if line.strip() != "```"]
        cleaned = "\n".join(lines).strip()
    # Find the first { and match its closing }
    start = cleaned.find("{")
    if start == -1:
        raise ValueError("No JSON object found in response")
    depth = 0
    in_string = False
    escape = False
    for i, ch in enumerate(cleaned[start:], start):
        if escape:
            escape = False
            continue
        if ch == "\\":
            escape = True
            continue
        if ch == '"' and not escape:
            in_string = not in_string
            continue
        if in_string:
            continue
        if ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                return json.loads(cleaned[start : i + 1])
    raise ValueError("Unterminated JSON object in response")


def load_soul() -> str:
    if SOUL_PATH.exists():
        return SOUL_PATH.read_text().strip()
    return ""


def get_ready_tasks(job: JobSpec) -> list[JobTask]:
    ready = []
    for task in job.tasks.values():
        if task.status != "pending":
            continue
        if all(job.tasks[dep].status == "done" for dep in task.depends_on):
            ready.append(task)
    return ready


def is_complete(job: JobSpec) -> bool:
    return all(t.status == "done" for t in job.tasks.values())


def build_context(job: JobSpec, task: JobTask) -> str:
    if not task.depends_on:
        return ""
    parts = []
    for dep_id in task.depends_on:
        if dep_id in job.results:
            parts.append(f"## Result from '{dep_id}':\n{job.results[dep_id]}")
    return "\n\n".join(parts)


def build_job_from_plan(
    chat_id: str, original_text: str, plan_result: dict, models: dict[str, str]
) -> JobSpec:
    job = JobSpec(chat_id=chat_id, original_text=original_text)
    plan_tasks = plan_result.get("tasks", [])
    default_model = list(models.keys())[0] if models else ""

    all_logical_ids = []
    for t in plan_tasks:
        logical_id = t["logical_id"]
        task_id = uuid.uuid4().hex[:12]
        model = t.get("model", default_model)
        if models and model not in models:
            log.warning(
                "[coordinator] Unknown model '%s' for task '%s', falling back to '%s'",
                model,
                logical_id,
                default_model,
            )
            model = default_model
        job.tasks[logical_id] = JobTask(
            logical_id=logical_id,
            task_id=task_id,
            agent=t.get("agent", "worker"),
            description=t["description"],
            soul=t.get("soul", ""),
            skills=t.get("skills", ""),
            depends_on=t.get("depends_on", []),
            model=model,
            max_turns=t.get("max_turns", 10),
            qa=t.get("qa", ""),
            max_retries=t.get("max_retries", 2),
            early_qa_interval=t.get("early_qa_interval", 0),
        )
        all_logical_ids.append(logical_id)

    # Add synthesize leaf task that depends on all other tasks
    synth_id = "synthesize"
    job.tasks[synth_id] = JobTask(
        logical_id=synth_id,
        task_id=uuid.uuid4().hex[:12],
        agent="writer",
        description=(
            f"Original user request: {original_text}\n\n"
            "Combine the results from all upstream tasks into a single coherent response for the user. "
            "Do not mention sub-agents, task IDs, or internal processes."
        ),
        soul="You are a synthesis agent. Combine sub-agent results into a clear, coherent response. Do NOT use any tools — just write your response.",
        skills="",
        depends_on=all_logical_ids,
        max_turns=0,
        model=SYNTH_MODEL,
    )

    return job


def spawn_worker(agent: str, chat_id: str, task_id: str) -> None:
    subprocess.Popen(
        [sys.executable, "-m", "skitter.worker", agent, chat_id, task_id],
    )
    log.info("[coordinator] Spawned %s worker for task %s", agent, task_id)


def find_task_by_task_id(job: JobSpec, task_id: str) -> JobTask | None:
    for task in job.tasks.values():
        if task.task_id == task_id:
            return task
    return None


def find_logical_id_by_task_id(job: JobSpec, task_id: str) -> str | None:
    for logical_id, task in job.tasks.items():
        if task.task_id == task_id:
            return logical_id
    return None


async def publish_task(client: aiomqtt.Client, job: JobSpec, task: JobTask) -> None:
    context = build_context(job, task)
    task_msg = TaskMessage(
        task_id=task.task_id,
        chat_id=job.chat_id,
        description=task.description,
        soul=task.soul,
        skills=task.skills,
        context=context,
        max_turns=task.max_turns,
        model=task.model,
    )
    topic = TOPIC_TASKS.format(
        agent=task.agent, chat_id=job.chat_id, task_id=task.task_id
    )
    await client.publish(topic, task_msg.to_json(), qos=1, retain=True)
    log.info(
        "[coordinator] Published task %s (%s) to %s",
        task.logical_id,
        task.task_id,
        topic,
    )


async def recover_jobs(client: aiomqtt.Client) -> dict[str, JobSpec]:
    """Subscribe to retained job specs and drain them to rebuild state."""
    jobs: dict[str, JobSpec] = {}
    await client.subscribe("skitter/jobs/+", qos=1)
    # Retained messages arrive immediately after subscribe. Wait briefly
    # then stop — any message that hasn't arrived isn't retained.
    try:
        async with asyncio.timeout(1.0):
            async for mqtt_msg in client.messages:
                payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""
                if not payload:
                    continue
                try:
                    job = JobSpec.from_json(payload)
                    jobs[job.chat_id] = job
                    log.info(
                        "[coordinator] Recovered job for chat %s (%d tasks)",
                        job.chat_id,
                        len(job.tasks),
                    )
                except Exception as e:
                    log.warning("[coordinator] Failed to parse retained job: %s", e)
    except TimeoutError:
        pass
    await client.unsubscribe("skitter/jobs/+")
    return jobs


def rebuild_task_map(jobs: dict[str, JobSpec]) -> dict[str, tuple[str, str]]:
    """Rebuild task_id -> (chat_id, agent) from recovered jobs."""
    task_to_chat: dict[str, tuple[str, str]] = {}
    for chat_id, job in jobs.items():
        for task in job.tasks.values():
            if task.status == "running":
                task_to_chat[task.task_id] = (chat_id, task.agent)
    return task_to_chat


async def respawn_running_tasks(
    client: aiomqtt.Client,
    jobs: dict[str, JobSpec],
    task_to_chat: dict[str, tuple[str, str]],
) -> None:
    """Respawn workers for tasks that were running when the coordinator died.

    The retained task message is still on the broker if the worker didn't
    finish. If the worker did finish and cleared it, the respawned worker
    will see no task and exit harmlessly.
    """
    for job in jobs.values():
        for task in job.tasks.values():
            if task.status == "running":
                log.info(
                    "[coordinator] Respawning %s worker for task %s (recovery)",
                    task.agent,
                    task.task_id,
                )
                spawn_worker(task.agent, job.chat_id, task.task_id)


async def run() -> None:
    jobs: dict[str, JobSpec] = {}
    task_to_chat: dict[str, tuple[str, str]] = {}
    snapshots: dict[str, StreamSnapshot] = {}  # task_id -> latest snapshot
    early_qa_attempts: dict[str, int] = {}  # logical_id -> attempt count

    async with aiomqtt.Client(
        MQTT_HOST,
        MQTT_PORT,
        identifier=f"skitter-coordinator-{uuid.uuid4().hex[:8]}",
    ) as client:
        # --- Recovery phase: read retained job specs from broker ---
        jobs = await recover_jobs(client)
        if jobs:
            task_to_chat = rebuild_task_map(jobs)
            await respawn_running_tasks(client, jobs, task_to_chat)
            log.info(
                "[coordinator] Recovery complete: %d jobs, %d running tasks",
                len(jobs),
                len(task_to_chat),
            )
        else:
            log.info("[coordinator] No jobs to recover")

        # --- Normal operation ---
        await client.subscribe("skitter/inbound/+", qos=1)
        await client.subscribe("skitter/results/+/+", qos=1)
        await client.subscribe("skitter/workers/+/+/status", qos=1)
        await client.subscribe("skitter/stream/+/+/snapshot", qos=1)
        log.info("[coordinator] Subscribed and ready")

        async for mqtt_msg in client.messages:
            topic = str(mqtt_msg.topic)
            payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""

            # --- Inbound user message ---
            if topic.startswith("skitter/inbound/"):
                try:
                    msg = InboundMessage.from_json(payload)
                except Exception as e:
                    log.error("[coordinator] Bad inbound message: %s", e)
                    continue

                log.info(
                    "[coordinator] Received message from %s: %.80s",
                    msg.chat_id,
                    msg.text,
                )

                # Create bootstrap job spec with a single planner task
                planner_task_id = uuid.uuid4().hex[:12]
                soul = load_soul()
                models = load_models()
                instructions = PLANNING_INSTRUCTIONS + "\n" + models_prompt(models)
                planner_soul = soul + instructions if soul else instructions.strip()

                job = JobSpec(
                    chat_id=msg.chat_id,
                    original_text=msg.text,
                    tasks={
                        "planner": JobTask(
                            logical_id="planner",
                            task_id=planner_task_id,
                            agent="planner",
                            description=msg.text,
                            soul=planner_soul,
                            skills="Output ONLY a JSON object. No tools, no code blocks, no explanation.",
                            max_turns=0,
                            model=PLANNER_MODEL,
                        ),
                    },
                )
                job.tasks["planner"].status = "running"
                jobs[msg.chat_id] = job

                # Publish retained job spec (for observability / crash recovery)
                job_topic = TOPIC_JOBS.format(chat_id=msg.chat_id)
                await client.publish(job_topic, job.to_json(), qos=1, retain=True)

                # Publish retained task message for planner
                await publish_task(client, job, job.tasks["planner"])

                task_to_chat[planner_task_id] = (msg.chat_id, "planner")
                spawn_worker("planner", msg.chat_id, planner_task_id)

            # --- Task result from worker ---
            elif topic.startswith("skitter/results/"):
                try:
                    result_msg = TaskResultMessage.from_json(payload)
                except Exception as e:
                    log.error("[coordinator] Bad result message: %s", e)
                    continue

                task_id = result_msg.task_id
                chat_id = result_msg.chat_id
                job_topic = TOPIC_JOBS.format(chat_id=chat_id)

                job = jobs.get(chat_id)
                if job is None:
                    log.warning("[coordinator] No job spec found for chat %s", chat_id)
                    continue

                logical_id = find_logical_id_by_task_id(job, task_id)
                if logical_id is None:
                    log.warning(
                        "[coordinator] Task %s not found in job for chat %s",
                        task_id,
                        chat_id,
                    )
                    continue

                # Mark task done and store result
                job.tasks[logical_id].status = "done"
                job.results[logical_id] = result_msg.result
                log.info(
                    "[coordinator] Task '%s' (%s) done for chat %s",
                    logical_id,
                    task_id,
                    chat_id,
                )

                # Clean up task_to_chat
                task_to_chat.pop(task_id, None)

                # --- Special handling for planner result ---
                if logical_id == "planner":
                    # Parse planner output as JSON
                    try:
                        plan_result = extract_json(result_msg.result)
                    except Exception as e:
                        log.error("[coordinator] Failed to parse planner result: %s", e)
                        out = OutboundMessage(
                            text=f"Planning error: {e}", chat_id=chat_id
                        )
                        await client.publish(
                            TOPIC_OUTBOUND.format(chat_id=chat_id), out.to_json(), qos=1
                        )
                        await client.publish(job_topic, b"", qos=1, retain=True)
                        jobs.pop(chat_id, None)
                        continue

                    action = plan_result.get("action")
                    log.info("[coordinator] Planner action: %s", action)

                    if action == "respond":
                        # Direct response — publish to outbound and clear job
                        out = OutboundMessage(text=plan_result["text"], chat_id=chat_id)
                        await client.publish(
                            TOPIC_OUTBOUND.format(chat_id=chat_id), out.to_json(), qos=1
                        )
                        await client.publish(job_topic, b"", qos=1, retain=True)
                        jobs.pop(chat_id, None)
                        log.info("[coordinator] Direct response to %s", chat_id)
                        continue

                    elif action == "delegate":
                        if not plan_result.get("tasks"):
                            out = OutboundMessage(
                                text="No tasks generated.", chat_id=chat_id
                            )
                            await client.publish(
                                TOPIC_OUTBOUND.format(chat_id=chat_id),
                                out.to_json(),
                                qos=1,
                            )
                            await client.publish(job_topic, b"", qos=1, retain=True)
                            jobs.pop(chat_id, None)
                            continue

                        # Build full job spec from planner output
                        models = load_models()
                        job = build_job_from_plan(
                            chat_id, job.original_text, plan_result, models
                        )
                        jobs[chat_id] = job

                        # Find and spawn ready tasks
                        ready = get_ready_tasks(job)
                        for task in ready:
                            task.status = "running"
                            await publish_task(client, job, task)
                            task_to_chat[task.task_id] = (chat_id, task.agent)
                            spawn_worker(task.agent, chat_id, task.task_id)

                        # Publish updated job spec
                        await client.publish(
                            job_topic, job.to_json(), qos=1, retain=True
                        )
                        log.info(
                            "[coordinator] Delegated %d tasks for %s, %d ready",
                            len(job.tasks),
                            chat_id,
                            len(ready),
                        )
                        continue

                    else:
                        log.error("[coordinator] Unknown planner action: %s", action)
                        out = OutboundMessage(
                            text=f"Unknown planner action: {action}", chat_id=chat_id
                        )
                        await client.publish(
                            TOPIC_OUTBOUND.format(chat_id=chat_id), out.to_json(), qos=1
                        )
                        await client.publish(job_topic, b"", qos=1, retain=True)
                        jobs.pop(chat_id, None)
                        continue

                # --- Early QA result handling ---
                if logical_id.startswith("early_qa:"):
                    original_id = logical_id[9:]
                    original_task = job.tasks.get(original_id)

                    qa_passed = True
                    qa_feedback = ""
                    try:
                        qa_result = extract_json(result_msg.result)
                        qa_passed = qa_result.get("pass", True)
                        qa_feedback = qa_result.get("feedback", "")
                    except Exception:
                        pass

                    # Clean up ephemeral early QA task and result
                    job.tasks.pop(logical_id, None)
                    job.results.pop(logical_id, None)

                    if (
                        original_task
                        and original_task.status == "running"
                        and not qa_passed
                        and qa_feedback
                    ):
                        attempt = early_qa_attempts.get(original_id, 1)
                        feedback = FeedbackSignal(
                            task_id=original_task.task_id,
                            chat_id=chat_id,
                            feedback=qa_feedback,
                            attempt=attempt,
                        )
                        feedback_topic = TOPIC_FEEDBACK.format(
                            chat_id=chat_id, task_id=original_task.task_id,
                        )
                        await client.publish(
                            feedback_topic, feedback.to_json(), qos=1, retain=True,
                        )
                        log.info(
                            "[coordinator] Early QA failed for '%s' (attempt %d): %s",
                            original_id,
                            attempt,
                            qa_feedback,
                        )
                    elif qa_passed:
                        log.info("[coordinator] Early QA passed for '%s'", original_id)

                    await client.publish(job_topic, job.to_json(), qos=1, retain=True)
                    continue

                # --- QA result handling ---
                if logical_id.startswith("qa:"):
                    original_id = logical_id[3:]
                    original_task = job.tasks.get(original_id)
                    if original_task is None:
                        log.warning(
                            "[coordinator] QA result for unknown task '%s'",
                            original_id,
                        )
                        continue

                    # Parse QA verdict
                    qa_passed = False
                    qa_feedback = ""
                    try:
                        qa_result = extract_json(result_msg.result)
                        qa_passed = qa_result.get("pass", False)
                        qa_feedback = qa_result.get("feedback", "")
                    except Exception as e:
                        log.warning(
                            "[coordinator] Failed to parse QA result for '%s': %s — treating as pass",
                            original_id,
                            e,
                        )
                        qa_passed = True

                    # Clean up ephemeral QA task and result
                    job.tasks.pop(logical_id, None)
                    job.results.pop(logical_id, None)

                    if qa_passed:
                        log.info("[coordinator] QA passed for '%s'", original_id)
                        # original task stays done, fall through to advance graph
                    elif original_task.retries < original_task.max_retries:
                        original_task.retries += 1
                        log.info(
                            "[coordinator] QA failed for '%s' (retry %d/%d): %s",
                            original_id,
                            original_task.retries,
                            original_task.max_retries,
                            qa_feedback,
                        )
                        # Reset original task for retry with feedback appended
                        original_task.status = "pending"
                        original_task.description += f"\n\n[QA feedback, attempt {original_task.retries}]: {qa_feedback}"
                        # Remove old result so worker starts fresh
                        job.results.pop(original_id, None)
                    else:
                        log.warning(
                            "[coordinator] QA failed for '%s' but retries exhausted (%d/%d) — advancing anyway",
                            original_id,
                            original_task.retries,
                            original_task.max_retries,
                        )
                        # Keep original result and status=done, advance graph

                    # Fall through to get_ready_tasks / advance graph
                    ready = get_ready_tasks(job)
                    for task in ready:
                        task.status = "running"
                        await publish_task(client, job, task)
                        task_to_chat[task.task_id] = (chat_id, task.agent)
                        spawn_worker(task.agent, chat_id, task.task_id)

                    await client.publish(job_topic, job.to_json(), qos=1, retain=True)
                    continue

                # --- Normal task with QA: spawn QA agent ---
                if (
                    logical_id not in ("planner", "synthesize")
                    and not logical_id.startswith("qa:")
                    and job.tasks[logical_id].qa
                ):
                    qa_logical_id = f"qa:{logical_id}"
                    qa_task_id = uuid.uuid4().hex[:12]
                    qa_task = JobTask(
                        logical_id=qa_logical_id,
                        task_id=qa_task_id,
                        agent="qa",
                        description=(
                            f"## QA Criteria\n{job.tasks[logical_id].qa}\n\n"
                            f"## Original Task\n{job.tasks[logical_id].description}\n\n"
                            f"## Worker Output\n{result_msg.result}"
                        ),
                        soul="You are a QA reviewer. Evaluate the work against the criteria. Return JSON only.",
                        skills='Output ONLY {"pass":true} or {"pass":false,"feedback":"specific actionable feedback"}',
                        max_turns=0,
                        model=QA_MODEL,
                    )
                    job.tasks[qa_logical_id] = qa_task
                    qa_task.status = "running"
                    await publish_task(client, job, qa_task)
                    task_to_chat[qa_task_id] = (chat_id, "qa")
                    spawn_worker("qa", chat_id, qa_task_id)
                    await client.publish(job_topic, job.to_json(), qos=1, retain=True)
                    log.info(
                        "[coordinator] Spawned QA for '%s' (%s)",
                        logical_id,
                        qa_task_id,
                    )
                    continue

                # --- Normal task result: advance the graph ---
                # Check if synthesize task just completed
                if logical_id == "synthesize":
                    out = OutboundMessage(text=result_msg.result, chat_id=chat_id)
                    await client.publish(
                        TOPIC_OUTBOUND.format(chat_id=chat_id), out.to_json(), qos=1, retain=True
                    )
                    # Clear job spec
                    await client.publish(job_topic, b"", qos=1, retain=True)
                    jobs.pop(chat_id, None)
                    log.info("[coordinator] Final response sent to %s", chat_id)
                    continue

                # Find newly unblocked tasks
                ready = get_ready_tasks(job)
                for task in ready:
                    task.status = "running"
                    await publish_task(client, job, task)
                    task_to_chat[task.task_id] = (chat_id, task.agent)
                    spawn_worker(task.agent, chat_id, task.task_id)

                # Re-publish updated job spec
                await client.publish(job_topic, job.to_json(), qos=1, retain=True)

                if ready:
                    log.info(
                        "[coordinator] Advanced graph: %d new tasks running", len(ready)
                    )
                elif is_complete(job):
                    log.info(
                        "[coordinator] All tasks complete for %s (should have been caught by synthesize)",
                        chat_id,
                    )

            # --- Stream snapshot (for early QA and crash recovery) ---
            elif (
                topic.startswith("skitter/stream/")
                and topic.endswith("/snapshot")
            ):
                if not payload:
                    # Cleared snapshot — remove from tracking
                    parts = topic.split("/")
                    if len(parts) == 5:
                        snapshots.pop(parts[3], None)
                    continue
                try:
                    snapshot = StreamSnapshot.from_json(payload)
                    snapshots[snapshot.task_id] = snapshot
                except Exception:
                    continue

                # --- Early QA trigger ---
                snap_task_id = snapshot.task_id
                if snap_task_id not in task_to_chat:
                    continue
                snap_chat_id, _ = task_to_chat[snap_task_id]
                snap_job = jobs.get(snap_chat_id)
                if not snap_job:
                    continue
                snap_logical_id = find_logical_id_by_task_id(snap_job, snap_task_id)
                if not snap_logical_id:
                    continue
                snap_task = snap_job.tasks[snap_logical_id]
                early_qa_id = f"early_qa:{snap_logical_id}"

                if (
                    snap_task.status == "running"
                    and snap_task.qa
                    and snap_task.early_qa_interval > 0
                    and snapshot.seq >= snap_task.early_qa_interval
                    and snapshot.seq % snap_task.early_qa_interval == 0
                    and early_qa_id not in snap_job.tasks
                ):
                    attempt = early_qa_attempts.get(snap_logical_id, 0) + 1
                    early_qa_attempts[snap_logical_id] = attempt

                    recent_text = snapshot.text[-1024:] if snapshot.text else "(no output yet)"
                    tool_log_str = (
                        "\n".join(snapshot.tool_log[-20:])
                        if snapshot.tool_log
                        else "(no tools used)"
                    )
                    description = EARLY_QA_TEMPLATE.format(
                        description=snap_task.description[:200],
                        qa=snap_task.qa,
                        elapsed_s=snapshot.elapsed_s,
                        tool_calls=snapshot.tool_calls,
                        errors=snapshot.errors,
                        tool_log=tool_log_str,
                        recent_text=recent_text,
                    )

                    qa_task_id = uuid.uuid4().hex[:12]
                    qa_task = JobTask(
                        logical_id=early_qa_id,
                        task_id=qa_task_id,
                        agent="qa",
                        description=description,
                        soul=EARLY_QA_SYSTEM,
                        skills=EARLY_QA_SKILLS,
                        max_turns=0,
                        model=EARLY_QA_MODEL,
                    )
                    snap_job.tasks[early_qa_id] = qa_task
                    qa_task.status = "running"
                    await publish_task(client, snap_job, qa_task)
                    task_to_chat[qa_task_id] = (snap_chat_id, "qa")
                    spawn_worker("qa", snap_chat_id, qa_task_id)

                    snap_job_topic = TOPIC_JOBS.format(chat_id=snap_chat_id)
                    await client.publish(
                        snap_job_topic, snap_job.to_json(), qos=1, retain=True,
                    )
                    log.info(
                        "[coordinator] Spawned early QA for '%s' (attempt %d)",
                        snap_logical_id,
                        attempt,
                    )

            # --- Worker status (LWT or explicit) ---
            elif topic.startswith("skitter/workers/"):
                try:
                    status = json.loads(payload)
                except Exception:
                    continue

                wk_state = status.get("status", "")
                wk_task_id = status.get("task_id", "")

                if not wk_task_id:
                    # Extract from topic: skitter/workers/{chat_id}/{task_id}/status
                    parts = topic.split("/")
                    if len(parts) >= 4:
                        wk_task_id = parts[3]

                if wk_state == "alive":
                    log.info("[coordinator] Worker alive for task %s", wk_task_id)
                elif wk_state == "done":
                    log.info("[coordinator] Worker done for task %s", wk_task_id)
                    snapshots.pop(wk_task_id, None)
                elif wk_state == "dead":
                    if wk_task_id in task_to_chat:
                        wk_chat_id, wk_agent = task_to_chat[wk_task_id]
                        snapshot = snapshots.pop(wk_task_id, None)
                        if snapshot and len(snapshot.text) > 100:
                            log.warning(
                                "[coordinator] Worker DEAD for task %s — using partial result (%d chars, %d tool calls)",
                                wk_task_id,
                                len(snapshot.text),
                                snapshot.tool_calls,
                            )
                            result_text = (
                                f"[PARTIAL — worker crashed after {snapshot.elapsed_s:.0f}s, "
                                f"{snapshot.tool_calls} tool calls]\n\n{snapshot.text}"
                            )
                            result_msg = TaskResultMessage(
                                task_id=wk_task_id,
                                chat_id=wk_chat_id,
                                result=result_text,
                            )
                            result_topic = TOPIC_RESULTS.format(
                                chat_id=wk_chat_id, task_id=wk_task_id,
                            )
                            await client.publish(
                                result_topic, result_msg.to_json(), qos=1,
                            )
                            # Clear retained snapshot
                            await client.publish(
                                TOPIC_STREAM_SNAPSHOT.format(
                                    chat_id=wk_chat_id, task_id=wk_task_id,
                                ),
                                b"",
                                qos=1,
                                retain=True,
                            )
                        else:
                            log.warning(
                                "[coordinator] Worker DEAD for task %s — respawning",
                                wk_task_id,
                            )
                            spawn_worker(wk_agent, wk_chat_id, wk_task_id)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("[coordinator] Shutting down")


if __name__ == "__main__":
    main()
