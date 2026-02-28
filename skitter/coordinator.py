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
    TOPIC_JOBS,
    TOPIC_OUTBOUND,
    TOPIC_TASKS,
)
from skitter.types import (
    InboundMessage,
    JobSpec,
    JobTask,
    OutboundMessage,
    TaskMessage,
    TaskResultMessage,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.coordinator")

SOUL_PATH = Path("SOUL.md")

DEFAULT_MODELS = "haiku:Fast and cheap, good for simple tasks and summaries|sonnet:Balanced, good for research and analysis|opus:Most capable, use for complex reasoning and coding"
PLANNER_MODEL = os.environ.get("SKITTER_PLANNER_MODEL", "sonnet")


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
        model=PLANNER_MODEL,
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

    async with aiomqtt.Client(
        MQTT_HOST,
        MQTT_PORT,
        identifier="skitter-coordinator",
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

                # --- Normal task result: advance the graph ---
                # Check if synthesize task just completed
                if logical_id == "synthesize":
                    out = OutboundMessage(text=result_msg.result, chat_id=chat_id)
                    await client.publish(
                        TOPIC_OUTBOUND.format(chat_id=chat_id), out.to_json(), qos=1
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

            # --- Worker status (LWT or explicit) ---
            elif topic.startswith("skitter/workers/"):
                try:
                    status = json.loads(payload)
                except Exception:
                    continue

                state = status.get("status", "")
                task_id = status.get("task_id", "")

                if not task_id:
                    # Extract from topic: skitter/workers/{chat_id}/{task_id}/status
                    parts = topic.split("/")
                    if len(parts) >= 4:
                        task_id = parts[3]

                if state == "alive":
                    log.info("[coordinator] Worker alive for task %s", task_id)
                elif state == "done":
                    log.info("[coordinator] Worker done for task %s", task_id)
                elif state == "dead":
                    log.warning(
                        "[coordinator] Worker DEAD for task %s — respawning", task_id
                    )
                    if task_id in task_to_chat:
                        chat_id, agent = task_to_chat[task_id]
                        spawn_worker(agent, chat_id, task_id)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("[coordinator] Shutting down")


if __name__ == "__main__":
    main()
