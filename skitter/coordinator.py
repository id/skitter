import asyncio
import json
import logging
import os
import subprocess
import sys
import uuid

import aiomqtt

from skitter.mqtt import (
    MQTT_HOST,
    MQTT_PORT,
    get_correlation_data,
    get_response_topic,
    make_properties,
    topic_discovery,
    topic_event_worker_wildcard,
    topic_reply,
    topic_request,
    topic_state_job,
    topic_state_job_wildcard,
)
from skitter.config import (
    AgentDef,
    PipelineDef,
    agent_def_to_card,
    load_agents,
    load_pipelines,
    safe_format,
)
from skitter.types import (
    A2AResponse,
    InboundMessage,
    JobSpec,
    JobTask,
    TaskMessage,
    TaskStatusUpdate,
)

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s %(message)s", datefmt="%H:%M:%S"
)
log = logging.getLogger("skitter.coordinator")

DEFAULT_MODELS = "haiku:Fast and cheap, good for simple tasks and summaries|sonnet:Balanced, good for research and analysis|opus:Most capable, use for complex reasoning and coding"


def load_models() -> dict[str, str]:
    raw = os.environ.get("SKITTER_MODELS", DEFAULT_MODELS)
    models = {}
    for entry in raw.split("|"):
        entry = entry.strip()
        if ":" in entry:
            name, desc = entry.split(":", 1)
            models[name.strip()] = desc.strip()
    return models


def extract_json(text: str) -> dict:
    """Extract the first JSON object from LLM output."""
    cleaned = text.strip()
    if cleaned.startswith("```"):
        lines = cleaned.split("\n")
        lines = [line for line in lines[1:] if line.strip() != "```"]
        cleaned = "\n".join(lines).strip()
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


def build_job_from_pipeline(
    chat_id: str,
    original_text: str,
    pipeline: PipelineDef,
    variables: dict[str, str],
    models: dict[str, str],
    agents: dict[str, AgentDef] | None = None,
) -> JobSpec:
    job = JobSpec(chat_id=chat_id, original_text=original_text)
    default_model = list(models.keys())[0] if models else ""
    agents = agents or {}

    for pt in pipeline.tasks:
        task_id = uuid.uuid4().hex[:12]
        agent_def = agents.get(pt.agent)

        description = safe_format(pt.description, variables)

        model = pt.model or (agent_def.model if agent_def else "") or default_model
        if models and model not in models:
            log.warning(
                "[coordinator] Unknown model '%s' for pipeline task '%s', falling back to '%s'",
                model,
                pt.logical_id,
                default_model,
            )
            model = default_model
        soul = pt.soul or (agent_def.soul if agent_def else "") or ""
        skills = pt.skills or (agent_def.skills if agent_def else "") or ""
        max_turns = pt.max_turns or (agent_def.max_turns if agent_def else 10)

        job.tasks[pt.logical_id] = JobTask(
            logical_id=pt.logical_id,
            task_id=task_id,
            agent=pt.agent,
            description=description,
            soul=soul,
            skills=skills,
            depends_on=list(pt.depends_on),
            model=model,
            max_turns=max_turns,
        )

    return job


def build_job_from_agent(
    chat_id: str,
    text: str,
    agent_id: str,
    agents: dict[str, AgentDef],
    models: dict[str, str],
) -> JobSpec:
    """Build a single-task job for a direct agent call (no pipeline)."""
    job = JobSpec(chat_id=chat_id, original_text=text)
    default_model = list(models.keys())[0] if models else ""
    agent_def = agents.get(agent_id)

    model = (agent_def.model if agent_def else "") or default_model
    if models and model not in models:
        model = default_model
    soul = (agent_def.soul if agent_def else "") or ""
    skills = (agent_def.skills if agent_def else "") or ""
    max_turns = agent_def.max_turns if agent_def else 10

    task_id = uuid.uuid4().hex[:12]
    job.tasks[agent_id] = JobTask(
        logical_id=agent_id,
        task_id=task_id,
        agent=agent_id,
        description=text,
        soul=soul,
        skills=skills,
        model=model,
        max_turns=max_turns,
    )
    return job


# --- Worker spawning ---

WORKER_MODE = os.environ.get("SKITTER_WORKER_MODE", "subprocess")
WORKER_IMAGE = os.environ.get("SKITTER_WORKER_IMAGE", "skitter-worker:latest")
DOCKER_NETWORK = os.environ.get("SKITTER_DOCKER_NETWORK", "skitter")


def spawn_worker(agent: str, chat_id: str, task_id: str) -> None:
    if WORKER_MODE == "docker":
        _spawn_worker_docker(agent, chat_id, task_id)
    else:
        _spawn_worker_subprocess(agent, chat_id, task_id)


def _spawn_worker_subprocess(agent: str, chat_id: str, task_id: str) -> None:
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    subprocess.Popen(
        [sys.executable, "-m", "skitter.worker", agent, chat_id, task_id],
        env=env,
    )
    log.info("[coordinator] Spawned %s worker subprocess for task %s", agent, task_id)


def _spawn_worker_docker(agent: str, chat_id: str, task_id: str) -> None:
    env_args: list[str] = []
    env_args.extend(
        ["-e", f"MQTT_HOST={os.environ.get('SKITTER_DOCKER_MQTT_HOST', 'emqx')}"]
    )
    env_args.extend(["-e", f"MQTT_PORT={os.environ.get('MQTT_PORT', '1883')}"])
    api_key = os.environ.get("ANTHROPIC_API_KEY", "")
    if api_key:
        env_args.extend(["-e", f"ANTHROPIC_API_KEY={api_key}"])
    subprocess.Popen(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            DOCKER_NETWORK,
            *env_args,
            WORKER_IMAGE,
            agent,
            chat_id,
            task_id,
        ],
    )
    log.info("[coordinator] Spawned %s worker container for task %s", agent, task_id)


# --- Job/task helpers ---


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


# --- A2A dispatch ---


async def dispatch_task(
    client: aiomqtt.Client,
    job: JobSpec,
    task: JobTask,
    reply_topic: str,
) -> None:
    """Build A2A request and publish to agent's request topic with v5 properties."""
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
    props = make_properties(
        response_topic=reply_topic,
        correlation_data=task.task_id,
    )
    target_topic = topic_request(task.agent)
    await client.publish(
        target_topic,
        task_msg.to_json(),
        qos=1,
        properties=props,
    )
    log.info(
        "[coordinator] Dispatched task %s (%s) to %s",
        task.logical_id,
        task.task_id,
        target_topic,
    )


# --- Recovery ---


async def recover_jobs(client: aiomqtt.Client) -> dict[str, JobSpec]:
    """Subscribe to retained job specs and drain them to rebuild state."""
    jobs: dict[str, JobSpec] = {}
    await client.subscribe(topic_state_job_wildcard(), qos=1)
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
    await client.unsubscribe(topic_state_job_wildcard())
    return jobs


def rebuild_task_map(jobs: dict[str, JobSpec]) -> dict[str, tuple[str, str]]:
    """Rebuild task_id -> (chat_id, agent) from recovered jobs."""
    task_to_chat: dict[str, tuple[str, str]] = {}
    for chat_id, job in jobs.items():
        for task in job.tasks.values():
            if task.status == "running":
                task_to_chat[task.task_id] = (chat_id, task.agent)
    return task_to_chat


# --- Main loop ---


async def run() -> None:
    jobs: dict[str, JobSpec] = {}
    task_to_chat: dict[str, tuple[str, str]] = {}
    # Pending dispatch: task_id -> (job, task) — queued until worker alive arrives
    pending_dispatch: dict[str, tuple[JobSpec, JobTask]] = {}

    # Generate unique session ID for reply topic
    session_id = uuid.uuid4().hex[:12]

    # Load predefined agents and pipelines
    agents = load_agents()
    pipelines = load_pipelines()
    if agents:
        log.info(
            "[coordinator] Loaded %d predefined agents: %s",
            len(agents),
            ", ".join(agents),
        )
    if pipelines:
        log.info(
            "[coordinator] Loaded %d pipelines: %s",
            len(pipelines),
            ", ".join(pipelines),
        )

    reply_t = topic_reply("coordinator", session_id)

    async with aiomqtt.Client(
        MQTT_HOST,
        MQTT_PORT,
        identifier=f"skitter-coordinator-{session_id}",
        protocol=aiomqtt.ProtocolVersion.V5,
    ) as client:
        # --- Publish Agent Cards (retained discovery messages) ---
        for agent_id, agent_def in agents.items():
            card = agent_def_to_card(agent_def)
            await client.publish(
                topic_discovery(agent_id),
                card.to_json(),
                qos=1,
                retain=True,
            )
        # Publish coordinator's own card
        coord_card_payload = json.dumps(
            {
                "agent_id": "coordinator",
                "name": "Skitter Coordinator",
                "description": "Pipeline orchestrator — routes tasks and manages DAGs",
                "capabilities": ["orchestration", "spawn"],
                "model": "",
                "max_turns": 0,
            }
        )
        await client.publish(
            topic_discovery("coordinator"),
            coord_card_payload,
            qos=1,
            retain=True,
        )
        log.info("[coordinator] Published %d Agent Cards", len(agents) + 1)

        # --- Recovery phase ---
        jobs = await recover_jobs(client)
        if jobs:
            task_to_chat = rebuild_task_map(jobs)
            # Respawn workers for tasks that were running
            for job in jobs.values():
                for task in job.tasks.values():
                    if task.status == "running":
                        log.info(
                            "[coordinator] Respawning %s worker for task %s (recovery)",
                            task.agent,
                            task.task_id,
                        )
                        pending_dispatch[task.task_id] = (job, task)
                        spawn_worker(task.agent, job.chat_id, task.task_id)
            log.info(
                "[coordinator] Recovery complete: %d jobs, %d running tasks",
                len(jobs),
                len(task_to_chat),
            )
        else:
            log.info("[coordinator] No jobs to recover")

        # --- Subscribe to A2A topics ---
        # Inbound requests to coordinator
        await client.subscribe(topic_request("coordinator"), qos=1)
        # Replies from workers (on our session-specific reply topic)
        await client.subscribe(reply_t, qos=1)
        # Worker liveness events
        await client.subscribe(topic_event_worker_wildcard(), qos=1)
        log.info("[coordinator] Subscribed and ready (reply=%s)", reply_t)

        async for mqtt_msg in client.messages:
            topic = str(mqtt_msg.topic)
            payload = mqtt_msg.payload.decode() if mqtt_msg.payload else ""

            # --- Inbound A2A request to coordinator ---
            if topic == topic_request("coordinator"):
                if not payload:
                    continue

                # Extract caller's reply info from v5 properties
                caller_reply_topic = get_response_topic(mqtt_msg)
                caller_correlation = get_correlation_data(mqtt_msg)

                try:
                    data = json.loads(payload)
                except Exception as e:
                    log.error("[coordinator] Bad inbound JSON: %s", e)
                    continue

                # Check if this is a JSON-RPC method call
                method = data.get("method", "")

                if method == "tasks/spawn":
                    # Agent-to-agent spawn request
                    params = data.get("params", {})
                    request_id = data.get("id", "")
                    spawn_agent_id = params.get("agent_id", "")
                    spawn_description = params.get("description", "")
                    spawn_reply_to = params.get("reply_to", caller_reply_topic or "")
                    spawn_chat_id = params.get(
                        "chat_id", f"spawn-{uuid.uuid4().hex[:8]}"
                    )

                    if not spawn_agent_id:
                        if spawn_reply_to and request_id:
                            err = A2AResponse(
                                id=request_id,
                                error={
                                    "code": -32602,
                                    "message": "Missing agent_id",
                                },
                            )
                            await client.publish(spawn_reply_to, err.to_json(), qos=1)
                        continue

                    # Create a mini-job for this spawned task
                    spawn_task_id = uuid.uuid4().hex[:12]
                    agent_def = agents.get(spawn_agent_id)
                    models = load_models()
                    default_model = list(models.keys())[0] if models else ""
                    model = (agent_def.model if agent_def else "") or default_model
                    soul = (agent_def.soul if agent_def else "") or ""
                    skills = (agent_def.skills if agent_def else "") or ""
                    max_turns = agent_def.max_turns if agent_def else 10

                    spawn_job = JobSpec(
                        chat_id=spawn_chat_id,
                        original_text=spawn_description,
                    )
                    spawn_task = JobTask(
                        logical_id="spawn_task",
                        task_id=spawn_task_id,
                        agent=spawn_agent_id,
                        description=spawn_description,
                        soul=soul,
                        skills=skills,
                        model=model,
                        max_turns=max_turns,
                    )
                    spawn_task.status = "running"
                    spawn_job.tasks["spawn_task"] = spawn_task
                    jobs[spawn_chat_id] = spawn_job
                    task_to_chat[spawn_task_id] = (spawn_chat_id, spawn_agent_id)

                    # Store spawn metadata so we can route the result back
                    spawn_job.results["_spawn_reply_to"] = spawn_reply_to or ""
                    spawn_job.results["_spawn_request_id"] = request_id
                    spawn_job.results["_spawn_correlation"] = caller_correlation or ""

                    # Queue dispatch and spawn worker
                    pending_dispatch[spawn_task_id] = (spawn_job, spawn_task)
                    spawn_worker(spawn_agent_id, spawn_chat_id, spawn_task_id)

                    await client.publish(
                        topic_state_job(spawn_chat_id),
                        spawn_job.to_json(),
                        qos=1,
                        retain=True,
                    )
                    log.info(
                        "[coordinator] Spawn request: %s for agent '%s' task %s",
                        request_id,
                        spawn_agent_id,
                        spawn_task_id,
                    )
                    continue

                # Standard inbound request (pipeline execution)
                try:
                    msg = InboundMessage.from_json(payload)
                except Exception:
                    # Try parsing as a raw dict with text/chat_id/sender
                    try:
                        msg = InboundMessage(
                            text=data.get("text", ""),
                            sender=data.get("sender", "unknown"),
                            chat_id=data.get("chat_id", f"req-{uuid.uuid4().hex[:8]}"),
                            pipeline_id=data.get("pipeline_id", ""),
                            pipeline_vars=data.get("pipeline_vars", {}),
                            agent_id=data.get("agent_id", ""),
                        )
                    except Exception as e:
                        log.error("[coordinator] Bad inbound message: %s", e)
                        continue

                log.info(
                    "[coordinator] Received message from %s: %.80s",
                    msg.chat_id,
                    msg.text,
                )

                # Store caller reply info for routing final response
                if caller_reply_topic:
                    # We'll store these on the job for later
                    pass

                if msg.agent_id:
                    # Direct agent call — single-task job
                    if msg.agent_id not in agents:
                        error_text = f"Unknown agent: {msg.agent_id}"
                        if caller_reply_topic:
                            resp = A2AResponse(
                                id=caller_correlation or "",
                                error={"code": -32602, "message": error_text},
                            )
                            props = make_properties(correlation_data=caller_correlation)
                            await client.publish(
                                caller_reply_topic,
                                resp.to_json(),
                                qos=1,
                                properties=props,
                            )
                        continue

                    models = load_models()
                    job = build_job_from_agent(
                        msg.chat_id,
                        msg.text,
                        msg.agent_id,
                        agents,
                        models,
                    )

                elif msg.pipeline_id:
                    # Pipeline execution
                    pipeline = pipelines.get(msg.pipeline_id)
                    if pipeline is None:
                        error_text = f"Unknown pipeline: {msg.pipeline_id}"
                        if caller_reply_topic:
                            resp = A2AResponse(
                                id=caller_correlation or "",
                                error={"code": -32602, "message": error_text},
                            )
                            props = make_properties(correlation_data=caller_correlation)
                            await client.publish(
                                caller_reply_topic,
                                resp.to_json(),
                                qos=1,
                                properties=props,
                            )
                        continue

                    models = load_models()
                    job = build_job_from_pipeline(
                        msg.chat_id,
                        msg.text,
                        pipeline,
                        msg.pipeline_vars,
                        models,
                        agents,
                    )

                else:
                    # Neither agent_id nor pipeline_id
                    error_text = (
                        "Specify agent_id or pipeline_id. Use: "
                        "skitter agent run <id> '<prompt>' or "
                        "skitter pipeline run <id> --var key=value"
                    )
                    if caller_reply_topic:
                        resp = A2AResponse(
                            id=caller_correlation or "",
                            error={"code": -32602, "message": error_text},
                        )
                        props = make_properties(correlation_data=caller_correlation)
                        await client.publish(
                            caller_reply_topic,
                            resp.to_json(),
                            qos=1,
                            properties=props,
                        )
                    else:
                        log.warning("[coordinator] No reply topic for error response")
                    continue
                jobs[msg.chat_id] = job

                # Store caller reply info on the job
                if caller_reply_topic:
                    job.results["_caller_reply_topic"] = caller_reply_topic
                if caller_correlation:
                    job.results["_caller_correlation"] = caller_correlation

                # Publish retained job spec
                await client.publish(
                    topic_state_job(msg.chat_id),
                    job.to_json(),
                    qos=1,
                    retain=True,
                )

                # Dispatch ready tasks
                ready = get_ready_tasks(job)
                for task in ready:
                    task.status = "running"
                    task_to_chat[task.task_id] = (msg.chat_id, task.agent)
                    pending_dispatch[task.task_id] = (job, task)
                    spawn_worker(task.agent, msg.chat_id, task.task_id)

                await client.publish(
                    topic_state_job(msg.chat_id),
                    job.to_json(),
                    qos=1,
                    retain=True,
                )
                label = (
                    f"agent '{msg.agent_id}'"
                    if msg.agent_id
                    else f"pipeline '{msg.pipeline_id}'"
                )
                log.info(
                    "[coordinator] %s started for %s (%d tasks, %d ready)",
                    label.capitalize(),
                    msg.chat_id,
                    len(job.tasks),
                    len(ready),
                )

            # --- Reply from worker (on our reply topic) ---
            elif topic == reply_t:
                if not payload:
                    continue

                corr_data = get_correlation_data(mqtt_msg)
                if not corr_data:
                    log.warning("[coordinator] Reply without Correlation Data")
                    continue

                task_id = corr_data

                try:
                    data = json.loads(payload)
                except Exception:
                    continue

                # Forward stream items to caller's reply topic
                if "seq" in data and "type" in data:
                    if task_id in task_to_chat:
                        chat_id, _ = task_to_chat[task_id]
                        job = jobs.get(chat_id)
                        if job:
                            caller_rt = job.results.get("_caller_reply_topic")
                            if caller_rt:
                                caller_corr = job.results.get("_caller_correlation")
                                fwd_props = make_properties(
                                    correlation_data=caller_corr
                                )
                                await client.publish(
                                    caller_rt,
                                    payload,
                                    qos=0,
                                    properties=fwd_props,
                                )
                    continue

                # Terminal status update
                if "state" in data and "task_id" in data:
                    status_update = TaskStatusUpdate.from_json(payload)
                    task_id = status_update.task_id
                    result_text = status_update.result

                    if task_id not in task_to_chat:
                        log.warning(
                            "[coordinator] Status update for unknown task %s",
                            task_id,
                        )
                        continue

                    chat_id, agent_name = task_to_chat[task_id]
                    job = jobs.get(chat_id)
                    if job is None:
                        log.warning("[coordinator] No job for chat %s", chat_id)
                        continue

                    logical_id = find_logical_id_by_task_id(job, task_id)
                    if logical_id is None:
                        log.warning(
                            "[coordinator] Task %s not in job %s",
                            task_id,
                            chat_id,
                        )
                        continue

                    # Mark done and store result
                    job.tasks[logical_id].status = "done"
                    job.results[logical_id] = result_text
                    task_to_chat.pop(task_id, None)
                    pending_dispatch.pop(task_id, None)
                    log.info(
                        "[coordinator] Task '%s' (%s) done for chat %s",
                        logical_id,
                        task_id,
                        chat_id,
                    )

                    job_topic = topic_state_job(chat_id)

                    # --- Spawn task completed: route back to requesting agent ---
                    if logical_id == "spawn_task":
                        spawn_rt = job.results.pop("_spawn_reply_to", None)
                        spawn_req_id = job.results.pop("_spawn_request_id", None)
                        spawn_corr = job.results.pop("_spawn_correlation", None)
                        if spawn_rt:
                            resp = A2AResponse(
                                id=spawn_req_id or "",
                                result={"output": result_text},
                            )
                            props = make_properties(correlation_data=spawn_corr)
                            await client.publish(
                                spawn_rt,
                                resp.to_json(),
                                qos=1,
                                properties=props,
                            )
                        await client.publish(job_topic, b"", qos=1, retain=True)
                        jobs.pop(chat_id, None)
                        log.info(
                            "[coordinator] Spawn result routed for %s",
                            chat_id,
                        )
                        continue

                    # --- Normal: advance the graph ---
                    ready = get_ready_tasks(job)
                    for task in ready:
                        task.status = "running"
                        task_to_chat[task.task_id] = (chat_id, task.agent)
                        pending_dispatch[task.task_id] = (job, task)
                        spawn_worker(task.agent, chat_id, task.task_id)

                    await client.publish(job_topic, job.to_json(), qos=1, retain=True)

                    if ready:
                        log.info(
                            "[coordinator] Advanced graph: %d new tasks running",
                            len(ready),
                        )
                    elif is_complete(job):
                        # Job complete — route result to caller
                        caller_rt = job.results.pop("_caller_reply_topic", None)
                        caller_corr = job.results.pop("_caller_correlation", None)
                        if caller_rt:
                            # Return leaf task results only (tasks nothing depends on)
                            all_deps = set()
                            for t in job.tasks.values():
                                all_deps.update(t.depends_on)
                            leaf_ids = [
                                lid
                                for lid in job.tasks
                                if lid not in all_deps and lid in job.results
                            ]
                            if len(leaf_ids) == 1:
                                final_text = job.results[leaf_ids[0]]
                            else:
                                final_text = "\n\n".join(
                                    f"## {lid}\n{job.results[lid]}" for lid in leaf_ids
                                )
                            final_status = TaskStatusUpdate(
                                task_id=job.chat_id,
                                state="completed",
                                result=final_text,
                            )
                            props = make_properties(correlation_data=caller_corr)
                            await client.publish(
                                caller_rt,
                                final_status.to_json(),
                                qos=1,
                                properties=props,
                            )
                        await client.publish(job_topic, b"", qos=1, retain=True)
                        jobs.pop(chat_id, None)
                        log.info(
                            "[coordinator] Job complete for %s",
                            chat_id,
                        )

            # --- Worker liveness events ---
            elif "/event/" in topic and "/workers/" in topic:
                if not payload:
                    continue
                try:
                    status = json.loads(payload)
                except Exception:
                    continue

                wk_state = status.get("status", "")
                wk_task_id = status.get("task_id", "")

                if not wk_task_id:
                    # Extract from topic: .../workers/{task_id}
                    parts = topic.split("/")
                    wk_task_id = parts[-1] if parts else ""

                if wk_state == "alive":
                    log.info("[coordinator] Worker alive for task %s", wk_task_id)
                    # Dispatch pending task now that worker is alive
                    if wk_task_id in pending_dispatch:
                        job, task = pending_dispatch.pop(wk_task_id)
                        await dispatch_task(client, job, task, reply_t)

                elif wk_state == "done":
                    log.info("[coordinator] Worker done for task %s", wk_task_id)

                elif wk_state == "dead":
                    if wk_task_id in task_to_chat:
                        wk_chat_id, wk_agent = task_to_chat[wk_task_id]
                        pending_dispatch.pop(wk_task_id, None)
                        log.warning(
                            "[coordinator] Worker DEAD for task %s — respawning",
                            wk_task_id,
                        )
                        job = jobs.get(wk_chat_id)
                        if job:
                            task = find_task_by_task_id(job, wk_task_id)
                            if task:
                                pending_dispatch[wk_task_id] = (job, task)
                        spawn_worker(wk_agent, wk_chat_id, wk_task_id)


def main() -> None:
    try:
        asyncio.run(run())
    except KeyboardInterrupt:
        log.info("[coordinator] Shutting down")


if __name__ == "__main__":
    main()
