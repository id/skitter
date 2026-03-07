"""Worker spawn backends — subprocess, docker, or cloud (future)."""

import logging
import os
import subprocess
import sys

log = logging.getLogger("skitter.spawn")

SPAWN_MODE = os.environ.get("SKITTER_SPAWN_MODE", "subprocess")
WORKER_IMAGE = os.environ.get("SKITTER_WORKER_IMAGE", "skitter-worker:latest")
DOCKER_NETWORK = os.environ.get("SKITTER_DOCKER_NETWORK", "skitter")
DOCKER_USER_HOME = "/home/skitter"


def spawn_worker(agent: str, session_id: str, task_id: str) -> None:
    if SPAWN_MODE == "docker":
        _spawn_docker(agent, session_id, task_id)
    else:
        _spawn_subprocess(agent, session_id, task_id)


def _spawn_subprocess(agent: str, session_id: str, task_id: str) -> None:
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    subprocess.Popen(
        [sys.executable, "-m", "skitter.worker", agent, session_id, task_id],
        env=env,
    )
    log.info("Spawned %s worker subprocess for task %s", agent, task_id)


def _spawn_docker(agent: str, session_id: str, task_id: str) -> None:
    env_args: list[str] = []
    env_args.extend(
        ["-e", f"MQTT_HOST={os.environ.get('SKITTER_DOCKER_MQTT_HOST', 'emqx')}"]
    )
    env_args.extend(["-e", f"MQTT_PORT={os.environ.get('MQTT_PORT', '1883')}"])
    for key in ("ANTHROPIC_API_KEY", "OPENAI_API_KEY"):
        val = os.environ.get(key, "")
        if val:
            env_args.extend(["-e", f"{key}={val}"])

    # Mount docker-claude dir (OAuth credentials + agent definitions + memory)
    # Created by `skitter docker login` + `skitter docker sync`
    from skitter.config import DOCKER_CLAUDE_DIR

    volume_args: list[str] = []
    if DOCKER_CLAUDE_DIR.is_dir():
        volume_args.extend(["-v", f"{DOCKER_CLAUDE_DIR}:{DOCKER_USER_HOME}/.claude:ro"])

    subprocess.Popen(
        [
            "docker",
            "run",
            "--rm",
            "--network",
            DOCKER_NETWORK,
            *env_args,
            *volume_args,
            WORKER_IMAGE,
            agent,
            session_id,
            task_id,
        ],
    )
    log.info("Spawned %s worker container for task %s", agent, task_id)
