"""Worker spawn backends — subprocess, docker, or cloud (future)."""

import logging
import os
import subprocess
import sys
from pathlib import Path

log = logging.getLogger("skitter.spawn")

SPAWN_MODE = os.environ.get("SKITTER_SPAWN_MODE", "subprocess")
WORKER_IMAGE = os.environ.get("SKITTER_WORKER_IMAGE", "skitter-worker:latest")
DOCKER_NETWORK = os.environ.get("SKITTER_DOCKER_NETWORK", "skitter")
DOCKER_USER_HOME = "/home/skitter"


def spawn_worker(agent: str, session_id: str, task: str) -> None:
    if SPAWN_MODE == "fly":
        _spawn_fly(agent, session_id, task)
    elif SPAWN_MODE == "docker":
        _spawn_docker(agent, session_id, task)
    else:
        _spawn_subprocess(agent, session_id, task)


def worker_env() -> dict[str, str]:
    """Build env for worker processes — strip CLAUDECODE, prefer OAuth over API key."""
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    if env.get("CLAUDE_CODE_OAUTH_TOKEN"):
        env.pop("ANTHROPIC_API_KEY", None)
    return env


def _spawn_subprocess(agent: str, session_id: str, task: str) -> None:
    env = worker_env()
    subprocess.Popen(
        [sys.executable, "-m", "skitter.worker", agent, session_id, task],
        env=env,
    )
    log.info("Spawned %s worker subprocess for task %s", agent, task)


def _spawn_docker(agent: str, session_id: str, task: str) -> None:
    env = worker_env()
    env_args: list[str] = []
    env_args.extend(
        ["-e", f"MQTT_HOST={os.environ.get('SKITTER_DOCKER_MQTT_HOST', 'emqx')}"]
    )
    env_args.extend(["-e", f"MQTT_PORT={os.environ.get('MQTT_PORT', '1883')}"])
    for key in (
        "MQTT_TLS",
        "MQTT_USER",
        "MQTT_PASS",
        "ANTHROPIC_API_KEY",
        "CLAUDE_CODE_OAUTH_TOKEN",
        "OPENAI_API_KEY",
    ):
        val = env.get(key, "")
        if val:
            env_args.extend(["-e", f"{key}={val}"])

    # Mount docker-claude dir (agent definitions + memory)
    # Created by `skitter docker sync`
    from skitter.config import DOCKER_CLAUDE_DIR

    volume_args: list[str] = []
    if DOCKER_CLAUDE_DIR.is_dir():
        volume_args.extend(["-v", f"{DOCKER_CLAUDE_DIR}:{DOCKER_USER_HOME}/.claude:ro"])

    # Mount rclone config for persistent workspaces
    rclone_config = Path.home() / ".config" / "rclone" / "rclone.conf"
    if rclone_config.is_file():
        volume_args.extend(
            ["-v", f"{rclone_config}:{DOCKER_USER_HOME}/.config/rclone/rclone.conf:ro"]
        )

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
            task,
        ],
    )
    log.info("Spawned %s worker container for task %s", agent, task)


def _spawn_fly(agent: str, session_id: str, task: str) -> None:
    from skitter.fly import create_worker

    result = create_worker(agent, session_id, task)
    log.info("Created Fly worker machine %s for task %s", result.get("id", "?"), task)
