"""Config loading backends — filesystem (default) or R2 (cloud)."""

import json
import logging
import os

from skitter.config import (
    CARDS_DIR,
    AgentDef,
    WorkflowDef,
    WorkflowTask,
    infer_task_next,
    load_agents as _load_fs,
    load_workflows as _load_fs_workflows,
)

log = logging.getLogger("skitter.storage")

STORAGE_MODE = os.environ.get("SKITTER_STORAGE", "filesystem")

# R2 config (S3-compatible)
R2_ENDPOINT = os.environ.get("R2_ENDPOINT", "")
R2_ACCESS_KEY = os.environ.get("R2_ACCESS_KEY", "")
R2_SECRET_KEY = os.environ.get("R2_SECRET_KEY", "")
R2_BUCKET = os.environ.get("R2_BUCKET", "skitter")


_r2_client = None


def _get_r2_client():
    """Lazy-import boto3 and return cached S3 client for R2."""
    global _r2_client
    if _r2_client is None:
        import boto3

        _r2_client = boto3.client(
            "s3",
            endpoint_url=R2_ENDPOINT,
            aws_access_key_id=R2_ACCESS_KEY,
            aws_secret_access_key=R2_SECRET_KEY,
            region_name="auto",
        )
    return _r2_client


def _r2_list_objects(prefix: str) -> list[dict]:
    """List objects in R2 bucket with given prefix. Assumes < 1000 objects."""
    resp = _get_r2_client().list_objects_v2(Bucket=R2_BUCKET, Prefix=prefix)
    return resp.get("Contents", [])


def _r2_get_text(key: str) -> str:
    """Get object content as text from R2."""
    resp = _get_r2_client().get_object(Bucket=R2_BUCKET, Key=key)
    return resp["Body"].read().decode()


def _load_agents_r2() -> dict[str, AgentDef]:
    """Load agent YAML stubs from R2 (config/agents/*.yaml as JSON)."""
    import yaml

    agents: dict[str, AgentDef] = {}
    from skitter.config import load_default_runtime

    default_runtime = load_default_runtime()
    for obj in _r2_list_objects("config/agents/"):
        key = obj["Key"]
        if not key.endswith(".yaml"):
            continue
        agent_id = key.rsplit("/", 1)[-1].removesuffix(".yaml")
        try:
            data = yaml.safe_load(_r2_get_text(key))
            if not isinstance(data, dict):
                continue
            agents[agent_id] = AgentDef(
                id=agent_id,
                name=data.get("name", agent_id),
                description=data.get("description", ""),
                runtime=data.get("runtime", "") or default_runtime,
                workspace=data.get("workspace", ""),
            )
        except Exception as e:
            log.warning("Failed to load agent %s from R2: %s", key, e)
    return agents


def _load_workflows_r2() -> dict[str, WorkflowDef]:
    """Load workflow definitions from R2 (config/workflows/*.yaml as JSON)."""
    import yaml

    workflows: dict[str, WorkflowDef] = {}
    for obj in _r2_list_objects("config/workflows/"):
        key = obj["Key"]
        if not key.endswith(".yaml"):
            continue
        wf_id = key.rsplit("/", 1)[-1].removesuffix(".yaml")
        try:
            data = yaml.safe_load(_r2_get_text(key))
            if not isinstance(data, dict):
                continue
            tasks = []
            for t in data.get("tasks", []):
                tasks.append(
                    WorkflowTask(
                        id=t.get("id", ""),
                        agent=t.get("agent", "worker"),
                        description=t.get("description", ""),
                        next=t.get("next", ""),
                        needs=t.get("needs", []),
                        model=t.get("model", ""),
                    )
                )
            infer_task_next(tasks)
            workflows[wf_id] = WorkflowDef(
                id=wf_id,
                name=data.get("name", wf_id),
                description=data.get("description", ""),
                variables=data.get("variables", []),
                tasks=tasks,
            )
        except Exception as e:
            log.warning("Failed to load workflow %s from R2: %s", key, e)
    return workflows


def _load_cards_r2() -> dict[str, str]:
    """Load pre-built agent card JSON from R2 (cards/*.json)."""
    cards: dict[str, str] = {}
    for obj in _r2_list_objects("cards/"):
        key = obj["Key"]
        if not key.endswith(".json"):
            continue
        card_id = key.rsplit("/", 1)[-1].removesuffix(".json")
        try:
            card_json = _r2_get_text(key)
            json.loads(card_json)  # validate
            cards[card_id] = card_json
        except Exception as e:
            log.warning("Failed to load card %s from R2: %s", key, e)
    return cards


def load_agents() -> dict[str, AgentDef]:
    if STORAGE_MODE == "r2":
        return _load_agents_r2()
    if STORAGE_MODE == "filesystem":
        return _load_fs()
    raise ValueError(f"Unknown storage mode: {STORAGE_MODE}")


def load_workflows() -> dict[str, WorkflowDef]:
    if STORAGE_MODE == "r2":
        return _load_workflows_r2()
    if STORAGE_MODE == "filesystem":
        return _load_fs_workflows()
    raise ValueError(f"Unknown storage mode: {STORAGE_MODE}")


def load_cards() -> dict[str, str]:
    """Load pre-built agent card JSON files."""
    if STORAGE_MODE == "r2":
        return _load_cards_r2()
    # Filesystem mode
    cards: dict[str, str] = {}
    if not CARDS_DIR.is_dir():
        return cards
    for path in sorted(CARDS_DIR.glob("*.json")):
        try:
            card_data = json.loads(path.read_text())
            cards[path.stem] = json.dumps(card_data)
        except Exception:
            pass
    return cards
