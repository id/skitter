"""Config loading backends — filesystem (default) or R2 (cloud)."""

import json
import logging
import os
import tempfile
from pathlib import Path

from skitter.config import (
    CARDS_DIR,
    AgentDef,
    WorkflowDef,
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


def _r2_download_to_dir(prefix: str, dest: Path, ext: str) -> None:
    """Download R2 objects matching prefix+ext into a local directory."""
    dest.mkdir(parents=True, exist_ok=True)
    for obj in _r2_list_objects(prefix):
        key = obj["Key"]
        if not key.endswith(ext):
            continue
        filename = key.rsplit("/", 1)[-1]
        (dest / filename).write_text(_r2_get_text(key))


def _load_agents_r2() -> dict[str, AgentDef]:
    """Load agent YAML stubs from R2, reusing filesystem parser."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dest = Path(tmpdir) / "agents"
        _r2_download_to_dir("config/agents/", dest, ".yaml")
        return _load_fs(agents_dir=dest)


def _load_workflows_r2() -> dict[str, WorkflowDef]:
    """Load workflow definitions from R2, reusing filesystem parser."""
    with tempfile.TemporaryDirectory() as tmpdir:
        dest = Path(tmpdir) / "workflows"
        _r2_download_to_dir("config/workflows/", dest, ".yaml")
        return _load_fs_workflows(workflows_dir=dest)


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
