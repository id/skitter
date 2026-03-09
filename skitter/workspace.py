"""Persistent workspace support — rclone sync for remote workers, local mount for subprocess."""

import asyncio
import logging
from pathlib import Path

from skitter.config import WORKSPACES_DIR, load_workspace_config

log = logging.getLogger("skitter.workspace")


def resolve_workspace(workspace_slug: str, spawn_mode: str) -> tuple[Path, str]:
    """Return (local_path, remote_path) for a workspace slug.

    For subprocess mode with a local_mount configured, the local path points
    directly to the mounted folder (e.g. Google Drive). Otherwise, uses
    WORKSPACES_DIR and rclone handles sync.

    remote_path is empty when local_mount is used (no rclone needed).
    """
    cfg = load_workspace_config()

    if spawn_mode == "subprocess" and cfg.local_mount:
        local = Path(cfg.local_mount) / cfg.base_path / workspace_slug
        local.mkdir(parents=True, exist_ok=True)
        return local, ""

    # Docker / Fly / subprocess without local_mount — use local staging dir
    local = WORKSPACES_DIR / workspace_slug
    local.mkdir(parents=True, exist_ok=True)

    if not cfg.remote:
        raise RuntimeError(
            f"Workspace '{workspace_slug}' declared but no rclone remote configured "
            "in ~/.skitter/config.yaml (workspace.remote)"
        )

    remote = f"{cfg.remote}:{cfg.base_path}/{workspace_slug}"
    return local, remote


async def sync_down(remote_path: str, local_path: Path) -> None:
    """rclone sync remote -> local. No-op if remote_path is empty."""
    if not remote_path:
        return
    log.info("sync_down: %s -> %s", remote_path, local_path)
    proc = await asyncio.create_subprocess_exec(
        "rclone",
        "sync",
        remote_path,
        str(local_path),
        "--create-empty-src-dirs",
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    if proc.returncode != 0:
        msg = stderr.decode().strip()
        if proc.returncode == 3:  # directory not found — first run
            log.info("sync_down: remote dir not found (first run), starting fresh")
        else:
            raise RuntimeError(f"sync_down failed (rc={proc.returncode}): {msg}")


async def sync_up(local_path: Path, remote_path: str) -> bool:
    """rclone sync local -> remote. No-op if remote_path is empty. Retries once on failure.

    Returns True on success (or no-op), False on failure.
    """
    if not remote_path:
        return True
    log.info("sync_up: %s -> %s", local_path, remote_path)
    for attempt in range(2):
        proc = await asyncio.create_subprocess_exec(
            "rclone",
            "sync",
            str(local_path),
            remote_path,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        _, stderr = await proc.communicate()
        if proc.returncode == 0:
            return True
        msg = stderr.decode().strip()
        if attempt == 0:
            log.warning("sync_up failed, retrying: %s", msg)
        else:
            log.error("sync_up failed after retry: %s", msg)
    return False
