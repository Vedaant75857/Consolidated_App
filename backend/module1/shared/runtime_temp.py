"""Runtime temp directory helpers for ProcIP backend modules."""

from __future__ import annotations

import os
import shutil
import tempfile
import time
import uuid

_APP_NAME = "ProcIP"
_RUNTIME_DIR_NAME = "runtime"
_STALE_SECONDS = 24 * 60 * 60


def _candidate_runtime_roots() -> list[str]:
    roots: list[str] = []
    explicit = os.environ.get("PROCIP_RUNTIME_ROOT")
    if explicit:
        roots.append(explicit)

    public_root = os.environ.get("PUBLIC", r"C:\Users\Public")
    roots.append(
        os.path.join(
            public_root,
            "Documents",
            "Wondershare",
            "CreatorTemp",
            _APP_NAME,
            _RUNTIME_DIR_NAME,
        )
    )

    local_app_data = os.environ.get("LOCALAPPDATA")
    if local_app_data:
        roots.append(os.path.join(local_app_data, _APP_NAME, _RUNTIME_DIR_NAME))

    roots.append(os.path.join(tempfile.gettempdir(), _APP_NAME, _RUNTIME_DIR_NAME))
    return roots


def runtime_root() -> str:
    """Return the first writable runtime root, creating it if needed."""
    for root in _candidate_runtime_roots():
        try:
            os.makedirs(root, exist_ok=True)
            return os.path.abspath(root)
        except OSError:
            continue

    fallback = os.path.join(tempfile.gettempdir(), _APP_NAME, _RUNTIME_DIR_NAME)
    os.makedirs(fallback, exist_ok=True)
    return os.path.abspath(fallback)


def runtime_run_id() -> str:
    """Return the current process run id, shared by launcher-managed modules."""
    run_id = os.environ.get("PROCIP_RUN_ID")
    if not run_id:
        run_id = f"{int(time.time())}-{os.getpid()}-{uuid.uuid4().hex[:8]}"
        os.environ["PROCIP_RUN_ID"] = run_id

    safe = "".join(ch if ch.isalnum() or ch in "-_" else "_" for ch in run_id)
    if safe != run_id:
        os.environ["PROCIP_RUN_ID"] = safe
    return safe


def cleanup_stale_runtime_dirs(max_age_seconds: int = _STALE_SECONDS) -> int:
    """Delete abandoned per-run temp folders older than max_age_seconds."""
    cutoff = time.time() - max_age_seconds
    cleaned = 0
    root = runtime_root()

    try:
        entries = list(os.scandir(root))
    except OSError:
        return 0

    for entry in entries:
        try:
            if not entry.is_dir(follow_symlinks=False):
                continue
            if entry.stat(follow_symlinks=False).st_mtime >= cutoff:
                continue
            shutil.rmtree(entry.path, ignore_errors=True)
            cleaned += 1
        except OSError:
            continue

    return cleaned


def module_runtime_dir(module_name: str) -> str:
    path = os.path.join(runtime_root(), runtime_run_id(), module_name)
    os.makedirs(path, exist_ok=True)
    return path


def sessions_dir_for(module_name: str) -> str:
    path = os.path.join(module_runtime_dir(module_name), "sessions")
    os.makedirs(path, exist_ok=True)
    return path


def cleanup_module_runtime_dir(module_name: str) -> bool:
    """Remove this module's runtime dir and the run dir if it is empty."""
    if os.environ.get("SESSION_DB_DIR"):
        return False

    root = runtime_root()
    module_dir = os.path.join(root, runtime_run_id(), module_name)
    run_dir = os.path.dirname(module_dir)
    removed = False

    try:
        shutil.rmtree(module_dir, ignore_errors=True)
    except OSError:
        pass

    try:
        if os.path.isdir(run_dir) and not os.listdir(run_dir):
            os.rmdir(run_dir)
            removed = True
    except OSError:
        pass

    try:
        if os.path.isdir(root) and not os.listdir(root):
            os.rmdir(root)
            removed = True
    except OSError:
        pass

    return removed
