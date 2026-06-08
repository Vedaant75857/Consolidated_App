"""Test-suite setup: redirect session DBs to a temp folder before any imports.

This must run before ``shared.db.session_db`` is first imported, so that
``_resolve_sessions_dir()`` picks up the temp directory and tests never
touch the developer's real ``.sessions/`` folder.
"""

from __future__ import annotations

import os
import shutil
import tempfile
import atexit
import time
import uuid

_creator_temp = os.path.join(
    os.environ.get("PUBLIC", r"C:\Users\Public"),
    "Documents",
    "Wondershare",
    "CreatorTemp",
)
_TEST_SESSIONS_ROOT = os.path.join(
    _creator_temp if os.path.isdir(_creator_temp) else (os.environ.get("LOCALAPPDATA") or tempfile.gettempdir()),
    "ProcIP",
    "test-sessions",
)
os.makedirs(_TEST_SESSIONS_ROOT, exist_ok=True)
_TEST_SESSIONS_DIR = os.path.join(
    _TEST_SESSIONS_ROOT,
    f"module1_test_sessions_{int(time.time())}_{os.getpid()}_{uuid.uuid4().hex[:8]}",
)
os.makedirs(_TEST_SESSIONS_DIR, exist_ok=True)
os.environ.setdefault("SESSION_DB_DIR", _TEST_SESSIONS_DIR)


def _cleanup_test_sessions_dir() -> None:
    try:
        from shared.db import cleanup_all_sessions

        cleanup_all_sessions()
    except Exception:
        pass
    shutil.rmtree(_TEST_SESSIONS_DIR, ignore_errors=True)


atexit.register(_cleanup_test_sessions_dir)


def pytest_sessionfinish(session, exitstatus):  # noqa: ARG001
    _cleanup_test_sessions_dir()
