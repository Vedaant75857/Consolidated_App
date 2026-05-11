"""Test-suite setup: redirect session DBs to a temp folder before any imports.

This must run before ``shared.db.session_db`` is first imported, so that
``_resolve_sessions_dir()`` picks up the temp directory and tests never
touch the developer's real ``.sessions/`` folder.
"""

from __future__ import annotations

import os
import tempfile

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
_TEST_SESSIONS_DIR = tempfile.mkdtemp(
    prefix="module1_test_sessions_",
    dir=_TEST_SESSIONS_ROOT,
)
os.environ.setdefault("SESSION_DB_DIR", _TEST_SESSIONS_DIR)
