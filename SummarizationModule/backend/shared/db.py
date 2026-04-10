import json
import os
import sqlite3
import sys
from typing import Any


def _resolve_sessions_dir() -> str:
    """Return a writable sessions directory, aware of PyInstaller frozen mode."""
    if getattr(sys, "frozen", False):
        return os.path.join(os.path.dirname(sys.executable), "sessions_module3")
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "sessions")


SESSIONS_DIR = _resolve_sessions_dir()
os.makedirs(SESSIONS_DIR, exist_ok=True)


def _db_path(session_id: str) -> str:
    safe = "".join(c for c in session_id if c.isalnum() or c in "-_")
    return os.path.join(SESSIONS_DIR, f"{safe}.sqlite")


def get_session_db(session_id: str) -> sqlite3.Connection:
    path = _db_path(session_id)
    conn = sqlite3.connect(path, check_same_thread=False)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA foreign_keys=ON")
    _ensure_meta(conn)
    return conn


def session_exists(session_id: str) -> bool:
    return os.path.isfile(_db_path(session_id))


def _ensure_meta(conn: sqlite3.Connection):
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _meta "
        "(key TEXT PRIMARY KEY, value TEXT)"
    )
    conn.commit()


def get_meta(conn: sqlite3.Connection, key: str) -> Any | None:
    row = conn.execute(
        "SELECT value FROM _meta WHERE key = ?", (key,)
    ).fetchone()
    if row is None:
        return None
    try:
        return json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return row[0]


def set_meta(conn: sqlite3.Connection, key: str, value: Any):
    serialized = json.dumps(value) if not isinstance(value, str) else value
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
        (key, serialized),
    )
    conn.commit()


def delete_meta(conn: sqlite3.Connection, key: str):
    conn.execute("DELETE FROM _meta WHERE key = ?", (key,))
    conn.commit()


def get_all_meta_keys(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT key FROM _meta").fetchall()
    return [r[0] for r in rows]


def delete_session(session_id: str):
    path = _db_path(session_id)
    if os.path.isfile(path):
        os.remove(path)
