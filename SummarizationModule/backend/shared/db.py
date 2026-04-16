import json
import os
import sys
import threading
from collections import OrderedDict
from typing import Any

from shared.duckdb_compat import DuckDBConnection, duckdb_connect


def _resolve_sessions_dir() -> str:
    """Return a writable sessions directory, aware of PyInstaller frozen mode."""
    if getattr(sys, "frozen", False):
        return os.path.join(os.path.dirname(sys.executable), "sessions_module3")
    return os.path.join(os.path.dirname(os.path.dirname(__file__)), "sessions")


SESSIONS_DIR = _resolve_sessions_dir()
try:
    os.makedirs(SESSIONS_DIR, exist_ok=True)
except OSError as exc:
    import logging as _logging
    _logging.getLogger(__name__).error(
        "Cannot create session folder at %s (%s). "
        "Try moving the EXE to a writable location like your Desktop.",
        SESSIONS_DIR, exc,
    )
    raise

_db_cache: "OrderedDict[str, DuckDBConnection]" = OrderedDict()
_db_lock = threading.Lock()
_MAX_CACHE = max(1, int(os.getenv("SESSION_DB_MAX_CACHE", "50")))

_session_locks: dict[str, threading.RLock] = {}
_session_locks_guard = threading.Lock()


def get_session_lock(session_id: str) -> threading.RLock:
    """Return a per-session reentrant lock for serializing DuckDB access."""
    with _session_locks_guard:
        lock = _session_locks.get(session_id)
        if lock is None:
            lock = threading.RLock()
            _session_locks[session_id] = lock
        return lock


def db_path(session_id: str) -> str:
    safe = "".join(c for c in session_id if c.isalnum() or c in "-_")
    return os.path.join(SESSIONS_DIR, f"{safe}.duckdb")


def get_session_db(session_id: str) -> DuckDBConnection:
    """Return a cached DuckDB connection for the given session.

    Reuses an existing connection from the LRU cache when possible, creating
    a new one only when necessary. Evicts the oldest connection when the
    cache exceeds _MAX_CACHE entries.
    """
    with _db_lock:
        if session_id in _db_cache:
            conn = _db_cache[session_id]
            try:
                conn.execute("SELECT 1").fetchone()
            except Exception:
                try:
                    conn.close()
                except Exception:
                    pass
                _db_cache.pop(session_id, None)
            else:
                _db_cache.move_to_end(session_id, last=True)
                return conn

        path = db_path(session_id)
        conn = duckdb_connect(path)
        _ensure_meta(conn)

        _db_cache[session_id] = conn
        _db_cache.move_to_end(session_id, last=True)
        while len(_db_cache) > _MAX_CACHE:
            _, evict_conn = _db_cache.popitem(last=False)
            try:
                evict_conn.close()
            except Exception:
                pass
        return conn


def close_session_db(session_id: str) -> None:
    """Close and remove a cached connection for the given session."""
    with _db_lock:
        conn = _db_cache.pop(session_id, None)
    if conn:
        try:
            conn.close()
        except Exception:
            pass


def session_exists(session_id: str) -> bool:
    return os.path.isfile(db_path(session_id))


def _ensure_meta(conn: DuckDBConnection):
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _meta "
        "(key VARCHAR PRIMARY KEY, value VARCHAR)"
    )
    conn.commit()


def get_meta(conn: DuckDBConnection, key: str) -> Any | None:
    row = conn.execute(
        "SELECT value FROM _meta WHERE key = ?", (key,)
    ).fetchone()
    if row is None:
        return None
    try:
        return json.loads(row[0])
    except (json.JSONDecodeError, TypeError):
        return row[0]


def set_meta(conn: DuckDBConnection, key: str, value: Any):
    serialized = json.dumps(value) if not isinstance(value, str) else value
    conn.execute(
        "INSERT OR REPLACE INTO _meta (key, value) VALUES (?, ?)",
        (key, serialized),
    )
    conn.commit()


def delete_meta(conn: DuckDBConnection, key: str):
    conn.execute("DELETE FROM _meta WHERE key = ?", (key,))
    conn.commit()


def get_all_meta_keys(conn: DuckDBConnection) -> list[str]:
    rows = conn.execute("SELECT key FROM _meta").fetchall()
    return [r[0] for r in rows]


def delete_session(session_id: str):
    """Close the connection and delete all session files (DB + WAL)."""
    close_session_db(session_id)
    path = db_path(session_id)
    for suffix in ("", ".wal"):
        try:
            os.unlink(path + suffix)
        except OSError:
            pass


def cleanup_all_sessions() -> int:
    """Close every cached connection and delete all files in the sessions dir."""
    cleaned = 0
    with _db_lock:
        for sid, conn in list(_db_cache.items()):
            try:
                conn.close()
            except Exception:
                pass
        _db_cache.clear()

    try:
        for f in os.listdir(SESSIONS_DIR):
            fpath = os.path.join(SESSIONS_DIR, f)
            if not os.path.isfile(fpath):
                continue
            try:
                os.unlink(fpath)
                cleaned += 1
            except OSError:
                pass
    except OSError:
        pass
    return cleaned
