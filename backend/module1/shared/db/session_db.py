"""
Session-based DuckDB database management.

Each user session gets its own on-disk .duckdb file in a per-run temp directory.
Connections are cached for reuse within the same process.
"""

import logging
import os
import re
import time
import threading
from collections import OrderedDict

from .duckdb_compat import DuckDBConnection, duckdb_connect
from ..runtime_temp import (
    cleanup_module_runtime_dir,
    cleanup_stale_runtime_dirs,
    sessions_dir_for,
)

_logger = logging.getLogger(__name__)
_MODULE_NAME = "module1"
_MODULE_SESSION_ENV = "MODULE1_SESSION_DB_DIR"
_RUNTIME_MANAGED = not (
    (os.environ.get(_MODULE_SESSION_ENV) or "").strip()
    or (os.environ.get("SESSION_DB_DIR") or "").strip()
)


def _resolve_sessions_dir() -> str:
    """Return the writable session DB directory for this process."""
    explicit = os.environ.get(_MODULE_SESSION_ENV)
    if explicit and explicit.strip():
        return os.path.abspath(explicit.strip())
    legacy_root = os.environ.get("SESSION_DB_DIR")
    if legacy_root and legacy_root.strip():
        return os.path.abspath(os.path.join(legacy_root.strip(), _MODULE_NAME))
    cleanup_stale_runtime_dirs()
    return sessions_dir_for(_MODULE_NAME)


_DB_DIR = _resolve_sessions_dir()
try:
    os.makedirs(_DB_DIR, exist_ok=True)
except OSError as exc:
    _logger.error(
        "Cannot create session folder at %s (%s). "
        "Try moving the EXE to a writable location like your Desktop.",
        _DB_DIR, exc,
    )
    raise

_db_cache: "OrderedDict[str, DuckDBConnection]" = OrderedDict()
_db_lock = threading.Lock()
_MAX_CACHE = max(1, int(os.getenv("SESSION_DB_MAX_CACHE", "50")))

_SESSION_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")

# Per-session locks to prevent concurrent DuckDB access on the same connection
_SESSION_LOCK_GUARD = threading.Lock()
_SESSION_LOCKS: dict[str, threading.RLock] = {}


def get_session_lock(session_id: str) -> threading.RLock:
    """Return a reentrant lock for the given session (created on first use)."""
    with _SESSION_LOCK_GUARD:
        lock = _SESSION_LOCKS.get(session_id)
        if lock is None:
            lock = threading.RLock()
            _SESSION_LOCKS[session_id] = lock
        return lock


def _open_connection(db_path: str) -> DuckDBConnection:
    """Open a new DuckDB connection and ensure the schema tables exist."""
    conn = duckdb_connect(db_path)
    conn.execute("CREATE TABLE IF NOT EXISTS meta (key VARCHAR PRIMARY KEY, value VARCHAR)")
    conn.execute("""CREATE TABLE IF NOT EXISTS table_registry (
        table_key VARCHAR PRIMARY KEY,
        sql_name  VARCHAR NOT NULL
    )""")
    conn.commit()
    # The connection is not published in the cache yet, so this initial legacy
    # cleanup is exclusive. Cached-session callers can use the locked wrapper.
    from .table_ops import sanitize_reserved_provenance_columns

    sanitize_reserved_provenance_columns(conn)
    return conn


def get_session_db(session_id: str) -> DuckDBConnection:
    """Return a cached DuckDB connection for the given session, creating one if needed.

    If the cached connection is broken (closed or stale), a fresh connection
    is transparently created.

    Args:
        session_id: Alphanumeric session identifier.

    Returns:
        A DuckDBConnection wrapping the underlying DuckDB engine.

    Raises:
        ValueError: If the session ID format is invalid.
    """
    if not _SESSION_ID_RE.match(session_id):
        raise ValueError("Invalid session ID format.")

    db_path = os.path.join(_DB_DIR, f"{session_id}.duckdb")

    with _db_lock:
        if session_id in _db_cache:
            conn = _db_cache[session_id]
            try:
                conn.execute("SELECT 1").fetchone()
            except Exception:
                _logger.warning("Cached connection for session %s is stale; reopening", session_id)
                try:
                    conn.close()
                except Exception:
                    pass
                _db_cache.pop(session_id, None)
                # Fall through to create a new connection below
            else:
                _db_cache.move_to_end(session_id, last=True)
                return conn

        conn = _open_connection(db_path)

        _db_cache[session_id] = conn
        _db_cache.move_to_end(session_id, last=True)
        # Evict oldest entries but do NOT close them — they may still be in
        # use by a streaming response or long-running operation in another
        # thread.  The Python GC will close the underlying DuckDB handle
        # once all references are dropped.
        while len(_db_cache) > _MAX_CACHE:
            evict_id, _evict_conn = _db_cache.popitem(last=False)
            _logger.debug("Evicted session %s from connection cache (not closed)", evict_id)
        return conn


def sanitize_session_reserved_columns(session_id: str) -> dict:
    """Run the idempotent legacy provenance cleanup under the session lock."""
    from .table_ops import sanitize_reserved_provenance_columns

    with get_session_lock(session_id):
        return sanitize_reserved_provenance_columns(get_session_db(session_id))


def close_session_db(session_id: str) -> None:
    """Close and remove a cached connection for the given session."""
    with _db_lock:
        conn = _db_cache.pop(session_id, None)
    if conn:
        try:
            conn.close()
        except Exception:
            pass


def delete_session_db(session_id: str) -> None:
    """Close the connection and delete all DuckDB files for a session."""
    close_session_db(session_id)
    db_path = os.path.join(_DB_DIR, f"{session_id}.duckdb")
    for suffix in ("", ".wal"):
        try:
            os.unlink(db_path + suffix)
        except OSError:
            pass


def safe_table_name(prefix: str, key: str) -> str:
    """Build a safe SQL table name from a prefix and arbitrary key string."""
    sanitised = re.sub(r"[^a-zA-Z0-9]", "_", key)
    return f"{prefix}__{sanitised}"[:120]


def register_table(conn: DuckDBConnection, table_key: str, sql_name: str, commit: bool = True) -> None:
    """Record a logical table_key -> physical sql_name mapping.

    Args:
        conn: DuckDB session connection.
        table_key: Logical key for the table.
        sql_name: Physical SQL table name.
        commit: Whether to commit after the insert.
    """
    conn.execute(
        "INSERT OR REPLACE INTO table_registry (table_key, sql_name) VALUES (?, ?)",
        (table_key, sql_name),
    )
    if commit:
        conn.commit()


def unregister_table(conn: DuckDBConnection, table_key: str, commit: bool = True) -> None:
    """Remove a table_key from the registry."""
    conn.execute("DELETE FROM table_registry WHERE table_key = ?", (table_key,))
    if commit:
        conn.commit()


def lookup_sql_name(conn: DuckDBConnection, table_key: str) -> str | None:
    """Look up the physical SQL table name for a logical table_key."""
    row = conn.execute(
        "SELECT sql_name FROM table_registry WHERE table_key = ?", (table_key,)
    ).fetchone()
    return row["sql_name"] if row else None


def all_registered_tables(conn: DuckDBConnection) -> list[dict]:
    """Return all registered table mappings as a list of dicts."""
    rows = conn.execute("SELECT table_key, sql_name FROM table_registry").fetchall()
    return [{"table_key": r["table_key"], "sql_name": r["sql_name"]} for r in rows]


def cleanup_stale_sessions(max_age_ms: int = 24 * 60 * 60 * 1000) -> int:
    """Delete session databases older than max_age_ms. Returns count deleted."""
    now = time.time() * 1000
    cleaned = 0
    try:
        for f in os.listdir(_DB_DIR):
            if not f.endswith(".duckdb"):
                continue
            fpath = os.path.join(_DB_DIR, f)
            try:
                mtime_ms = os.path.getmtime(fpath) * 1000
                if now - mtime_ms > max_age_ms:
                    session_id = f.replace(".duckdb", "")
                    delete_session_db(session_id)
                    cleaned += 1
            except OSError:
                pass
    except OSError:
        pass
    return cleaned


def cleanup_all_sessions() -> int:
    """Close cached connections, delete session files, and remove runtime temp."""
    cleaned = 0
    with _db_lock:
        for sid, conn in list(_db_cache.items()):
            try:
                conn.close()
            except Exception:
                pass
        _db_cache.clear()
    with _SESSION_LOCK_GUARD:
        _SESSION_LOCKS.clear()

    try:
        for f in os.listdir(_DB_DIR):
            fpath = os.path.join(_DB_DIR, f)
            if not os.path.isfile(fpath):
                continue
            try:
                os.unlink(fpath)
                cleaned += 1
            except OSError:
                pass
    except OSError:
        pass

    if _RUNTIME_MANAGED:
        cleanup_module_runtime_dir(_MODULE_NAME)

    return cleaned


DB_DIR = _DB_DIR
