"""
Session-based DuckDB database management.

Each user session gets its own on-disk .duckdb file in the .sessions/ directory.
Connections are cached for reuse within the same process.
"""

import os
import re
import sys
import time
import threading
from collections import OrderedDict

from .duckdb_compat import DuckDBConnection, duckdb_connect


def _resolve_sessions_dir() -> str:
    """Return the writable .sessions/ directory, aware of PyInstaller frozen mode."""
    explicit = os.environ.get("SESSION_DB_DIR")
    if explicit:
        return explicit
    if getattr(sys, "frozen", False):
        return os.path.join(os.path.dirname(sys.executable), ".sessions")
    return os.path.join(os.getcwd(), ".sessions")


_DB_DIR = _resolve_sessions_dir()
try:
    os.makedirs(_DB_DIR, exist_ok=True)
except OSError as exc:
    import logging as _logging
    _logging.getLogger(__name__).error(
        "Cannot create session folder at %s (%s). "
        "Try moving the EXE to a writable location like your Desktop.",
        _DB_DIR, exc,
    )
    raise

_db_cache: "OrderedDict[str, DuckDBConnection]" = OrderedDict()
_db_lock = threading.Lock()
_MAX_CACHE = max(1, int(os.getenv("SESSION_DB_MAX_CACHE", "50")))

_SESSION_ID_RE = re.compile(r"^[a-zA-Z0-9_-]+$")


def get_session_db(session_id: str) -> DuckDBConnection:
    """Return a cached DuckDB connection for the given session, creating one if needed.

    Args:
        session_id: Alphanumeric session identifier.

    Returns:
        A DuckDBConnection wrapping the underlying DuckDB engine.

    Raises:
        ValueError: If the session ID format is invalid.
    """
    if not _SESSION_ID_RE.match(session_id):
        raise ValueError("Invalid session ID format.")
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

        db_path = os.path.join(_DB_DIR, f"{session_id}.duckdb")
        conn = duckdb_connect(db_path)

        conn.execute("CREATE TABLE IF NOT EXISTS meta (key VARCHAR PRIMARY KEY, value VARCHAR)")
        conn.execute("""CREATE TABLE IF NOT EXISTS table_registry (
            table_key VARCHAR PRIMARY KEY,
            sql_name  VARCHAR NOT NULL
        )""")
        conn.commit()

        _db_cache[session_id] = conn
        _db_cache.move_to_end(session_id, last=True)
        while len(_db_cache) > _MAX_CACHE:
            evict_id, evict_conn = _db_cache.popitem(last=False)
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
    """Close every cached connection and delete all session DuckDB files."""
    cleaned = 0
    with _db_lock:
        for sid, conn in list(_db_cache.items()):
            try:
                conn.close()
            except Exception:
                pass
        _db_cache.clear()

    try:
        for f in os.listdir(_DB_DIR):
            if not f.endswith((".duckdb", ".duckdb.wal")):
                continue
            try:
                os.unlink(os.path.join(_DB_DIR, f))
                if f.endswith(".duckdb"):
                    cleaned += 1
            except OSError:
                pass
    except OSError:
        pass
    return cleaned


DB_DIR = _DB_DIR
