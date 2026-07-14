"""JSON metadata storage in the session database's `meta` table."""

from __future__ import annotations

import json
import math
from datetime import datetime, date, time
from typing import Any, TypeVar

from .duckdb_compat import DuckDBConnection

T = TypeVar("T")


def json_default(value: Any) -> Any:
    """Convert common non-JSON-native Python values into JSON-safe forms."""
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    raise TypeError(f"Object of type {type(value).__name__} is not JSON serializable")


def json_safe(value: Any) -> Any:
    """Recursively convert nested values into JSON-safe primitives."""
    if isinstance(value, float) and (math.isnan(value) or math.isinf(value)):
        return None
    if isinstance(value, dict):
        return {str(k): json_safe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [json_safe(v) for v in value]
    if isinstance(value, tuple):
        return [json_safe(v) for v in value]
    if isinstance(value, set):
        return [json_safe(v) for v in value]
    if isinstance(value, (datetime, date, time)):
        return value.isoformat()
    return value


def get_meta(conn: DuckDBConnection, key: str, default: Any = None) -> Any:
    """Retrieve a JSON-encoded value from the meta table."""
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    if not row:
        return default
    try:
        return json.loads(row["value"])
    except (json.JSONDecodeError, TypeError):
        return default


def set_meta(conn: DuckDBConnection, key: str, value: Any, commit: bool = True) -> None:
    """Store a value as JSON in the meta table."""
    safe_value = json_safe(value)
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        (key, json.dumps(safe_value, default=json_default)),
    )
    if commit:
        conn.commit()


def delete_meta(conn: DuckDBConnection, key: str) -> None:
    """Remove a key from the meta table."""
    conn.execute("DELETE FROM meta WHERE key = ?", (key,))
    conn.commit()


def get_all_meta_keys(conn: DuckDBConnection) -> list[str]:
    """Return all keys present in the meta table."""
    rows = conn.execute("SELECT key FROM meta").fetchall()
    return [r["key"] for r in rows]
