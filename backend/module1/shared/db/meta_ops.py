"""JSON metadata storage in the session database's ``meta`` table."""

from __future__ import annotations

import json
from typing import Any, TypeVar

from shared.utils.json_helpers import json_default, json_safe
from .duckdb_compat import DuckDBConnection

T = TypeVar("T")


def get_meta(conn: DuckDBConnection, key: str, default: Any = None) -> Any:
    """Retrieve a JSON-encoded value from the meta table.

    Args:
        conn: DuckDB session connection.
        key: The meta key to look up.
        default: Value returned when the key is missing or cannot be decoded.

    Returns:
        The decoded JSON value, or *default*.
    """
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    if not row:
        return default
    try:
        return json.loads(row["value"])
    except (json.JSONDecodeError, TypeError):
        return default


def set_meta(conn: DuckDBConnection, key: str, value: Any, commit: bool = True) -> None:
    """Store a value as JSON in the meta table.

    Args:
        conn: DuckDB session connection.
        key: The meta key.
        value: Any JSON-serialisable value.
        commit: Whether to commit after the upsert.
    """
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
