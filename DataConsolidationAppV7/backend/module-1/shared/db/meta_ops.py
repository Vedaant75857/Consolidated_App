"""JSON metadata storage in the session database's `meta` table."""

from __future__ import annotations

import json
import sqlite3
from typing import Any, TypeVar

from shared.utils.json_helpers import json_default, json_safe

T = TypeVar("T")


def get_meta(conn: sqlite3.Connection, key: str, default: Any = None) -> Any:
    row = conn.execute("SELECT value FROM meta WHERE key = ?", (key,)).fetchone()
    if not row:
        return default
    try:
        return json.loads(row["value"])
    except (json.JSONDecodeError, TypeError):
        return default


def set_meta(conn: sqlite3.Connection, key: str, value: Any, commit: bool = True) -> None:
    safe_value = json_safe(value)
    conn.execute(
        "INSERT OR REPLACE INTO meta (key, value) VALUES (?, ?)",
        (key, json.dumps(safe_value, default=json_default)),
    )
    if commit:
        conn.commit()


def delete_meta(conn: sqlite3.Connection, key: str) -> None:
    conn.execute("DELETE FROM meta WHERE key = ?", (key,))
    conn.commit()


def get_all_meta_keys(conn: sqlite3.Connection) -> list[str]:
    rows = conn.execute("SELECT key FROM meta").fetchall()
    return [r["key"] for r in rows]
