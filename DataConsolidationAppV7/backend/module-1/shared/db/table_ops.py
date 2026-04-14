"""
Core table CRUD operations against SQLite.

All data is stored as TEXT columns. Rows are inserted in batches for performance.
"""

from __future__ import annotations

import sqlite3
from typing import Any, Iterator


def quote_id(name: str) -> str:
    return '"' + name.replace('"', '""') + '"'


def normalize_for_match(expr: str) -> str:
    """SQL expression that normalizes a value for join-key matching.

    Handles case folding, whitespace trimming, and numeric format differences
    (e.g. "123", "123.0", "00123" all normalize to the same value).
    """
    return (
        f"LOWER(TRIM(CASE "
        f"WHEN TRIM({expr}) GLOB '*[0-9]*' AND TRIM({expr}) NOT GLOB '*[^0-9.eE+-]*' "
        f"THEN CAST(CAST(TRIM({expr}) AS REAL) AS TEXT) "
        f"ELSE {expr} END))"
    )


def store_table(conn: sqlite3.Connection, table_name: str, rows: list[dict[str, Any]]) -> None:
    """Store a list of row-dicts as a SQLite table. Drops any existing table first."""
    if not rows:
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
        conn.commit()
        return

    first = rows[0]
    if not isinstance(first, dict) or not first:
        return

    columns = list(first.keys())
    col_defs = ", ".join(f"{quote_id(c)} TEXT" for c in columns)
    conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
    conn.execute(f"CREATE TABLE {quote_id(table_name)} ({col_defs})")

    placeholders = ", ".join("?" for _ in columns)
    quoted_cols = ", ".join(quote_id(c) for c in columns)
    sql = f"INSERT INTO {quote_id(table_name)} ({quoted_cols}) VALUES ({placeholders})"

    batch_size = 5000
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        conn.executemany(
            sql,
            [
                tuple(
                    None if row.get(c) is None else str(row[c])
                    for c in columns
                )
                for row in batch
            ],
        )
    conn.commit()


def store_table_streaming(
    conn: sqlite3.Connection,
    table_name: str,
    columns: list[str],
    row_iterator: Iterator,
    commit: bool = True,
) -> int:
    """Stream rows from an iterator directly into a SQLite table.

    Never materialises the full dataset in memory. Returns the number of rows inserted.
    """
    if not columns:
        return 0

    col_defs = ", ".join(f"{quote_id(c)} TEXT" for c in columns)
    conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
    conn.execute(f"CREATE TABLE {quote_id(table_name)} ({col_defs})")

    placeholders = ", ".join("?" for _ in columns)
    quoted_cols = ", ".join(quote_id(c) for c in columns)
    sql = f"INSERT INTO {quote_id(table_name)} ({quoted_cols}) VALUES ({placeholders})"

    total = 0
    batch: list[tuple] = []
    num_cols = len(columns)

    for raw_row in row_iterator:
        vals: list
        if isinstance(raw_row, dict):
            vals = [raw_row.get(c) for c in columns]
        else:
            vals = list(raw_row) if not isinstance(raw_row, list) else raw_row

        if len(vals) < num_cols:
            vals.extend([None] * (num_cols - len(vals)))
        elif len(vals) > num_cols:
            vals = vals[:num_cols]

        batch.append(tuple(None if v is None else str(v) for v in vals))
        if len(batch) >= 5000:
            conn.executemany(sql, batch)
            total += len(batch)
            batch.clear()

    if batch:
        conn.executemany(sql, batch)
        total += len(batch)

    if commit:
        conn.commit()
    return total


def read_table(conn: sqlite3.Connection, table_name: str, limit: int | None = None) -> list[dict]:
    if not table_exists(conn, table_name):
        return []
    tbl = quote_id(table_name)
    if limit is not None:
        rows = conn.execute(f"SELECT * FROM {tbl} LIMIT ?", (limit,)).fetchall()
    else:
        rows = conn.execute(f"SELECT * FROM {tbl}").fetchall()
    return [dict(r) for r in rows]


def read_table_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    if not table_exists(conn, table_name):
        return []
    rows = conn.execute(f"PRAGMA table_info({quote_id(table_name)})").fetchall()
    return [r["name"] for r in sorted(rows, key=lambda r: r["cid"])]


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    row = conn.execute(
        "SELECT 1 FROM sqlite_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    if row:
        return True
    row = conn.execute(
        "SELECT 1 FROM sqlite_temp_master WHERE type='table' AND name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def drop_table(conn: sqlite3.Connection, table_name: str, *, commit: bool = True) -> None:
    conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
    if commit:
        conn.commit()


def table_row_count(conn: sqlite3.Connection, table_name: str) -> int:
    if not table_exists(conn, table_name):
        return 0
    row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {quote_id(table_name)}").fetchone()
    return row["cnt"] if row else 0


PREVIEW_POOL = 1000


def pick_best_rows(rows: list[dict], limit: int) -> list[dict]:
    """Return up to *limit* rows ranked by number of populated columns.

    Selects the rows that have the most non-null, non-empty values so that
    data previews show the most informative rows instead of whatever
    happened to be first in insertion order.
    """
    if len(rows) <= limit:
        return rows

    def _score(row: dict) -> int:
        return sum(
            1 for v in row.values()
            if v is not None and str(v).strip() != ""
        )

    return sorted(rows, key=_score, reverse=True)[:limit]


def pick_best_raw_rows(rows: list[list], limit: int) -> list[list]:
    """Same as pick_best_rows but for list-of-lists (raw preview) format."""
    if len(rows) <= limit:
        return rows

    def _score(row: list) -> int:
        return sum(
            1 for v in row
            if v is not None and str(v).strip() != ""
        )

    return sorted(rows, key=_score, reverse=True)[:limit]


def iterate_table(conn: sqlite3.Connection, table_name: str) -> Iterator[dict]:
    """Iterate rows without loading them all into memory."""
    cursor = conn.execute(f"SELECT * FROM {quote_id(table_name)}")
    cols = [desc[0] for desc in cursor.description]
    for row in cursor:
        yield dict(zip(cols, row))
