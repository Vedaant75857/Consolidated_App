"""
Core table CRUD operations against DuckDB.

All data is stored as VARCHAR columns. Bulk operations use DuckDB's native
DataFrame ingestion when possible; row-streaming is the fallback for generators.
"""

from __future__ import annotations

from typing import Any, Iterator

import pandas as pd

from .duckdb_compat import DuckDBConnection


def quote_id(name: str) -> str:
    """Double-quote a SQL identifier, escaping embedded quotes."""
    return '"' + name.replace('"', '""') + '"'


def normalize_for_match(expr: str) -> str:
    """SQL expression that normalizes a value for join-key matching.

    Handles case folding, whitespace trimming, and numeric format differences
    (e.g. "123", "123.0", "00123" all normalize to the same value).

    Uses DuckDB's TRY_CAST and regexp_matches instead of SQLite GLOB/CAST.
    """
    return (
        f"LOWER(TRIM(CASE "
        f"WHEN regexp_matches(TRIM({expr}), '^[0-9eE.+-]+$') "
        f"THEN CAST(TRY_CAST(TRIM({expr}) AS DOUBLE) AS VARCHAR) "
        f"ELSE {expr} END))"
    )


def store_table(conn: DuckDBConnection, table_name: str, rows: list[dict[str, Any]]) -> None:
    """Store a list of row-dicts as a DuckDB table. Drops any existing table first.

    Args:
        conn: DuckDB session connection.
        table_name: Target table name.
        rows: List of dictionaries, each representing one row.
    """
    if not rows:
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
        conn.commit()
        return

    first = rows[0]
    if not isinstance(first, dict) or not first:
        return

    columns = list(first.keys())
    col_defs = ", ".join(f"{quote_id(c)} VARCHAR" for c in columns)
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
    conn: DuckDBConnection,
    table_name: str,
    columns: list[str],
    row_iterator: Iterator,
    commit: bool = True,
) -> int:
    """Stream rows from an iterator directly into a DuckDB table.

    Never materialises the full dataset in memory. Returns the number of rows inserted.

    Args:
        conn: DuckDB session connection.
        table_name: Target table name.
        columns: Ordered column names.
        row_iterator: Yields rows as dicts, tuples, or lists.
        commit: Whether to commit after all inserts.

    Returns:
        Number of rows inserted.
    """
    if not columns:
        return 0

    col_defs = ", ".join(f"{quote_id(c)} VARCHAR" for c in columns)
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


def store_df_native(
    conn: DuckDBConnection,
    table_name: str,
    df: pd.DataFrame,
    *,
    commit: bool = True,
    as_varchar: bool = True,
) -> int:
    """Persist a DataFrame using DuckDB's zero-copy register path.

    Avoids Python row iteration entirely — orders of magnitude faster than
    store_table_streaming for large DataFrames.

    Args:
        conn: DuckDB session connection.
        table_name: Target table name (will be dropped first if it exists).
        df: The pandas DataFrame to store.
        commit: Whether to commit after the write.
        as_varchar: If True, cast all columns to VARCHAR for consistency.

    Returns:
        Number of rows written.
    """
    if df is None or df.empty:
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
        if commit:
            conn.commit()
        return 0

    view_name = f"_tmp_df_{id(df)}"
    raw_conn = conn._conn
    raw_conn.register(view_name, df)
    try:
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
        if as_varchar:
            cols = ", ".join(
                f"CAST({quote_id(str(c))} AS VARCHAR) AS {quote_id(str(c))}"
                for c in df.columns
            )
            conn.execute(
                f"CREATE TABLE {quote_id(table_name)} AS "
                f"SELECT {cols} FROM {quote_id(view_name)}"
            )
        else:
            conn.execute(
                f"CREATE TABLE {quote_id(table_name)} AS "
                f"SELECT * FROM {quote_id(view_name)}"
            )
        if commit:
            conn.commit()
    finally:
        raw_conn.unregister(view_name)
    return len(df)


def read_table(conn: DuckDBConnection, table_name: str, limit: int | None = None) -> list[dict]:
    """Read all (or up to limit) rows from a table as a list of dicts.

    Args:
        conn: DuckDB session connection.
        table_name: Table to read.
        limit: Optional max rows.

    Returns:
        List of row dicts.
    """
    if not table_exists(conn, table_name):
        return []
    tbl = quote_id(table_name)
    if limit is not None:
        rows = conn.execute(f"SELECT * FROM {tbl} LIMIT ?", (limit,)).fetchall()
    else:
        rows = conn.execute(f"SELECT * FROM {tbl}").fetchall()
    return [dict(zip(r.keys(), r)) for r in rows]


def read_table_columns(conn: DuckDBConnection, table_name: str) -> list[str]:
    """Return ordered column names for a table.

    Uses DuckDB's information_schema instead of SQLite's PRAGMA table_info.
    """
    if not table_exists(conn, table_name):
        return []
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position",
        (table_name,),
    ).fetchall()
    return [r["column_name"] for r in rows]


def table_exists(conn: DuckDBConnection, table_name: str) -> bool:
    """Check whether a table exists in the database.

    Uses DuckDB's information_schema instead of sqlite_master.
    """
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE table_name = ?",
        (table_name,),
    ).fetchone()
    return row is not None


def drop_table(conn: DuckDBConnection, table_name: str, *, commit: bool = True) -> None:
    """Drop a table if it exists."""
    conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
    if commit:
        conn.commit()


def table_row_count(conn: DuckDBConnection, table_name: str) -> int:
    """Return the number of rows in a table, or 0 if the table does not exist."""
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


def iterate_table(conn: DuckDBConnection, table_name: str) -> Iterator[dict]:
    """Iterate rows without loading them all into memory."""
    cursor = conn.execute(f"SELECT * FROM {quote_id(table_name)}")
    cols = [desc[0] for desc in cursor.description]
    for row in cursor:
        yield dict(zip(cols, row))
