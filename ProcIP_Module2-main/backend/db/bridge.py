"""Bridge between SQLite tables and pandas DataFrames.

Provides the two-way conversion that lets agents keep receiving DataFrames
while data is persisted in SQLite between requests.
"""

from __future__ import annotations

import sqlite3

import pandas as pd

from .table_ops import quote_id, table_exists, store_table_streaming, drop_table


PREVIEW_POOL = 1000


def pick_best_rows(rows: list[dict], limit: int) -> list[dict]:
    """Return up to *limit* rows ranked by number of populated columns."""
    if len(rows) <= limit:
        return rows

    def _score(row: dict) -> int:
        return sum(
            1 for v in row.values()
            if v is not None and str(v).strip() != ""
        )

    return sorted(rows, key=_score, reverse=True)[:limit]


def pick_best_df_rows(df: pd.DataFrame, limit: int) -> pd.DataFrame:
    """Return the *limit* rows with the most non-null, non-empty values."""
    if df is None or len(df) <= limit:
        return df
    scores = df.apply(
        lambda row: sum(
            1 for v in row
            if pd.notna(v) and str(v).strip() != ""
        ), axis=1,
    )
    return df.loc[scores.nlargest(limit).index].reset_index(drop=True)


def sqlite_to_df(conn: sqlite3.Connection, table_name: str, limit: int | None = None) -> pd.DataFrame | None:
    """Load a SQLite table into a pandas DataFrame.

    Args:
        conn: An open SQLite connection.
        table_name: The physical table name in the database.
        limit: Optional row limit (useful for previews).

    Returns:
        A DataFrame, or None if the table does not exist.
    """
    if not table_exists(conn, table_name):
        return None

    tbl = quote_id(table_name)
    query = f"SELECT * FROM {tbl}" if limit is None else f"SELECT * FROM {tbl} LIMIT {int(limit)}"
    return pd.read_sql_query(query, conn)


def df_to_sqlite(
    conn: sqlite3.Connection,
    table_name: str,
    df: pd.DataFrame,
    commit: bool = True,
) -> int:
    """Save a pandas DataFrame to a SQLite table (drop + create).

    All values are stored as TEXT to match Module 1 conventions.

    Args:
        conn: An open SQLite connection.
        table_name: The physical table name to write.
        df: The DataFrame to persist.
        commit: Whether to commit after writing.

    Returns:
        The number of rows written.
    """
    if df is None or df.empty:
        drop_table(conn, table_name, commit=commit)
        return 0

    columns = [str(c) for c in df.columns]

    def _row_iter():
        for row in df.itertuples(index=False, name=None):
            yield [None if pd.isna(v) else v for v in row]

    return store_table_streaming(conn, table_name, columns, _row_iter(), commit=commit)
