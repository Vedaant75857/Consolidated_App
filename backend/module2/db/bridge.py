"""Bridge between DuckDB tables and pandas DataFrames.

Provides the two-way conversion that lets agents keep receiving DataFrames
while data is persisted in DuckDB between requests.
"""

from __future__ import annotations

import pandas as pd

from .duckdb_compat import DuckDBConnection
from .table_ops import quote_id, table_exists, store_df_native, drop_table


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


def sqlite_to_df(conn: DuckDBConnection, table_name: str, limit: int | None = None) -> pd.DataFrame | None:
    """Load a DuckDB table into a pandas DataFrame.

    Args:
        conn: An open DuckDB-backed session connection.
        table_name: The physical table name in the database.
        limit: Optional row limit (useful for previews).

    Returns:
        A DataFrame, or None if the table does not exist.
    """
    if not table_exists(conn, table_name):
        return None

    tbl = quote_id(table_name)
    query = f"SELECT * FROM {tbl}" if limit is None else f"SELECT * FROM {tbl} LIMIT {int(limit)}"
    return conn._conn.execute(query).df()


def df_to_sqlite(
    conn: DuckDBConnection,
    table_name: str,
    df: pd.DataFrame,
    commit: bool = True,
) -> int:
    """Save a pandas DataFrame to a DuckDB table (drop + create).

    Uses DuckDB's native DataFrame ingestion — no Python row iteration.
    All values are cast to VARCHAR to match Module 1 conventions.

    Args:
        conn: An open DuckDB-backed session connection.
        table_name: The physical table name to write.
        df: The DataFrame to persist.
        commit: Whether to commit after writing.

    Returns:
        The number of rows written.
    """
    return store_df_native(conn, table_name, df, commit=commit, as_varchar=True)
