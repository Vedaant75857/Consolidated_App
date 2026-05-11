"""Tests for get_raw_array_from_table (raw header picker grid)."""

from __future__ import annotations

from data_loading.file_loader import get_raw_array_from_table
from shared.db import duckdb_connect, quote_id, safe_table_name


def test_get_raw_array_from_table_rectangular_rows():
    """Returned rows must be list-of-lists with consistent width (no DictRow skew)."""
    conn = duckdb_connect(":memory:")
    conn.execute(
        "CREATE TABLE IF NOT EXISTS meta (key VARCHAR PRIMARY KEY, value VARCHAR)"
    )
    conn.execute(
        """CREATE TABLE IF NOT EXISTS table_registry (
            table_key VARCHAR PRIMARY KEY,
            sql_name  VARCHAR NOT NULL
        )"""
    )
    conn.commit()

    table_key = "demo.xlsx::Sheet1"
    raw_name = safe_table_name("raw", table_key)
    q = quote_id(raw_name)
    conn.execute(
        f"CREATE TABLE {q} ("
        f'{quote_id("RAW_1")} VARCHAR, {quote_id("RAW_2")} VARCHAR, {quote_id("RAW_3")} VARCHAR'
        f")"
    )
    conn.execute(
        f"INSERT INTO {q} VALUES ('a', 'b', 'c'), ('1', '2', '3'), (NULL, '', 'x')"
    )
    conn.commit()

    rows = get_raw_array_from_table(conn, table_key, limit=50)
    assert len(rows) == 3
    widths = {len(r) for r in rows}
    assert widths == {3}
    assert rows[0] == ["a", "b", "c"]
    assert rows[1][0] == "1" and rows[1][1] == "2" and rows[1][2] == "3"

    conn.close()
