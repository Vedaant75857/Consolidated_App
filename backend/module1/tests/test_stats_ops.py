"""Unit tests for shared.db.stats_ops."""

from __future__ import annotations

import uuid

from shared.db import (
    column_distinct_values,
    distinct_values_by_column_sql,
    get_overlap_sql,
    get_session_db,
    quote_id,
    register_table,
    safe_table_name,
)


def _create_table_with_row_id(
    conn,
    table_key: str,
    data_columns: list[str],
    rows: list[tuple],
) -> str:
    """Create a table mimicking post-preview state with BIGINT __row_id."""
    sql_name = safe_table_name("tbl", table_key)
    tq = quote_id(sql_name)
    col_defs = ", ".join(f"{quote_id(c)} VARCHAR" for c in data_columns)
    col_defs += f", {quote_id('__row_id')} BIGINT"
    conn.execute(f"CREATE TABLE {tq} ({col_defs})")

    quoted_cols = ", ".join(quote_id(c) for c in data_columns)
    quoted_cols += f", {quote_id('__row_id')}"
    placeholders = ", ".join("?" for _ in data_columns) + ", ?"
    for row in rows:
        conn.execute(
            f"INSERT INTO {tq} ({quoted_cols}) VALUES ({placeholders})",
            list(row),
        )
    register_table(conn, table_key, sql_name)
    conn.commit()
    return sql_name


def test_distinct_values_skips_row_id_bigint():
    """Profiling must ignore __row_id and not call TRIM on BIGINT."""
    session_id = uuid.uuid4().hex
    conn = get_session_db(session_id)
    table_key = "sample-a.xlsx::Base"
    sql_name = _create_table_with_row_id(
        conn,
        table_key,
        ["RECORD_ID", "Status"],
        [("1", "Open", 1), ("2", "Closed", 2)],
    )

    result = distinct_values_by_column_sql(conn, sql_name, max_per_col=50)

    assert "__row_id" not in result
    assert set(result.keys()) == {"RECORD_ID", "Status"}
    assert set(result["RECORD_ID"]) == {"1", "2"}
    assert set(result["Status"]) == {"Open", "Closed"}


def test_get_overlap_sql_ignores_row_id():
    """Overlap analysis must not treat __row_id as a common data column."""
    session_id = uuid.uuid4().hex
    conn = get_session_db(session_id)

    key_a = "file-a.xlsx::Base"
    key_b = "file-b.xlsx::Base"
    sql_a = _create_table_with_row_id(
        conn,
        key_a,
        ["PO Number", "Supplier"],
        [("1001", "Acme", 1), ("1002", "Beta", 2)],
    )
    sql_b = _create_table_with_row_id(
        conn,
        key_b,
        ["PO Number", "Supplier"],
        [("1001", "Acme", 1), ("1003", "Gamma", 2)],
    )

    table_map = [
        {"table_key": key_a, "sql_name": sql_a},
        {"table_key": key_b, "sql_name": sql_b},
    ]
    overlap = get_overlap_sql(conn, table_map)

    assert key_a in overlap
    assert key_b in overlap[key_a]
    metrics = overlap[key_a][key_b]
    assert metrics["common_column_count"] == 2
    assert metrics["column_name_overlap"] == 1.0
    assert metrics.get("value_overlap_avg") is not None


def test_non_blank_cond_works_on_bigint_column():
    """Direct distinct-value lookup on INTEGER/BIGINT columns must not raise."""
    session_id = uuid.uuid4().hex
    conn = get_session_db(session_id)
    table_key = "numeric.xlsx::Base"
    sql_name = safe_table_name("tbl", table_key)
    tq = quote_id(sql_name)

    conn.execute(
        f"CREATE TABLE {tq} ("
        f"{quote_id('Amount')} INTEGER, "
        f"{quote_id('__row_id')} BIGINT)"
    )
    conn.execute(
        f"INSERT INTO {tq} VALUES (100, 1), (200, 2), (NULL, 3)"
    )
    register_table(conn, table_key, sql_name)
    conn.commit()

    values = column_distinct_values(conn, sql_name, "Amount", limit=10)

    assert set(values) == {"100", "200"}
