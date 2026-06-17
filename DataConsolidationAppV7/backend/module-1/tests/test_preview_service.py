"""Unit tests for preview_operations service."""

from __future__ import annotations

import uuid

import pytest

from preview_operations import service as preview_ops
from shared.db import get_session_db, quote_id, register_table, safe_table_name


@pytest.fixture
def preview_session():
    """Create a session with a seeded table for preview operations."""
    session_id = uuid.uuid4().hex
    conn = get_session_db(session_id)
    table_key = "sample.csv::Sheet1"
    sql_name = safe_table_name("tbl", table_key)
    tq = quote_id(sql_name)

    conn.execute(
        f"CREATE TABLE {tq} ("
        f"{quote_id('FILE_NAME')} VARCHAR, "
        f"{quote_id('SUPPLIER NAME')} VARCHAR, "
        f"{quote_id('AMOUNT')} DOUBLE)"
    )
    conn.execute(
        f"INSERT INTO {tq} VALUES "
        "('file_a.xls', 'Alpha Co', 100), "
        "('file_a.xls', 'Beta Co', 200), "
        "('file_b.xls', 'Gamma Co', 50), "
        "('file_b.xls', NULL, 25)"
    )
    register_table(conn, table_key, sql_name)
    conn.commit()
    return conn, table_key


def test_get_preview_data_returns_rows_and_total(preview_session):
    """get_preview_data must return paginated rows without using fetchdf."""
    conn, table_key = preview_session

    result = preview_ops.get_preview_data(conn, table_key, limit=2, offset=0)

    assert result["totalRows"] == 4
    assert len(result["rows"]) == 2
    assert "FILE_NAME" in result["columns"]
    assert "__row_id" not in result["columns"]
    assert "__row_id" in result["rows"][0]
    assert result["rows"][0]["__row_id"] == 1


def test_get_preview_data_sort_and_filter(preview_session):
    """Sort and filter parameters affect returned rows."""
    conn, table_key = preview_session

    filtered = preview_ops.get_preview_data(
        conn,
        table_key,
        filters=[{"column": "FILE_NAME", "op": "eq", "value": "file_a.xls"}],
    )
    assert filtered["totalRows"] == 2
    assert all(r["FILE_NAME"] == "file_a.xls" for r in filtered["rows"])

    sorted_desc = preview_ops.get_preview_data(
        conn,
        table_key,
        sort=[{"column": "AMOUNT", "dir": "desc"}],
    )
    amounts = [r["AMOUNT"] for r in sorted_desc["rows"]]
    assert amounts == sorted(amounts, reverse=True)


def test_get_preview_data_in_filter_with_blanks(preview_session):
    """IN filter with includeBlanks returns matching values and null rows."""
    conn, table_key = preview_session

    filtered = preview_ops.get_preview_data(
        conn,
        table_key,
        filters=[{
            "column": "SUPPLIER NAME",
            "op": "in",
            "values": ["Alpha Co"],
            "includeBlanks": True,
        }],
    )
    assert filtered["totalRows"] == 2
    suppliers = {r["SUPPLIER NAME"] for r in filtered["rows"]}
    assert suppliers == {"Alpha Co", None}


def test_get_column_filter_values_respects_other_filters(preview_session):
    """Column value list must reflect filters on other columns."""
    conn, table_key = preview_session

    result = preview_ops.get_column_filter_values(
        conn,
        table_key,
        "SUPPLIER NAME",
        filters=[{"column": "FILE_NAME", "op": "eq", "value": "file_a.xls"}],
    )

    assert result["totalDistinct"] == 2
    assert set(result["values"]) == {"Alpha Co", "Beta Co"}
    assert result["hasBlanks"] is False


def test_get_column_filter_values_includes_blanks_flag(preview_session):
    """hasBlanks is true when null/empty values exist for the column."""
    conn, table_key = preview_session

    result = preview_ops.get_column_filter_values(
        conn,
        table_key,
        "SUPPLIER NAME",
    )

    assert result["hasBlanks"] is True
    assert "Gamma Co" in result["values"]


def test_create_pivot_without_column_fields(preview_session):
    """Row-only pivot must include value aggregation columns."""
    conn, table_key = preview_session

    result = preview_ops.create_pivot(
        conn,
        table_key,
        row_fields=["FILE_NAME"],
        column_fields=[],
        value_fields=[{"field": "SUPPLIER NAME", "aggregation": "count"}],
    )

    assert result["success"] is True
    assert "SUPPLIER NAME_count" in result["columns"]
    assert result["totalRows"] == 2

    pivot_key = result["newTableKey"]
    pivot_data = preview_ops.get_preview_data(conn, pivot_key)
    assert pivot_data["totalRows"] == 2
    count_col = "SUPPLIER NAME_count"
    by_file = {r["FILE_NAME"]: r[count_col] for r in pivot_data["rows"]}
    assert by_file["file_a.xls"] == 2
    assert by_file["file_b.xls"] == 1


@pytest.fixture
def varchar_preview_session():
    """Session with VARCHAR numeric-like columns (pipeline default)."""
    session_id = uuid.uuid4().hex
    conn = get_session_db(session_id)
    table_key = "orders.xlsx::Base"
    sql_name = safe_table_name("tbl", table_key)
    tq = quote_id(sql_name)

    conn.execute(
        f"CREATE TABLE {tq} ("
        f"{quote_id('PO_NO')} VARCHAR, "
        f"{quote_id('BUYER')} VARCHAR)"
    )
    conn.execute(
        f"INSERT INTO {tq} VALUES ('100', 'Alice'), ('200', 'Bob'), ('300', 'Alice')"
    )
    register_table(conn, table_key, sql_name)
    conn.commit()
    return conn, table_key


def test_column_delete_blocks_last_column(preview_session):
    """Deleting the only remaining data column must be rejected."""
    conn, table_key = preview_session

    preview_ops.column_delete(conn, table_key, "FILE_NAME")
    preview_ops.column_delete(conn, table_key, "SUPPLIER NAME")
    with pytest.raises(ValueError, match="Cannot delete the last column"):
        preview_ops.column_delete(conn, table_key, "AMOUNT")


def test_rows_delete_rebuilds_row_ids(preview_session):
    """rows_delete removes rows and renumbers __row_id sequentially."""
    conn, table_key = preview_session

    preview_ops.get_preview_data(conn, table_key)
    result = preview_ops.rows_delete(conn, table_key, [2])

    assert result["totalRows"] == 3
    row_ids = [r["__row_id"] for r in result["rows"]]
    assert row_ids == [1, 2, 3]
    assert all(r["SUPPLIER NAME"] != "Beta Co" for r in result["rows"])


def test_create_pivot_sum_on_varchar_column(varchar_preview_session):
    """SUM on VARCHAR columns must succeed via TRY_CAST."""
    conn, table_key = varchar_preview_session

    result = preview_ops.create_pivot(
        conn,
        table_key,
        row_fields=["BUYER"],
        column_fields=[],
        value_fields=[{"field": "PO_NO", "aggregation": "sum"}],
    )

    assert result["success"] is True
    assert "PO_NO_sum" in result["columns"]

    pivot_data = preview_ops.get_preview_data(conn, result["newTableKey"])
    by_buyer = {r["BUYER"]: r["PO_NO_sum"] for r in pivot_data["rows"]}
    assert by_buyer["Alice"] == 400
    assert by_buyer["Bob"] == 200


def test_get_preview_data_returns_column_types(varchar_preview_session):
    """Preview response must include inferred columnTypes map."""
    conn, table_key = varchar_preview_session

    result = preview_ops.get_preview_data(conn, table_key)

    assert "columnTypes" in result
    assert result["columnTypes"]["PO_NO"] == "DOUBLE"
    assert result["columnTypes"]["BUYER"] == "TEXT"


def test_sort_varchar_po_no_numeric_order(varchar_preview_session):
    """Sorting PO_NO must use numeric order when inferred as DOUBLE."""
    conn, table_key = varchar_preview_session

    conn.execute(
        f"DELETE FROM {quote_id(preview_ops.lookup_sql_name(conn, table_key))}"
    )
    tq = quote_id(preview_ops.lookup_sql_name(conn, table_key))
    conn.execute(
        f"INSERT INTO {tq} ({quote_id('PO_NO')}, {quote_id('BUYER')}) VALUES "
        "('9', 'A'), ('100', 'B'), ('20', 'C')"
    )
    conn.commit()

    result = preview_ops.get_preview_data(
        conn,
        table_key,
        sort=[{"column": "PO_NO", "dir": "asc"}],
    )
    values = [r["PO_NO"] for r in result["rows"]]
    assert values == ["9", "20", "100"]


@pytest.fixture
def date_varchar_session():
    """Session with VARCHAR date-like columns."""
    session_id = uuid.uuid4().hex
    conn = get_session_db(session_id)
    table_key = "dates.xlsx::Base"
    sql_name = safe_table_name("tbl", table_key)
    tq = quote_id(sql_name)

    conn.execute(
        f"CREATE TABLE {tq} ("
        f"{quote_id('CREATION_DATE')} VARCHAR, "
        f"{quote_id('LABEL')} VARCHAR)"
    )
    conn.execute(
        f"INSERT INTO {tq} VALUES "
        "('2025-03-01 10:00:00', 'c'), "
        "('2025-01-15 08:00:00', 'a'), "
        "('2025-02-10 12:00:00', 'b')"
    )
    register_table(conn, table_key, sql_name)
    conn.commit()
    return conn, table_key


def test_sort_varchar_date_chronological(date_varchar_session):
    """Sorting CREATION_DATE must be chronological when inferred as DATE."""
    conn, table_key = date_varchar_session

    result = preview_ops.get_preview_data(
        conn,
        table_key,
        sort=[{"column": "CREATION_DATE", "dir": "asc"}],
    )
    labels = [r["LABEL"] for r in result["rows"]]
    assert labels == ["a", "b", "c"]


def test_column_change_type_updates_column_types_meta(varchar_preview_session):
    """column_change_type must persist override in columnTypes response."""
    conn, table_key = varchar_preview_session

    result = preview_ops.column_change_type(conn, table_key, "PO_NO", "DOUBLE")
    assert result["columnTypes"]["PO_NO"] == "DOUBLE"
