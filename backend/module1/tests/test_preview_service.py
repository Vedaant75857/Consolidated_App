"""Unit tests for preview_operations service."""

from __future__ import annotations

import uuid
from typing import Any

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


@pytest.fixture
def synthetic_table_factory():
    """Create schema-agnostic VARCHAR-backed preview tables for tests."""
    def create_table(
        columns: list[str],
        rows: list[dict[str, Any]],
        table_key: str | None = None,
    ):
        session_id = uuid.uuid4().hex
        conn = get_session_db(session_id)
        key = table_key or f"synthetic-{uuid.uuid4().hex}.xlsx::Sheet1"
        sql_name = safe_table_name("tbl", key)
        tq = quote_id(sql_name)

        col_defs = ", ".join(f"{quote_id(c)} VARCHAR" for c in columns)
        conn.execute(f"CREATE TABLE {tq} ({col_defs})")

        if rows:
            quoted_cols = ", ".join(quote_id(c) for c in columns)
            placeholders = ", ".join("?" for _ in columns)
            conn.executemany(
                f"INSERT INTO {tq} ({quoted_cols}) VALUES ({placeholders})",
                [[row.get(c) for c in columns] for row in rows],
            )

        register_table(conn, key, sql_name)
        conn.commit()
        return conn, key

    return create_table


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


@pytest.mark.parametrize(
    "columns, rows, expression, data_type, expected",
    [
        (
            ["A", "B"],
            [{"A": "2", "B": "3"}, {"A": "bad", "B": "5"}],
            "[A] * [B]",
            "DOUBLE",
            [6.0, None],
        ),
        (
            ["RECORD_ID", "AMOUNT"],
            [{"RECORD_ID": "9", "AMOUNT": "10"}, {"RECORD_ID": "10", "AMOUNT": ""}],
            "[RECORD_ID] * [AMOUNT]",
            "INTEGER",
            [90, None],
        ),
        (
            ["PO Number", "Quantity"],
            [{"PO Number": "1001", "Quantity": "2"}, {"PO Number": "1002", "Quantity": "3"}],
            "[PO Number] * [Quantity]",
            "DOUBLE",
            [2002.0, 3006.0],
        ),
        (
            ["Amount Received", "Exchange Rate"],
            [
                {"Amount Received": "12.5", "Exchange Rate": "2"},
                {"Amount Received": "not available", "Exchange Rate": "4"},
            ],
            "[Amount Received] * [Exchange Rate]",
            "DOUBLE",
            [25.0, None],
        ),
        (
            ["GROSS AMT", "MYR AMT"],
            [
                {"GROSS AMT": "2,797.76", "MYR AMT": "2,797.76"},
                {"GROSS AMT": "1,000.50", "MYR AMT": "500.25"},
            ],
            "[GROSS AMT] / [MYR AMT]",
            "DOUBLE",
            [1.0, 2.0],
        ),
        (
            ["GROSS AMT", "MYR AMT"],
            [{"GROSS AMT": "2797.76", "MYR AMT": "2797.76"}],
            "[GROSS AMT] / [MYR AMT]",
            "INTEGER",
            [1],
        ),
    ],
)
def test_calculated_column_numeric_formulas_are_schema_agnostic(
    synthetic_table_factory,
    columns,
    rows,
    expression,
    data_type,
    expected,
):
    """Calculated numeric formulas must work across arbitrary uploaded schemas."""
    conn, table_key = synthetic_table_factory(columns, rows)

    result = preview_ops.add_calculated_column(
        conn,
        table_key,
        "Calc Value",
        expression,
        data_type,
    )

    assert [row["Calc Value"] for row in result["rows"]] == expected


def test_calculated_column_uses_table_context_not_column_default(synthetic_table_factory):
    """Row-scoped formulas must not be evaluated as DuckDB column defaults."""
    conn, table_key = synthetic_table_factory(
        ["RECORD_ID", "Amount Received"],
        [{"RECORD_ID": "1", "Amount Received": "10"}],
    )

    result = preview_ops.add_calculated_column(
        conn,
        table_key,
        "Testing",
        "[RECORD_ID] * [Amount Received]",
        "INTEGER",
    )

    assert result["rows"][0]["Testing"] == 10


def test_failed_calculated_column_removes_partial_column(synthetic_table_factory):
    """If population fails after ADD COLUMN, the partial column is removed."""
    conn, table_key = synthetic_table_factory(
        ["Text Value"],
        [{"Text Value": "abc"}],
    )

    with pytest.raises(ValueError, match="Invalid calculated column expression"):
        preview_ops.add_calculated_column(
            conn,
            table_key,
            "Broken Calc",
            "LOWER([Text Value])",
            "INTEGER",
        )

    sql_name = preview_ops.lookup_sql_name(conn, table_key)
    assert "Broken Calc" not in preview_ops.read_table_columns(conn, sql_name)


@pytest.mark.parametrize(
    "columns, rows, target_column, filters, expected_values, has_blanks",
    [
        (
            ["RECORD_ID", "Status"],
            [{"RECORD_ID": "1", "Status": "Open"}, {"RECORD_ID": "2", "Status": "Closed"}],
            "RECORD_ID",
            None,
            {"1", "2"},
            False,
        ),
        (
            ["PO Number", "Supplier Name"],
            [
                {"PO Number": "1001", "Supplier Name": "Acme"},
                {"PO Number": "1002", "Supplier Name": "Beta"},
            ],
            "Supplier Name",
            [{"column": "PO Number", "op": "in", "values": ["1001"], "includeBlanks": False}],
            {"Acme"},
            False,
        ),
        (
            ["Amount Received", "Business Unit"],
            [
                {"Amount Received": "10", "Business Unit": "150"},
                {"Amount Received": "", "Business Unit": "150GA"},
                {"Amount Received": None, "Business Unit": "200"},
            ],
            "Amount Received",
            None,
            {"10"},
            True,
        ),
    ],
)
def test_get_column_filter_values_are_schema_agnostic(
    synthetic_table_factory,
    columns,
    rows,
    target_column,
    filters,
    expected_values,
    has_blanks,
):
    """Filter values must come from the active table schema, whatever its shape."""
    conn, table_key = synthetic_table_factory(columns, rows)

    result = preview_ops.get_column_filter_values(
        conn,
        table_key,
        target_column,
        filters=filters,
    )

    assert set(result["values"]) == expected_values
    assert result["hasBlanks"] is has_blanks
    assert result["totalDistinct"] == len(expected_values)


def test_filter_validation_reports_missing_columns(synthetic_table_factory):
    """Stale filter state should return a validation error, not a DuckDB binder error."""
    conn, table_key = synthetic_table_factory(
        ["Current Column", "Value"],
        [{"Current Column": "A", "Value": "1"}],
    )
    stale_filter = [{"column": "Missing Column", "op": "eq", "value": "A"}]

    with pytest.raises(ValueError, match="Column not found: Missing Column"):
        preview_ops.get_preview_data(conn, table_key, filters=stale_filter)

    with pytest.raises(ValueError, match="Column not found: Missing Column"):
        preview_ops.get_column_filter_values(
            conn,
            table_key,
            "Current Column",
            filters=stale_filter,
        )


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


def test_rows_delete_preserves_surviving_row_ids(preview_session):
    """rows_delete removes rows without changing immutable survivor identities."""
    conn, table_key = preview_session

    preview_ops.get_preview_data(conn, table_key)
    result = preview_ops.rows_delete(conn, table_key, [2])

    assert result["totalRows"] == 3
    row_ids = [r["__row_id"] for r in result["rows"]]
    assert row_ids == [1, 3, 4]
    assert all(r["SUPPLIER NAME"] != "Beta Co" for r in result["rows"])


def test_preview_mutations_reject_internal_identity(preview_session):
    """No public column operation may mutate, rename, or reference __row_id."""
    conn, table_key = preview_session
    preview_ops.get_preview_data(conn, table_key)

    rejected = [
        lambda: preview_ops.cell_edit(conn, table_key, 1, "__row_id", 99),
        lambda: preview_ops.column_rename(conn, table_key, "__row_id", "Identity"),
        lambda: preview_ops.column_rename(conn, table_key, "AMOUNT", "__source_table"),
        lambda: preview_ops.column_delete(conn, table_key, "__row_id"),
        lambda: preview_ops.column_reorder(conn, table_key, ["__row_id"]),
        lambda: preview_ops.column_change_type(conn, table_key, "__row_id", "TEXT"),
        lambda: preview_ops.add_calculated_column(
            conn, table_key, "__row_id", "[AMOUNT]", "DOUBLE",
        ),
        lambda: preview_ops.add_calculated_column(
            conn, table_key, "Identity Copy", "[__row_id]", "INTEGER",
        ),
        lambda: preview_ops.create_pivot(
            conn,
            table_key,
            ["__row_id"],
            [],
            [{"field": "AMOUNT", "aggregation": "sum"}],
        ),
    ]
    for operation in rejected:
        with pytest.raises(ValueError):
            operation()


@pytest.mark.parametrize(
    "expression",
    [
        "__row_id",
        "CAST(__row_id AS VARCHAR)",
        "TRIM(_source_table)",
        'CAST("__row_id" AS VARCHAR)',
    ],
)
def test_calculated_expression_rejects_bare_internal_identifiers(
    preview_session,
    expression,
):
    conn, table_key = preview_session
    preview_ops.get_preview_data(conn, table_key)

    with pytest.raises(ValueError, match="internal column"):
        preview_ops.add_calculated_column(
            conn, table_key, "Safe Output", expression, "TEXT",
        )


def test_calculated_expression_allows_internal_text_literal():
    parsed = preview_ops._parse_calc_expression("'__row_id'", ["Amount"])
    assert parsed == "'__row_id'"


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


def test_create_pivot_sum_on_comma_formatted_amounts(synthetic_table_factory):
    """SUM on comma-formatted VARCHAR amounts must aggregate correctly."""
    conn, table_key = synthetic_table_factory(
        ["FILE_NAME", "GROSS AMT"],
        [
            {"FILE_NAME": "file_a.xls", "GROSS AMT": "1,000.50"},
            {"FILE_NAME": "file_a.xls", "GROSS AMT": "500.25"},
            {"FILE_NAME": "file_b.xls", "GROSS AMT": "2,000"},
        ],
    )

    result = preview_ops.create_pivot(
        conn,
        table_key,
        row_fields=["FILE_NAME"],
        column_fields=[],
        value_fields=[{"field": "GROSS AMT", "aggregation": "sum"}],
    )

    pivot_data = preview_ops.get_preview_data(conn, result["newTableKey"])
    by_file = {r["FILE_NAME"]: r["GROSS AMT_sum"] for r in pivot_data["rows"]}
    assert by_file["file_a.xls"] == 1500.75
    assert by_file["file_b.xls"] == 2000


def test_create_pivot_rejects_all_null_value_results(synthetic_table_factory):
    """Pivot on non-numeric value fields must fail with a clear error."""
    conn, table_key = synthetic_table_factory(
        ["FILE_NAME", "STATUS"],
        [
            {"FILE_NAME": "file_a.xls", "STATUS": "Open"},
            {"FILE_NAME": "file_b.xls", "STATUS": "Closed"},
        ],
    )

    with pytest.raises(ValueError, match="no numeric results"):
        preview_ops.create_pivot(
            conn,
            table_key,
            row_fields=["FILE_NAME"],
            column_fields=[],
            value_fields=[{"field": "STATUS", "aggregation": "sum"}],
        )


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
