"""Unit tests for value_distribution multi-column fill rate tables."""

from __future__ import annotations

import uuid

from shared.db import get_session_db, quote_id

from data_quality_assessment.currency_analysis import run_currency_analysis_sql
from data_quality_assessment.value_distribution import (
    compute_value_distribution_table,
    merge_identified_columns,
)
from data_quality_assessment.column_resolver import find_currency_columns


def _create_test_table(conn, table_name: str, rows: list[tuple[str, str, str]]) -> None:
    """Create a table with two currency columns and three rows."""
    tq = quote_id(table_name)
    conn.execute(
        f"CREATE TABLE {tq} ("
        f"{quote_id('Local Currency Code')} VARCHAR, "
        f"{quote_id('PO Local Currency Code')} VARCHAR, "
        f"{quote_id('Notes')} VARCHAR)"
    )
    for local, po, notes in rows:
        conn.execute(
            f"INSERT INTO {tq} VALUES (?, ?, ?)",
            [local, po, notes],
        )
    conn.commit()


def test_compute_value_distribution_column_and_within_column_fill_rates():
    """Column fill rates use all rows; cell rates use filled rows only."""
    session_id = uuid.uuid4().hex
    conn = get_session_db(session_id)
    table_name = "test_currency"
    _create_test_table(
        conn,
        table_name,
        [
            ("USD", "USD", "a"),
            ("GBP", None, "b"),
            ("EUR", "EUR", "c"),
        ],
    )

    result = compute_value_distribution_table(
        conn,
        table_name,
        ["Local Currency Code", "PO Local Currency Code"],
    )

    assert result["exists"] is True
    col_by_name = {c["name"]: c["fillRate"] for c in result["columns"]}
    assert col_by_name["Local Currency Code"] == 100.0
    assert col_by_name["PO Local Currency Code"] == 66.67

    usd_row = next(r for r in result["rows"] if r["value"] == "USD")
    assert usd_row["fillRates"]["Local Currency Code"] == 33.33
    assert usd_row["fillRates"]["PO Local Currency Code"] == 50.0

    gbp_row = next(r for r in result["rows"] if r["value"] == "GBP")
    assert gbp_row["fillRates"]["Local Currency Code"] == 33.33
    assert gbp_row["fillRates"]["PO Local Currency Code"] is None

    eur_row = next(r for r in result["rows"] if r["value"] == "EUR")
    assert eur_row["fillRates"]["Local Currency Code"] == 33.33
    assert eur_row["fillRates"]["PO Local Currency Code"] == 50.0


def test_merge_identified_columns_falls_back_to_rule_based():
    """Empty AI list still discovers columns via rule-based resolver."""
    available = {"Local Currency Code", "PO Local Currency Code", "Notes"}
    merged = merge_identified_columns(None, available, find_currency_columns)
    assert "Local Currency Code" in merged
    assert "PO Local Currency Code" in merged
    assert "Notes" not in merged


def test_merge_identified_columns_uses_ai_only_when_provided():
    """Non-empty AI list is used without adding rule-based extras."""
    available = {"Local Currency Code", "PO Local Currency Code", "Notes"}
    merged = merge_identified_columns(
        ["Local Currency Code"], available, find_currency_columns,
    )
    assert merged == ["Local Currency Code"]


def test_currency_analysis_sql_uses_identified_columns():
    """Currency panel SQL returns distributionTable for AI-identified columns."""
    session_id = uuid.uuid4().hex
    conn = get_session_db(session_id)
    table_name = "test_currency_panel"
    _create_test_table(
        conn,
        table_name,
        [("USD", "EUR", "x"), ("GBP", "GBP", "y")],
    )

    result = run_currency_analysis_sql(
        conn,
        table_name,
        identified_columns=["Local Currency Code"],
    )

    table = result["distributionTable"]
    assert table["exists"] is True
    assert len(table["columns"]) == 1
    assert table["columns"][0]["name"] == "Local Currency Code"
    assert len(table["rows"]) == 2
