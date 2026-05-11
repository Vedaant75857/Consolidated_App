"""Tests for Excel loading into DuckDB (mixed text + dates in one column)."""

from __future__ import annotations

import io

import pandas as pd
import pytest

from data_loading.file_loader import _load_excel_sheet
from shared.db import duckdb_connect, quote_id, safe_table_name


@pytest.fixture
def memory_conn():
    """In-memory DuckDB with the same registry schema as session DB."""
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
    yield conn
    conn.close()


def test_excel_sheet_loads_mixed_date_and_label_text(memory_conn):
    """Literal header-like text in a date column must not fail timestamp cast."""
    buf = io.BytesIO()
    with pd.ExcelWriter(buf, engine="openpyxl") as writer:
        sheet = pd.DataFrame(
            [
                ["Invoice date", "Amount"],
                ["2025-01-15", "100"],
                ["Invoice date", "200"],
            ]
        )
        sheet.to_excel(writer, sheet_name="Sheet1", header=False, index=False)
    buf.seek(0)

    excel_file = pd.ExcelFile(buf, engine="calamine")
    try:
        warnings: list[dict] = []
        table_key = "test.xlsx::Sheet1"
        _load_excel_sheet(
            memory_conn,
            table_key,
            excel_file,
            "Sheet1",
            warnings,
            commit=True,
        )
    finally:
        excel_file.close()

    assert warnings == []

    tbl = safe_table_name("tbl", table_key)
    qcol = quote_id("INVOICE DATE")
    rows = memory_conn.execute(
        f"SELECT {qcol} AS d FROM {quote_id(tbl)} ORDER BY {quote_id('RECORD_ID')}"
    ).fetchall()
    values = [r["d"] for r in rows]
    assert values[0] == "2025-01-15"
    assert values[1] == "Invoice date"
