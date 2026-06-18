"""CAPEX / OPEX Spend — classification from an explicit indicator or GL account column.

Uses only a dedicated CAPEX/OPEX flag column and/or GL account column mapped in
``analysis_data``. Description and taxonomy columns are never scanned.

Rules (deterministic, no AI):
  - CAPEX: values that explicitly indicate CAPEX (``CAPEX``, ``Y``, ``1``, etc.)
  - OPEX: every other non-empty value in that column (including ``OPEX``, ``0``,
    ``N``, and any other label)
  - Empty / null values are excluded from both buckets
"""

from __future__ import annotations

import logging
from typing import Any

from shared.duckdb_compat import DuckDBConnection

from services.spend_quality_assessment.analysis_data import (
    field_mapped_with_data,
    get_cast_report_fields,
    get_mapped_field_keys,
    list_analysis_columns,
    quote_id,
)

# Mapped fields checked in priority order.
CANDIDATE_FIELD_KEYS: list[str] = [
    "capex_opex_indicator",
    "gl_account",
]

FIELD_DISPLAY_NAMES: dict[str, str] = {
    "capex_opex_indicator": "CAPEX/OPEX Indicator",
    "gl_account": "GL Account",
}

# Substrings in column names that suggest a CAPEX/OPEX flag column.
COLUMN_NAME_HINTS: tuple[str, ...] = (
    "capex",
    "opex",
    "cap_ex",
    "op_ex",
    "expense type",
    "expenditure",
    "capex_opex",
    "cap/opex",
)

ASSUMPTIONS: list[str] = [
    "Only an explicit CAPEX/OPEX indicator or GL account column is used — descriptions are not scanned.",
    "Values that explicitly indicate CAPEX (e.g. CAPEX, Y, 1) are counted as CAPEX spend.",
    "All other non-empty values in that column (including OPEX, 0, N, or any other label) are counted as OPEX spend.",
    "Rows with a blank indicator are excluded from both totals.",
]


def _column_name_matches_hint(column: str) -> bool:
    lowered = column.lower().replace("_", " ")
    return any(hint in lowered for hint in COLUMN_NAME_HINTS)


def _indicator_value_patterns_sql(v: str) -> str:
    """SQL fragment matching known CAPEX/OPEX indicator values."""
    return f"""(
        {v} IN (
            'capex', 'cap ex', 'cap-ex', 'opex', 'op ex', 'op-ex',
            'capital expenditure', 'operating expenditure', 'operating expense',
            'capital expense', 'capital', 'operating',
            'y', 'n', 'yes', 'no', '0', '1', 'true', 'false'
        )
        OR {v} LIKE '%capex%'
        OR {v} LIKE '%opex%'
        OR {v} LIKE '%capital%'
        OR {v} LIKE '%operating%'
    )"""


def _is_qualifying_indicator_column(conn: DuckDBConnection, column: str) -> bool:
    """Return True when *column* contains CAPEX/OPEX-style indicator values."""
    if field_mapped_with_data(conn, column):
        cast_info = get_cast_report_fields(conn).get(column, {})
        if cast_info.get("mapped") and int(cast_info.get("validRows") or 0) > 0:
            if column == "capex_opex_indicator":
                return True
    qc = quote_id(column)
    v = f"TRIM(LOWER(CAST({qc} AS VARCHAR)))"
    row = conn.execute(
        f"""
        SELECT 1 FROM "analysis_data"
        WHERE {qc} IS NOT NULL
          AND TRIM(CAST({qc} AS VARCHAR)) != ''
          AND {_indicator_value_patterns_sql(v)}
        LIMIT 1
        """
    ).fetchone()
    return row is not None


def get_candidate_columns(conn: DuckDBConnection) -> list[dict[str, str]]:
    """Return indicator / GL columns mapped with data."""
    mapped = get_mapped_field_keys(conn, CANDIDATE_FIELD_KEYS)
    result: list[dict[str, str]] = []
    for fk in mapped:
        if _is_qualifying_indicator_column(conn, fk):
            result.append({
                "fieldKey": fk,
                "displayName": FIELD_DISPLAY_NAMES.get(fk, fk),
            })
    return result


def resolve_indicator_column(
    conn: DuckDBConnection,
    preferred: str | None = None,
) -> str | None:
    """Pick the column used for CAPEX/OPEX classification."""
    present = set(list_analysis_columns(conn))

    if preferred:
        if preferred not in present:
            raise ValueError(f"Column '{preferred}' is not in analysis_data.")
        if not _is_qualifying_indicator_column(conn, preferred):
            raise ValueError(
                f"Column '{preferred}' is not mapped with CAPEX/OPEX indicator data."
            )
        return preferred

    for fk in CANDIDATE_FIELD_KEYS:
        if fk in present and _is_qualifying_indicator_column(conn, fk):
            return fk

    for col in list_analysis_columns(conn):
        if col in CANDIDATE_FIELD_KEYS:
            continue
        if _column_name_matches_hint(col) and _is_qualifying_indicator_column(conn, col):
            return col

    return None


def _capex_value_sql(qc: str) -> str:
    """SQL expression: TRUE when the column value is explicitly CAPEX."""
    v = f"TRIM(LOWER(CAST({qc} AS VARCHAR)))"
    return f"""(
        {v} IN (
            'capex', 'cap ex', 'cap-ex', 'capital expenditure',
            'capital expense', 'capital',
            'y', 'yes', '1', 'true'
        )
        OR ({v} LIKE '%capex%' AND {v} NOT LIKE '%opex%')
        OR ({v} LIKE '%capital%' AND {v} NOT LIKE '%operating%')
    )"""


def classify_capex_opex_spend(
    conn: DuckDBConnection,
    column: str | None = None,
) -> dict[str, Any]:
    """Classify spend into CAPEX and OPEX from an explicit indicator column.

    Args:
        conn: DuckDB session connection.
        column: Optional field key to use; auto-detected when omitted.

    Returns:
        Classification result with categories, source column, assumptions, and
        feasibility flag.
    """
    if not field_mapped_with_data(conn, "total_spend"):
        return {
            "feasible": False,
            "message": "Total Spend is not mapped. Confirm column mapping in Step 3.",
            "sourceColumn": None,
            "sourceColumnDisplayName": None,
            "categories": [],
            "assumptions": ASSUMPTIONS,
            "unclassifiedRows": None,
            "unclassifiedSpend": None,
        }

    source = resolve_indicator_column(conn, preferred=column)
    if not source:
        return {
            "feasible": False,
            "message": (
                "No CAPEX/OPEX indicator column found. In Step 3, map "
                "CAPEX/OPEX Indicator (or a GL column with CAPEX/OPEX values)."
            ),
            "sourceColumn": None,
            "sourceColumnDisplayName": None,
            "categories": [],
            "assumptions": ASSUMPTIONS,
            "unclassifiedRows": None,
            "unclassifiedSpend": None,
        }

    qc = quote_id(source)
    capex_cond = _capex_value_sql(qc)
    populated = f"{qc} IS NOT NULL AND TRIM(CAST({qc} AS VARCHAR)) != ''"

    row = conn.execute(
        f"""
        SELECT
            COUNT(*) FILTER (WHERE {populated} AND {capex_cond}) AS capex_rows,
            COALESCE(
                SUM(TRY_CAST(total_spend AS DOUBLE))
                FILTER (WHERE {populated} AND {capex_cond}),
                0
            ) AS capex_spend,
            COUNT(*) FILTER (WHERE {populated} AND NOT ({capex_cond})) AS opex_rows,
            COALESCE(
                SUM(TRY_CAST(total_spend AS DOUBLE))
                FILTER (WHERE {populated} AND NOT ({capex_cond})),
                0
            ) AS opex_spend,
            COUNT(*) FILTER (WHERE NOT ({populated})) AS unclassified_rows,
            COALESCE(
                SUM(TRY_CAST(total_spend AS DOUBLE))
                FILTER (WHERE NOT ({populated})),
                0
            ) AS unclassified_spend
        FROM "analysis_data"
        """
    ).fetchone()

    display = FIELD_DISPLAY_NAMES.get(source, source.replace("_", " ").title())

    return {
        "feasible": True,
        "message": None,
        "sourceColumn": source,
        "sourceColumnDisplayName": display,
        "categories": [
            {
                "label": "CAPEX",
                "matchingRows": int(row[0] or 0),
                "totalSpend": round(float(row[1] or 0)),
            },
            {
                "label": "OPEX",
                "matchingRows": int(row[2] or 0),
                "totalSpend": round(float(row[3] or 0)),
            },
        ],
        "assumptions": ASSUMPTIONS,
        "unclassifiedRows": int(row[4] or 0),
        "unclassifiedSpend": round(float(row[5] or 0)),
    }


__all__ = [
    "ASSUMPTIONS",
    "CANDIDATE_FIELD_KEYS",
    "FIELD_DISPLAY_NAMES",
    "classify_capex_opex_spend",
    "get_candidate_columns",
    "resolve_indicator_column",
]
