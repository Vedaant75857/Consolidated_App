"""Multi-column value distribution for DQA panels.

Computes per-column fill rates and within-column value percentages for a set
of identified columns (from AI or rule-based fallback).
"""

from __future__ import annotations

import logging
from typing import Any

from shared.db import (
    DuckDBConnection,
    filter_data_columns,
    quote_id,
    read_table_columns,
    table_row_count,
)

from .fill_rate_analysis import _effective_non_null_condition
from .metrics import _safe_pct

logger = logging.getLogger(__name__)

_MAX_DISTINCT_VALUES = 200


def merge_identified_columns(
    ai_columns: list[str] | None,
    available: set[str],
    fallback_fn,
) -> list[str]:
    """Merge AI-identified columns with rule-based discovery.

    Args:
        ai_columns: Column names returned by AI (may be empty or None).
        available: Column names present in the table.
        fallback_fn: Callable like ``find_currency_columns`` for rule-based discovery.

    Returns:
        Deduplicated list of valid column names.
    """
    available_lookup = {c.strip().lower(): c for c in available}
    seen: set[str] = set()
    result: list[str] = []

    if ai_columns:
        for col in ai_columns:
            if not isinstance(col, str):
                continue
            resolved = available_lookup.get(col.strip().lower())
            if resolved and resolved not in seen:
                seen.add(resolved)
                result.append(resolved)

    for col in fallback_fn(available):
        if col in available and col not in seen:
            seen.add(col)
            result.append(col)

    return result


def compute_value_distribution_table(
    conn: DuckDBConnection,
    table_name: str,
    columns: list[str],
) -> dict[str, Any]:
    """Build a cross-column value distribution table.

    Column header fill rates are % of all rows with a non-null value.
    Cell fill rates are % of filled rows in that column with the given value.

    Args:
        conn: DuckDB session connection.
        table_name: Target table.
        columns: Column names to include (must exist in the table).

    Returns:
        Dict with ``exists``, ``columns`` metadata, and ``rows``.
    """
    available = set(filter_data_columns(read_table_columns(conn, table_name)))
    valid_columns = [c for c in columns if c in available]

    if not valid_columns:
        return {"exists": False, "columns": [], "rows": []}

    tbl = quote_id(table_name)
    total_rows = table_row_count(conn, table_name)
    if total_rows == 0:
        return {
            "exists": True,
            "columns": [{"name": c, "fillRate": 0.0} for c in valid_columns],
            "rows": [],
        }

    column_meta: list[dict[str, Any]] = []
    value_counts: dict[str, dict[str, int]] = {}
    filled_counts: dict[str, int] = {}

    for col in valid_columns:
        qc = quote_id(col)
        nn = _effective_non_null_condition(qc)

        filled_row = conn.execute(
            f"SELECT COUNT(*) AS cnt FROM {tbl} WHERE {nn}"
        ).fetchone()
        filled = int(filled_row["cnt"]) if filled_row else 0
        filled_counts[col] = filled
        column_meta.append({
            "name": col,
            "fillRate": _safe_pct(filled, total_rows),
        })

        rows = conn.execute(
            f"SELECT TRIM(CAST({qc} AS TEXT)) AS val, COUNT(*) AS cnt "
            f"FROM {tbl} WHERE {nn} "
            f"GROUP BY TRIM(CAST({qc} AS TEXT)) "
            f"ORDER BY cnt DESC "
            f"LIMIT ?",
            (_MAX_DISTINCT_VALUES,),
        ).fetchall()
        value_counts[col] = {str(r["val"]): int(r["cnt"]) for r in rows}

    all_values: set[str] = set()
    for counts in value_counts.values():
        all_values.update(counts.keys())

    def _row_sort_key(value: str) -> int:
        return max(value_counts[col].get(value, 0) for col in valid_columns)

    sorted_values = sorted(all_values, key=_row_sort_key, reverse=True)

    table_rows: list[dict[str, Any]] = []
    for value in sorted_values:
        fill_rates: dict[str, float | None] = {}
        for col in valid_columns:
            count = value_counts[col].get(value)
            if count is None:
                fill_rates[col] = None
            else:
                fill_rates[col] = _safe_pct(count, filled_counts[col])
        table_rows.append({"value": value, "fillRates": fill_rates})

    return {
        "exists": True,
        "columns": column_meta,
        "rows": table_rows,
    }
