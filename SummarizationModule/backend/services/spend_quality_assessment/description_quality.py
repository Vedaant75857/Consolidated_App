"""Description Quality Analysis for the Spend Quality Assessment step.

Computes per-description-column metrics (spend coverage, top descriptions,
backend stats) and generates AI insights via the LLM.
"""

from __future__ import annotations

import logging
import random
from typing import Any

from shared.ai_client import call_ai_json
from shared.duckdb_compat import DuckDBConnection
from services.spend_quality_assessment.ai_prompts import DESCRIPTION_QUALITY_PROMPT

logger = logging.getLogger(__name__)

DESCRIPTION_FIELD_KEYS: list[str] = [
    "description",
]

DESCRIPTION_DISPLAY_NAMES: dict[str, str] = {
    "description": "Description",
}

NULL_PROXY_VALUES: list[str] = [
    "na", "#na", "n/a", "#n/a", "none", "null", "nan",
    "-", "unknown", "not applicable", "tbd",
    "misc", "miscellaneous", "other", "general",
    "unclassified", "0", ".",
]


def _quote_id(name: str) -> str:
    """Double-quote a SQLite identifier."""
    return f'"{name}"'


def _nn(qc: str) -> str:
    """SQL fragment: column is non-null and non-empty after trimming."""
    return f"{qc} IS NOT NULL AND TRIM(CAST({qc} AS TEXT)) != ''"


def _build_null_proxy_sql(qc: str) -> str:
    """SQL CASE expression counting rows matching null-proxy patterns."""
    conditions = " OR ".join(
        f"LOWER(TRIM(CAST({qc} AS TEXT))) = '{v}'" for v in NULL_PROXY_VALUES
    )
    return f"({conditions})"


def _compute_description_column_stats(
    conn: DuckDBConnection,
    field_key: str,
    spend_col: str,
    total_rows: int,
) -> dict[str, Any]:
    """Compute full-dataset stats and top-10 descriptions for one column.

    Args:
        conn: DuckDB connection with ``analysis_data`` table.
        field_key: Column name in analysis_data (e.g. ``description``).
        spend_col: Spend column to aggregate (``total_spend``).
        total_rows: Total row count in analysis_data.

    Returns:
        Dict with spendCovered, top10, backendStats, and mapped flag.
    """
    qd = _quote_id(field_key)
    qs = _quote_id(spend_col)
    nn_d = _nn(qd)
    null_proxy_cond = _build_null_proxy_sql(qd)

    numeric_check = f"(TRY_CAST({qs} AS DOUBLE) IS NOT NULL)"
    spend_expr = (
        f"SUM(CASE WHEN {numeric_check} THEN CAST({qs} AS REAL) ELSE 0 END)"
    )

    # --- 1. Spend covered by populated rows ---
    spend_row = conn.execute(
        f"SELECT {spend_expr} FROM \"analysis_data\" WHERE {nn_d}"
    ).fetchone()
    spend_covered = round(float(spend_row[0] or 0))

    # --- 2. Top 10 descriptions by spend ---
    top10_rows = conn.execute(
        f"SELECT TRIM(CAST({qd} AS TEXT)) AS desc_val, "
        f"  {spend_expr} AS spend "
        f"FROM \"analysis_data\" "
        f"WHERE {nn_d} "
        f"GROUP BY desc_val "
        f"ORDER BY spend DESC "
        f"LIMIT 10"
    ).fetchall()
    top10 = [
        {"description": str(r[0]), "spend": round(float(r[1] or 0))}
        for r in top10_rows
    ]

    # --- 3. Backend stats (entire dataset) ---
    stats_sql = (
        f"SELECT "
        f"  COUNT(CASE WHEN {nn_d} THEN 1 END) AS populated, "
        f"  AVG(CASE WHEN {nn_d} THEN LENGTH(TRIM(CAST({qd} AS TEXT))) END) AS avg_len, "
        f"  COUNT(CASE WHEN {nn_d} "
        f"    AND LENGTH(TRIM(CAST({qd} AS TEXT))) - "
        f"        LENGTH(REPLACE(TRIM(CAST({qd} AS TEXT)), ' ', '')) > 0 "
        f"    THEN 1 END) AS multi_word_ct, "
        f"  COUNT(CASE WHEN {nn_d} AND {null_proxy_cond} THEN 1 END) AS proxy_ct "
        f"FROM \"analysis_data\""
    )
    stats_row = conn.execute(stats_sql).fetchone()
    populated = int(stats_row[0] or 0)
    avg_len = round(float(stats_row[1] or 0), 1)
    multi_word_ct = int(stats_row[2] or 0)
    proxy_ct = int(stats_row[3] or 0)

    # Multi-word spend
    mw_spend_row = conn.execute(
        f"SELECT {spend_expr} FROM \"analysis_data\" "
        f"WHERE {nn_d} "
        f"  AND LENGTH(TRIM(CAST({qd} AS TEXT))) - "
        f"      LENGTH(REPLACE(TRIM(CAST({qd} AS TEXT)), ' ', '')) > 0"
    ).fetchone()
    multi_word_spend = round(float(mw_spend_row[0] or 0))

    # Null-proxy spend
    np_spend_row = conn.execute(
        f"SELECT {spend_expr} FROM \"analysis_data\" "
        f"WHERE {nn_d} AND {null_proxy_cond}"
    ).fetchone()
    null_proxy_spend = round(float(np_spend_row[0] or 0))

    # Total spend (all rows)
    total_spend_row = conn.execute(
        f"SELECT {spend_expr} FROM \"analysis_data\""
    ).fetchone()
    total_spend = round(float(total_spend_row[0] or 0))

    return {
        "mapped": True,
        "displayName": DESCRIPTION_DISPLAY_NAMES.get(field_key, field_key),
        "spendCovered": spend_covered,
        "top10": top10,
        "backendStats": {
            "avgLength": avg_len,
            "multiWordCount": multi_word_ct,
            "multiWordSpend": multi_word_spend,
            "nullProxyCount": proxy_ct,
            "nullProxySpend": null_proxy_spend,
            "totalPopulated": populated,
            "totalSpend": total_spend,
        },
    }


def _sample_descriptions_for_ai(
    conn: DuckDBConnection,
    field_key: str,
    spend_col: str,
    sample_size: int = 100,
) -> list[str]:
    """Sample up to *sample_size* descriptions from the top 80% of spend.

    Aggregates descriptions by spend descending, walks until 80% cumulative,
    then randomly samples from that set.
    """
    qd = _quote_id(field_key)
    qs = _quote_id(spend_col)
    nn_d = _nn(qd)

    numeric_check = f"(TRY_CAST({qs} AS DOUBLE) IS NOT NULL)"
    spend_expr = (
        f"SUM(CASE WHEN {numeric_check} THEN CAST({qs} AS REAL) ELSE 0 END)"
    )

    rows = conn.execute(
        f"SELECT TRIM(CAST({qd} AS TEXT)) AS desc_val, "
        f"  {spend_expr} AS spend "
        f"FROM \"analysis_data\" "
        f"WHERE {nn_d} "
        f"GROUP BY desc_val "
        f"ORDER BY spend DESC"
    ).fetchall()

    if not rows:
        return []

    total = sum(float(r[1] or 0) for r in rows)
    if total <= 0:
        return [str(r[0]) for r in rows[:sample_size]]

    threshold = total * 0.80
    cumulative = 0.0
    top80_descs: list[str] = []
    for r in rows:
        cumulative += float(r[1] or 0)
        top80_descs.append(str(r[0]))
        if cumulative >= threshold:
            break

    if len(top80_descs) <= sample_size:
        return top80_descs

    return random.sample(top80_descs, sample_size)


def _top_descriptions_by_frequency(
    conn: DuckDBConnection,
    field_key: str,
    spend_col: str,
    limit: int = 100,
) -> list[dict[str, Any]]:
    """Return the *limit* most frequently occurring descriptions with spend.

    Args:
        conn: DuckDB connection with ``analysis_data`` table.
        field_key: Description column name (e.g. ``description``).
        spend_col: Spend column to aggregate (``total_spend``).
        limit: Max descriptions to return (default 100).

    Returns:
        List of dicts ``{"description": str, "count": int, "spend": float}``
        ordered by row count descending.
    """
    qd = _quote_id(field_key)
    qs = _quote_id(spend_col)
    nn_d = _nn(qd)

    numeric_check = f"(TRY_CAST({qs} AS DOUBLE) IS NOT NULL)"
    spend_expr = (
        f"SUM(CASE WHEN {numeric_check} THEN CAST({qs} AS REAL) ELSE 0 END)"
    )

    rows = conn.execute(
        f"SELECT TRIM(CAST({qd} AS TEXT)) AS desc_val, "
        f"  COUNT(*) AS freq, "
        f"  {spend_expr} AS spend "
        f"FROM \"analysis_data\" "
        f"WHERE {nn_d} "
        f"GROUP BY desc_val "
        f"ORDER BY freq DESC "
        f"LIMIT {int(limit)}"
    ).fetchall()

    return [
        {
            "description": str(r[0]),
            "count": int(r[1]),
            "spend": round(float(r[2] or 0)),
        }
        for r in rows
    ]


def _generate_description_insight(
    field_key: str,
    backend_stats: dict[str, Any],
    sampled_descriptions: list[str],
    top_by_frequency: list[dict[str, Any]],
    api_key: str,
) -> str:
    """Call the LLM to produce a quality insight for one description column."""
    payload = {
        "descriptionType": DESCRIPTION_DISPLAY_NAMES.get(field_key, field_key),
        "sampledDescriptions": sampled_descriptions,
        "topByFrequency": top_by_frequency,
        "backendStats": backend_stats,
    }
    try:
        result = call_ai_json(DESCRIPTION_QUALITY_PROMPT, payload, api_key=api_key)
        return str(result.get("insight", ""))
    except Exception as exc:
        logger.error("AI description insight failed for %s: %s", field_key, exc)
        return f"AI insight unavailable: {exc}"


def run_description_quality_analysis(
    conn: DuckDBConnection,
    available_columns: set[str],
    api_key: str,
) -> list[dict[str, Any]]:
    """Run description quality analysis for the description column.

    Args:
        conn: DuckDB connection with ``analysis_data`` table.
        available_columns: Set of column names present in analysis_data.
        api_key: API key for LLM calls.

    Returns:
        List of dicts, one per description column, with stats and AI insight.
    """
    spend_col = "total_spend" if "total_spend" in available_columns else None
    if not spend_col:
        logger.warning("total_spend not available; description quality analysis limited")

    total_rows_row = conn.execute(
        'SELECT COUNT(*) FROM "analysis_data"'
    ).fetchone()
    total_rows = int(total_rows_row[0] or 0)

    results: list[dict[str, Any]] = []

    for fk in DESCRIPTION_FIELD_KEYS:
        display = DESCRIPTION_DISPLAY_NAMES[fk]

        if fk not in available_columns or not spend_col:
            results.append({
                "fieldKey": fk,
                "displayName": display,
                "mapped": fk in available_columns,
                "spendCovered": None,
                "top10": [],
                "backendStats": None,
                "aiInsight": "Column not mapped." if fk not in available_columns
                    else "Spend column not available for analysis.",
            })
            continue

        stats = _compute_description_column_stats(
            conn, fk, spend_col, total_rows
        )

        sampled = _sample_descriptions_for_ai(conn, fk, spend_col)
        freq_descs = _top_descriptions_by_frequency(conn, fk, spend_col)

        if sampled:
            insight = _generate_description_insight(
                fk, stats["backendStats"], sampled, freq_descs, api_key
            )
        else:
            insight = "No populated descriptions found for AI analysis."

        results.append({
            "fieldKey": fk,
            "displayName": display,
            "mapped": True,
            "spendCovered": stats["spendCovered"],
            "top10": stats["top10"],
            "backendStats": stats["backendStats"],
            "aiInsight": insight,
        })

    return results
