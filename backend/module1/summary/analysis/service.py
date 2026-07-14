"""Data quality analysis on `final_merged` using SQL stats + AI profiler/auditor."""

from __future__ import annotations

import importlib.util
import os
from concurrent.futures import ThreadPoolExecutor
from typing import Any

from shared.db import (
    DuckDBConnection,
    column_stats,
    get_meta,
    quote_id,
    read_table_columns,
    table_exists,
    table_row_count,
)
from shared.ai import call_ai_json


def _load_mod(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


_insights_root = os.path.join(os.path.dirname(__file__), "..", "insights")
_prompts = _load_mod("insights_prompts", os.path.join(_insights_root, "ai", "prompts.py"))
_stats = _load_mod("insights_stats", os.path.join(_insights_root, "stats", "column_stats_computer.py"))

DATA_PROFILER_PROMPT = _prompts.DATA_PROFILER_PROMPT
QUALITY_AUDITOR_PROMPT = _prompts.QUALITY_AUDITOR_PROMPT
estimate_duplicate_rows = _stats.estimate_duplicate_rows


def _numeric_predicate(qc: str) -> str:
    t = f"TRIM(CAST({qc} AS TEXT))"
    return f"(regexp_matches({t}, '[0-9]') AND regexp_matches({t}, '^[0-9eE.+-]+$'))"


def _enrich_column_stats(
    conn: DuckDBConnection,
    table_name: str,
    columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Shared column_stats plus numeric_ratio — single-pass batched query."""
    if not table_exists(conn, table_name):
        return []
    all_cols = read_table_columns(conn, table_name)
    cols = [c for c in columns if c in set(all_cols)] if columns else all_cols
    if not cols:
        return []
    tbl = quote_id(table_name)
    basic = column_stats(conn, table_name, cols)

    # Single batched query for all numeric_ratios
    ratio_parts: list[str] = []
    for col in cols:
        qc = quote_id(col)
        num_pred = _numeric_predicate(qc)
        nn = f"{qc} IS NOT NULL AND TRIM(CAST({qc} AS TEXT)) != ''"
        ratio_parts.append(
            f"CAST(COUNT(CASE WHEN {num_pred} AND {nn} THEN 1 END) AS REAL)"
            f" / MAX(COUNT(CASE WHEN {nn} THEN 1 END), 1)"
        )
    row = conn.execute(f"SELECT {', '.join(ratio_parts)} FROM {tbl}").fetchone()

    out: list[dict[str, Any]] = []
    for i, b in enumerate(basic):
        base = {k: b[k] for k in b.keys()}
        base["numeric_ratio"] = float(row[i] or 0) if row else 0.0
        out.append(base)
    return out


def run_analysis(
    conn: DuckDBConnection,
    session_id: str,
    api_key: str,
    columns: list[str] | None = None,
) -> dict[str, Any]:
    """
    Run SQL column statistics on `final_merged`, then DATA_PROFILER_PROMPT and
    QUALITY_AUDITOR_PROMPT with that payload (no raw rows).
    """
    _ = session_id
    if not table_exists(conn, "final_merged"):
        raise ValueError("No result table found. Complete the merge step first.")

    selected_columns: list[str] = []
    if isinstance(columns, list):
        selected_columns = [str(c) for c in columns if isinstance(c, str) and str(c).strip()]

    total_rows = table_row_count(conn, "final_merged")
    column_stat_rows = _enrich_column_stats(conn, "final_merged", selected_columns or None)
    dup_est = estimate_duplicate_rows(conn, "final_merged")

    profiler_payload = {
        "table": "final_merged",
        "totalRows": total_rows,
        "columns": [
            {
                "name": c["column_name"], "non_null_count": c["non_null_count"],
                "null_count": c["null_count"], "fill_rate": c["fill_rate"],
                "distinct_count": c["distinct_count"], "numeric_ratio": c["numeric_ratio"],
            }
            for c in column_stat_rows
        ],
        "sourceTables": [],
    }

    auditor_payload = {
        "table": "final_merged", "totalRows": total_rows,
        "columnStats": column_stat_rows, "profilerOutput": {},
        "crossTableConsistency": 1.0, "duplicateRowEstimate": dup_est,
    }

    def _run_profiler() -> dict[str, Any]:
        try:
            return call_ai_json(DATA_PROFILER_PROMPT, profiler_payload, api_key=api_key)
        except Exception:
            return {
                "dataDescription": "Merged dataset",
                "columnRoles": [{"name": c["column_name"], "role": "auxiliary", "description": ""} for c in column_stat_rows],
                "domainKeywords": [], "dataCharacteristics": "",
            }

    def _run_auditor() -> dict[str, Any]:
        try:
            return call_ai_json(QUALITY_AUDITOR_PROMPT, auditor_payload, api_key=api_key)
        except Exception:
            avg_fill = sum(float(c.get("fill_rate") or 0.0) for c in column_stat_rows) / len(column_stat_rows) if column_stat_rows else 0.0
            return {
                "overallScore": int(round(avg_fill * 100)), "completeness": avg_fill,
                "uniqueness": 0.0, "consistency": 1.0, "issues": [], "recommendations": [],
            }

    with ThreadPoolExecutor(max_workers=2) as pool:
        fut_profiler = pool.submit(_run_profiler)
        fut_auditor = pool.submit(_run_auditor)
        profiler = fut_profiler.result()
        auditor = fut_auditor.result()

    selected = [str(c) for c in (selected_columns or []) if isinstance(c, str) and c.strip()]
    if not selected:
        selected = [str(c["column_name"]) for c in column_stat_rows[:3]]
    selected_set = set(selected)

    role_map = {
        str(r.get("name")): {
            "role": str(r.get("role") or "auxiliary"),
            "description": str(r.get("description") or ""),
        }
        for r in (profiler.get("columnRoles") or [])
        if isinstance(r, dict) and r.get("name")
    }
    issues = [i for i in (auditor.get("issues") or []) if isinstance(i, dict)]
    recs = [str(r) for r in (auditor.get("recommendations") or []) if r]

    dq_columns: list[dict[str, Any]] = []
    consistency_columns: list[dict[str, Any]] = []
    usability_columns: list[dict[str, Any]] = []

    for c in column_stat_rows:
        col = str(c["column_name"])
        if col not in selected_set:
            continue
        fill = float(c.get("fill_rate") or 0.0)
        non_null = int(c.get("non_null_count") or 0)
        distinct = int(c.get("distinct_count") or 0)
        uniq_ratio = (distinct / max(non_null, 1)) if non_null > 0 else 0.0
        num_ratio = float(c.get("numeric_ratio") or 0.0)
        role = role_map.get(col, {}).get("role", "auxiliary")
        desc = role_map.get(col, {}).get("description", "")

        col_issues = [
            str(i.get("description") or "")
            for i in issues
            if str(i.get("column") or "") == col
        ]
        consistency_score = int(round((0.6 * fill + 0.4 * max(0.0, 1.0 - abs(0.5 - num_ratio))) * 100))
        quality_score = int(round((0.7 * fill + 0.3 * min(uniq_ratio, 1.0)) * 100))
        usability_score = int(round((0.55 * quality_score + 0.45 * consistency_score)))

        dq_columns.append(
            {
                "column": col,
                "overall_score": quality_score,
                "inferred_description": desc or f"{role} column",
                "inferred_type": "numeric" if num_ratio >= 0.8 else "text",
                "buckets": [
                    {
                        "range": "all",
                        "count": non_null,
                        "percentage": round(fill * 100, 2),
                        "quality": {
                            "clarity": min(100, int(round((1.0 - abs(0.5 - num_ratio)) * 100))),
                            "consistency": consistency_score,
                            "completeness": int(round(fill * 100)),
                            "issues": col_issues[:4],
                        },
                    }
                ],
                "key_issues": col_issues[:6],
            }
        )

        patterns: list[str] = []
        if num_ratio >= 0.8:
            patterns.append("mostly_numeric")
        elif num_ratio <= 0.2:
            patterns.append("mostly_text")
        else:
            patterns.append("mixed_numeric_text")
        if fill >= 0.95:
            patterns.append("high_fill")

        consistency_columns.append(
            {
                "column": col,
                "consistency_score": consistency_score,
                "detected_patterns": patterns,
                "violations": [
                    {
                        "severity": str(i.get("severity") or "low"),
                        "description": str(i.get("description") or ""),
                        "estimated_pct": round((1.0 - fill) * 100, 2),
                    }
                    for i in issues
                    if str(i.get("column") or "") == col
                ],
                "mixed_types": {
                    "detected": 0.2 < num_ratio < 0.8,
                    "details": "Column appears to mix numeric and text-like values."
                    if 0.2 < num_ratio < 0.8
                    else "",
                },
                "recommendations": recs[:3],
            }
        )

        enabled = ["filter", "group"]
        if num_ratio >= 0.8:
            enabled.extend(["sum", "avg"])
        if role in ("timestamp",):
            enabled.append("trend")

        usability_columns.append(
            {
                "column": col,
                "usability_rating": usability_score,
                "characterization": desc or f"Usable as {role}",
                "enabled_analyses": enabled,
                "potential_problems": col_issues[:4],
                "remediation": recs[:3],
            }
        )

    return {
        "table": "final_merged",
        "totalRows": total_rows,
        "columnStats": column_stat_rows,
        "profiler": profiler,
        "quality": {
            "overallScore": auditor.get("overallScore"),
            "completeness": auditor.get("completeness"),
            "uniqueness": auditor.get("uniqueness"),
            "consistency": auditor.get("consistency"),
            "issues": auditor.get("issues", []),
            "recommendations": auditor.get("recommendations", []),
        },
        "rawQuality": auditor,
        "dataQuality": {
            "columns": dq_columns,
        },
        "consistency": {
            "columns": consistency_columns,
            "cross_column_issues": [],
        },
        "usability": {
            "overall_assessment": str(profiler.get("dataDescription") or "Merged dataset quality summary"),
            "columns": usability_columns,
        },
    }


PROCUREMENT_ANALYSIS_PROMPT = """You are a procurement data analyst. Given the following computed statistics about a final merged procurement dataset, craft a clear and concise narrative summary.

The data provided includes:
- Date columns with their min/max date ranges
- Local currency columns with unique currency values
- Spend columns with totals broken down by currency

Your response must be a JSON object with:
{
  "narrative": "A well-structured narrative summary (2-4 paragraphs) covering the data timeframe, currency coverage, and spend distribution. Use specific numbers.",
  "highlights": ["List of 3-5 key highlights/observations about the data"],
  "dataQualityNotes": ["Any concerns about data quality, gaps, or inconsistencies observed from the stats"]
}
"""


def _detect_date_columns(conn: DuckDBConnection, table_name: str, columns: list[str]) -> list[str]:
    """Heuristic: columns whose names suggest dates or that have parseable date values."""
    date_keywords = {"date", "dt", "period", "month", "year", "time", "timestamp", "invoice_date",
                     "po_date", "order_date", "delivery_date", "payment_date", "created", "updated"}
    candidates = []
    for col in columns:
        col_lower = col.lower().replace(" ", "_")
        if any(kw in col_lower for kw in date_keywords):
            candidates.append(col)
    return candidates


def _detect_currency_columns(columns: list[str]) -> list[str]:
    currency_keywords = {"currency", "curr", "ccy", "local_currency", "doc_currency",
                         "transaction_currency", "invoice_currency"}
    return [c for c in columns if any(kw in c.lower().replace(" ", "_") for kw in currency_keywords)]


def _detect_spend_columns(columns: list[str]) -> list[str]:
    spend_keywords = {"spend", "amount", "value", "cost", "price", "total", "net_value",
                      "gross_value", "invoice_amount", "po_value", "order_value"}
    return [c for c in columns if any(kw in c.lower().replace(" ", "_") for kw in spend_keywords)]


def run_procurement_analysis(
    conn: DuckDBConnection,
    session_id: str,
    api_key: str,
) -> dict[str, Any]:
    """Procurement-focused analysis: date ranges, currency uniques, spend pivots + AI narrative."""
    _ = session_id
    if not table_exists(conn, "final_merged"):
        raise ValueError("No result table found. Complete the merge step first.")

    tbl = quote_id("final_merged")
    all_cols = read_table_columns(conn, "final_merged")
    total_rows = table_row_count(conn, "final_merged")

    header_decisions = get_meta(conn, "headerNormDecisions") or []
    proc_mappings = get_meta(conn, "procMappings") or []

    mapped_dates: list[str] = []
    mapped_currencies: list[str] = []
    mapped_spends: list[str] = []

    for pm in proc_mappings:
        if isinstance(pm, dict):
            std = str(pm.get("standard_field") or "").lower()
            col = str(pm.get("column") or pm.get("source_col") or "")
            if col and col in all_cols:
                if "date" in std:
                    mapped_dates.append(col)
                elif "currency" in std:
                    mapped_currencies.append(col)
                elif "spend" in std or "amount" in std or "value" in std:
                    mapped_spends.append(col)

    for tbl_dec in header_decisions:
        if not isinstance(tbl_dec, dict):
            continue
        for d in tbl_dec.get("decisions") or []:
            if not isinstance(d, dict):
                continue
            mapped = str(d.get("mapped_to") or d.get("suggested_std_field") or "").lower()
            src = str(d.get("source_col") or "")
            if src and src in all_cols:
                if "date" in mapped and src not in mapped_dates:
                    mapped_dates.append(src)
                elif "currency" in mapped and src not in mapped_currencies:
                    mapped_currencies.append(src)
                elif ("spend" in mapped or "amount" in mapped or "value" in mapped) and src not in mapped_spends:
                    mapped_spends.append(src)

    if not mapped_dates:
        mapped_dates = _detect_date_columns(conn, "final_merged", all_cols)
    if not mapped_currencies:
        mapped_currencies = _detect_currency_columns(all_cols)
    if not mapped_spends:
        mapped_spends = _detect_spend_columns(all_cols)

    date_ranges: list[dict[str, Any]] = []
    for col in mapped_dates:
        qc = quote_id(col)
        try:
            row = conn.execute(
                f"SELECT MIN(TRIM({qc})) AS min_val, MAX(TRIM({qc})) AS max_val "
                f"FROM {quote_id('final_merged')} WHERE {qc} IS NOT NULL AND TRIM(CAST({qc} AS TEXT)) != ''"
            ).fetchone()
            if row:
                date_ranges.append({
                    "column": col,
                    "min_date": str(row["min_val"] or ""),
                    "max_date": str(row["max_val"] or ""),
                })
        except Exception:
            pass

    currency_uniques: list[dict[str, Any]] = []
    for col in mapped_currencies:
        qc = quote_id(col)
        try:
            rows = conn.execute(
                f"SELECT DISTINCT TRIM(CAST({qc} AS TEXT)) AS val "
                f"FROM {quote_id('final_merged')} WHERE {qc} IS NOT NULL AND TRIM(CAST({qc} AS TEXT)) != '' "
                f"ORDER BY val LIMIT 200"
            ).fetchall()
            currency_uniques.append({
                "column": col,
                "values": [str(r["val"]) for r in rows],
            })
        except Exception:
            pass

    spend_by_currency: list[dict[str, Any]] = []
    for spend_col in mapped_spends:
        sq = quote_id(spend_col)
        if mapped_currencies:
            for curr_col in mapped_currencies:
                cq = quote_id(curr_col)
                try:
                    rows = conn.execute(
                        f"SELECT TRIM(CAST({cq} AS TEXT)) AS currency, "
                        f"SUM(TRY_CAST({sq} AS REAL)) AS total_spend, "
                        f"COUNT(*) AS row_count "
                        f"FROM {quote_id('final_merged')} "
                        f"WHERE {sq} IS NOT NULL AND TRIM(CAST({sq} AS TEXT)) != '' "
                        f"GROUP BY TRIM(CAST({cq} AS TEXT)) "
                        f"ORDER BY total_spend DESC"
                    ).fetchall()
                    spend_by_currency.append({
                        "spend_column": spend_col,
                        "currency_column": curr_col,
                        "breakdown": [
                            {"currency": str(r["currency"] or "N/A"), "total_spend": float(r["total_spend"] or 0), "row_count": int(r["row_count"])}
                            for r in rows
                        ],
                    })
                except Exception:
                    pass
        else:
            try:
                row = conn.execute(
                    f"SELECT SUM(TRY_CAST({sq} AS REAL)) AS total_spend, COUNT(*) AS row_count "
                    f"FROM {quote_id('final_merged')} WHERE {sq} IS NOT NULL AND TRIM(CAST({sq} AS TEXT)) != ''"
                ).fetchone()
                if row:
                    spend_by_currency.append({
                        "spend_column": spend_col,
                        "currency_column": None,
                        "breakdown": [{"currency": "ALL", "total_spend": float(row["total_spend"] or 0), "row_count": int(row["row_count"])}],
                    })
            except Exception:
                pass

    stats_payload = {
        "totalRows": total_rows,
        "totalColumns": len(all_cols),
        "dateRanges": date_ranges,
        "currencyUniques": currency_uniques,
        "spendByCurrency": spend_by_currency,
    }

    ai_narrative: dict[str, Any] = {}
    try:
        ai_narrative = call_ai_json(PROCUREMENT_ANALYSIS_PROMPT, stats_payload, api_key=api_key)
    except Exception:
        ai_narrative = {
            "narrative": "Unable to generate AI narrative at this time.",
            "highlights": [],
            "dataQualityNotes": [],
        }

    return {
        "totalRows": total_rows,
        "totalColumns": len(all_cols),
        "dateRanges": date_ranges,
        "currencyUniques": currency_uniques,
        "spendByCurrency": spend_by_currency,
        "aiNarrative": ai_narrative,
    }
