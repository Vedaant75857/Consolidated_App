"""Deep per-column statistics computed entirely via SQLite SQL."""

from __future__ import annotations

import sqlite3
from typing import Any

from shared.db import (
    column_stats,
    column_distinct_values,
    quote_id,
    read_table_columns,
    table_exists,
    table_row_count,
)


def _numeric_predicate(qc: str) -> str:
    """Expression true when trimmed text looks numeric (GLOB, not CAST)."""
    t = f"TRIM(CAST({qc} AS TEXT))"
    return f"({t} GLOB '*[0-9]*' AND {t} NOT GLOB '*[^0-9.eE+-]*')"


def _infer_type(numeric_ratio: float, distinct_count: int, non_null: int) -> str:
    if numeric_ratio >= 0.85:
        if non_null > 0 and distinct_count / non_null > 0.95:
            return "id"
        return "numeric"
    return "text"


def compute_deep_column_stats(
    conn: sqlite3.Connection, table_name: str
) -> list[dict[str, Any]]:
    """Per-column deep stats using consolidated SQL queries for speed."""
    if not table_exists(conn, table_name):
        return []

    columns = read_table_columns(conn, table_name)
    total_rows = table_row_count(conn, table_name)
    if total_rows == 0 or not columns:
        return []

    tbl = quote_id(table_name)
    basic = {s["column_name"]: s for s in column_stats(conn, table_name, columns)}
    results: list[dict[str, Any]] = []

    for col in columns:
        qc = quote_id(col)
        b = basic.get(col, {})
        null_count = int(b.get("null_count", 0))
        non_null = int(b.get("non_null_count", 0))
        fill_rate = float(b.get("fill_rate", 0.0))
        distinct_count = int(b.get("distinct_count", 0))
        uniqueness = distinct_count / non_null if non_null else 0.0

        num_pred = _numeric_predicate(qc)
        nn_filter = f"{qc} IS NOT NULL AND TRIM(CAST({qc} AS TEXT)) != ''"

        # Consolidated query: numeric_ratio + length_stats + pattern_flags in one scan
        combo_row = conn.execute(
            f"""SELECT
                CAST(COUNT(CASE WHEN {num_pred} AND {nn_filter} THEN 1 END) AS REAL)
                    / MAX(COUNT(CASE WHEN {nn_filter} THEN 1 END), 1) AS numeric_ratio,
                MIN(LENGTH(CAST({qc} AS TEXT))) AS min_len,
                MAX(LENGTH(CAST({qc} AS TEXT))) AS max_len,
                AVG(LENGTH(CAST({qc} AS TEXT))) AS avg_len,
                SUM(CASE WHEN UPPER(TRIM(CAST({qc} AS TEXT))) = TRIM(CAST({qc} AS TEXT))
                    AND LOWER(TRIM(CAST({qc} AS TEXT))) != TRIM(CAST({qc} AS TEXT)) THEN 1 ELSE 0 END) AS n_upper,
                SUM(CASE WHEN LOWER(TRIM(CAST({qc} AS TEXT))) = TRIM(CAST({qc} AS TEXT))
                    AND LENGTH(TRIM(CAST({qc} AS TEXT))) > 0 THEN 1 ELSE 0 END) AS n_lower,
                SUM(CASE WHEN TRIM(CAST({qc} AS TEXT)) GLOB '*[!0-9A-Za-z ./_-]*' THEN 1 ELSE 0 END) AS n_special,
                SUM(CASE WHEN TRIM(CAST({qc} AS TEXT)) GLOB '*[0-9]*' THEN 1 ELSE 0 END) AS n_digit
            FROM {tbl}
            WHERE {nn_filter}"""
        ).fetchone()

        numeric_ratio = float(combo_row["numeric_ratio"] or 0) if combo_row else 0.0
        inferred_type = _infer_type(numeric_ratio, distinct_count, non_null)
        length_stats = {
            "min": int(combo_row["min_len"] or 0),
            "max": int(combo_row["max_len"] or 0),
            "avg": float(combo_row["avg_len"] or 0),
        } if combo_row else {"min": 0, "max": 0, "avg": 0.0}

        pattern_flags: list[str] = []
        if combo_row and non_null:
            nu, nl = int(combo_row["n_upper"] or 0), int(combo_row["n_lower"] or 0)
            pu, pl = nu / non_null, nl / non_null
            if pu > 0.8:
                pattern_flags.append("mostly_uppercase")
            if pl > 0.8:
                pattern_flags.append("mostly_lowercase")
            if pu < 0.8 and pl < 0.8 and (nu + nl) / non_null < 0.9:
                pattern_flags.append("mixed_case")
            if int(combo_row["n_digit"] or 0) / non_null > 0.5:
                pattern_flags.append("mostly_numeric_text")
            if int(combo_row["n_special"] or 0) / non_null > 0.1:
                pattern_flags.append("has_special_chars")

        top_rows = conn.execute(
            f"""SELECT CAST({qc} AS TEXT) AS v, COUNT(*) AS cnt
            FROM {tbl} WHERE {nn_filter}
            GROUP BY CAST({qc} AS TEXT) ORDER BY cnt DESC LIMIT 10"""
        ).fetchall()
        top_values = [
            {"value": str(r["v"]), "count": int(r["cnt"]), "pct": int(r["cnt"]) / total_rows if total_rows else 0.0}
            for r in top_rows
        ]

        numeric_stats: dict[str, float] | None = None
        if inferred_type in ("numeric", "id") and non_null > 0:
            # Consolidated numeric query: min/max/mean/stddev in one pass
            num_row = conn.execute(
                f"""SELECT
                    MIN(CAST({qc} AS REAL)) AS min_val,
                    MAX(CAST({qc} AS REAL)) AS max_val,
                    AVG(CAST({qc} AS REAL)) AS mean_val,
                    AVG(CAST({qc} AS REAL) * CAST({qc} AS REAL)) AS avg_sq
                FROM {tbl}
                WHERE {nn_filter} AND {num_pred}
                  AND CAST({qc} AS REAL) = CAST({qc} AS REAL)"""
            ).fetchone()
            if num_row and num_row["min_val"] is not None:
                offset = max(0, non_null // 2)
                med_row = conn.execute(
                    f"""SELECT CAST({qc} AS REAL) AS v FROM {tbl}
                    WHERE {nn_filter} AND {num_pred}
                    ORDER BY CAST({qc} AS REAL) LIMIT 1 OFFSET ?""",
                    (offset,),
                ).fetchone()
                avg_v = float(num_row["mean_val"] or 0)
                avg_sq = float(num_row["avg_sq"] or 0)
                variance = max(0.0, avg_sq - avg_v * avg_v)
                numeric_stats = {
                    "min": float(num_row["min_val"]),
                    "max": float(num_row["max_val"]),
                    "mean": avg_v,
                    "median": float(med_row["v"]) if med_row and med_row["v"] is not None else avg_v,
                    "stddev": variance**0.5,
                }

        sample_values = column_distinct_values(conn, table_name, col, 10)

        results.append({
            "name": col, "totalRows": total_rows,
            "nullCount": null_count, "fillRate": fill_rate,
            "distinctCount": distinct_count, "uniqueness": uniqueness,
            "numericRatio": numeric_ratio, "inferredType": inferred_type,
            "topValues": top_values, "numericStats": numeric_stats,
            "lengthStats": length_stats, "patternFlags": pattern_flags,
            "sampleValues": sample_values,
        })

    return results


def estimate_duplicate_rows(
    conn: sqlite3.Connection, table_name: str, max_cols: int = 5
) -> int:
    if not table_exists(conn, table_name):
        return 0
    cols = read_table_columns(conn, table_name)[:max_cols]
    if not cols:
        return 0
    tbl = quote_id(table_name)
    total_rows = table_row_count(conn, table_name)
    parts = " || '|' || ".join(f"COALESCE({quote_id(c)}, '')" for c in cols)
    row = conn.execute(
        f"SELECT COUNT(DISTINCT ({parts})) AS distinct_rows FROM {tbl}"
    ).fetchone()
    distinct_rows = int(row["distinct_rows"] or 0) if row else total_rows
    return max(0, total_rows - distinct_rows)


def compute_cross_table_consistency(
    conn: sqlite3.Connection, table_sql_names: list[str]
) -> float:
    if len(table_sql_names) <= 1:
        return 1.0
    schemas: list[set[str]] = []
    for t in table_sql_names:
        if not table_exists(conn, t):
            continue
        schemas.append({c.lower().strip() for c in read_table_columns(conn, t)})
    if len(schemas) <= 1:
        return 1.0
    total_sim = 0.0
    pairs = 0
    for i in range(len(schemas)):
        for j in range(i + 1, len(schemas)):
            a, b = schemas[i], schemas[j]
            inter = len(a & b)
            union = len(a | b)
            total_sim += inter / union if union else 0.0
            pairs += 1
    return total_sim / pairs if pairs else 1.0


def analyze_cross_group_sql(
    conn: sqlite3.Connection,
    group_profiles: list[dict[str, Any]],
    group_sql_names: dict[str, str],
) -> dict[str, Any]:
    """Schema and value overlap between groups (SQL only)."""
    from shared.db.stats_ops import column_distinct_count, compute_overlap

    total_groups = len(group_profiles)
    total_rows = sum(int(g.get("totalRows", 0)) for g in group_profiles)
    schema_overlap: dict[str, dict[str, Any]] = {g["groupId"]: {} for g in group_profiles}
    value_overlap: list[dict[str, Any]] = []

    for i, g_a in enumerate(group_profiles):
        gid_a = g_a["groupId"]
        sql_a = group_sql_names.get(gid_a)
        if not sql_a or not table_exists(conn, sql_a):
            continue
        cols_a = read_table_columns(conn, sql_a)
        norm_a = {c.lower().strip(): c for c in cols_a}
        set_a = set(norm_a.keys())

        for j in range(i + 1, len(group_profiles)):
            g_b = group_profiles[j]
            gid_b = g_b["groupId"]
            sql_b = group_sql_names.get(gid_b)
            if not sql_b or not table_exists(conn, sql_b):
                continue
            cols_b = read_table_columns(conn, sql_b)
            norm_b = {c.lower().strip(): c for c in cols_b}
            shared = sorted(set_a & set(norm_b.keys()))
            min_cols = min(len(cols_a), len(cols_b))
            overlap_pct = len(shared) / min_cols if min_cols else 0.0

            schema_overlap[gid_a][gid_b] = {
                "sharedColumns": [norm_a[n] for n in shared],
                "overlapPct": overlap_pct,
            }
            schema_overlap[gid_b][gid_a] = {
                "sharedColumns": [norm_b[n] for n in shared],
                "overlapPct": overlap_pct,
            }

            for norm_col in shared[:5]:
                real_a = norm_a.get(norm_col)
                real_b = norm_b.get(norm_col)
                if not real_a or not real_b:
                    continue
                dist_a = column_distinct_count(conn, sql_a, real_a)
                if dist_a == 0:
                    continue
                overlap = compute_overlap(conn, sql_a, real_a, sql_b, real_b)
                rate = overlap / dist_a
                if rate > 0.05:
                    value_overlap.append(
                        {
                            "groupA": gid_a,
                            "groupB": gid_b,
                            "column": real_a,
                            "overlapRate": rate,
                        }
                    )

    value_overlap.sort(key=lambda x: -x["overlapRate"])
    return {
        "totalGroups": total_groups,
        "totalRows": total_rows,
        "schemaOverlap": schema_overlap,
        "valueOverlap": value_overlap,
    }
