"""
Module-1-specific advanced join operations on SQLite.

Covers: join key discovery, composite match rate, left joins with dedup,
format pattern detection, and adaptive normalization.
"""

from __future__ import annotations

import re
import sqlite3
import time
from typing import Any, TypedDict

from shared.db.table_ops import (
    quote_id,
    read_table_columns,
    table_exists,
    table_row_count,
    normalize_for_match,
)
from shared.db.stats_ops import column_distinct_count


class ColumnProfile(TypedDict, total=False):
    name: str
    fill_rate: float
    distinct_count: int
    is_numeric: bool
    numeric_ratio: float
    sample_values: list[str]


def _name_tokens(col_name: str) -> set[str]:
    parts = re.split(r"[^a-z0-9]+", (col_name or "").lower())
    return {p for p in parts if p and p not in {"id", "key", "code", "num", "no", "number"}}


def _id_like(col_name: str) -> bool:
    n = (col_name or "").lower()
    return any(k in n for k in ("id", "key", "code", "number", "_no", "num"))


# ── Key discovery ──────────────────────────────────────────────────


def match_keys_distinct_sql(
    conn: sqlite3.Connection,
    fact_table: str,
    dim_table: str,
    fact_profiles: list[ColumnProfile] | None = None,
    dim_profiles: list[ColumnProfile] | None = None,
) -> list[dict[str, Any]]:
    if not table_exists(conn, fact_table) or not table_exists(conn, dim_table):
        return []

    fact_cols = read_table_columns(conn, fact_table)
    dim_cols = read_table_columns(conn, dim_table)
    if not fact_cols or not dim_cols:
        return []

    ft = quote_id(fact_table)
    dt = quote_id(dim_table)

    fact_profile_map: dict[str, ColumnProfile] = {}
    dim_profile_map: dict[str, ColumnProfile] = {}
    if fact_profiles:
        for p in fact_profiles:
            fact_profile_map[p["name"]] = p
    if dim_profiles:
        for p in dim_profiles:
            dim_profile_map[p["name"]] = p
    has_profiles = bool(fact_profile_map) and bool(dim_profile_map)

    fact_distinct: dict[str, int] = {}
    for c in fact_cols:
        fp = fact_profile_map.get(c)
        fact_distinct[c] = fp["distinct_count"] if fp else column_distinct_count(conn, fact_table, c)

    dim_distinct: dict[str, int] = {}
    for c in dim_cols:
        dp = dim_profile_map.get(c)
        dim_distinct[c] = dp["distinct_count"] if dp else column_distinct_count(conn, dim_table, c)

    results: list[dict[str, Any]] = []

    for dim_col in dim_cols:
        dim_count = dim_distinct[dim_col]
        if dim_count == 0:
            continue
        if has_profiles and dim_count < 3:
            continue

        dim_prof = dim_profile_map.get(dim_col)

        for fact_col in fact_cols:
            main_count = fact_distinct[fact_col]
            if main_count == 0:
                continue
            if has_profiles and main_count < 3:
                continue

            # Early pruning to avoid expensive O(F*D) overlap queries on clearly poor pairs.
            card_ratio = max(main_count, dim_count) / max(min(main_count, dim_count), 1)
            token_overlap = _name_tokens(fact_col) & _name_tokens(dim_col)
            if card_ratio > 10 and not token_overlap:
                continue
            if _id_like(fact_col) != _id_like(dim_col) and card_ratio > 6 and not token_overlap:
                continue

            if has_profiles and dim_prof:
                fact_prof = fact_profile_map.get(fact_col)
                if fact_prof:
                    f_num = fact_prof.get("numeric_ratio", 0)
                    d_num = dim_prof.get("numeric_ratio", 0)
                    if (f_num >= 0.8 and d_num < 0.2) or (d_num >= 0.8 and f_num < 0.2):
                        continue

            qf = quote_id(fact_col)
            qd = quote_id(dim_col)
            norm_f = normalize_for_match(qf)
            norm_d = normalize_for_match(qd)

            row = conn.execute(f"""
                SELECT COUNT(*) AS cnt FROM (
                    SELECT DISTINCT {norm_f} AS v FROM {ft}
                        WHERE {qf} IS NOT NULL AND TRIM({qf}) != ''
                    INTERSECT
                    SELECT DISTINCT {norm_d} AS v FROM {dt}
                        WHERE {qd} IS NOT NULL AND TRIM({qd}) != ''
                )
            """).fetchone()

            overlap = row["cnt"] if row else 0
            if overlap == 0:
                continue

            match_rate = overlap / max(main_count, 1)
            card_diff = abs(dim_count / max(main_count, 1) - 1)

            results.append({
                "main_column": fact_col,
                "dimension_column": dim_col,
                "main_distinct": main_count,
                "dim_distinct": dim_count,
                "distinct_matches": overlap,
                "match_rate_distinct": match_rate,
                "cardinality_diff_abs": card_diff,
            })

    results.sort(key=lambda r: (
        -r["dim_distinct"],
        -r["distinct_matches"],
        -r["match_rate_distinct"],
        r["cardinality_diff_abs"],
    ))
    return results


# ── Composite match rate ───────────────────────────────────────────


def compute_composite_match_rate_sql(
    conn: sqlite3.Connection,
    fact_table: str,
    dim_table: str,
    fact_keys: list[str],
    dim_keys: list[str],
) -> dict[str, Any]:
    zero = {"match_rate": 0, "distinct_matches": 0, "valid_fact_keys": 0}
    if not table_exists(conn, fact_table) or not table_exists(conn, dim_table):
        return zero
    if not fact_keys or len(fact_keys) != len(dim_keys):
        return zero

    ft = quote_id(fact_table)
    dt = quote_id(dim_table)

    fact_cols_set = set(read_table_columns(conn, fact_table))
    dim_cols_set = set(read_table_columns(conn, dim_table))
    for fk in fact_keys:
        if fk not in fact_cols_set:
            return zero
    for dk in dim_keys:
        if dk not in dim_cols_set:
            return zero

    fact_key_expr = " || '\\0' || ".join(normalize_for_match(quote_id(k)) for k in fact_keys)
    dim_key_expr = " || '\\0' || ".join(normalize_for_match(quote_id(k)) for k in dim_keys)

    fact_null_filter = " AND ".join(
        f"{quote_id(k)} IS NOT NULL AND TRIM({quote_id(k)}) != ''" for k in fact_keys
    )
    dim_null_filter = " AND ".join(
        f"{quote_id(k)} IS NOT NULL AND TRIM({quote_id(k)}) != ''" for k in dim_keys
    )

    row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {ft} WHERE {fact_null_filter}").fetchone()
    valid_fact_keys = row["cnt"] if row else 0
    if valid_fact_keys == 0:
        return zero

    row = conn.execute(f"""
        SELECT COUNT(*) AS cnt FROM {ft}
        WHERE {fact_null_filter}
            AND ({fact_key_expr}) IN (
                SELECT DISTINCT ({dim_key_expr}) FROM {dt} WHERE {dim_null_filter}
            )
    """).fetchone()
    match_count = row["cnt"] if row else 0

    row = conn.execute(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT DISTINCT ({fact_key_expr}) AS v FROM {ft}
            WHERE {fact_null_filter}
                AND ({fact_key_expr}) IN (
                    SELECT DISTINCT ({dim_key_expr}) FROM {dt} WHERE {dim_null_filter}
                )
        )
    """).fetchone()
    distinct_matches = row["cnt"] if row else 0

    return {
        "match_rate": match_count / valid_fact_keys if valid_fact_keys else 0,
        "distinct_matches": distinct_matches,
        "valid_fact_keys": valid_fact_keys,
    }


# ── Left join ──────────────────────────────────────────────────────


def left_join_sql(
    conn: sqlite3.Connection,
    fact_table: str,
    dim_table: str,
    fact_keys: list[str],
    dim_keys: list[str],
    output_table: str,
    dim_cols_to_keep: list[str] | None,
    dim_name: str,
    format_hints: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    metrics: dict[str, Any] = {
        "dimension_group": dim_name,
        "fact_key": " + ".join(fact_keys),
        "dim_key": " + ".join(dim_keys),
    }

    if not table_exists(conn, fact_table):
        metrics["status"] = "skipped"
        metrics["reason"] = "fact table empty"
        return metrics
    if not table_exists(conn, dim_table):
        metrics["status"] = "skipped"
        metrics["reason"] = "dim table empty"
        return metrics

    fact_cols = read_table_columns(conn, fact_table)
    dim_cols = read_table_columns(conn, dim_table)
    fact_rows_before = table_row_count(conn, fact_table)
    dim_rows_before = table_row_count(conn, dim_table)
    dim_key_set = set(dim_keys)

    for fk in fact_keys:
        if fk not in fact_cols:
            metrics["status"] = "skipped"
            metrics["reason"] = f'fact key "{fk}" missing'
            return metrics
    for dk in dim_keys:
        if dk not in dim_cols:
            metrics["status"] = "skipped"
            metrics["reason"] = f'dim key "{dk}" missing'
            return metrics

    add_cols = [c for c in dim_cols if c not in dim_key_set]
    if dim_cols_to_keep is not None:
        keep_set = set(dim_cols_to_keep)
        add_cols = [c for c in add_cols if c in keep_set]

    fact_col_set = set(fact_cols)
    rename_map: dict[str, str] = {}
    for c in add_cols:
        rename_map[c] = f"{c}_{dim_name}" if c in fact_col_set else c

    added_column_names = list(rename_map.values())

    dim_null_filter = " AND ".join(
        f"{quote_id(k)} IS NOT NULL AND TRIM({quote_id(k)}) != ''" for k in dim_keys
    )
    dim_key_expr = ", ".join(quote_id(k) for k in dim_keys)
    dedup_dim = f"_dedup_dim_{int(time.time() * 1000)}"
    dim_select_cols = ", ".join(quote_id(c) for c in [*dim_keys, *add_cols])

    conn.execute(f"DROP TABLE IF EXISTS {quote_id(dedup_dim)}")
    conn.execute(f"""
        CREATE TEMP TABLE {quote_id(dedup_dim)} AS
        SELECT {dim_select_cols} FROM {quote_id(dim_table)}
        WHERE rowid IN (
            SELECT MIN(rowid) FROM {quote_id(dim_table)}
            WHERE {dim_null_filter}
            GROUP BY {dim_key_expr}
        )
    """)

    row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {quote_id(dedup_dim)}").fetchone()
    dedup_dim_count = row["cnt"] if row else 0
    dup_count = dim_rows_before - dedup_dim_count

    on_parts: list[str] = []
    for i, fk in enumerate(fact_keys):
        dk = dim_keys[i]
        if format_hints and i < len(format_hints):
            f_expr = format_hints[i]["factExpr"].replace(quote_id(fk), f"f.{quote_id(fk)}")
            d_expr = format_hints[i]["dimExpr"].replace(quote_id(dk), f"d.{quote_id(dk)}")
            on_parts.append(f"{f_expr} = {d_expr}")
        else:
            on_parts.append(
                f"{normalize_for_match(f'f.{quote_id(fk)}')} = {normalize_for_match(f'd.{quote_id(dk)}')}"
            )
    on_clause = " AND ".join(on_parts)

    fact_select = [f"f.{quote_id(c)}" for c in fact_cols]
    dim_select = [f"d.{quote_id(c)} AS {quote_id(rename_map[c])}" for c in add_cols]
    select_list = ", ".join(fact_select + dim_select)

    conn.execute(f"DROP TABLE IF EXISTS {quote_id(output_table)}")
    conn.execute(f"""
        CREATE TABLE {quote_id(output_table)} AS
        SELECT {select_list}
        FROM {quote_id(fact_table)} f
        LEFT JOIN {quote_id(dedup_dim)} d ON {on_clause}
    """)
    conn.execute(f"DROP TABLE IF EXISTS {quote_id(dedup_dim)}")
    conn.commit()

    out_row_count = table_row_count(conn, output_table)
    out_cols = read_table_columns(conn, output_table)
    row_mult = out_row_count / max(fact_rows_before, 1)

    fact_null_filter = " AND ".join(
        f"{quote_id(k)} IS NOT NULL AND TRIM({quote_id(k)}) != ''" for k in fact_keys
    )
    row = conn.execute(
        f"SELECT COUNT(*) AS cnt FROM {quote_id(fact_table)} WHERE {fact_null_filter}"
    ).fetchone()
    valid_fact_keys_count = row["cnt"] if row else 0

    mr = 0.0
    if added_column_names and valid_fact_keys_count > 0:
        sample_col = quote_id(added_column_names[0])
        row = conn.execute(f"""
            SELECT COUNT(CASE WHEN {sample_col} IS NOT NULL THEN 1 END) AS matched
            FROM {quote_id(output_table)}
        """).fetchone()
        mr = (row["matched"] if row else 0) / max(out_row_count, 1)

    metrics["status"] = "ok"
    metrics["dim_before"] = [dim_rows_before, len(dim_cols)]
    metrics["dim_used"] = [dedup_dim_count, len(dim_cols)]
    metrics["dup_dropped"] = dup_count
    metrics["out_shape"] = [out_row_count, len(out_cols)]
    metrics["row_multiplier"] = row_mult
    metrics["match_rate"] = mr
    metrics["added_cols"] = len(added_column_names)
    metrics["unused_fact_keys"] = 0
    metrics["unused_dim_keys"] = 0
    metrics["added_column_names"] = added_column_names

    MAX_ROW_MULTIPLIER = 1.02
    MIN_MATCH_RATE = 0.30

    if row_mult > MAX_ROW_MULTIPLIER:
        metrics["status"] = "skipped"
        metrics["reason"] = f"row explosion risk ({row_mult:.3f})"
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(output_table)}")
        conn.commit()
        return metrics

    if mr < MIN_MATCH_RATE:
        metrics["status"] = "warning"
        metrics["reason"] = f"low match rate ({mr:.3f})"
        metrics["low_quality_join"] = True

    return metrics


# ── Dim uniqueness ─────────────────────────────────────────────────


def check_dim_uniqueness(
    conn: sqlite3.Connection,
    dim_table: str,
    keys: list[str],
) -> dict[str, Any]:
    if not table_exists(conn, dim_table) or not keys:
        return {"total": 0, "distinct_keys": 0, "is_unique": False}

    dt = quote_id(dim_table)
    null_filter = " AND ".join(
        f"{quote_id(k)} IS NOT NULL AND TRIM({quote_id(k)}) != ''" for k in keys
    )
    key_expr = " || '\\0' || ".join(normalize_for_match(quote_id(k)) for k in keys)

    row = conn.execute(f"""
        SELECT COUNT(*) AS total,
               COUNT(DISTINCT ({key_expr})) AS distinct_keys
        FROM {dt} WHERE {null_filter}
    """).fetchone()

    total = row["total"] if row else 0
    distinct_keys = row["distinct_keys"] if row else 0
    return {"total": total, "distinct_keys": distinct_keys, "is_unique": total > 0 and total == distinct_keys}


# ── Column profiling ───────────────────────────────────────────────


def profile_all_columns(
    conn: sqlite3.Connection,
    table_name: str,
) -> list[ColumnProfile]:
    if not table_exists(conn, table_name):
        return []
    cols = read_table_columns(conn, table_name)
    tbl = quote_id(table_name)
    results: list[ColumnProfile] = []

    for col in cols:
        qc = quote_id(col)
        row = conn.execute(f"""
            SELECT
                COUNT(*) AS total,
                COUNT(CASE WHEN {qc} IS NOT NULL AND TRIM({qc}) != '' THEN 1 END) AS non_null,
                COUNT(DISTINCT CASE WHEN {qc} IS NOT NULL AND TRIM({qc}) != '' THEN {qc} END) AS distinct_count,
                COUNT(CASE WHEN TRIM({qc}) GLOB '*[0-9]*' AND TRIM({qc}) NOT GLOB '*[^0-9.eE+-]*' THEN 1 END) AS numeric_count
            FROM {tbl}
        """).fetchone()

        total = row["total"]
        non_null = row["non_null"]
        distinct_count = row["distinct_count"]
        numeric_count = row["numeric_count"]
        fill_rate = non_null / total if total > 0 else 0.0
        numeric_ratio = numeric_count / non_null if non_null > 0 else 0.0
        is_numeric = numeric_ratio >= 0.8

        sample_rows = conn.execute(
            f"SELECT DISTINCT {qc} AS v FROM {tbl} WHERE {qc} IS NOT NULL AND TRIM({qc}) != '' LIMIT 10"
        ).fetchall()

        results.append({
            "name": col,
            "fill_rate": fill_rate,
            "distinct_count": distinct_count,
            "is_numeric": is_numeric,
            "numeric_ratio": numeric_ratio,
            "sample_values": [str(r["v"]) for r in sample_rows],
        })

    return results


def classify_dim_columns(
    conn: sqlite3.Connection,
    table_name: str,
    exclude_keys: list[str],
) -> list[dict[str, Any]]:
    if not table_exists(conn, table_name):
        return []
    cols = read_table_columns(conn, table_name)
    key_set = set(exclude_keys)
    tbl = quote_id(table_name)
    results: list[dict[str, Any]] = []

    for col in cols:
        if col in key_set:
            continue
        qc = quote_id(col)
        row = conn.execute(f"""
            SELECT
                COUNT(*) AS total,
                COUNT(CASE WHEN {qc} IS NOT NULL AND TRIM({qc}) != '' THEN 1 END) AS non_null,
                COUNT(DISTINCT CASE WHEN {qc} IS NOT NULL AND TRIM({qc}) != '' THEN {qc} END) AS distinct_count,
                COUNT(CASE WHEN TRIM({qc}) GLOB '*[0-9]*' AND TRIM({qc}) NOT GLOB '*[^0-9.eE+-]*' THEN 1 END) AS numeric_count
            FROM {tbl}
        """).fetchone()

        total = row["total"]
        non_null = row["non_null"]
        distinct_count = row["distinct_count"]
        numeric_count = row["numeric_count"]
        fill_rate = non_null / total if total > 0 else 0.0
        is_numeric = (numeric_count / non_null >= 0.8) if non_null > 0 else False

        sample_rows = conn.execute(
            f"SELECT DISTINCT {qc} AS v FROM {tbl} WHERE {qc} IS NOT NULL AND TRIM({qc}) != '' LIMIT 5"
        ).fetchall()

        results.append({
            "name": col,
            "fill_rate": fill_rate,
            "distinct_count": distinct_count,
            "is_numeric": is_numeric,
            "sample_values": [str(r["v"]) for r in sample_rows],
        })

    return results


# ── Format pattern detection ───────────────────────────────────────


def _sample_column_values(conn: sqlite3.Connection, table: str, col: str, limit: int = 50) -> list[str]:
    try:
        rows = conn.execute(
            f"SELECT DISTINCT {quote_id(col)} AS v FROM {quote_id(table)} "
            f"WHERE {quote_id(col)} IS NOT NULL AND TRIM({quote_id(col)}) != '' LIMIT ?",
            (limit,),
        ).fetchall()
        return [str(r["v"]) for r in rows]
    except Exception:
        return []


def _detect_consistent_prefix(values: list[str]) -> str | None:
    if len(values) < 3:
        return None
    trimmed = [v.strip() for v in values if v.strip()]
    if len(trimmed) < 3:
        return None

    prefix = trimmed[0]
    for v in trimmed[1:]:
        while prefix and not v.startswith(prefix):
            prefix = prefix[:-1]
        if not prefix:
            return None

    if not prefix or len(prefix) >= len(trimmed[0]):
        return None

    if re.search(r"[^a-zA-Z0-9]$", prefix) or re.match(r"^[^0-9]+$", prefix):
        match_count = sum(1 for v in trimmed if v.startswith(prefix))
        if match_count / len(trimmed) >= 0.8:
            return prefix

    return None


def detect_format_pattern(
    conn: sqlite3.Connection,
    table_a: str,
    col_a: str,
    table_b: str,
    col_b: str,
) -> dict[str, str]:
    vals_a = _sample_column_values(conn, table_a, col_a)
    vals_b = _sample_column_values(conn, table_b, col_b)
    qc_a = quote_id(col_a)
    qc_b = quote_id(col_b)
    default = {
        "type": "none",
        "detail": "",
        "expressionA": normalize_for_match(qc_a),
        "expressionB": normalize_for_match(qc_b),
    }

    if not vals_a or not vals_b:
        return default

    prefix_a = _detect_consistent_prefix(vals_a)
    prefix_b = _detect_consistent_prefix(vals_b)

    if prefix_a and not prefix_b:
        esc = prefix_a.replace("'", "''")
        return {
            "type": "prefix_strip",
            "detail": f'Column A has prefix "{prefix_a}"',
            "expressionA": normalize_for_match(f"REPLACE({qc_a}, '{esc}', '')"),
            "expressionB": normalize_for_match(qc_b),
        }
    if prefix_b and not prefix_a:
        esc = prefix_b.replace("'", "''")
        return {
            "type": "prefix_strip",
            "detail": f'Column B has prefix "{prefix_b}"',
            "expressionA": normalize_for_match(qc_a),
            "expressionB": normalize_for_match(f"REPLACE({qc_b}, '{esc}', '')"),
        }
    if prefix_a and prefix_b and prefix_a != prefix_b:
        esc_a = prefix_a.replace("'", "''")
        esc_b = prefix_b.replace("'", "''")
        return {
            "type": "prefix_strip",
            "detail": f'Column A has prefix "{prefix_a}", Column B has prefix "{prefix_b}"',
            "expressionA": normalize_for_match(f"REPLACE({qc_a}, '{esc_a}', '')"),
            "expressionB": normalize_for_match(f"REPLACE({qc_b}, '{esc_b}', '')"),
        }

    has_leading_zeros_a = any(re.match(r"^0\d+$", v.strip()) for v in vals_a)
    has_leading_zeros_b = any(re.match(r"^0\d+$", v.strip()) for v in vals_b)
    all_numeric_a = all(re.match(r"^\d+$", v.strip()) for v in vals_a)
    all_numeric_b = all(re.match(r"^\d+$", v.strip()) for v in vals_b)

    if (has_leading_zeros_a or has_leading_zeros_b) and all_numeric_a and all_numeric_b:
        return {
            "type": "zero_pad",
            "detail": "Zero-padding difference detected",
            "expressionA": f"CAST(CAST(TRIM({qc_a}) AS INTEGER) AS TEXT)",
            "expressionB": f"CAST(CAST(TRIM({qc_b}) AS INTEGER) AS TEXT)",
        }

    has_dashes_a = any("-" in v for v in vals_a)
    has_dashes_b = any("-" in v for v in vals_b)
    has_underscores_a = any("_" in v for v in vals_a)
    has_underscores_b = any("_" in v for v in vals_b)

    if (has_dashes_a and has_underscores_b) or (has_underscores_a and has_dashes_b):
        return {
            "type": "separator_normalize",
            "detail": "Separator mismatch (dash vs underscore)",
            "expressionA": f"LOWER(REPLACE(REPLACE(TRIM({qc_a}), '-', ''), '_', ''))",
            "expressionB": f"LOWER(REPLACE(REPLACE(TRIM({qc_b}), '-', ''), '_', ''))",
        }

    return default


def build_adaptive_normalization(
    conn: sqlite3.Connection,
    table_a: str,
    col_a: str,
    table_b: str,
    col_b: str,
) -> dict[str, Any]:
    pattern = detect_format_pattern(conn, table_a, col_a, table_b, col_b)
    return {
        "exprA": pattern["expressionA"],
        "exprB": pattern["expressionB"],
        "pattern": pattern,
    }
