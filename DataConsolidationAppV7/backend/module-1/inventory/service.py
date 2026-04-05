"""SQL-based table cleaning: copy raw -> work table, transform, replace tbl__."""

from __future__ import annotations

import sqlite3
from typing import Any, Mapping

from shared.db import (
    column_stats,
    drop_table,
    get_meta,
    lookup_sql_name,
    quote_id,
    read_table,
    read_table_columns,
    register_table,
    safe_table_name,
    set_meta,
    table_exists,
    table_row_count,
)

from data_loading.service import (
    PREVIEW_ROWS,
    build_files_payload_from_db,
    build_inventory_from_db,
)


def _shadow_name(table_key: str) -> str:
    return safe_table_name("tmp_clean_b", table_key)


def _work_name(table_key: str) -> str:
    return safe_table_name("tmp_clean", table_key)


def _rebuild_from_select(
    conn: sqlite3.Connection,
    work: str,
    shadow: str,
    select_list_sql: str,
) -> None:
    q_work = quote_id(work)
    q_shadow = quote_id(shadow)
    drop_table(conn, shadow)
    conn.execute(f"CREATE TABLE {q_shadow} AS SELECT {select_list_sql} FROM {q_work}")
    conn.commit()
    drop_table(conn, work)
    conn.execute(f"ALTER TABLE {q_shadow} RENAME TO {q_work}")
    conn.commit()


def _delete_null_or_empty_rows(conn: sqlite3.Connection, table: str, columns: list[str]) -> None:
    if not columns:
        return
    tbl = quote_id(table)
    parts = [f"({quote_id(c)} IS NOT NULL AND TRIM({quote_id(c)}) != '')" for c in columns]
    cond = " OR ".join(parts)
    conn.execute(f"DELETE FROM {tbl} WHERE NOT ({cond})")
    conn.commit()


def _apply_case_and_trim(
    conn: sqlite3.Connection,
    table: str,
    columns: list[str],
    case_mode: str,
    trim_whitespace: bool,
) -> None:
    if case_mode not in ("upper", "lower") and not trim_whitespace:
        return
    assignments: list[str] = []
    for c in columns:
        qc = quote_id(c)
        expr = qc
        if case_mode == "upper":
            expr = f"UPPER({expr})"
        elif case_mode == "lower":
            expr = f"LOWER({expr})"
        if trim_whitespace:
            expr = f"TRIM({expr})"
        assignments.append(f"{qc} = {expr}")
    if not assignments:
        return
    conn.execute(f"UPDATE {quote_id(table)} SET {', '.join(assignments)}")
    conn.commit()


def _apply_column_types(
    conn: sqlite3.Connection,
    table: str,
    column_types: Mapping[str, Any],
    columns: list[str],
) -> None:
    col_set = set(columns)
    tbl = quote_id(table)
    dirty = False
    for col, target in column_types.items():
        if col not in col_set:
            continue
        if not target or target == "string":
            continue
        qc = quote_id(col)
        if target == "number":
            conn.execute(
                f"""UPDATE {tbl}
                SET {qc} = CAST(CAST(TRIM({qc}) AS REAL) AS TEXT)
                WHERE TRIM({qc}) != ''
                  AND TRIM({qc}) GLOB '*[0-9]*'
                  AND TRIM({qc}) NOT GLOB '*[^0-9.eE+-]*'"""
            )
            dirty = True
        elif target == "date":
            conn.execute(
                f"""UPDATE {tbl}
                SET {qc} = date(TRIM({qc}))
                WHERE date(TRIM({qc})) IS NOT NULL"""
            )
            dirty = True
    if dirty:
        conn.commit()


def _deduplicate_rows(conn: sqlite3.Connection, table: str, dedup_cols: list[str]) -> int:
    cols = read_table_columns(conn, table)
    existing = [c for c in dedup_cols if c in cols]
    if not existing:
        return 0
    before = table_row_count(conn, table)
    if before == 0:
        return 0
    tbl = quote_id(table)
    group_exprs = ", ".join(quote_id(c) for c in existing)
    conn.execute(
        f"""DELETE FROM {tbl}
        WHERE rowid NOT IN (
            SELECT MIN(rowid) FROM {tbl} GROUP BY {group_exprs}
        )"""
    )
    conn.commit()
    return before - table_row_count(conn, table)


def dedup_preview_stats(
    conn: sqlite3.Connection,
    group_id: str,
    dedup_columns: list[str],
) -> dict[str, Any]:
    """Return dedup statistics WITHOUT modifying data."""
    sql_name = lookup_sql_name(conn, group_id)
    if not sql_name or not table_exists(conn, sql_name):
        raise ValueError(f"Table not found for group: {group_id}")
    cols = read_table_columns(conn, sql_name)
    existing = [c for c in dedup_columns if c in cols]
    if not existing:
        raise ValueError("None of the selected columns exist in this table.")
    total_rows = table_row_count(conn, sql_name)
    if total_rows == 0:
        return {
            "group_id": group_id, "total_rows": 0, "unique_rows": 0,
            "duplicate_rows": 0, "decrease_pct": 0.0,
        }
    tbl = quote_id(sql_name)
    group_exprs = ", ".join(quote_id(c) for c in existing)
    row = conn.execute(
        f"SELECT COUNT(*) FROM (SELECT 1 FROM {tbl} GROUP BY {group_exprs})"
    ).fetchone()
    unique_rows = row[0] if row else total_rows
    duplicate_rows = total_rows - unique_rows
    decrease_pct = round(100 * duplicate_rows / total_rows, 2) if total_rows else 0.0
    return {
        "group_id": group_id,
        "total_rows": total_rows,
        "unique_rows": unique_rows,
        "duplicate_rows": duplicate_rows,
        "decrease_pct": decrease_pct,
        "dedup_columns": existing,
    }


def dedup_apply_group(
    conn: sqlite3.Connection,
    group_id: str,
    dedup_columns: list[str],
) -> dict[str, Any]:
    """Apply deduplication to a group table, keeping first occurrence."""
    sql_name = lookup_sql_name(conn, group_id)
    if not sql_name or not table_exists(conn, sql_name):
        raise ValueError(f"Table not found for group: {group_id}")
    before = table_row_count(conn, sql_name)
    removed = _deduplicate_rows(conn, sql_name, dedup_columns)
    after = table_row_count(conn, sql_name)

    schema = get_meta(conn, "groupSchemaTableRows") or []
    for entry in schema:
        if entry.get("group_id") == group_id:
            entry["rows"] = after
    set_meta(conn, "groupSchemaTableRows", schema)

    return {
        "group_id": group_id,
        "rows_before": before,
        "rows_after": after,
        "duplicates_removed": removed,
        "decrease_pct": round(100 * removed / before, 2) if before else 0.0,
    }


def delete_rows_sql(
    conn: sqlite3.Connection,
    table_key: str,
    row_ids: list[int],
) -> dict[str, Any]:
    """Delete specific rows by RECORD_ID (or rowid) from tbl__ and raw__ tables."""
    tbl_sql = safe_table_name("tbl", table_key)
    raw_sql = safe_table_name("raw", table_key)

    if not table_exists(conn, tbl_sql):
        raise ValueError(f'Table "{table_key}" not found.')

    tbl_cols = read_table_columns(conn, tbl_sql)
    use_record_id = "RECORD_ID" in tbl_cols

    placeholders = ",".join("?" for _ in row_ids)
    if use_record_id:
        conn.execute(f"DELETE FROM {quote_id(tbl_sql)} WHERE RECORD_ID IN ({placeholders})", row_ids)
        if table_exists(conn, raw_sql) and "RECORD_ID" in read_table_columns(conn, raw_sql):
            conn.execute(f"DELETE FROM {quote_id(raw_sql)} WHERE RECORD_ID IN ({placeholders})", row_ids)
    else:
        conn.execute(f"DELETE FROM {quote_id(tbl_sql)} WHERE rowid IN ({placeholders})", row_ids)
        if table_exists(conn, raw_sql):
            conn.execute(f"DELETE FROM {quote_id(raw_sql)} WHERE rowid IN ({placeholders})", row_ids)
    conn.commit()

    inv = build_inventory_from_db(conn)
    files_payload = build_files_payload_from_db(conn)
    set_meta(conn, "inv", inv)
    set_meta(conn, "filesPayload", files_payload)

    out_cols = read_table_columns(conn, tbl_sql)
    preview_rows = read_table(conn, tbl_sql, PREVIEW_ROWS)
    inv_row = next((r for r in inv if r["table_key"] == table_key), None)

    return {
        "preview": {"columns": out_cols, "rows": preview_rows},
        "inventoryRow": inv_row,
        "deletedCount": len(row_ids),
    }


def clean_table_sql(
    conn: sqlite3.Connection,
    table_key: str,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    raw_sql = safe_table_name("raw", table_key)
    if not table_exists(conn, raw_sql):
        raise ValueError(f'Table "{table_key}" not found.')

    remove_null_rows = bool(config.get("removeNullRows", False))
    remove_null_columns = bool(config.get("removeNullColumns", False))
    drop_columns = list(config.get("dropColumns") or [])
    case_mode = str(config.get("caseMode") or "upper")
    trim_whitespace = bool(config.get("trimWhitespace", True))
    column_types = config.get("columnTypes") or {}
    if not isinstance(column_types, Mapping):
        column_types = {}
    deduplicate_columns = list(config.get("deduplicateColumns") or [])

    work = _work_name(table_key)
    shadow = _shadow_name(table_key)

    drop_table(conn, work, commit=False)
    drop_table(conn, shadow, commit=False)
    conn.execute(f"CREATE TABLE {quote_id(work)} AS SELECT * FROM {quote_id(raw_sql)}")
    conn.commit()

    # 1. Drop columns
    if drop_columns:
        drop_set = set(drop_columns)
        cols = read_table_columns(conn, work)
        kept = [c for c in cols if c not in drop_set]
        if not kept:
            raise ValueError("Cannot drop all columns.")
        select_list = ", ".join(quote_id(c) for c in kept)
        _rebuild_from_select(conn, work, shadow, select_list)

    # 2. Remove null/empty rows
    if remove_null_rows:
        cols = read_table_columns(conn, work)
        _delete_null_or_empty_rows(conn, work, cols)

    # 3. Remove null/empty columns
    if remove_null_columns and table_row_count(conn, work) > 0:
        stats = column_stats(conn, work)
        kept = [s["column_name"] for s in stats if s.get("non_null_count", 0) > 0]
        if not kept:
            raise ValueError("All columns are empty; nothing to keep.")
        select_list = ", ".join(quote_id(c) for c in kept)
        _rebuild_from_select(conn, work, shadow, select_list)

    # 4-5. Case + trim
    cols = read_table_columns(conn, work)
    _apply_case_and_trim(conn, work, cols, case_mode, trim_whitespace)

    # 6. Column type conversion
    cols = read_table_columns(conn, work)
    _apply_column_types(conn, work, column_types, cols)

    # 7. Deduplication
    duplicates_removed = _deduplicate_rows(conn, work, deduplicate_columns)

    tbl_name = safe_table_name("tbl", table_key)
    drop_table(conn, tbl_name)
    conn.execute(f"ALTER TABLE {quote_id(work)} RENAME TO {quote_id(tbl_name)}")
    conn.commit()
    drop_table(conn, shadow)

    register_table(conn, table_key, tbl_name)

    inv = build_inventory_from_db(conn)
    files_payload = build_files_payload_from_db(conn)
    set_meta(conn, "inv", inv)
    set_meta(conn, "filesPayload", files_payload)

    out_cols = read_table_columns(conn, tbl_name)
    preview_rows = read_table(conn, tbl_name, PREVIEW_ROWS)
    inv_row = next((r for r in inv if r["table_key"] == table_key), None)

    return {
        "preview": {"columns": out_cols, "rows": preview_rows},
        "inventoryRow": inv_row,
        "duplicatesRemoved": duplicates_removed,
    }


def clean_group_sql(
    conn: sqlite3.Connection,
    group_id: str,
    config: Mapping[str, Any],
) -> dict[str, Any]:
    """Clean an appended/hn group table in-place (post-normalisation cleaning)."""
    source_sql = lookup_sql_name(conn, group_id)
    if not source_sql or not table_exists(conn, source_sql):
        raise ValueError(f'Group table "{group_id}" not found.')

    remove_null_rows = bool(config.get("removeNullRows", False))
    remove_null_columns = bool(config.get("removeNullColumns", False))
    drop_columns = list(config.get("dropColumns") or [])
    case_mode = str(config.get("caseMode") or "upper")
    trim_whitespace = bool(config.get("trimWhitespace", True))
    column_types = config.get("columnTypes") or {}
    if not isinstance(column_types, Mapping):
        column_types = {}
    deduplicate_columns = list(config.get("deduplicateColumns") or [])

    work = _work_name(group_id)
    shadow = _shadow_name(group_id)

    drop_table(conn, work, commit=False)
    drop_table(conn, shadow, commit=False)
    conn.execute(f"CREATE TABLE {quote_id(work)} AS SELECT * FROM {quote_id(source_sql)}")
    conn.commit()

    if drop_columns:
        drop_set = set(drop_columns)
        cols = read_table_columns(conn, work)
        kept = [c for c in cols if c not in drop_set]
        if not kept:
            raise ValueError("Cannot drop all columns.")
        select_list = ", ".join(quote_id(c) for c in kept)
        _rebuild_from_select(conn, work, shadow, select_list)

    if remove_null_rows:
        cols = read_table_columns(conn, work)
        _delete_null_or_empty_rows(conn, work, cols)

    if remove_null_columns and table_row_count(conn, work) > 0:
        stats = column_stats(conn, work)
        kept = [s["column_name"] for s in stats if s.get("non_null_count", 0) > 0]
        if not kept:
            raise ValueError("All columns are empty; nothing to keep.")
        select_list = ", ".join(quote_id(c) for c in kept)
        _rebuild_from_select(conn, work, shadow, select_list)

    cols = read_table_columns(conn, work)
    _apply_case_and_trim(conn, work, cols, case_mode, trim_whitespace)

    cols = read_table_columns(conn, work)
    _apply_column_types(conn, work, column_types, cols)

    duplicates_removed = _deduplicate_rows(conn, work, deduplicate_columns)

    drop_table(conn, source_sql)
    conn.execute(f"ALTER TABLE {quote_id(work)} RENAME TO {quote_id(source_sql)}")
    conn.commit()
    drop_table(conn, shadow)

    register_table(conn, group_id, source_sql)

    out_cols = read_table_columns(conn, source_sql)
    n_rows = table_row_count(conn, source_sql)
    preview_rows = read_table(conn, source_sql, PREVIEW_ROWS)

    group_schema = get_meta(conn, "groupSchemaTableRows") or []
    for gs in group_schema:
        if gs.get("group_id") == group_id:
            gs["rows"] = n_rows
            gs["cols"] = len(out_cols)
            gs["columns"] = out_cols
            gs["columns_preview"] = ", ".join(out_cols[:60]) + (" ..." if len(out_cols) > 60 else "")
            break
    set_meta(conn, "groupSchemaTableRows", group_schema)

    return {
        "preview": {"columns": out_cols, "rows": preview_rows},
        "groupRow": {"group_id": group_id, "rows": n_rows, "cols": len(out_cols), "columns": out_cols},
        "duplicatesRemoved": duplicates_removed,
    }


# ---------------------------------------------------------------------------
# 5c: Column value standardization (leading/trailing zeros)
# ---------------------------------------------------------------------------

def analyze_column_format(
    conn: sqlite3.Connection,
    group_id: str,
    columns: list[str],
) -> list[dict[str, Any]]:
    """Analyze selected columns for leading-zero patterns and numeric format.

    Returns per-column stats: has_leading_zeros, pct_leading_zeros, all_numeric,
    min_len, max_len, mode_len, recommendation ('strip'|'pad'|'none'), reason.
    """
    sql_name = lookup_sql_name(conn, group_id)
    if not sql_name or not table_exists(conn, sql_name):
        raise ValueError(f"Table not found for group: {group_id}")

    existing_cols = read_table_columns(conn, sql_name)
    valid = [c for c in columns if c in existing_cols]
    if not valid:
        raise ValueError("None of the selected columns exist in this table.")

    tbl = quote_id(sql_name)
    results: list[dict[str, Any]] = []

    for col in valid:
        qc = quote_id(col)
        row = conn.execute(
            f"""SELECT
                COUNT(*) AS total,
                SUM(CASE WHEN TRIM({qc}) GLOB '0[0-9]*' THEN 1 ELSE 0 END) AS leading_zeros,
                SUM(CASE WHEN TRIM({qc}) GLOB '[0-9]*'
                          AND TRIM({qc}) NOT GLOB '*[^0-9]*' THEN 1 ELSE 0 END) AS all_numeric,
                MIN(LENGTH(TRIM({qc}))) AS min_len,
                MAX(LENGTH(TRIM({qc}))) AS max_len
            FROM {tbl}
            WHERE TRIM({qc}) != ''"""
        ).fetchone()

        total = row[0] or 0
        leading_zeros_count = row[1] or 0
        all_numeric_count = row[2] or 0
        min_len = row[3] or 0
        max_len = row[4] or 0

        pct_leading = round(100 * leading_zeros_count / total, 1) if total else 0.0
        pct_numeric = round(100 * all_numeric_count / total, 1) if total else 0.0
        is_all_numeric = pct_numeric >= 90
        has_leading = pct_leading >= 5 and is_all_numeric

        mode_row = conn.execute(
            f"""SELECT LENGTH(TRIM({qc})) AS len, COUNT(*) AS cnt
                FROM {tbl}
                WHERE TRIM({qc}) != ''
                GROUP BY len ORDER BY cnt DESC LIMIT 1"""
        ).fetchone()
        mode_len = mode_row[0] if mode_row else 0

        if has_leading and min_len != max_len:
            recommendation = "pad"
            reason = (
                f"{pct_leading}% of values have leading zeros. "
                f"Lengths vary ({min_len}-{max_len}), most common length: {mode_len}. "
                f"Recommend padding to {max_len} characters for consistency."
            )
        elif has_leading and min_len == max_len:
            recommendation = "none"
            reason = (
                f"{pct_leading}% of values have leading zeros but all are {max_len} chars. "
                f"Already consistent — no action needed."
            )
        elif is_all_numeric and not has_leading and min_len != max_len:
            recommendation = "pad"
            reason = (
                f"All numeric values with varying lengths ({min_len}-{max_len}). "
                f"No leading zeros detected — may need padding to match other datasets. "
                f"Most common length: {mode_len}."
            )
        else:
            recommendation = "none"
            reason = (
                f"Column is {'numeric' if is_all_numeric else 'mixed-type'}. "
                f"No leading-zero issues detected."
            )

        results.append({
            "column": col,
            "total_values": total,
            "has_leading_zeros": has_leading,
            "pct_leading_zeros": pct_leading,
            "all_numeric": is_all_numeric,
            "pct_numeric": pct_numeric,
            "min_len": min_len,
            "max_len": max_len,
            "mode_len": mode_len,
            "recommendation": recommendation,
            "reason": reason,
        })

    return results


def apply_column_standardize(
    conn: sqlite3.Connection,
    group_id: str,
    actions: list[dict[str, Any]],
) -> dict[str, Any]:
    """Apply strip/pad operations to selected columns.

    Each action dict: {column, operation: 'strip'|'pad'|'none', pad_length: int}.
    Returns summary of changes made.
    """
    sql_name = lookup_sql_name(conn, group_id)
    if not sql_name or not table_exists(conn, sql_name):
        raise ValueError(f"Table not found for group: {group_id}")

    existing_cols = read_table_columns(conn, sql_name)
    tbl = quote_id(sql_name)
    applied: list[dict[str, str]] = []

    for act in actions:
        col = act.get("column", "")
        operation = act.get("operation", "none")
        if col not in existing_cols or operation == "none":
            continue

        qc = quote_id(col)

        if operation == "strip":
            conn.execute(
                f"""UPDATE {tbl}
                    SET {qc} = CAST(CAST(TRIM({qc}) AS INTEGER) AS TEXT)
                    WHERE TRIM({qc}) != ''
                      AND TRIM({qc}) GLOB '[0-9]*'
                      AND TRIM({qc}) NOT GLOB '*[^0-9]*'"""
            )
            applied.append({"column": col, "operation": "strip"})

        elif operation == "pad":
            pad_length = int(act.get("pad_length", 0))
            if pad_length < 1 or pad_length > 50:
                continue
            zeros = "0" * pad_length
            conn.execute(
                f"""UPDATE {tbl}
                    SET {qc} = SUBSTR('{zeros}' || TRIM({qc}), -{pad_length})
                    WHERE TRIM({qc}) != ''
                      AND TRIM({qc}) GLOB '[0-9]*'
                      AND TRIM({qc}) NOT GLOB '*[^0-9]*'"""
            )
            applied.append({"column": col, "operation": "pad", "pad_length": str(pad_length)})

    if applied:
        conn.commit()

    out_cols = read_table_columns(conn, sql_name)
    n_rows = table_row_count(conn, sql_name)

    group_schema = get_meta(conn, "groupSchemaTableRows") or []
    for gs in group_schema:
        if gs.get("group_id") == group_id:
            gs["rows"] = n_rows
            gs["cols"] = len(out_cols)
            gs["columns"] = out_cols
            break
    set_meta(conn, "groupSchemaTableRows", group_schema)

    preview_rows = read_table(conn, sql_name, PREVIEW_ROWS)

    return {
        "applied": applied,
        "preview": {"columns": out_cols, "rows": preview_rows},
        "groupRow": {"group_id": group_id, "rows": n_rows, "cols": len(out_cols), "columns": out_cols},
    }
