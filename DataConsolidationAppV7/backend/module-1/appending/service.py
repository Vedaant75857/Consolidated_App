"""Append-plan, append-mapping, and append-execute logic (SQLite + AI)."""

from __future__ import annotations

import os
import sys
import sqlite3
from typing import Any

_this_dir = os.path.dirname(os.path.abspath(__file__))
_backend_dir = os.path.normpath(os.path.join(_this_dir, ".."))
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

from shared.ai import call_ai_json
from shared.db import (
    all_registered_tables,
    column_stats,
    get_meta,
    get_overlap_sql,
    lookup_sql_name,
    quote_id,
    read_table_columns,
    register_table,
    safe_table_name,
    set_meta,
    table_exists,
    table_row_count,
)
from shared.utils import chunk_list

from appending.ai.prompts import SYSTEM_PROMPT_APPEND_ONLY, SYSTEM_PROMPT_HEADER_MAPPING

try:
    from ai_core.procurement_reframer import reframe_procurement as _reframe
except Exception:
    _reframe = None

MAX_TABLES_PER_CALL = 25
EXAMPLES_PER_COLUMN = 10


def _trim_payload(
    files: list[dict[str, Any]],
    overlap_map: dict[str, dict[str, Any]],
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
    for f in files:
        trimmed: dict[str, list] = {}
        dec = f.get("distinct_examples_by_column") or {}
        if isinstance(dec, dict):
            for col, values in dec.items():
                trimmed[col] = (values[:EXAMPLES_PER_COLUMN] if isinstance(values, list) else [])
        out.append({
            "table_key": f.get("table_key"),
            "file_name": f.get("file_name"),
            "columns": f.get("columns"),
            "n_rows": f.get("n_rows"),
            "distinct_examples_by_column": trimmed,
            "file_type": f.get("file_type"),
            "column_overlap_with_other_tables": overlap_map.get(f.get("table_key", ""), {}),
        })
    return out


def run_append_plan(conn: sqlite3.Connection, api_key: str | None) -> dict[str, Any]:
    from data_loading.service import build_files_payload_from_db

    files_payload = build_files_payload_from_db(conn)
    set_meta(conn, "filesPayload", files_payload)

    registered = all_registered_tables(conn)
    tbl_entries = [
        {"table_key": r["table_key"], "sql_name": r["sql_name"]}
        for r in registered
        if str(r.get("sql_name", "")).startswith(("tbl__", "hn__"))
    ]
    overlap_per_file = get_overlap_sql(conn, tbl_entries)

    plans: list[dict] = []
    if len(files_payload) <= MAX_TABLES_PER_CALL:
        plans.append(call_ai_json(
            SYSTEM_PROMPT_APPEND_ONLY,
            {"input": {"files": _trim_payload(files_payload, overlap_per_file)}},
            api_key,
        ))
    else:
        for batch in chunk_list(files_payload, MAX_TABLES_PER_CALL):
            plans.append(call_ai_json(
                SYSTEM_PROMPT_APPEND_ONLY,
                {"input": {"files": _trim_payload(batch, overlap_per_file)}},
                api_key,
            ))

    append_groups: list[dict] = []
    unassigned: list = []
    notes: list = []
    gid = 1

    for p in plans:
        if not isinstance(p, dict):
            continue
        for g in p.get("append_groups") or []:
            g2 = dict(g)
            if not g2.get("group_id"):
                g2["group_id"] = f"group_{gid}"
            g2["group_id"] = f"{g2['group_id']}_b{gid}"
            gid += 1
            append_groups.append(g2)
        unassigned.extend(p.get("unassigned") or [])
        notes.extend(p.get("notes") or [])

    for u in list(unassigned):
        tk = u.get("table_key") if isinstance(u, dict) else str(u)
        if not tk:
            continue
        parts = str(tk).split("::")
        display_name = (parts[0] or "").rsplit("/", 1)[-1] or tk
        append_groups.append({
            "group_id": f"auto_{gid}",
            "group_name": display_name,
            "tables": [tk],
            "reason": (u.get("reason") if isinstance(u, dict) else None) or "Auto-grouped (no matching tables found)",
        })
        gid += 1
    unassigned = []

    if api_key and _reframe:
        reason_map: dict[str, str] = {}
        for i, g in enumerate(append_groups):
            if g.get("reason"):
                reason_map[f"grp_{i}"] = g["reason"]
        for i, n in enumerate(notes):
            if isinstance(n, str) and n.strip():
                reason_map[f"note_{i}"] = n
        if reason_map:
            reframed = _reframe(reason_map, api_key)
            for i, g in enumerate(append_groups):
                key = f"grp_{i}"
                if key in reframed:
                    g["reason"] = reframed[key]
            for i in range(len(notes)):
                key = f"note_{i}"
                if key in reframed:
                    notes[i] = reframed[key]

    set_meta(conn, "appendGroups", append_groups)
    set_meta(conn, "unassigned", unassigned)

    return {"appendGroups": append_groups, "unassigned": unassigned, "notes": notes}


def save_append_groups(
    conn: sqlite3.Connection,
    append_groups: list | None,
    unassigned: list | None,
) -> None:
    set_meta(conn, "appendGroups", append_groups or [])
    set_meta(conn, "unassigned", unassigned or [])


def run_append_mapping(
    conn: sqlite3.Connection,
    append_groups: list[dict],
    api_key: str | None,
) -> dict[str, Any]:
    files_payload = get_meta(conn, "filesPayload", []) or []
    by_key = {f.get("table_key"): f for f in files_payload if isinstance(f, dict)}
    mappings: list[dict] = []

    for g in append_groups:
        group_id = g.get("group_id")
        raw_tables = g.get("tables") or []
        group_tables = [
            t for t in raw_tables
            if isinstance(t, str) and lookup_sql_name(conn, t) and table_row_count(conn, lookup_sql_name(conn, t)) > 0
        ]

        if len(group_tables) < 2:
            single_sql = lookup_sql_name(conn, group_tables[0]) if group_tables else None
            cols = read_table_columns(conn, single_sql) if single_sql else []
            mappings.append({
                "group_id": group_id,
                "canonical_schema": cols,
                "per_table": [
                    {"table_key": t, "column_mapping": {c: c for c in cols}}
                    for t in group_tables
                ],
                "notes": ["Single table or empty group"],
            })
            continue

        tables_out = []
        for t in group_tables:
            full = by_key.get(t)
            if not full:
                tables_out.append({"table_key": t, "columns": [], "distinct_examples_by_column": {}})
                continue
            trimmed = {}
            dec = full.get("distinct_examples_by_column") or {}
            for col, vals in (dec.items() if isinstance(dec, dict) else []):
                trimmed[col] = (vals[:EXAMPLES_PER_COLUMN] if isinstance(vals, list) else [])
            tables_out.append({"table_key": t, "columns": full.get("columns"), "distinct_examples_by_column": trimmed})

        mapping = call_ai_json(
            SYSTEM_PROMPT_HEADER_MAPPING,
            {"group_id": group_id, "input": {"tables": tables_out}},
            api_key,
        )
        mappings.append(mapping)

    return {"appendGroupMappings": mappings}


def run_append_execute(
    conn: sqlite3.Connection,
    append_group_mappings: list[dict],
    unassigned_tables: list[str] | None,
) -> dict[str, Any]:
    append_log: list[dict] = []
    group_schema: list[dict] = []
    append_report: list[dict] = []

    append_groups = get_meta(conn, "appendGroups") or []
    group_name_lookup: dict[str, str] = {
        str(g.get("group_id", "")): str(g.get("group_name", ""))
        for g in append_groups if g.get("group_name")
    }

    for gm in append_group_mappings:
        group_id = gm.get("group_id")
        canonical: list[str] = list(gm.get("canonical_schema") or [])
        per_table = {p["table_key"]: p for p in (gm.get("per_table") or []) if isinstance(p, dict) and p.get("table_key")}

        all_columns = [*canonical, "_source_table"]
        appended_sql = safe_table_name("appended", str(group_id))

        col_defs = ", ".join(f"{quote_id(c)} TEXT" for c in all_columns)
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(appended_sql)}")
        conn.execute(f"CREATE TABLE {quote_id(appended_sql)} ({col_defs})")

        table_row_counts: dict[str, int] = {}

        for p in gm.get("per_table") or []:
            t = p.get("table_key") if isinstance(p, dict) else None
            if not t:
                continue
            src_sql = lookup_sql_name(conn, t)
            if not src_sql or not table_exists(conn, src_sql) or table_row_count(conn, src_sql) == 0:
                append_log.append({"stage": "append", "group_id": group_id, "table_key": t, "status": "skipped_empty"})
                table_row_counts[t] = 0
                continue

            table_row_counts[t] = table_row_count(conn, src_sql)
            mapping = (per_table.get(t) or {}).get("column_mapping") or {}

            select_parts = []
            for c in canonical:
                src_col = mapping.get(c)
                select_parts.append(quote_id(str(src_col)) if src_col else "NULL")
            select_parts.append(f"'{t.replace(chr(39), chr(39)+chr(39))}'")

            dest_cols = ", ".join(quote_id(c) for c in all_columns)
            conn.execute(f"INSERT INTO {quote_id(appended_sql)} ({dest_cols}) SELECT {', '.join(select_parts)} FROM {quote_id(src_sql)}")

        conn.commit()
        register_table(conn, str(group_id), appended_sql)

        appended_rows = table_row_count(conn, appended_sql)
        append_log.append({"stage": "append", "group_id": group_id, "status": "ok", "out_shape": [appended_rows, len(all_columns)]})
        expected = sum(table_row_counts.values())

        group_schema.append({
            "group_id": group_id,
            "group_name": group_name_lookup.get(str(group_id), ""),
            "rows": appended_rows,
            "cols": len(all_columns),
            "columns_preview": ", ".join(canonical[:60]) + (" ..." if len(canonical) > 60 else ""),
            "columns": canonical,
        })
        append_report.append({
            "group_id": group_id,
            "total_rows": appended_rows,
            "total_cols": len(all_columns),
            "tables_detail": [{"table_key": tk, "rows_contributed": rc} for tk, rc in table_row_counts.items()],
            "expected_total_rows": expected,
            "row_integrity": appended_rows == expected,
            "column_stats": column_stats(conn, appended_sql, all_columns),
        })

    for t in (unassigned_tables or []):
        src_sql = lookup_sql_name(conn, t)
        if not src_sql or not table_exists(conn, src_sql) or table_row_count(conn, src_sql) == 0:
            continue
        cols = read_table_columns(conn, src_sql)
        n_rows = table_row_count(conn, src_sql)
        appended_sql = safe_table_name("appended", t)
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(appended_sql)}")
        conn.execute(f"CREATE TABLE {quote_id(appended_sql)} AS SELECT * FROM {quote_id(src_sql)}")
        conn.commit()
        register_table(conn, t, appended_sql)

        group_schema.append({
            "group_id": t, "group_name": group_name_lookup.get(t, ""),
            "rows": n_rows, "cols": len(cols),
            "columns_preview": ", ".join(cols[:60]) + (" ..." if len(cols) > 60 else ""),
            "columns": cols, "is_standalone": True,
        })
        append_report.append({
            "group_id": t, "total_rows": n_rows, "total_cols": len(cols),
            "tables_detail": [{"table_key": t, "rows_contributed": n_rows}],
            "expected_total_rows": n_rows, "row_integrity": True, "is_standalone": True,
            "column_stats": column_stats(conn, appended_sql, cols),
        })

    group_schema.sort(key=lambda x: x.get("rows", 0), reverse=True)
    set_meta(conn, "groupSchemaTableRows", group_schema)
    set_meta(conn, "appendLog", append_log)

    return {"groupSchema": group_schema, "appendLog": append_log, "appendReport": append_report}
