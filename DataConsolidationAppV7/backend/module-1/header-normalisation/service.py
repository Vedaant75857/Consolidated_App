"""Header normalisation orchestration -- 8-tier matching engine + AI paths.

Phase 1: Run the deterministic 8-tier engine (T1-T7) on every column.
Phase 2 (if API key): Path A maps remaining UNMAPPED headers via AI;
                       Path B re-validates all mapped headers via AI.
Converts internal results to the UI-expected decision shape and persists.
"""

from __future__ import annotations

import importlib.util
import os
import sys
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from typing import Any

_this_dir = os.path.dirname(os.path.abspath(__file__))
if _this_dir not in sys.path:
    sys.path.insert(0, _this_dir)

_backend_dir = os.path.normpath(os.path.join(_this_dir, "..", ".."))
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

from shared.db import (
    all_registered_tables,
    quote_id,
    read_table,
    read_table_columns,
    register_table,
    safe_table_name,
    set_meta,
    get_meta,
    table_exists,
    table_row_count,
    drop_table,
)


def _load_mod(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    sys.modules[name] = mod
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


_profiler_mod = _load_mod("hn_profiler", os.path.join(_this_dir, "profiler.py"))
_aliases_mod = _load_mod("hn_aliases_svc", os.path.join(_this_dir, "aliases.py"))
_schema_mod = _load_mod("hn_schema", os.path.join(_this_dir, "schema_mapper.py"))
_engine_mod = _load_mod("hn_engine", os.path.join(_this_dir, "matching_engine.py"))
_ai_mod = _load_mod("hn_ai_mapper", os.path.join(_this_dir, "ai_mapper.py"))
_prompts_mod = _load_mod("hn_prompts_svc", os.path.join(_this_dir, "ai", "prompts.py"))
_alias_store_mod = _load_mod("hn_alias_store", os.path.join(_this_dir, "alias_store.py"))

profile_table_columns = _profiler_mod.profile_table_columns
map_single_header = _engine_mod.map_single_header
ai_map_unmapped = _ai_mod.ai_map_unmapped
ai_validate_mapped = _ai_mod.ai_validate_mapped
alias_add = _alias_store_mod.alias_add
merge_into_lookup = _alias_store_mod.merge_into_lookup

STANDARD_FIELDS = _schema_mod.STANDARD_FIELDS
STD_FIELD_NAMES = _schema_mod.STD_FIELD_NAMES
SYSTEM_PROMPT_HEADER_NORM_COLUMN = _prompts_mod.SYSTEM_PROMPT_HEADER_NORM_COLUMN
FIELD_ALIASES_LIST = _aliases_mod.FIELD_ALIASES_LIST
EXPECTED_DTYPE = _aliases_mod.EXPECTED_DTYPE
FIELD_TO_SEMANTIC_TAGS = _aliases_mod.FIELD_TO_SEMANTIC_TAGS

_VALID_STANDARD_FIELD_NAMES = {f["name"] for f in STANDARD_FIELDS}
_STD_FIELD_PAYLOAD = [
    {
        "id": f.get("id"),
        "name": f["name"],
        "type": f.get("type", ""),
        "description": f.get("description", ""),
        "aliases": FIELD_ALIASES_LIST.get(f["name"], []),
        "expected_dtype": EXPECTED_DTYPE.get(f["name"], "text"),
        "semantic_tags": FIELD_TO_SEMANTIC_TAGS.get(f["name"], []),
    }
    for f in STANDARD_FIELDS
]

merge_into_lookup(_engine_mod.ALIAS_LOOKUP)

try:
    from ai_core.procurement_reframer import reframe_procurement as _reframe
except Exception:
    _reframe = None


# ---------------------------------------------------------------------------
# 1.  run_header_norm  -- 8-tier engine + AI paths
# ---------------------------------------------------------------------------

def run_header_norm(
    conn: sqlite3.Connection,
    api_key: str | None,
) -> dict[str, Any]:
    """Profile tables, run 8-tier matching, optionally AI-enhance, return decisions.

    Prefers appended__ group tables when they exist (post-append workflow).
    Falls back to tbl__ tables for pre-append usage.
    """
    registered = all_registered_tables(conn)
    appended_entries = [
        r for r in registered
        if str(r.get("sql_name", "")).startswith("appended__")
        and table_exists(conn, r["sql_name"])
    ]
    tbl_entries = appended_entries if appended_entries else [
        r for r in registered
        if str(r.get("sql_name", "")).startswith("tbl__")
        and table_exists(conn, r["sql_name"])
    ]
    if not tbl_entries:
        raise ValueError("No inventory tables found. Complete the upload/cleaning step first.")

    def _process_table(entry: dict) -> dict[str, Any]:
        table_key = entry["table_key"]
        sql_name = entry["sql_name"]
        profiles = profile_table_columns(conn, sql_name)

        # ----- Phase 1: 8-tier deterministic engine (T1-T7) -----
        engine_results: list[dict] = []
        for prof in profiles:
            result = map_single_header(prof.source_name, prof.sample_values)
            engine_results.append(result)

        # ----- Phase 2: AI paths A + B run concurrently -----
        if api_key:
            data_rows = _get_data_rows(conn, sql_name, limit=50)
            profiles_for_ai = [p.to_dict() for p in profiles]

            det_hints_cache: list[list[dict]] = []
            for er in engine_results:
                top = er.get("top_scores", [])
                det_hints_cache.append([{"std_field": f, "score": s} for f, s in top[:5]])

            unmapped_items = [
                {**er, "col_idx": idx}
                for idx, er in enumerate(engine_results)
                if er.get("action") == "AI_NEEDED"
            ]

            with ThreadPoolExecutor(max_workers=2) as ai_pool:
                fut_map = ai_pool.submit(
                    ai_map_unmapped, unmapped_items, data_rows, api_key,
                    profiles=profiles_for_ai, det_results_cache=det_hints_cache,
                    std_field_payload=_STD_FIELD_PAYLOAD,
                    system_prompt=SYSTEM_PROMPT_HEADER_NORM_COLUMN, batch_size=10,
                ) if unmapped_items else None
                fut_val = ai_pool.submit(ai_validate_mapped, engine_results, data_rows, api_key)

                if fut_map is not None:
                    ai_results = fut_map.result()
                    ai_map = {r["raw"]: r for r in ai_results}
                    for er in engine_results:
                        if er.get("action") == "AI_NEEDED" and er["raw"] in ai_map:
                            updated = ai_map[er["raw"]]
                            er["tier"] = updated.get("tier", er["tier"])
                            er["mapped_to"] = updated.get("mapped_to", er["mapped_to"])
                            er["confidence"] = updated.get("confidence", er["confidence"])
                            er["action"] = updated.get("action", er["action"])
                            if "reason" in updated:
                                er["reason"] = updated["reason"]

                fut_val.result()

        # Mark remaining AI_NEEDED as KEEP (no API key or AI didn't resolve)
        for er in engine_results:
            if er.get("action") == "AI_NEEDED":
                er["action"] = "KEEP"
                er["tier"] = "T8_SKIPPED"
            if er.get("action") == "UNMAPPED":
                er["action"] = "KEEP"

        # ----- Convert to UI-expected shape -----
        col_decisions: list[dict[str, Any]] = []
        for idx, er in enumerate(engine_results):
            decision = _to_ui_decision(er)
            col_decisions.append(decision)

        # Reframe reason fields via procurement reframer
        if api_key and _reframe:
            reason_map = {
                f"r_{i}": d["reason"]
                for i, d in enumerate(col_decisions)
                if d.get("reason")
            }
            if reason_map:
                reframed = _reframe(reason_map, api_key)
                for i, d in enumerate(col_decisions):
                    key = f"r_{i}"
                    if key in reframed and reframed[key] != reason_map.get(key):
                        d["reason"] = reframed[key]

        return {
            "tableKey": table_key,
            "sqlName": sql_name,
            "totalRows": table_row_count(conn, sql_name),
            "totalCols": len(profiles),
            "decisions": col_decisions,
        }

    all_tables = [_process_table(entry) for entry in tbl_entries]

    set_meta(conn, "headerNormDecisions", all_tables)

    return {
        "tables": all_tables,
        "standardFields": STANDARD_FIELDS,
    }


def _get_data_rows(conn: sqlite3.Connection, sql_name: str, limit: int = 50) -> list[list]:
    """Read rows as list-of-lists for AI sample extraction."""
    cols = read_table_columns(conn, sql_name)
    rows = read_table(conn, sql_name, limit)
    result: list[list] = []
    for row in rows:
        if isinstance(row, dict):
            result.append([row.get(c) for c in cols])
        else:
            result.append(list(row))
    return result


def _to_ui_decision(engine_result: dict) -> dict[str, Any]:
    """Convert an engine result into the shape the UI expects.

    Uses T7 ``top_scores`` (already computed by the engine) for alternatives,
    eliminating the separate ``score_deterministic`` pass.
    """
    raw = engine_result.get("raw", "")
    mapped_to = engine_result.get("mapped_to")
    confidence = engine_result.get("confidence", 0.0)
    action = engine_result.get("action", "KEEP")
    if action == "REVIEW":
        action = "AUTO"
    tier = engine_result.get("tier", "")
    reason = engine_result.get("reason", "")

    if not reason:
        reason = f"Matched via {tier}" if mapped_to else "No deterministic match found"

    top_scores = engine_result.get("top_scores", [])
    det_score = top_scores[0][1] if top_scores else 0.0
    det_field = top_scores[0][0] if top_scores else None
    top_alts = [f for f, _s in top_scores[:5]]

    return {
        "source_col": raw,
        "suggested_std_field": mapped_to,
        "confidence": round(confidence, 4),
        "reason": reason,
        "action": action,
        "top_alternatives": top_alts,
        "det_score": round(det_score, 4),
        "deterministic_top_match": det_field,
        "deterministic_top_score": round(det_score, 4),
    }


# ---------------------------------------------------------------------------
# 2.  apply_header_norm  -- create hn__ tables from approved decisions
# ---------------------------------------------------------------------------

def apply_header_norm(
    conn: sqlite3.Connection,
    decisions: dict[str, list[dict[str, Any]]],
) -> dict[str, Any]:
    """Apply user-approved mapping decisions and create hn__ tables.

    *decisions* maps ``table_key`` -> list of column decisions, each with:
      - source_col, action ("AUTO"|"REVIEW"|"DROP"|"KEEP"), mapped_to (str|None)
    """
    applied: list[dict[str, Any]] = []

    for table_key, col_decisions in decisions.items():
        sql_name = _resolve_tbl(conn, table_key)
        if not sql_name or not table_exists(conn, sql_name):
            continue

        select_parts: list[str] = []
        mapped_count = 0
        dropped_count = 0
        kept_count = 0

        for cd in col_decisions:
            src = cd["source_col"]
            action = cd.get("action", "KEEP")
            if action == "REVIEW":
                action = "AUTO"
            mapped_to = cd.get("mapped_to")
            user_edited = cd.get("user_edited", False)
            is_custom_name = cd.get("custom_name", False)

            if action == "DROP":
                dropped_count += 1
                continue

            if action == "KEEP":
                select_parts.append(quote_id(src))
                kept_count += 1
            elif mapped_to and mapped_to.strip():
                select_parts.append(f'{quote_id(src)} AS {quote_id(mapped_to)}')
                mapped_count += 1
                # Only learn alias for standard field mappings, not custom names
                if user_edited and not is_custom_name:
                    alias_add(mapped_to, src)
            else:
                select_parts.append(quote_id(src))
                kept_count += 1

        if not select_parts:
            continue

        hn_sql = safe_table_name("hn", table_key)
        drop_table(conn, hn_sql)

        select_clause = ", ".join(select_parts)
        conn.execute(
            f'CREATE TABLE {quote_id(hn_sql)} AS SELECT {select_clause} FROM {quote_id(sql_name)}'
        )
        conn.commit()

        register_table(conn, table_key, hn_sql)

        applied.append({
            "tableKey": table_key,
            "hnSqlName": hn_sql,
            "mapped": mapped_count,
            "dropped": dropped_count,
            "kept": kept_count,
        })

    _rebuild_meta(conn)

    set_meta(conn, "headerNormApplied", True)

    return {"appliedTables": applied}


def _resolve_tbl(conn: sqlite3.Connection, table_key: str) -> str | None:
    """Find the source SQL name for a table_key.

    Prefers appended__ tables (group-level), then tbl__ (individual source).
    Ignores hn__ tables if already applied.
    """
    registered = all_registered_tables(conn)
    for r in registered:
        if r["table_key"] == table_key:
            name = r["sql_name"]
            if name.startswith("appended__"):
                return name
            if name.startswith("tbl__"):
                return name
            appended_name = safe_table_name("appended", table_key)
            if table_exists(conn, appended_name):
                return appended_name
            tbl_name = safe_table_name("tbl", table_key)
            if table_exists(conn, tbl_name):
                return tbl_name
    appended_name = safe_table_name("appended", table_key)
    if table_exists(conn, appended_name):
        return appended_name
    tbl_name = safe_table_name("tbl", table_key)
    if table_exists(conn, tbl_name):
        return tbl_name
    return None


def _rebuild_meta(conn: sqlite3.Connection) -> None:
    """Rebuild inv and filesPayload meta to reflect current table state."""
    from data_loading.service import build_inventory_from_db, build_files_payload_from_db
    set_meta(conn, "inv", build_inventory_from_db(conn))
    set_meta(conn, "filesPayload", build_files_payload_from_db(conn))


# ---------------------------------------------------------------------------
# 3.  get_table_preview  -- first N rows for the review UI
# ---------------------------------------------------------------------------

def get_table_preview(
    conn: sqlite3.Connection,
    table_key: str,
    limit: int = 100,
) -> dict[str, Any]:
    """Return first *limit* rows of the tbl__ table for user review."""
    sql_name = _resolve_tbl(conn, table_key)
    if not sql_name or not table_exists(conn, sql_name):
        raise ValueError(f"Table not found for key: {table_key}")

    cols = read_table_columns(conn, sql_name)
    rows = read_table(conn, sql_name, limit)
    total = table_row_count(conn, sql_name)

    return {
        "tableKey": table_key,
        "columns": cols,
        "rows": [dict(r) for r in rows],
        "totalRows": total,
    }
