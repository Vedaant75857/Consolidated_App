"""Per-group insights pipeline, cross-group synthesis, and chatbot."""

from __future__ import annotations

import importlib.util
import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from typing import Any

import sqlite3


def _load_mod(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


_insights_dir = os.path.dirname(os.path.abspath(__file__))
_prompts = _load_mod("insights_prompts", os.path.join(_insights_dir, "ai", "prompts.py"))
_stats = _load_mod("insights_stats", os.path.join(_insights_dir, "stats", "column_stats_computer.py"))

ANALYTICS_STRATEGIST_PROMPT = _prompts.ANALYTICS_STRATEGIST_PROMPT
CROSS_GROUP_SYNTHESIZER_PROMPT = _prompts.CROSS_GROUP_SYNTHESIZER_PROMPT
DATA_PROFILER_PROMPT = _prompts.DATA_PROFILER_PROMPT
INSIGHT_SYNTHESIZER_PROMPT = _prompts.INSIGHT_SYNTHESIZER_PROMPT
QUALITY_AUDITOR_PROMPT = _prompts.QUALITY_AUDITOR_PROMPT
SYSTEM_PROMPT_CHATBOT = _prompts.SYSTEM_PROMPT_CHATBOT

analyze_cross_group_sql = _stats.analyze_cross_group_sql
compute_cross_table_consistency = _stats.compute_cross_table_consistency
compute_deep_column_stats = _stats.compute_deep_column_stats
estimate_duplicate_rows = _stats.estimate_duplicate_rows

from shared.ai import call_ai_json, get_client, get_model
from shared.db import (
    all_registered_tables,
    get_meta,
    lookup_sql_name,
    quote_id,
    read_table_columns,
    table_exists,
    table_row_count,
)


def _resolve_appended_map(conn: sqlite3.Connection) -> dict[str, str]:
    out: dict[str, str] = {}
    for r in all_registered_tables(conn):
        if str(r.get("sql_name", "")).startswith("appended__"):
            out[str(r["table_key"])] = str(r["sql_name"])
    return out


def resolve_group_table(
    conn: sqlite3.Connection,
    group_id: str,
    group_tables: list[str] | None = None,
) -> str | None:
    appended = _resolve_appended_map(conn)
    if group_id in appended:
        return appended[group_id]
    sql = lookup_sql_name(conn, group_id)
    if sql and table_exists(conn, sql):
        return sql
    if group_tables:
        for tk in group_tables:
            sql = lookup_sql_name(conn, tk)
            if sql and table_exists(conn, sql):
                return sql
    return None


def _stats_for_prompt(column_stats: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Shape stats for LLM prompts (camelCase keys, JSON-friendly)."""
    out = []
    for c in column_stats:
        out.append(
            {
                "name": c["name"],
                "totalRows": c["totalRows"],
                "nullCount": c["nullCount"],
                "fillRate": c["fillRate"],
                "distinctCount": c["distinctCount"],
                "uniqueness": c["uniqueness"],
                "inferredType": c["inferredType"],
                "numericRatio": c.get("numericRatio"),
                "topValues": c.get("topValues", []),
                "numericStats": c.get("numericStats"),
                "lengthStats": c.get("lengthStats"),
                "patternFlags": c.get("patternFlags", []),
                "sampleValues": c.get("sampleValues", []),
            }
        )
    return out


def _execute_slices(
    conn: sqlite3.Connection, sql_name: str, slices: list[dict[str, Any]]
) -> list[dict[str, Any]]:
    tbl = quote_id(sql_name)
    col_set = set(read_table_columns(conn, sql_name))
    results: list[dict[str, Any]] = []

    for sl in slices:
        dim = sl.get("dimension")
        measure = sl.get("measure")
        agg = (sl.get("aggregation") or "count").lower()
        if not dim or dim not in col_set:
            continue
        if measure and measure not in col_set:
            continue
        dim_q = quote_id(dim)
        try:
            if measure and agg == "sum":
                mq = quote_id(measure)
                sql = (
                    f"SELECT {dim_q} AS k, SUM(CAST({mq} AS REAL)) AS v FROM {tbl} "
                    f"WHERE {dim_q} IS NOT NULL AND TRIM({dim_q}) != '' "
                    f"GROUP BY {dim_q} ORDER BY v DESC LIMIT 15"
                )
                label = f"sum({measure})"
            elif measure and agg == "avg":
                mq = quote_id(measure)
                sql = (
                    f"SELECT {dim_q} AS k, AVG(CAST({mq} AS REAL)) AS v FROM {tbl} "
                    f"WHERE {dim_q} IS NOT NULL AND TRIM({dim_q}) != '' "
                    f"GROUP BY {dim_q} ORDER BY v DESC LIMIT 15"
                )
                label = f"avg({measure})"
            elif agg == "distinct_count" and measure:
                mq = quote_id(measure)
                sql = (
                    f"SELECT {dim_q} AS k, COUNT(DISTINCT {mq}) AS v FROM {tbl} "
                    f"WHERE {dim_q} IS NOT NULL AND TRIM({dim_q}) != '' "
                    f"GROUP BY {dim_q} ORDER BY v DESC LIMIT 15"
                )
                label = f"distinct({measure})"
            else:
                sql = (
                    f"SELECT {dim_q} AS k, COUNT(*) AS v FROM {tbl} "
                    f"WHERE {dim_q} IS NOT NULL AND TRIM({dim_q}) != '' "
                    f"GROUP BY {dim_q} ORDER BY v DESC LIMIT 15"
                )
                label = "count"

            rows = conn.execute(sql).fetchall()
            top_values = [
                {"key": str(r["k"] or ""), "value": float(r["v"] or 0)} for r in rows
            ]
            results.append(
                {
                    "slice": sl,
                    "valueLabel": label,
                    "topValues": top_values,
                }
            )
        except Exception as exc:  # noqa: BLE001
            results.append(
                {
                    "slice": sl,
                    "error": str(exc),
                    "topValues": [],
                }
            )
    return results


def _run_ai_pipeline_for_group(
    db_data: dict[str, Any],
    api_key: str,
) -> tuple[dict[str, Any], dict[str, Any], str, str]:
    """AI-only pipeline for one group. No DB access — all data pre-fetched."""
    gid = db_data["gid"]
    tables = db_data["tables"]
    col_stats = db_data["col_stats"]
    dup_est = db_data["dup_est"]
    cross_consistency = db_data["cross_consistency"]
    total_rows = db_data["total_rows"]
    total_cols = db_data["total_cols"]
    sql_name = db_data["sql_name"]
    col_set = db_data["col_set"]
    stats_payload = _stats_for_prompt(col_stats)

    profiler_payload = {
        "groupId": gid, "tables": tables,
        "reason": db_data.get("reason", ""), "totalRows": total_rows,
        "columnStats": stats_payload,
    }
    try:
        profiler = call_ai_json(DATA_PROFILER_PROMPT, profiler_payload, api_key=api_key)
    except Exception:
        profiler = {
            "dataDescription": "",
            "columnRoles": [{"name": c["name"], "role": "auxiliary", "description": ""} for c in stats_payload],
            "domainKeywords": [], "dataCharacteristics": "",
        }

    auditor_payload = {
        "groupId": gid, "columnStats": stats_payload,
        "profilerOutput": profiler, "crossTableConsistency": cross_consistency,
        "duplicateRowEstimate": dup_est, "totalRows": total_rows,
    }
    try:
        quality = call_ai_json(QUALITY_AUDITOR_PROMPT, auditor_payload, api_key=api_key)
    except Exception:
        avg_fill = sum(c["fillRate"] for c in col_stats) / len(col_stats) if col_stats else 0.0
        quality = {
            "overallScore": int(round(avg_fill * 100)), "completeness": avg_fill,
            "uniqueness": 0.0, "consistency": cross_consistency,
            "issues": [], "recommendations": [],
        }

    strategist_payload = {
        "groupId": gid, "columnStats": stats_payload,
        "profilerOutput": profiler, "qualityOutput": quality, "relationships": [],
    }
    try:
        strat = call_ai_json(ANALYTICS_STRATEGIST_PROMPT, strategist_payload, api_key=api_key)
        strategist_slices = list(strat.get("suggestedSlices") or [])
    except Exception:
        strategist_slices = []

    strategist_slices = [
        s for s in strategist_slices
        if s.get("dimension") in col_set
        and (not s.get("measure") or s.get("measure") in col_set)
    ][:5]

    # Slice results need DB — stored pre-fetched col_set for validation only.
    # Slices will be executed after AI pipeline returns to caller.
    synth_payload = {
        "groupId": gid, "totalRows": total_rows, "totalCols": total_cols,
        "profilerOutput": profiler, "qualityOutput": quality,
        "sliceResults": [], "columnStats": stats_payload,
    }
    try:
        synthesized = call_ai_json(INSIGHT_SYNTHESIZER_PROMPT, synth_payload, api_key=api_key)
    except Exception:
        synthesized = {
            "summary": profiler.get("dataDescription") or f"Group {gid}",
            "topInsights": [],
            "suggestedActions": quality.get("recommendations", [])[:4],
        }

    roles = profiler.get("columnRoles") or []
    key_columns = [
        {"name": r.get("name"), "role": r.get("role"), "description": r.get("description")}
        for r in roles if r.get("role") != "auxiliary"
    ][:15]

    report = {
        "groupId": gid,
        "profile": {
            "groupId": gid, "totalRows": total_rows, "totalCols": total_cols,
            "tables": tables, "columnStats": col_stats,
            "duplicateRowEstimate": dup_est, "crossTableConsistency": cross_consistency,
        },
        "quality": {
            "groupId": gid, "overallScore": quality.get("overallScore"),
            "completeness": quality.get("completeness"), "uniqueness": quality.get("uniqueness"),
            "consistency": quality.get("consistency"),
            "issues": quality.get("issues", []), "recommendations": quality.get("recommendations", []),
        },
        "analysisResults": [],
        "insights": {
            "summary": synthesized.get("summary"),
            "dataDescription": profiler.get("dataDescription"),
            "keyColumns": key_columns,
            "qualityNotes": [
                (f"[{i.get('column')}] " if i.get("column") else "") + str(i.get("description") or "")
                for i in (quality.get("issues") or [])
                if i.get("severity") in ("high", "medium")
            ],
            "topInsights": synthesized.get("topInsights", []),
            "suggestedActions": synthesized.get("suggestedActions", []),
        },
        "profiler": profiler,
        "_strategist_slices": strategist_slices,
    }
    profile_meta = {"groupId": gid, "totalRows": total_rows, "totalCols": total_cols, "tables": tables}
    return report, profile_meta, str(gid), sql_name


def run_insights(
    conn: sqlite3.Connection,
    session_id: str,
    groups: list[dict[str, Any]] | None,
    api_key: str,
) -> dict[str, Any]:
    """
    Per-group: deep SQL column stats -> profiler -> quality auditor -> strategist
    -> execute slice SQL -> insight synthesizer. AI calls for all groups run in PARALLEL.
    Then cross-group SQL + synthesizer.
    """
    _ = session_id
    if groups is None:
        groups = get_meta(conn, "appendGroups") or []
    if not groups:
        raise ValueError("No append groups provided or stored in session meta.")

    # Phase 1: gather all DB data sequentially (single conn, thread-safe)
    group_db_data: list[dict[str, Any]] = []
    for group in groups:
        gid = group.get("group_id") or group.get("groupId")
        if not gid:
            continue
        tables: list[str] = list(group.get("tables") or [])
        sql_name = resolve_group_table(conn, str(gid), group_tables=tables)
        if not sql_name or not table_exists(conn, sql_name):
            continue
        col_stats = compute_deep_column_stats(conn, sql_name)
        dup_est = estimate_duplicate_rows(conn, sql_name)
        source_sqls = [
            n for n in (lookup_sql_name(conn, tk) for tk in tables)
            if n and table_exists(conn, n)
        ]
        cross_consistency = compute_cross_table_consistency(conn, source_sqls)
        total_rows = table_row_count(conn, sql_name)
        cols = read_table_columns(conn, sql_name)
        group_db_data.append({
            "gid": gid, "tables": tables, "reason": group.get("reason", ""),
            "sql_name": sql_name, "col_stats": col_stats, "dup_est": dup_est,
            "cross_consistency": cross_consistency, "total_rows": total_rows,
            "total_cols": len(cols), "col_set": {c["name"] for c in col_stats},
        })

    # Phase 2: run AI pipelines in parallel (no DB access)
    group_reports: list[dict[str, Any]] = []
    group_profiles: list[dict[str, Any]] = []
    group_sql_names: dict[str, str] = {}

    with ThreadPoolExecutor(max_workers=min(6, max(len(group_db_data), 1))) as pool:
        futures = {
            pool.submit(_run_ai_pipeline_for_group, data, api_key): data
            for data in group_db_data
        }
        for fut in as_completed(futures):
            report, profile_meta, gid, sql_name = fut.result()
            group_reports.append(report)
            group_profiles.append(profile_meta)
            group_sql_names[gid] = sql_name

    # Phase 3: execute slice SQL (needs DB) and patch reports
    for report in group_reports:
        slices = report.pop("_strategist_slices", [])
        sql_name = group_sql_names.get(report["groupId"])
        if sql_name and slices:
            report["analysisResults"] = _execute_slices(conn, sql_name, slices)

    cross_stats = analyze_cross_group_sql(conn, group_profiles, group_sql_names)
    narrative = ""
    merge_hints: list[str] = []

    if api_key and len(group_profiles) >= 2:
        cross_input = {
            "groups": [
                {
                    "groupId": g["groupId"], "totalRows": g["totalRows"],
                    "totalCols": g["totalCols"], "tables": g["tables"],
                    "description": next(
                        (r["insights"].get("dataDescription", "") for r in group_reports if r["groupId"] == g["groupId"]),
                        "",
                    ),
                }
                for g in group_profiles
            ],
            "schemaOverlap": cross_stats["schemaOverlap"],
            "valueOverlap": cross_stats["valueOverlap"][:15],
        }
        try:
            cg = call_ai_json(CROSS_GROUP_SYNTHESIZER_PROMPT, cross_input, api_key=api_key)
            narrative = str(cg.get("narrative", ""))
            merge_hints = list(cg.get("mergeHints") or [])
        except Exception:
            pass

    insights_map = {str(r.get("groupId")): r.get("insights", {}) for r in group_reports}
    return {
        "insights": insights_map,
        "groupReports": group_reports,
        "crossGroupOverview": {**cross_stats, "narrative": narrative, "mergeHints": merge_hints},
    }


def _priority_label(score: float) -> str:
    if score >= 0.65:
        return "high"
    if score >= 0.4:
        return "medium"
    if score >= 0.2:
        return "low"
    return "skip"


def run_pre_merge_analysis(
    conn: sqlite3.Connection, session_id: str, api_key: str
) -> dict[str, Any]:
    """Cross-group narrative using SQL overlap stats + CROSS_GROUP_SYNTHESIZER_PROMPT."""
    _ = session_id
    groups = get_meta(conn, "appendGroups") or []
    if not groups:
        raise ValueError("No append groups available.")

    group_profiles: list[dict[str, Any]] = []
    group_sql_names: dict[str, str] = {}

    for g in groups:
        gid = g.get("group_id") or g.get("groupId")
        if not gid:
            continue
        tables = list(g.get("tables") or [])
        sql_name = resolve_group_table(conn, str(gid), group_tables=tables)
        if not sql_name or not table_exists(conn, sql_name):
            continue
        group_sql_names[str(gid)] = sql_name
        group_profiles.append(
            {
                "groupId": str(gid),
                "totalRows": table_row_count(conn, sql_name),
                "totalCols": len(read_table_columns(conn, sql_name)),
                "tables": tables,
            }
        )

    if len(group_profiles) < 2:
        return {
            "should_merge": False,
            "rationale": "Only one group available - nothing to compare.",
            "skip_merge_reason": "Only one group available.",
            "recommended_fact_group": group_profiles[0]["groupId"] if group_profiles else None,
            "dimension_recommendations": [],
            "schemaOverlap": {},
            "valueOverlap": [],
            "narrative": "",
            "mergeHints": [],
        }

    cross_stats = analyze_cross_group_sql(conn, group_profiles, group_sql_names)
    cross_input = {
        "groups": group_profiles,
        "schemaOverlap": cross_stats["schemaOverlap"],
        "valueOverlap": cross_stats["valueOverlap"][:15],
    }
    try:
        cg = call_ai_json(
            CROSS_GROUP_SYNTHESIZER_PROMPT, cross_input, api_key=api_key
        )
    except Exception:
        cg = {}
    recommended_fact_group = max(
        group_profiles,
        key=lambda g: int(g.get("totalRows") or 0),
    )["groupId"]
    fact_sql = group_sql_names.get(recommended_fact_group)
    fact_cols = set(read_table_columns(conn, fact_sql)) if fact_sql else set()

    dimension_recommendations: list[dict[str, Any]] = []
    for g in group_profiles:
        gid = g["groupId"]
        if gid == recommended_fact_group:
            continue

        schema_row = (
            cross_stats.get("schemaOverlap", {})
            .get(recommended_fact_group, {})
            .get(gid, {})
        )
        shared_columns = list(schema_row.get("sharedColumns") or [])
        schema_overlap = float(schema_row.get("overlapPct") or 0.0)

        pair_value_overlap = [
            vo
            for vo in (cross_stats.get("valueOverlap") or [])
            if {vo.get("groupA"), vo.get("groupB")} == {recommended_fact_group, gid}
        ]
        pair_value_overlap.sort(key=lambda x: -float(x.get("overlapRate") or 0.0))
        best_overlap = float(pair_value_overlap[0].get("overlapRate") or 0.0) if pair_value_overlap else 0.0

        priority_score = min(1.0, 0.65 * best_overlap + 0.35 * schema_overlap)
        priority = _priority_label(priority_score)

        dim_sql = group_sql_names.get(gid)
        dim_cols = read_table_columns(conn, dim_sql) if dim_sql else []
        enrichment_columns = [c for c in dim_cols if c not in fact_cols][:12]
        likely_join_hint = [str(v.get("column")) for v in pair_value_overlap[:3] if v.get("column")]

        rationale_bits = [
            f"{len(shared_columns)} shared columns",
            f"schema overlap {(schema_overlap * 100):.1f}%",
        ]
        if likely_join_hint:
            rationale_bits.append(f"best join hint: {likely_join_hint[0]}")
        rationale = "; ".join(rationale_bits)

        dimension_recommendations.append(
            {
                "group_id": gid,
                "priority": priority,
                "priority_score": round(priority_score, 4),
                "rationale": rationale,
                "enrichment_columns": enrichment_columns,
                "likely_join_hint": likely_join_hint,
            }
        )

    dimension_recommendations.sort(
        key=lambda r: -float(r.get("priority_score") or 0.0)
    )
    should_merge = any(r.get("priority") in ("high", "medium") for r in dimension_recommendations)
    rationale = (
        f"Fact group `{recommended_fact_group}` has compatible dimensions with useful overlap."
        if should_merge
        else "Cross-group overlap is weak; merging may add little value."
    )

    return {
        **cross_stats,
        **cg,
        "should_merge": should_merge,
        "rationale": rationale,
        "skip_merge_reason": "" if should_merge else "Low overlap between candidate groups.",
        "recommended_fact_group": recommended_fact_group,
        "dimension_recommendations": dimension_recommendations,
        "narrative": str(cg.get("narrative") or ""),
        "mergeHints": list(cg.get("mergeHints") or []),
    }


def run_chat(
    conn: sqlite3.Connection,
    session_id: str,
    message: str,
    context: str,
    api_key: str | None = None,
    history: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Chat using SYSTEM_PROMPT_CHATBOT + context (optional short history)."""
    _ = conn, session_id
    system_message = (
        f"{SYSTEM_PROMPT_CHATBOT}\n\n--- CONTEXT ---\n{context}\n--- END CONTEXT ---"
    )
    chat_messages: list[dict[str, str]] = [{"role": "system", "content": system_message}]
    for msg in (history or [])[-12:]:
        role = str(msg.get("role") or "").strip()
        content = str(msg.get("content") or "").strip()
        if role in ("user", "assistant") and content:
            chat_messages.append({"role": role, "content": content})
    if not any(m["role"] == "user" for m in chat_messages[1:]):
        chat_messages.append({"role": "user", "content": message})

    client = get_client(api_key)
    model = get_model()
    resp = client.chat.completions.create(
        messages=chat_messages,
        model=model,
    )
    raw = resp.choices[0].message.content if resp.choices else None
    return {"reply": (raw or "").strip()}
