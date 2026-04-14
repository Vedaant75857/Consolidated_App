"""Execution engine / insights / chat API routes."""

from __future__ import annotations

import importlib.util
import json
import os
import threading
import time
import uuid
from typing import Any

from flask import Blueprint, Response, jsonify, request, stream_with_context

from shared.db import (
    all_registered_tables,
    delete_meta,
    drop_table,
    get_all_meta_keys,
    get_meta,
    get_session_db,
    lookup_sql_name,
    read_table,
    read_table_columns,
    set_meta,
    table_exists,
    table_row_count,
    unregister_table,
    PREVIEW_POOL,
    pick_best_rows,
)
from shared.db.stats_ops import column_stats as compute_column_stats
from appending.service import run_append_execute, run_append_mapping, run_append_plan
from data_loading.service import (
    build_files_payload_from_db,
    build_inventory_from_db,
)
insights_bp = Blueprint("insights_bp", __name__)

_module_dir = os.path.dirname(os.path.abspath(__file__))
_summary_root = os.path.normpath(os.path.join(_module_dir, "..", "summary"))


def _load_service(module_qual: str, relative_path: str):
    path = os.path.join(_summary_root, relative_path)
    spec = importlib.util.spec_from_file_location(module_qual, path)
    if spec is None or spec.loader is None:
        raise ImportError(f"Cannot load {path}")
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_insights = _load_service("summary_insights_service", os.path.join("insights", "service.py"))
_hn_root = os.path.normpath(os.path.join(_module_dir, "..", "header-normalisation"))
_hn_service = _load_service("summary_header_norm_service", os.path.join("..", "header-normalisation", "service.py"))
_hn_schema = _load_service("summary_header_norm_schema", os.path.join("..", "header-normalisation", "schema_mapper.py"))
run_insights = _insights.run_insights
run_pre_merge_analysis = _insights.run_pre_merge_analysis
run_chat = _insights.run_chat
run_header_norm = _hn_service.run_header_norm
apply_header_norm = _hn_service.apply_header_norm
STANDARD_FIELDS = _hn_schema.STANDARD_FIELDS


def _build_chat_context(conn, body: dict) -> str:
    stage = body.get("stage")
    selected = body.get("selectedItem")
    keys = [
        "inv",
        "appendGroups",
        "groupSchemaTableRows",
        "mergeBaseGroupId",
        "mergeApprovedSources",
        "normalizeExport",
    ]
    meta_summary: dict[str, object] = {}
    for k in keys:
        v = get_meta(conn, k)
        if isinstance(v, list):
            meta_summary[k] = {"count": len(v), "sample": v[:3]}
        elif isinstance(v, dict):
            meta_summary[k] = {"keys": list(v.keys())[:20]}
        elif v is not None:
            meta_summary[k] = v
    payload = {
        "stage": stage,
        "selectedItem": selected,
        "meta": meta_summary,
    }
    return json.dumps(payload)


_SESSION_LOCK_GUARD = threading.Lock()
_SESSION_LOCKS: dict[str, threading.RLock] = {}


def _session_lock(session_id: str) -> threading.RLock:
    with _SESSION_LOCK_GUARD:
        lock = _SESSION_LOCKS.get(session_id)
        if lock is None:
            lock = threading.RLock()
            _SESSION_LOCKS[session_id] = lock
        return lock


def _table_artifact_rows_cols(conn, table_name: str) -> dict[str, int]:
    if not table_exists(conn, table_name):
        return {"rows": 0, "cols": 0}
    return {
        "rows": int(table_row_count(conn, table_name) or 0),
        "cols": len(read_table_columns(conn, table_name)),
    }


def _build_merge_result_from_table(conn, table_name: str = "final_merged") -> dict[str, Any] | None:
    if not table_exists(conn, table_name):
        return None
    rows_count = int(table_row_count(conn, table_name) or 0)
    columns_list = read_table_columns(conn, table_name)
    preview = pick_best_rows(read_table(conn, table_name, PREVIEW_POOL), 50)
    col_stats = compute_column_stats(conn, table_name)

    merge_history = get_meta(conn, "merge_history") or []
    latest = merge_history[-1] if merge_history else None
    skipped = not bool(get_meta(conn, "mergeApprovedSources"))

    result: dict[str, Any] = {
        "final_table": table_name,
        "rows": rows_count,
        "cols": len(columns_list),
        "columns": columns_list,
        "column_stats": col_stats,
        "preview": preview,
        "skipped": skipped,
    }
    if latest:
        result["version"] = latest.get("version")
        result["file_label"] = latest.get("file_label")
        result["merge_history"] = merge_history
    return result


def _auto_header_norm_decisions(conn) -> dict[str, list[dict[str, Any]]]:
    raw = get_meta(conn, "headerNormDecisions") or []
    out: dict[str, list[dict[str, Any]]] = {}
    for t in raw:
        if not isinstance(t, dict):
            continue
        tk = str(t.get("tableKey") or "").strip()
        if not tk:
            continue
        col_decisions: list[dict[str, Any]] = []
        for d in (t.get("decisions") or []):
            if not isinstance(d, dict):
                continue
            src = str(d.get("source_col") or "").strip()
            if not src:
                continue
            action = str(d.get("action") or "KEEP")
            if action == "REVIEW":
                action = "AUTO"
            mapped = d.get("suggested_std_field")
            col_decisions.append(
                {
                    "source_col": src,
                    "action": action,
                    "mapped_to": mapped if action == "AUTO" else None,
                }
            )
        if col_decisions:
            out[tk] = col_decisions
    return out


_OPERATION_SPECS: dict[str, dict[str, Any]] = {
    "header_norm_run": {
        "required_inputs": [],
        "required_artifacts": ["inv"],
        "requires_api_key": True,
        "auto_prereqs": [],
        "produces": ["headerNormDecisions"],
    },
    "header_norm_apply": {
        "required_inputs": [],
        "required_artifacts": ["headerNormDecisions"],
        "requires_api_key": False,
        "auto_prereqs": ["header_norm_run"],
        "produces": ["headerNormApplied"],
    },
    "append_plan": {
        "required_inputs": [],
        "required_artifacts": ["filesPayload"],
        "requires_api_key": True,
        "auto_prereqs": [],
        "produces": ["appendGroups"],
    },
    "append_mapping": {
        "required_inputs": [],
        "required_artifacts": ["appendGroups"],
        "requires_api_key": True,
        "auto_prereqs": ["append_plan"],
        "produces": ["appendGroupMappings"],
    },
    "append_execute": {
        "required_inputs": [],
        "required_artifacts": ["appendGroupMappings"],
        "requires_api_key": False,
        "auto_prereqs": ["append_mapping"],
        "produces": ["groupSchemaTableRows"],
    },
}

_OPERATION_INPUT_HINTS: dict[str, dict[str, Any]] = {
    "header_norm_run": {},
    "header_norm_apply": {
        "decisions": {"type": "object", "required": False, "description": "Per-table column decisions map."},
    },
    "append_plan": {
        "filesPayload": {"type": "array", "required": False, "description": "Optional explicit files payload override."},
        "tableKeys": {"type": "array", "required": False, "description": "Optional table keys to scope append planning."},
    },
    "append_mapping": {
        "appendGroups": {"type": "array", "required": False, "description": "Append groups. Falls back to session meta."},
    },
    "append_execute": {
        "appendGroupMappings": {"type": "array", "required": False, "description": "Group mappings to execute."},
        "unassignedTables": {"type": "array", "required": False, "description": "Optional excluded table keys."},
    },
}


def _artifact_exists(conn, artifact: str) -> bool:
    if artifact.startswith("table:"):
        return table_exists(conn, artifact.split(":", 1)[1])
    value = get_meta(conn, artifact)
    if value is None:
        return False
    if isinstance(value, (list, dict, str)):
        return len(value) > 0
    if isinstance(value, bool):
        return value
    return True


def _operation_is_ready(conn, operation: str) -> bool:
    spec = _OPERATION_SPECS.get(operation) or {}
    produces = spec.get("produces") or []
    if not produces:
        return False
    return all(_artifact_exists(conn, a) for a in produces)


def _missing_requirements(conn, operation: str, input_data: dict[str, Any], api_key: str | None) -> list[str]:
    spec = _OPERATION_SPECS.get(operation) or {}
    missing: list[str] = []
    for artifact in spec.get("required_artifacts") or []:
        if artifact == "filesPayload":
            explicit_files = input_data.get("filesPayload")
            explicit_keys = input_data.get("tableKeys")
            if (isinstance(explicit_files, list) and len(explicit_files) > 0) or (
                isinstance(explicit_keys, list) and len(explicit_keys) > 0
            ):
                continue
        if artifact == "appendGroups":
            explicit_groups = input_data.get("appendGroups")
            if isinstance(explicit_groups, list) and len(explicit_groups) > 0:
                continue
        if artifact == "appendGroupMappings":
            explicit_mappings = input_data.get("appendGroupMappings")
            if isinstance(explicit_mappings, list) and len(explicit_mappings) > 0:
                continue
        if not _artifact_exists(conn, artifact):
            missing.append(f"artifact:{artifact}")
    for field in spec.get("required_inputs") or []:
        value = input_data.get(field)
        if value is None or value == "" or value == []:
            missing.append(f"input:{field}")
    if spec.get("requires_api_key") and not (api_key and str(api_key).strip()):
        missing.append("input:apiKey")
    return missing


def _apply_preferred_files_payload(conn, input_data: dict[str, Any]) -> None:
    files_payload = input_data.get("filesPayload")
    table_keys = input_data.get("tableKeys")
    if isinstance(files_payload, list) and files_payload:
        set_meta(conn, "filesPayload", files_payload)
        return
    if isinstance(table_keys, list) and table_keys:
        all_payload = build_files_payload_from_db(conn)
        wanted = {str(k) for k in table_keys}
        filtered = [f for f in all_payload if str(f.get("table_key")) in wanted]
        if filtered:
            set_meta(conn, "filesPayload", filtered)


def _execute_operation(conn, session_id: str, operation: str, input_data: dict[str, Any], api_key: str | None) -> dict[str, Any]:
    if operation == "header_norm_run":
        return run_header_norm(conn, api_key)

    if operation == "header_norm_apply":
        decisions = input_data.get("decisions")
        if not isinstance(decisions, dict) or not decisions:
            decisions = _auto_header_norm_decisions(conn)
        if not isinstance(decisions, dict) or not decisions:
            raise ValueError("Missing or invalid decisions.")
        return apply_header_norm(conn, decisions)

    if operation == "append_plan":
        _apply_preferred_files_payload(conn, input_data)
        return run_append_plan(conn, api_key)

    if operation == "append_mapping":
        append_groups = input_data.get("appendGroups")
        if append_groups is None:
            append_groups = get_meta(conn, "appendGroups") or []
        if not isinstance(append_groups, list) or not append_groups:
            raise ValueError("No append groups available.")
        result = run_append_mapping(conn, append_groups, api_key)
        set_meta(conn, "appendGroupMappings", result.get("appendGroupMappings") or [])
        return result

    if operation == "append_execute":
        mappings = input_data.get("appendGroupMappings")
        if mappings is None:
            mappings = get_meta(conn, "appendGroupMappings") or []
        if not isinstance(mappings, list) or not mappings:
            raise ValueError("No append mappings available.")
        unassigned = input_data.get("unassignedTables")
        if unassigned is None:
            raw_unassigned = get_meta(conn, "unassigned") or []
            unassigned = [
                str(u.get("table_key"))
                for u in raw_unassigned
                if isinstance(u, dict) and u.get("table_key")
            ]
        return run_append_execute(conn, mappings, unassigned)

    raise ValueError(f"Unsupported operation: {operation}")


def _build_state_patch(conn) -> dict[str, Any]:
    patch: dict[str, Any] = {
        "inventory": build_inventory_from_db(conn),
        "filesPayload": get_meta(conn, "filesPayload") or [],
        "headerNormDecisions": get_meta(conn, "headerNormDecisions") or [],
        "headerNormStandardFields": STANDARD_FIELDS,
        "appendGroups": get_meta(conn, "appendGroups") or [],
        "appendGroupMappings": get_meta(conn, "appendGroupMappings") or [],
        "unassigned": get_meta(conn, "unassigned") or [],
        "groupSchema": get_meta(conn, "groupSchemaTableRows") or [],
        "groupSchemaTableRows": get_meta(conn, "groupSchemaTableRows") or [],
        "mergeBaseGroupId": get_meta(conn, "mergeBaseGroupId") or "",
        "mergeApprovedSources": get_meta(conn, "mergeApprovedSources") or [],
        "dateDetectResult": get_meta(conn, "dateDetectLast"),
        "dateAnalyzeResult": get_meta(conn, "dateAnalyzeLast"),
        "dateStandardizeResult": get_meta(conn, "dateStandardizeLast"),
    }
    merged = _build_merge_result_from_table(conn, "final_merged")
    if merged:
        patch["mergeResult"] = merged
    return patch


def _build_artifact_summary(conn) -> dict[str, Any]:
    registry = all_registered_tables(conn)
    tables = [
        {"table_key": r.get("table_key"), "sql_name": r.get("sql_name")}
        for r in registry
    ]
    return {
        "meta": {
            "inv": bool(get_meta(conn, "inv")),
            "filesPayload": bool(get_meta(conn, "filesPayload")),
            "headerNormDecisions": bool(get_meta(conn, "headerNormDecisions")),
            "appendGroups": bool(get_meta(conn, "appendGroups")),
            "appendGroupMappings": bool(get_meta(conn, "appendGroupMappings")),
            "groupSchemaTableRows": bool(get_meta(conn, "groupSchemaTableRows")),
            "mergeBaseGroupId": bool(get_meta(conn, "mergeBaseGroupId")),
            "mergeApprovedSources": bool(get_meta(conn, "mergeApprovedSources")),
        },
        "tables": tables,
        "final_merged": _table_artifact_rows_cols(conn, "final_merged"),
    }


def execute_operation_kernel(
    session_id: str | None,
    operation: str,
    api_key: str | None,
    input_data: dict[str, Any] | None = None,
    options: dict[str, Any] | None = None,
    request_id: str | None = None,
) -> tuple[dict[str, Any], int]:
    input_data = input_data if isinstance(input_data, dict) else {}
    options = options if isinstance(options, dict) else {}
    mode = str(options.get("mode") or "pipeline")
    auto_prepare = bool(options.get("autoPrepare", True))
    persist = bool(options.get("persist", True))

    if operation not in _OPERATION_SPECS:
        return {"ok": False, "error": f"Unsupported operation: {operation}"}, 400

    effective_session_id = str(session_id).strip() if session_id else uuid.uuid4().hex
    conn = get_session_db(effective_session_id)
    rid = (request_id or uuid.uuid4().hex[:12]).strip()

    lock = _session_lock(effective_session_id)
    with lock:
        executed_operations: list[str] = []
        auto_prepared: list[str] = []
        warnings: list[str] = []
        metrics: dict[str, Any] = {}

        def _run_one(op_name: str) -> dict[str, Any]:
            if auto_prepare:
                for prereq in (_OPERATION_SPECS.get(op_name, {}).get("auto_prereqs") or []):
                    if _operation_is_ready(conn, prereq):
                        continue
                    _run_one(prereq)
                    if prereq not in auto_prepared:
                        auto_prepared.append(prereq)

            missing = _missing_requirements(conn, op_name, input_data if op_name == operation else {}, api_key)
            if missing:
                raise ValueError(json.dumps({"missing_requirements": missing, "operation": op_name}))

            started = time.perf_counter()
            result = _execute_operation(conn, effective_session_id, op_name, input_data if op_name == operation else {}, api_key)
            metrics[op_name] = {"duration_ms": round((time.perf_counter() - started) * 1000, 1)}
            if op_name not in executed_operations:
                executed_operations.append(op_name)
            return result

        try:
            result_payload = _run_one(operation)
            artifacts = _build_artifact_summary(conn)
            state_patch = _build_state_patch(conn)
            if not persist:
                warnings.append("persist=false requested, but some operations persist by design for compatibility.")

            return (
                {
                    "ok": True,
                    "sessionId": effective_session_id,
                    "operation": operation,
                    "mode": mode,
                    "executed_operations": executed_operations,
                    "auto_prepared": auto_prepared,
                    "warnings": warnings,
                    "errors": [],
                    "metrics": metrics,
                    "artifacts": artifacts,
                    "statePatch": state_patch,
                    "result": result_payload,
                    "requestId": rid,
                },
                200,
            )
        except ValueError as exc:
            message = str(exc)
            try:
                payload = json.loads(message)
                return {
                    "ok": False,
                    "sessionId": effective_session_id,
                    "error": "Missing requirements",
                    "errors": [{"type": "missing_requirements", "details": payload.get("missing_requirements") or []}],
                    **payload,
                    "requestId": rid,
                }, 400
            except Exception:
                return {
                    "ok": False,
                    "sessionId": effective_session_id,
                    "error": message,
                    "errors": [{"type": "validation_error", "message": message}],
                    "requestId": rid,
                }, 400
        except Exception as exc:  # noqa: BLE001
            message = str(exc)
            return {
                "ok": False,
                "sessionId": effective_session_id,
                "error": message,
                "errors": [{"type": "execution_error", "message": message}],
                "requestId": rid,
            }, 500


@insights_bp.route("/execution/state", methods=["GET"])
def execution_state():
    try:
        session_id = request.args.get("sessionId")
        if not session_id:
            return jsonify({"error": "Missing sessionId"}), 400
        conn = get_session_db(str(session_id))
        readiness = {
            op: {
                "ready": len(_missing_requirements(conn, op, {}, None)) == 0,
                "missing_requirements": _missing_requirements(conn, op, {}, None),
            }
            for op in _OPERATION_SPECS.keys()
        }
        return jsonify(
            {
                "sessionId": session_id,
                "readiness": readiness,
                "artifactSummary": _build_artifact_summary(conn),
                "statePatch": _build_state_patch(conn),
            }
        )
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@insights_bp.route("/execution/run", methods=["POST"])
def execution_run():
    try:
        body = request.get_json(force=True, silent=True) or {}
        operation = str(body.get("operation") or "").strip()
        session_id = body.get("sessionId")
        api_key = body.get("apiKey")
        options = body.get("options")
        input_data = body.get("input")
        payload, status = execute_operation_kernel(
            session_id=session_id,
            operation=operation,
            api_key=api_key,
            input_data=input_data if isinstance(input_data, dict) else {},
            options=options if isinstance(options, dict) else {},
            request_id=request.headers.get("X-Request-Id"),
        )
        return jsonify(payload), status
    except Exception as exc:  # noqa: BLE001
        return jsonify({"ok": False, "error": str(exc)}), 500


@insights_bp.route("/group-insights", methods=["POST"])
def group_insights():
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        api_key = body.get("apiKey")
        if not session_id:
            return jsonify({"error": "Missing sessionId"}), 400
        if not (api_key and str(api_key).strip()):
            return jsonify({"error": "Missing API key"}), 400
        conn = get_session_db(session_id)
        groups = body.get("groups")
        if groups is None:
            groups = get_meta(conn, "appendGroups")
        result = run_insights(conn, session_id, groups, str(api_key).strip())
        if "insights" not in result:
            reports = result.get("groupReports") or []
            result["insights"] = {
                str(r.get("groupId")): r.get("insights", {})
                for r in reports
            }
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@insights_bp.route("/pre-merge-analysis", methods=["POST"])
def pre_merge_analysis():
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        api_key = body.get("apiKey")
        if not session_id:
            return jsonify({"error": "Missing sessionId"}), 400
        if not (api_key and str(api_key).strip()):
            return jsonify({"error": "Missing API key"}), 400
        conn = get_session_db(session_id)
        result = run_pre_merge_analysis(conn, session_id, str(api_key).strip())
        if "should_merge" not in result:
            result["should_merge"] = bool(result.get("mergeHints"))
            result["rationale"] = str(result.get("narrative") or "")
        result.setdefault("dimension_recommendations", [])
        result.setdefault("recommended_fact_group", None)
        result.setdefault("skip_merge_reason", "" if result.get("should_merge") else str(result.get("rationale") or ""))
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


@insights_bp.route("/chat", methods=["POST"])
def chat():
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        messages = body.get("messages") or []
        message = body.get("message")
        if not message and isinstance(messages, list):
            user_msgs = [m for m in messages if isinstance(m, dict) and m.get("role") == "user" and m.get("content")]
            if user_msgs:
                message = user_msgs[-1].get("content")
        context = body.get("context") or ""
        api_key = body.get("apiKey")
        if not session_id or not message:
            return jsonify({"error": "Missing sessionId or message"}), 400
        conn = get_session_db(session_id)
        key = str(api_key).strip() if api_key else None
        if not context:
            context = _build_chat_context(conn, body)

        history = []
        if isinstance(messages, list):
            history = [
                {"role": str(m.get("role")), "content": str(m.get("content"))}
                for m in messages
                if isinstance(m, dict) and m.get("role") in ("user", "assistant")
            ]
        result = run_chat(
            conn,
            session_id,
            str(message),
            str(context),
            api_key=key,
            history=history,
        )

        if isinstance(messages, list) and messages:
            reply = str(result.get("reply") or "")

            def _event_stream():
                yield f"data: {json.dumps({'content': reply})}\n\n"
                yield "data: [DONE]\n\n"

            return Response(
                stream_with_context(_event_stream()),
                mimetype="text/event-stream",
                headers={
                    "Cache-Control": "no-cache",
                    "X-Accel-Buffering": "no",
                },
            )

        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500


# ── Step-aware cache invalidation ──────────────────────────────────────

_STEP_ARTIFACTS: dict[int, dict] = {
    3: {
        "meta": ["appendGroups", "appendGroupMappings", "groupSchemaTableRows", "unassigned"],
        "table_prefix": ["grp__"],
    },
    4: {
        "meta": ["headerNormDecisions"],
        "table_prefix": [],
    },
    5: {
        "meta": [],
        "table_prefix": [],
    },
    6: {
        "meta": ["mergeBaseGroupId", "mergeApprovedSources", "merge_history"],
        "table_prefix": ["final_merged", "merge_v"],
    },
}


@insights_bp.route("/invalidate-downstream", methods=["POST"])
def invalidate_downstream():
    """Clear all cached artifacts for steps after *fromStep*."""
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        from_step = int(body.get("fromStep", 0))

        if not session_id:
            return jsonify({"error": "Missing sessionId"}), 400

        conn = get_session_db(str(session_id))

        if from_step == 0:
            for key in get_all_meta_keys(conn):
                delete_meta(conn, key)
            for reg in all_registered_tables(conn):
                sql_name = reg.get("sql_name") or reg.get("table_name", "")
                if sql_name:
                    drop_table(conn, sql_name, commit=False)
                tk = reg.get("table_key", "")
                if tk:
                    unregister_table(conn, tk, commit=False)
            conn.commit()
        else:
            for step_num in sorted(_STEP_ARTIFACTS.keys()):
                if step_num > from_step:
                    spec = _STEP_ARTIFACTS[step_num]
                    for key in spec["meta"]:
                        delete_meta(conn, key)
                    for prefix in spec["table_prefix"]:
                        for reg in all_registered_tables(conn):
                            sql_name = reg.get("sql_name") or reg.get("table_name", "")
                            tk = reg.get("table_key", "")
                            if sql_name and sql_name.startswith(prefix):
                                drop_table(conn, sql_name, commit=False)
                                if tk:
                                    unregister_table(conn, tk, commit=False)
            conn.commit()

        return jsonify({"ok": True})
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500
