"""Guided Merge API — recommend base, common columns, simulate, execute, validate, finalize, download."""

from __future__ import annotations

import csv
import io
import json
import os
import time
import zipfile
import uuid
from typing import Any
from urllib.parse import urlsplit

import requests as _requests
from openpyxl import Workbook

from flask import Blueprint, Response, current_app, jsonify, request, stream_with_context


def _configured_backend_url(key: str) -> str | None:
    raw = current_app.config.get(key)
    if raw is None or not str(raw).strip():
        raw = os.environ.get(key)
    if raw is None or not str(raw).strip():
        return None
    value = str(raw).strip().rstrip("/")
    parsed = urlsplit(value)
    try:
        parsed.port
        valid_port = True
    except ValueError:
        valid_port = False
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.netloc
        or not parsed.hostname
        or any(char.isspace() for char in parsed.netloc)
        or not valid_port
        or parsed.query
        or parsed.fragment
    ):
        raise RuntimeError(f"{key} must be an absolute http(s) URL with a valid host")
    return value


def _unified_backend_url() -> str:
    """Resolve the one-process host URL used by cross-module handoffs."""
    configured = _configured_backend_url("UNIFIED_BACKEND_URL")
    if configured:
        return configured
    port = current_app.config.get("UNIFIED_BACKEND_PORT") or os.environ.get("UNIFIED_BACKEND_PORT", "8000")
    return f"http://127.0.0.1:{int(port)}"


def _normalizer_be() -> str:
    """Resolve the Module 2 namespace on the unified backend."""
    return f"{_unified_backend_url()}/api/module2"


def _analyzer_be() -> str:
    """Resolve the Module 3 namespace on the unified backend."""
    return f"{_unified_backend_url()}/api/module3"

from shared.db import (
    drop_table,
    get_meta,
    get_session_db,
    get_session_lock,
    lookup_sql_name,
    read_table,
    read_table_columns,
    register_table,
    set_meta,
    table_exists,
    table_row_count,
    quote_id,
    PREVIEW_POOL,
    pick_best_rows,
    filter_data_columns,
    public_projection,
)

from merging.guided_merge_service import (
    SYSTEM_COLUMNS_TO_EXCLUDE,
    _is_system_column,
    classify_all_columns,
    classify_columns,
    delete_merge_output,
    execute_merge,
    finalize_merge,
    find_common_columns,
    generate_validation_report,
    persist_merge_output,
    recommend_base_file,
    simulate_join,
    skip_merge,
    suggest_join_keys,
)

merging_bp = Blueprint("merging_bp", __name__)


def _export_columns(conn, table_name: str) -> list[str]:
    """Return the exact user-facing schema for a merge export.

    Merge tables retain ``__row_id`` for preview/edit stability, but exported
    files and cross-module transfers must use the same public schema as their
    SQL projection.  Keeping the filtering at this boundary prevents header /
    row mismatches and avoids leaking provenance fields.
    """
    return filter_data_columns(read_table_columns(conn, table_name))


@merging_bp.route("/merge/recommend-base", methods=["POST"])
def recommend_base():
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        api_key = body.get("apiKey")
        if not session_id:
            return jsonify({"error": "Missing sessionId"}), 400
        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            result = recommend_base_file(conn, session_id, api_key)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/common-columns", methods=["POST"])
def common_columns():
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        base_group_id = body.get("baseGroupId")
        source_group_id = body.get("sourceGroupId")
        api_key = body.get("apiKey")
        include_preview = body.get("includePreview", False)
        if not session_id or not base_group_id or not source_group_id:
            return jsonify({"error": "Missing sessionId, baseGroupId, or sourceGroupId"}), 400

        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            base_sql = lookup_sql_name(conn, base_group_id)
            source_sql = lookup_sql_name(conn, source_group_id)
            if not base_sql or not source_sql:
                return jsonify({"error": "Invalid group ID(s)"}), 400

            # Column lists + instant classification (no DB, runs in <1ms)
            base_cols = read_table_columns(conn, base_sql)
            source_cols = read_table_columns(conn, source_sql)

            # Filter out system columns from the column lists
            base_cols = [c for c in base_cols if not _is_system_column(c)]
            source_cols = [c for c in source_cols if not _is_system_column(c)]

            base_col_classes = classify_all_columns(base_cols)
            source_col_classes = classify_all_columns(source_cols)

            common = find_common_columns(conn, base_sql, source_sql)
            classified = classify_columns(conn, session_id, api_key, common, base_sql_name=base_sql)

            result: dict[str, Any] = {
                "common_columns": classified,
                "base_columns": base_cols,
                "source_columns": source_cols,
                "base_column_classes": base_col_classes,
                "source_column_classes": source_col_classes,
                "base_group_id": base_group_id,
                "source_group_id": source_group_id,
            }
            if include_preview:
                base_rows = pick_best_rows(read_table(conn, base_sql, PREVIEW_POOL), 50)
                source_rows = pick_best_rows(read_table(conn, source_sql, PREVIEW_POOL), 50)
                # Filter out system columns from preview rows
                def filter_system_cols_from_rows(rows):
                    return [
                        {k: v for k, v in row.items() if not _is_system_column(k)}
                        for row in rows
                    ]
                result["base_preview"] = {"columns": base_cols, "rows": filter_system_cols_from_rows(base_rows), "total_rows": table_row_count(conn, base_sql)}
                result["source_preview"] = {"columns": source_cols, "rows": filter_system_cols_from_rows(source_rows), "total_rows": table_row_count(conn, source_sql)}
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/simulate", methods=["POST"])
def simulate():
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        base_group_id = body.get("baseGroupId")
        source_group_id = body.get("sourceGroupId")
        key_pairs = body.get("keyPairs", [])
        if not session_id or not base_group_id or not source_group_id:
            return jsonify({"error": "Missing required fields"}), 400
        if not key_pairs:
            return jsonify({"error": "At least one key pair required"}), 400

        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            base_sql = lookup_sql_name(conn, base_group_id)
            source_sql = lookup_sql_name(conn, source_group_id)
            if not base_sql or not source_sql:
                return jsonify({"error": "Invalid group ID(s)"}), 400

            pull_columns = body.get("pullColumns", [])
            result = simulate_join(conn, base_sql, source_sql, key_pairs, pull_columns=pull_columns)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/suggest-keys", methods=["POST"])
def suggest_keys():
    """Get AI-suggested join key pairs for base and source tables."""
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        base_group_id = body.get("baseGroupId")
        source_group_id = body.get("sourceGroupId")
        api_key = body.get("apiKey")

        if not session_id or not base_group_id or not source_group_id:
            return jsonify({"error": "Missing sessionId, baseGroupId, or sourceGroupId"}), 400

        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            base_sql = lookup_sql_name(conn, base_group_id)
            source_sql = lookup_sql_name(conn, source_group_id)
            if not base_sql or not source_sql:
                return jsonify({"error": "Invalid group ID(s)"}), 400

            result = suggest_join_keys(conn, base_sql, source_sql, api_key)

        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/execute", methods=["POST"])
def execute():
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        base_group_id = body.get("baseGroupId")
        source_group_id = body.get("sourceGroupId")
        key_pairs = body.get("keyPairs", [])
        pull_columns = body.get("pullColumns", [])
        dedup_config = body.get("dedupConfig") or {"strategy": "first"}
        if not session_id or not base_group_id or not source_group_id:
            return jsonify({"error": "Missing required fields"}), 400

        conn = get_session_db(session_id)
        base_sql = lookup_sql_name(conn, base_group_id)
        source_sql = lookup_sql_name(conn, source_group_id)
        if not base_sql or not source_sql:
            return jsonify({"error": "Invalid group ID(s)"}), 400

        def _sse(payload: dict) -> str:
            return f"data: {json.dumps(payload)}\n\n"

        lock = get_session_lock(session_id)

        def _stream():
            with lock:
                try:
                    yield _sse({"stage": "dedup", "progress": 15, "message": "Deduplicating source & executing join..."})

                    merge_log = execute_merge(
                        conn, session_id, base_sql, source_sql, key_pairs, pull_columns, source_group_id, dedup_config
                    )

                    conn.commit()

                    yield _sse({"stage": "stats", "progress": 55, "message": "Computing column statistics..."})

                    report = generate_validation_report(
                        conn, base_sql, source_sql, merge_log["result_table"], merge_log
                    )

                    yield _sse({"stage": "persist", "progress": 80, "message": "Saving versioned output..."})

                    persist_result = persist_merge_output(
                        conn, session_id, merge_log["result_table"],
                        base_group_id, source_group_id, key_pairs, pull_columns,
                    )

                    yield _sse({
                        "stage": "done", "progress": 100, "message": "Merge complete!",
                        "result": {
                            "merge_log": merge_log,
                            "validation_report": report,
                            "persist": persist_result,
                        },
                    })
                except Exception as exc:
                    yield _sse({"stage": "error", "progress": 0, "message": str(exc)})

        return Response(
            stream_with_context(_stream()),
            mimetype="text/event-stream",
            headers={
                "Cache-Control": "no-cache",
                "X-Accel-Buffering": "no",
            },
        )
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/finalize", methods=["POST"])
def finalize():
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        approved_merges = body.get("approvedMerges", [])
        if not session_id:
            return jsonify({"error": "Missing sessionId"}), 400
        if not approved_merges:
            return jsonify({"error": "No approved merges provided"}), 400

        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            result = finalize_merge(conn, session_id, approved_merges)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/skip", methods=["POST"])
def skip():
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        base_group_id = body.get("baseGroupId")
        if not session_id or not base_group_id:
            return jsonify({"error": "Missing sessionId or baseGroupId"}), 400
        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            result = skip_merge(conn, session_id, base_group_id)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/redo-clear-cache", methods=["POST"])
def redo_clear_cache():
    """Drop the latest finalized merge tables and history entry so the user can redo."""
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        if not session_id:
            return jsonify({"error": "Missing sessionId"}), 400

        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            merge_history = get_meta(conn, "merge_history") or []

            if merge_history:
                latest = merge_history.pop()
                versioned_table = latest.get("table_name", "")
                if versioned_table and table_exists(conn, versioned_table):
                    drop_table(conn, versioned_table)
                set_meta(conn, "merge_history", merge_history)

            if table_exists(conn, "final_merged"):
                drop_table(conn, "final_merged")

            set_meta(conn, "mergeApprovedSources", [])

        return jsonify({"cleared": True})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/delete-output", methods=["POST"])
def delete_output():
    """Delete a specific versioned merge output, its history entry, and group registration."""
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        version = body.get("version")
        if not session_id or version is None:
            return jsonify({"error": "Missing sessionId or version"}), 400

        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            result = delete_merge_output(conn, session_id, int(version))
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/register-merged-group", methods=["POST"])
def register_merged_group():
    """Copy final_merged into a new group so it can be used in subsequent merges."""
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        group_name = body.get("groupName", "Merged Output")
        if not session_id:
            return jsonify({"error": "Missing sessionId"}), 400

        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            if not table_exists(conn, "final_merged"):
                return jsonify({"error": "No merged data found"}), 404

            group_id = f"merged_{int(time.time() * 1000)}"
            sql_name = f"merged_output_{int(time.time())}"

            source_columns = read_table_columns(conn, "final_merged")
            if not source_columns:
                return jsonify({"error": "Merged data has no public columns"}), 400
            conn.execute(
                f"CREATE TABLE {quote_id(sql_name)} AS SELECT "
                f"{public_projection(source_columns)}, "
                f"CAST(ROW_NUMBER() OVER () AS BIGINT) AS {quote_id('__row_id')} "
                f"FROM {quote_id('final_merged')}"
            )
            conn.commit()
            register_table(conn, group_id, sql_name)

            columns = read_table_columns(conn, sql_name)
            rows = table_row_count(conn, sql_name)
            new_group_row = {
                "group_id": group_id,
                "group_name": group_name,
                "rows": rows,
                "columns": columns,
            }

            schema = get_meta(conn, "groupSchemaTableRows") or []
            schema.append(new_group_row)
            set_meta(conn, "groupSchemaTableRows", schema)

        return jsonify({
            "group_id": group_id,
            "group_name": group_name,
            "group_row": new_group_row,
            "groupSchema": schema,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/download-csv", methods=["GET"])
def download_csv():
    try:
        session_id = request.args.get("sessionId")
        version_str = request.args.get("version")
        if not session_id:
            return jsonify({"error": "sessionId query parameter is required"}), 400
        conn = get_session_db(session_id)

        target_table = "final_merged"
        filename = "final_merged.csv"

        if version_str:
            version = int(version_str)
            merge_history = get_meta(conn, "merge_history") or []
            entry = next((e for e in merge_history if e["version"] == version), None)
            if not entry:
                return jsonify({"error": f"Version {version_str} not found in merge history"}), 404
            target_table = entry["table_name"]
            label = entry.get("file_label", f"merge_v{version}")
            safe_label = "".join(c if c.isalnum() or c in "._- " else "_" for c in label)
            filename = f"{safe_label}.csv"

        if not table_exists(conn, target_table):
            return jsonify({"error": "No merged data found"}), 404

        def _generate():
            columns = _export_columns(conn, target_table)
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(columns)
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate()

            cursor = conn.execute(f"SELECT {public_projection(columns)} FROM {quote_id(target_table)}")
            while True:
                rows = cursor.fetchmany(2000)
                if not rows:
                    break
                for row in rows:
                    writer.writerow([row[c] for c in columns])
                yield buf.getvalue()
                buf.seek(0)
                buf.truncate()

        return Response(
            stream_with_context(_generate()),
            headers={
                "Content-Type": "text/csv; charset=utf-8",
                "Content-Disposition": f'attachment; filename="{filename}"',
            },
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


def _table_to_xlsx_bytes(conn, table_name: str) -> bytes:
    """Write a SQLite table into an in-memory xlsx buffer and return bytes."""
    columns = _export_columns(conn, table_name)
    wb = Workbook(write_only=True)
    ws = wb.create_sheet()
    ws.append(columns)
    cursor = conn.execute(f"SELECT {public_projection(columns)} FROM {quote_id(table_name)}")
    while True:
        rows = cursor.fetchmany(2000)
        if not rows:
            break
        for row in rows:
            ws.append([row[c] for c in columns])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf.getvalue()


@merging_bp.route("/merge/download-step-xlsx", methods=["GET"])
def download_step_xlsx():
    """Download a per-source _merge_step_ table as xlsx immediately after execution."""
    try:
        session_id = request.args.get("sessionId")
        source_group_id = request.args.get("sourceGroupId")
        if not session_id or not source_group_id:
            return jsonify({"error": "sessionId and sourceGroupId are required"}), 400
        conn = get_session_db(session_id)
        step_table = f"_merge_step_{source_group_id}"
        if not table_exists(conn, step_table):
            return jsonify({"error": "Step table not found — not yet executed or already finalized"}), 404

        xlsx_bytes = _table_to_xlsx_bytes(conn, step_table)

        schema = get_meta(conn, "groupSchemaTableRows") or []
        name_map = {g["group_id"]: g.get("group_name", g["group_id"]) for g in schema}
        src_name = name_map.get(source_group_id, source_group_id)
        safe_name = "".join(c if c.isalnum() or c in "._- " else "_" for c in src_name)
        filename = f"step_merge_{safe_name}.xlsx"

        return Response(
            xlsx_bytes,
            headers={
                "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "Content-Disposition": f'attachment; filename="{filename}"',
            },
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/download-step-csv", methods=["GET"])
def download_step_csv():
    """Streaming CSV download of a per-source _merge_step_ table."""
    try:
        session_id = request.args.get("sessionId")
        source_group_id = request.args.get("sourceGroupId")
        if not session_id or not source_group_id:
            return jsonify({"error": "sessionId and sourceGroupId are required"}), 400
        conn = get_session_db(session_id)
        step_table = f"_merge_step_{source_group_id}"
        if not table_exists(conn, step_table):
            return jsonify({"error": "Step table not found — not yet executed or already finalized"}), 404

        schema = get_meta(conn, "groupSchemaTableRows") or []
        name_map = {g["group_id"]: g.get("group_name", g["group_id"]) for g in schema}
        src_name = name_map.get(source_group_id, source_group_id)
        safe_name = "".join(c if c.isalnum() or c in "._- " else "_" for c in src_name)
        filename = f"step_merge_{safe_name}.csv"

        def _generate():
            columns = _export_columns(conn, step_table)
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(columns)
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate()

            cursor = conn.execute(f"SELECT {public_projection(columns)} FROM {quote_id(step_table)}")
            while True:
                rows = cursor.fetchmany(2000)
                if not rows:
                    break
                for row in rows:
                    writer.writerow([row[c] for c in columns])
                yield buf.getvalue()
                buf.seek(0)
                buf.truncate()

        return Response(
            stream_with_context(_generate()),
            headers={
                "Content-Type": "text/csv; charset=utf-8",
                "Content-Disposition": f'attachment; filename="{filename}"',
            },
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/download-xlsx", methods=["GET"])
def download_xlsx():
    """Download a finalized versioned merge output as xlsx."""
    try:
        session_id = request.args.get("sessionId")
        version_str = request.args.get("version")
        if not session_id:
            return jsonify({"error": "sessionId query parameter is required"}), 400
        conn = get_session_db(session_id)

        merge_history = get_meta(conn, "merge_history") or []
        if not merge_history:
            if not table_exists(conn, "final_merged"):
                return jsonify({"error": "No merged data found"}), 404
            xlsx_bytes = _table_to_xlsx_bytes(conn, "final_merged")
            return Response(
                xlsx_bytes,
                headers={
                    "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                    "Content-Disposition": 'attachment; filename="final_merged.xlsx"',
                },
            )

        if version_str:
            version = int(version_str)
            entry = next((e for e in merge_history if e["version"] == version), None)
        else:
            entry = merge_history[-1]

        if not entry:
            return jsonify({"error": f"Version {version_str} not found in merge history"}), 404

        tbl = entry["table_name"]
        if not table_exists(conn, tbl):
            return jsonify({"error": f"Table {tbl} no longer exists"}), 404

        xlsx_bytes = _table_to_xlsx_bytes(conn, tbl)
        filename = entry.get("file_label", f"merge_v{entry['version']}.xlsx")

        return Response(
            xlsx_bytes,
            headers={
                "Content-Type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
                "Content-Disposition": f'attachment; filename="{filename}"',
            },
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/download-all", methods=["GET"])
def download_all():
    """Download all versioned merge outputs as a single ZIP of xlsx files."""
    try:
        session_id = request.args.get("sessionId")
        if not session_id:
            return jsonify({"error": "sessionId query parameter is required"}), 400
        conn = get_session_db(session_id)

        merge_history = get_meta(conn, "merge_history") or []
        if not merge_history:
            return jsonify({"error": "No merge history found"}), 404

        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for entry in merge_history:
                tbl = entry["table_name"]
                if not table_exists(conn, tbl):
                    continue
                xlsx_bytes = _table_to_xlsx_bytes(conn, tbl)
                filename = entry.get("file_label", f"merge_v{entry['version']}.xlsx")
                zf.writestr(filename, xlsx_bytes)
        zip_buf.seek(0)

        return Response(
            zip_buf.getvalue(),
            headers={
                "Content-Type": "application/zip",
                "Content-Disposition": 'attachment; filename="all_merge_outputs.zip"',
            },
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/download-all-csv", methods=["GET"])
def download_all_csv():
    """Download all versioned merge outputs as a single ZIP of CSV files."""
    try:
        session_id = request.args.get("sessionId")
        if not session_id:
            return jsonify({"error": "sessionId query parameter is required"}), 400
        conn = get_session_db(session_id)

        merge_history = get_meta(conn, "merge_history") or []
        if not merge_history:
            return jsonify({"error": "No merge history found"}), 404

        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for entry in merge_history:
                tbl = entry["table_name"]
                if not table_exists(conn, tbl):
                    continue
                columns = _export_columns(conn, tbl)
                csv_buf = io.StringIO()
                writer = csv.writer(csv_buf)
                writer.writerow(columns)
                cursor = conn.execute(f"SELECT {public_projection(columns)} FROM {quote_id(tbl)}")
                while True:
                    rows = cursor.fetchmany(2000)
                    if not rows:
                        break
                    for row in rows:
                        writer.writerow([row[c] for c in columns])
                label = entry.get("file_label", f"merge_v{entry['version']}")
                safe_label = "".join(c if c.isalnum() or c in "._- " else "_" for c in label)
                zf.writestr(f"{safe_label}.csv", csv_buf.getvalue())
        zip_buf.seek(0)

        return Response(
            zip_buf.getvalue(),
            headers={
                "Content-Type": "application/zip",
                "Content-Disposition": 'attachment; filename="all_merge_outputs.zip"',
            },
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/history", methods=["GET"])
def merge_history_route():
    """Return the merge history metadata list."""
    try:
        session_id = request.args.get("sessionId")
        if not session_id:
            return jsonify({"error": "sessionId query parameter is required"}), 400
        conn = get_session_db(session_id)
        merge_history = get_meta(conn, "merge_history") or []
        return jsonify({"merge_history": merge_history})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/preview", methods=["GET"])
def merge_preview():
    """Paginated preview for a merge output version (latest or specific version)."""
    try:
        session_id = request.args.get("sessionId")
        version_arg = request.args.get("version", "latest")
        offset = int(request.args.get("offset", 0))
        limit = min(int(request.args.get("limit", 200)), 500)
        if not session_id:
            return jsonify({"error": "sessionId is required"}), 400

        conn = get_session_db(session_id)
        if version_arg in ("latest", ""):
            sql_name = "final_merged"
        elif version_arg.startswith("final_merged"):
            sql_name = version_arg
        else:
            sql_name = f"final_merged_v{int(version_arg)}"

        if not table_exists(conn, sql_name):
            return jsonify({"error": f"Merge table {sql_name} not found"}), 404

        columns = read_table_columns(conn, sql_name)
        total = table_row_count(conn, sql_name)
        col_select = ", ".join(quote_id(c) for c in columns) if columns else "*"
        rows_raw = conn.execute(
            f"SELECT {col_select} FROM {quote_id(sql_name)} LIMIT ? OFFSET ?",
            (limit, offset),
        ).fetchall()
        rows = [dict(zip(r.keys(), r)) for r in rows_raw]

        return jsonify({
            "version": version_arg,
            "table_name": sql_name,
            "columns": columns,
            "rows": rows,
            "total_rows": total,
            "offset": offset,
            "limit": limit,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/merge/table-preview", methods=["POST"])
def table_preview():
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        group_id = body.get("groupId")
        limit = min(int(body.get("limit", 50)), 200)
        if not session_id or not group_id:
            return jsonify({"error": "Missing sessionId or groupId"}), 400

        conn = get_session_db(session_id)
        sql_name = lookup_sql_name(conn, group_id)
        if not sql_name or not table_exists(conn, sql_name):
            return jsonify({"error": f"Table not found for group {group_id}"}), 404

        columns = read_table_columns(conn, sql_name)
        rows = pick_best_rows(read_table(conn, sql_name, PREVIEW_POOL), limit)
        total = table_row_count(conn, sql_name)

        return jsonify({
            "group_id": group_id,
            "columns": columns,
            "rows": rows,
            "total_rows": total,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@merging_bp.route("/group-preview", methods=["POST"])
def group_preview_route():
    """Backward-compatible group preview endpoint."""
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        group_ids = body.get("groupIds") or body.get("group_ids") or []
        if not session_id or not group_ids:
            return jsonify({"error": "sessionId and groupIds are required"}), 400
        conn = get_session_db(session_id)
        result: dict[str, Any] = {}
        for gid in group_ids:
            gid = str(gid)
            sql_name = lookup_sql_name(conn, gid)
            if not sql_name:
                sql_name = gid if table_exists(conn, gid) else None
            if not sql_name or not table_exists(conn, sql_name):
                continue
            columns = read_table_columns(conn, sql_name)
            rows = pick_best_rows(read_table(conn, sql_name, PREVIEW_POOL), 50)
            total = table_row_count(conn, sql_name)
            result[gid] = {"columns": columns, "rows": rows, "total_rows": total}
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ── Cross-module transfer helpers ──────────────────────────────────────────────


def _read_version_csv(conn, version: int | None) -> tuple[bytes, str]:
    """Read a merge version (or latest final_merged) as CSV bytes + filename."""
    target_table = "final_merged"
    filename = "final_merged.csv"

    if version is not None:
        merge_history = get_meta(conn, "merge_history") or []
        entry = next((e for e in merge_history if e["version"] == version), None)
        if not entry:
            raise ValueError(f"Version {version} not found in merge history")
        target_table = entry["table_name"]
        label = entry.get("file_label", f"merge_v{version}")
        safe_label = "".join(c if c.isalnum() or c in "._- " else "_" for c in label)
        filename = f"{safe_label}.csv"

    if not table_exists(conn, target_table):
        raise ValueError("No merged data found")

    columns = _export_columns(conn, target_table)
    buf = io.StringIO()
    writer = csv.writer(buf)
    writer.writerow(columns)
    cursor = conn.execute(f"SELECT {public_projection(columns)} FROM {quote_id(target_table)}")
    while True:
        rows = cursor.fetchmany(2000)
        if not rows:
            break
        for row in rows:
            writer.writerow([row[c] for c in columns])

    return buf.getvalue().encode("utf-8"), filename


def _version_target(conn, version: int | None) -> tuple[str, str, list[str]]:
    """Resolve a merge version while the caller holds the session lock."""
    target_table = "final_merged"
    filename = "final_merged"
    if version is not None:
        merge_history = get_meta(conn, "merge_history") or []
        entry = next((e for e in merge_history if e["version"] == version), None)
        if not entry:
            raise ValueError(f"Version {version} not found in merge history")
        target_table = entry["table_name"]
        label = entry.get("file_label", f"merge_v{version}")
        filename = "".join(c if c.isalnum() or c in "._- " else "_" for c in label)
    if not table_exists(conn, target_table):
        raise ValueError("No merged data found")
    columns = _export_columns(conn, target_table)
    if not columns:
        raise ValueError("Merged data has no exportable columns")
    return target_table, (filename or "merged_data"), columns


def _typed_manifest(conn, target_table: str, filename: str, columns: list[str], destination: str):
    """Build a schema-bound manifest without materialising table rows."""
    from backend.ingestion.models import OrderedColumnSchema, TableArtifact, ValueType
    from backend.ingestion.transfer import TransferManifest

    type_rows = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position", (target_table,)
    ).fetchall()
    duck_types = {str(row[0]): str(row[1]).upper() for row in type_rows}
    def value_type(name: str):
        dtype = duck_types.get(name, "")
        if "BOOL" in dtype:
            return ValueType.BOOLEAN
        if "INT" in dtype:
            return ValueType.INTEGER
        if any(token in dtype for token in ("DECIMAL", "NUMERIC")):
            return ValueType.DECIMAL
        if any(token in dtype for token in ("DOUBLE", "FLOAT", "REAL")):
            return ValueType.FLOAT
        if "TIMESTAMP" in dtype:
            return ValueType.TIMESTAMP
        if "DATE" in dtype:
            return ValueType.DATE
        if "TIME" in dtype:
            return ValueType.TIME
        return ValueType.TEXT

    schema = tuple(
        OrderedColumnSchema(
            key=f"COL_{index + 1}", display_name=name, ordinal=index,
            value_type=value_type(name), physical_name=name,
        )
        for index, name in enumerate(columns)
    )
    rows = table_row_count(conn, target_table)
    artifact = TableArtifact(
        table_key=target_table, display_name=filename,
        columns=schema, row_count=rows,
    )
    return TransferManifest.from_tables(
        artifact_id=f"module1-{uuid.uuid4().hex}", source_module="module1",
        destination_module=destination, tables=[artifact],
    )


def _export_typed_artifact(conn, target_table: str, filename: str,
                           columns: list[str], destination: str):
    """Export Parquet and bind the manifest to its actual payload bytes."""
    from backend.ingestion.typed_artifact import (
        TypedArtifactLimits, export_duckdb_parquet, serialize_transport,
    )

    # Export a SQL projection so internal preview/provenance columns never
    # cross the module boundary.  DuckDB performs this copy column-wise; no
    # Python DataFrame or all-row object materialisation is involved.
    projection_table = f"m1_transfer_{uuid.uuid4().hex}"
    conn.execute(
        f"CREATE TEMP TABLE {quote_id(projection_table)} AS "
        f"SELECT {public_projection(columns)} FROM {quote_id(target_table)}"
    )
    try:
        manifest = _typed_manifest(conn, projection_table, filename, columns, destination)
        bound_manifest, payload = export_duckdb_parquet(
            conn, projection_table, manifest, limits=TypedArtifactLimits(),
        )
        return bytes(payload), f"{filename}.parquet", serialize_transport(bound_manifest)
    finally:
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(projection_table)}")


def _typed_unsupported(response) -> bool:
    """Fallback only for an explicit typed-artifact capability response."""
    try:
        body = response.json()
    except Exception:
        return False
    if not isinstance(body, dict):
        return False
    return str(body.get("code", "")).upper() in {
        "UNSUPPORTED_TYPED_ARTIFACT", "INGEST_UNSUPPORTED_TYPED_ARTIFACT",
    }


@merging_bp.route("/merge/transfer-to-normalizer", methods=["POST"])
def transfer_to_normalizer():
    """Send a merge output to the Data Normalizer (Module 2) via backend-to-backend transfer."""
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        version = body.get("version")
        if not session_id:
            return jsonify({"ok": False, "error": "Missing sessionId"}), 400

        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            target, label, columns = _version_target(conn, version)
            parquet_bytes, filename, manifest_bytes = _export_typed_artifact(
                conn, target, label, columns, "module2",
            )

        resp = _requests.post(
            f"{_normalizer_be()}/import-from-stitcher",
            files={
                "file": (filename, io.BytesIO(parquet_bytes), "application/vnd.apache.parquet"),
                "manifest": ("manifest.json", io.BytesIO(manifest_bytes), "application/json"),
            },
            timeout=120,
        )
        transport = "typed_artifact"
        warning = None
        if _typed_unsupported(resp):
            with get_session_lock(session_id):
                conn = get_session_db(session_id)
                csv_bytes, csv_filename = _read_version_csv(conn, version)
            resp = _requests.post(
                f"{_normalizer_be()}/import-from-stitcher",
                files={"file": (csv_filename, io.BytesIO(csv_bytes), "text/csv")},
                timeout=120,
            )
            transport = "csv"
            warning = "Typed artifact unsupported by destination; CSV transfer is lossy."
        if resp.status_code != 200:
            err_text = resp.text
            try:
                err_text = resp.json().get("error", err_text)
            except Exception:
                pass
            return jsonify({"ok": False, "error": f"Normalizer import failed: {err_text}"}), 502

        normalizer_session_id = resp.json().get("sessionId", "")
        if not normalizer_session_id:
            return jsonify({"ok": False, "error": "Normalizer did not return a session ID"}), 502
        result = {"ok": True, "normalizerSessionId": normalizer_session_id, "transport": transport}
        if warning:
            result["warning"] = warning
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        err_str = str(exc).lower()
        if "connection" in err_str or "refused" in err_str or "httpconnectionpool" in err_str:
            return jsonify({"ok": False, "error": "Cannot reach the Data Normalizer backend. Is it running?"}), 502
        return jsonify({"ok": False, "error": str(exc)}), 500


@merging_bp.route("/merge/transfer-to-analyzer", methods=["POST"])
def transfer_to_analyzer():
    """Send a merge output to the Summarization Module (Module 3) via backend-to-backend transfer."""
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        version = body.get("version")
        if not session_id:
            return jsonify({"ok": False, "error": "Missing sessionId"}), 400

        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            target, label, columns = _version_target(conn, version)
            parquet_bytes, filename, manifest_bytes = _export_typed_artifact(
                conn, target, label, columns, "module3",
            )

        resp = _requests.post(
            f"{_analyzer_be()}/import",
            files={
                "file": (filename, io.BytesIO(parquet_bytes), "application/vnd.apache.parquet"),
                "manifest": ("manifest.json", io.BytesIO(manifest_bytes), "application/json"),
            },
            timeout=120,
        )
        transport = "typed_artifact"
        warning = None
        if _typed_unsupported(resp):
            with get_session_lock(session_id):
                conn = get_session_db(session_id)
                csv_bytes, csv_filename = _read_version_csv(conn, version)
            resp = _requests.post(
                f"{_analyzer_be()}/import",
                files={"file": (csv_filename, io.BytesIO(csv_bytes), "text/csv")},
                timeout=120,
            )
            transport = "csv"
            warning = "Typed artifact unsupported by destination; CSV transfer is lossy."
        if resp.status_code != 200:
            err_text = resp.text
            try:
                err_text = resp.json().get("error", err_text)
            except Exception:
                pass
            return jsonify({"ok": False, "error": f"Analyzer upload failed: {err_text}"}), 502

        data = resp.json()
        analyzer_session_id = data.get("sessionId")
        if not analyzer_session_id:
            return jsonify({"ok": False, "error": "Analyzer did not return a session ID"}), 502
        result = {"ok": True, "analyzerSessionId": analyzer_session_id, "transport": transport}
        if warning:
            result["warning"] = warning
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"ok": False, "error": str(exc)}), 400
    except Exception as exc:
        err_str = str(exc).lower()
        if "connection" in err_str or "refused" in err_str or "httpconnectionpool" in err_str:
            return jsonify({"ok": False, "error": "Cannot reach the Data Analyzer backend. Is it running?"}), 502
        return jsonify({"ok": False, "error": str(exc)}), 500
