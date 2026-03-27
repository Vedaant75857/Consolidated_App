"""Guided Merge API — recommend base, common columns, simulate, execute, validate, finalize, download."""

from __future__ import annotations

import csv
import io
import json
import time
import zipfile
from typing import Any

from openpyxl import Workbook

from flask import Blueprint, Response, jsonify, request, stream_with_context

from shared.db import (
    drop_table,
    get_meta,
    get_session_db,
    lookup_sql_name,
    read_table,
    read_table_columns,
    register_table,
    set_meta,
    table_exists,
    table_row_count,
    quote_id,
)

from merging.guided_merge_service import (
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
)

merging_bp = Blueprint("merging_bp", __name__)


@merging_bp.route("/merge/recommend-base", methods=["POST"])
def recommend_base():
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        api_key = body.get("apiKey")
        if not session_id:
            return jsonify({"error": "Missing sessionId"}), 400
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

        conn = get_session_db(session_id)
        base_sql = lookup_sql_name(conn, base_group_id)
        source_sql = lookup_sql_name(conn, source_group_id)
        if not base_sql or not source_sql:
            return jsonify({"error": "Invalid group ID(s)"}), 400

        # Column lists + instant classification (no DB, runs in <1ms)
        base_cols = read_table_columns(conn, base_sql)
        source_cols = read_table_columns(conn, source_sql)
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
            base_rows = read_table(conn, base_sql, 50)
            source_rows = read_table(conn, source_sql, 50)
            result["base_preview"] = {"columns": base_cols, "rows": base_rows, "total_rows": table_row_count(conn, base_sql)}
            result["source_preview"] = {"columns": source_cols, "rows": source_rows, "total_rows": table_row_count(conn, source_sql)}
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


@merging_bp.route("/merge/execute", methods=["POST"])
def execute():
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        base_group_id = body.get("baseGroupId")
        source_group_id = body.get("sourceGroupId")
        key_pairs = body.get("keyPairs", [])
        pull_columns = body.get("pullColumns", [])
        if not session_id or not base_group_id or not source_group_id:
            return jsonify({"error": "Missing required fields"}), 400

        conn = get_session_db(session_id)
        base_sql = lookup_sql_name(conn, base_group_id)
        source_sql = lookup_sql_name(conn, source_group_id)
        if not base_sql or not source_sql:
            return jsonify({"error": "Invalid group ID(s)"}), 400

        def _sse(payload: dict) -> str:
            return f"data: {json.dumps(payload)}\n\n"

        def _stream():
            try:
                yield _sse({"stage": "dedup", "progress": 15, "message": "Deduplicating source & executing join..."})

                merge_log = execute_merge(
                    conn, session_id, base_sql, source_sql, key_pairs, pull_columns, source_group_id
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
                "Connection": "close",
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

        conn = get_session_db(session_id)
        if not table_exists(conn, "final_merged"):
            return jsonify({"error": "No merged data found"}), 404

        group_id = f"merged_{int(time.time() * 1000)}"
        sql_name = f"merged_output_{int(time.time())}"

        conn.execute(f"CREATE TABLE {quote_id(sql_name)} AS SELECT * FROM {quote_id('final_merged')}")
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
            columns = read_table_columns(conn, target_table)
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(columns)
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate()

            cursor = conn.execute(f"SELECT * FROM {quote_id(target_table)}")
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
    columns = read_table_columns(conn, table_name)
    wb = Workbook(write_only=True)
    ws = wb.create_sheet()
    ws.append(columns)
    cursor = conn.execute(f"SELECT * FROM {quote_id(table_name)}")
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
            columns = read_table_columns(conn, step_table)
            buf = io.StringIO()
            writer = csv.writer(buf)
            writer.writerow(columns)
            yield buf.getvalue()
            buf.seek(0)
            buf.truncate()

            cursor = conn.execute(f"SELECT * FROM {quote_id(step_table)}")
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
                columns = read_table_columns(conn, tbl)
                csv_buf = io.StringIO()
                writer = csv.writer(csv_buf)
                writer.writerow(columns)
                cursor = conn.execute(f"SELECT * FROM {quote_id(tbl)}")
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
        rows = read_table(conn, sql_name, limit)
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
            rows = read_table(conn, sql_name, 50)
            total = table_row_count(conn, sql_name)
            result[gid] = {"columns": columns, "rows": rows, "total_rows": total}
        return jsonify(result)
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
