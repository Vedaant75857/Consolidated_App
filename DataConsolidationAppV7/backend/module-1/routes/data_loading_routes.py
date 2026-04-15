"""Data-loading routes: upload, delete-table, set-header-row, get-raw-preview."""

from __future__ import annotations

import json
import logging
import random
import time

from flask import Blueprint, Response, request, jsonify, stream_with_context

from shared.db import (
    get_session_db,
    safe_table_name,
    register_table,
    lookup_sql_name,
    drop_table,
    store_table,
    set_meta,
    get_meta,
    delete_meta,
)

from data_loading.file_loader import (
    load_zip_to_session,
    array_to_objects,
    get_raw_array_from_table,
    rebuild_table_from_raw_table,
    bulk_clean_table,
)
from data_loading.service import (
    build_inventory_from_db,
    build_files_payload_from_db,
    build_previews_from_db,
    build_single_preview,
    PREVIEW_ROWS,
)

logger = logging.getLogger(__name__)

data_loading_bp = Blueprint("data_loading", __name__)


def _rebuild_meta(conn, skip_distinct: bool = False):
    """Build inv, filesPayload, and previews."""
    inv = build_inventory_from_db(conn)
    files_payload = build_files_payload_from_db(conn, skip_distinct=True) if skip_distinct else build_files_payload_from_db(conn)
    previews = build_previews_from_db(conn)
    return inv, files_payload, previews


@data_loading_bp.route("/upload", methods=["POST"])
def upload():
    """Stream upload progress as SSE events, with the final result in a 'done' event."""
    f = request.files.get("file")
    if not f:
        return jsonify({"error": "No file uploaded."}), 400

    t0 = time.perf_counter()
    file_data = f.read()
    logger.info("Upload: read %d bytes in %.1fs", len(file_data), time.perf_counter() - t0)

    session_id = str(int(time.time() * 1000)) + hex(random.getrandbits(32))[2:]

    def _sse(payload: dict) -> str:
        return f"data: {json.dumps(payload)}\n\n"

    def _stream():
        try:
            yield _sse({"stage": "reading", "message": "Reading uploaded bytes…"})

            conn = get_session_db(session_id)
            # Mutable container so the nested callback can stash total for us
            totals = {"total": 0}

            def _on_progress(evt: dict) -> None:
                """Bridge file_loader progress events into the SSE generator.

                Because Python generators can't be ``yield``-ed from a callback,
                we stash the event; the generator polls ``pending`` after each
                blocking call.  For the initial zip_info we just save the total.
                """
                nonlocal pending
                if evt.get("stage") == "zip_info":
                    totals["total"] = evt.get("total", 0)
                pending.append(evt)

            pending: list[dict] = []

            t1 = time.perf_counter()
            _raw_arrays, warnings = load_zip_to_session(conn, file_data, on_progress=_on_progress)
            logger.info("Upload: parsed ZIP in %.1fs", time.perf_counter() - t1)

            # Flush all progress events collected during load_zip_to_session
            for evt in pending:
                stage = evt.get("stage")
                if stage == "zip_info":
                    yield _sse({
                        "stage": "parsing",
                        "total": evt.get("total", 0),
                        "message": f"ZIP contains {evt.get('total', 0)} files",
                    })
                elif stage == "file_loaded":
                    total = totals["total"]
                    current = evt.get("current", 0)
                    name = evt.get("name", "")
                    short_name = name.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]
                    elapsed = evt.get("elapsed", 0)
                    yield _sse({
                        "stage": "file",
                        "current": current,
                        "total": total,
                        "name": short_name,
                        "elapsed": elapsed,
                        "message": f"Loaded {short_name} ({current}/{total})",
                    })
                elif stage == "committed":
                    yield _sse({
                        "stage": "committed",
                        "message": f"Committed {evt.get('file_count', 0)} files to database",
                    })

            yield _sse({"stage": "inventory", "message": "Building table inventory…"})

            t2 = time.perf_counter()
            inv = build_inventory_from_db(conn)
            set_meta(conn, "inv", inv)
            logger.info("Upload: built inventory (%d tables) in %.1fs", len(inv), time.perf_counter() - t2)

            yield _sse({
                "stage": "done",
                "result": {
                    "sessionId": session_id,
                    "inventory": inv,
                    "previews": {},
                    "warnings": warnings,
                },
            })
        except Exception as exc:
            logger.exception("Upload failed")
            yield _sse({"stage": "error", "message": str(exc)})

    return Response(
        stream_with_context(_stream()),
        mimetype="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "X-Accel-Buffering": "no",
            "Connection": "close",
        },
    )


@data_loading_bp.route("/get-preview", methods=["GET", "POST"])
def get_preview():
    """Return a single table's preview (first 50 rows). Lazy-loaded by the frontend."""
    try:
        if request.method == "POST":
            body = request.get_json(force=True)
            session_id = body.get("sessionId")
            table_key = body.get("tableKey")
        else:
            session_id = request.args.get("sessionId")
            table_key = request.args.get("tableKey")

        if not session_id or not table_key:
            return jsonify({"error": "sessionId and tableKey are required."}), 400

        conn = get_session_db(session_id)
        preview = build_single_preview(conn, table_key)
        if preview is None:
            return jsonify({"error": "Table not found."}), 404
        return jsonify({"preview": preview})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@data_loading_bp.route("/delete-table", methods=["POST"])
def delete_table_route():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        table_key = body.get("tableKey")
        if not session_id or not table_key:
            return jsonify({"error": "sessionId and tableKey are required."}), 400

        conn = get_session_db(session_id)
        sql_name = lookup_sql_name(conn, table_key)
        if not sql_name:
            return jsonify({"error": "Table not found in session."}), 404

        drop_table(conn, sql_name)
        drop_table(conn, safe_table_name("raw", table_key))
        delete_meta(conn, f"rawArray__{table_key}")
        conn.execute("DELETE FROM table_registry WHERE table_key = ?", (table_key,))
        conn.commit()

        inv, files_payload, previews = _rebuild_meta(conn)
        set_meta(conn, "inv", inv)
        set_meta(conn, "filesPayload", files_payload)

        return jsonify({"inventory": inv, "previews": previews})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@data_loading_bp.route("/set-header-row", methods=["POST"])
def set_header_row():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        table_key = body.get("tableKey")
        header_row_index = body.get("headerRowIndex")
        custom_column_names = body.get("customColumnNames")

        if not session_id or not table_key or header_row_index is None:
            return jsonify({"error": "sessionId, tableKey, and headerRowIndex are required."}), 400
        try:
            header_row_index = int(header_row_index)
        except (TypeError, ValueError):
            return jsonify({"error": "headerRowIndex must be an integer."}), 400

        conn = get_session_db(session_id)

        custom_map = None
        if custom_column_names and isinstance(custom_column_names, dict):
            custom_map = {}
            for k, v in custom_column_names.items():
                custom_map[int(k)] = str(v)

        raw_arr = get_raw_array_from_table(conn, table_key, 1)
        if raw_arr:
            rebuild_table_from_raw_table(conn, table_key, header_row_index, custom_map)
        else:
            # Backward compatibility for older sessions that only stored preview meta.
            raw_arr = get_meta(conn, f"rawArray__{table_key}")
            if not raw_arr:
                return jsonify({"error": "Raw array data not found for this table. Please re-upload."}), 400
            if header_row_index < 0 or header_row_index >= len(raw_arr):
                return jsonify({"error": f"headerRowIndex {header_row_index} is out of range."}), 400
            new_df_raw = array_to_objects(raw_arr, header_row_index, custom_map)
            tbl_name = safe_table_name("tbl", table_key)
            store_table(conn, tbl_name, new_df_raw)
            bulk_clean_table(conn, tbl_name)
            register_table(conn, table_key, tbl_name)

        inv, files_payload, previews = _rebuild_meta(conn)
        set_meta(conn, "inv", inv)
        set_meta(conn, "filesPayload", files_payload)

        raw_preview = get_raw_array_from_table(conn, table_key, PREVIEW_ROWS)
        if not raw_preview:
            raw_preview = (get_meta(conn, f"rawArray__{table_key}") or [])[:PREVIEW_ROWS]

        return jsonify({
            "inventory": inv,
            "previews": previews,
            "rawPreview": raw_preview,
        })
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@data_loading_bp.route("/get-raw-preview", methods=["POST"])
def get_raw_preview():
    """Return raw rows in original file order for the header-row picker."""
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        table_key = body.get("tableKey")
        if not session_id or not table_key:
            return jsonify({"error": "sessionId and tableKey are required."}), 400

        conn = get_session_db(session_id)
        raw_arr = get_raw_array_from_table(conn, table_key, PREVIEW_ROWS)
        if not raw_arr:
            raw_arr = get_meta(conn, f"rawArray__{table_key}") or []
        if not raw_arr:
            return jsonify({"error": "Raw array data not found."}), 400

        return jsonify({"rawPreview": raw_arr[:PREVIEW_ROWS]})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
