import io
import random
import time
import zipfile

from flask import Blueprint, jsonify, request

from shared.db import get_session_db, set_meta, get_meta, session_exists
from services.column_mapper import STANDARD_FIELDS
from services.file_loader import (
    load_zip_to_session,
    load_single_file,
    collect_column_info,
    build_inventory,
    build_preview,
    delete_table_from_session,
    get_raw_preview,
    set_header_row_for_table,
    delete_rows_from_table,
)

upload_bp = Blueprint("upload", __name__)


@upload_bp.route("/upload", methods=["POST"])
def upload():
    try:
        f = request.files.get("file")
        if not f:
            return jsonify({"error": "No file uploaded."}), 400

        filename = f.filename or "upload.zip"
        file_data = f.read()
        session_id = str(int(time.time() * 1000)) + hex(random.getrandbits(32))[2:]
        conn = get_session_db(session_id)

        ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else ""
        if ext == "zip" or zipfile.is_zipfile(io.BytesIO(file_data)):
            table_keys, warnings = load_zip_to_session(conn, file_data)
        else:
            table_keys, warnings = load_single_file(conn, filename, file_data)

        if not table_keys:
            return jsonify({
                "error": "No valid data files found in upload.",
                "warnings": warnings,
            }), 400

        columns = collect_column_info(conn, table_keys)
        inventory = build_inventory(conn)
        previews = build_preview(conn)

        set_meta(conn, "table_keys", table_keys)
        set_meta(conn, "inventory", inventory)
        set_meta(conn, "columns", columns)
        set_meta(conn, "step", 2)

        conn.close()
        return jsonify({
            "sessionId": session_id,
            "columns": columns,
            "fileInventory": inventory,
            "previews": previews,
            "warnings": warnings,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@upload_bp.route("/session/<session_id>/state", methods=["GET"])
def get_session_state(session_id: str):
    if not session_exists(session_id):
        return jsonify({"error": "Session not found"}), 404
    conn = get_session_db(session_id)
    step = get_meta(conn, "step") or 1
    columns = get_meta(conn, "columns")
    inventory_meta = get_meta(conn, "inventory")
    mapping = get_meta(conn, "mapping")
    cast_report = get_meta(conn, "cast_report")
    view_results = get_meta(conn, "view_results")
    ai_mappings = get_meta(conn, "ai_mappings")
    if isinstance(ai_mappings, list):
        for m in ai_mappings:
            bm = m.get("bestMatch")
            if isinstance(bm, dict):
                m["bestMatch"] = bm.get("column") or bm.get("name") or None
            alts = m.get("alternatives")
            if isinstance(alts, list):
                m["alternatives"] = [
                    (a.get("column") or a.get("name") or "")
                    if isinstance(a, dict) else str(a)
                    for a in alts
                ]

    previews = None
    if inventory_meta:
        try:
            previews = build_preview(conn)
        except Exception:
            pass

    conn.close()
    return jsonify({
        "step": step,
        "columns": columns,
        "fileInventory": inventory_meta,
        "previews": previews,
        "mapping": mapping,
        "castReport": cast_report,
        "viewResults": view_results,
        "aiMappings": ai_mappings,
        "standardFields": STANDARD_FIELDS if ai_mappings else None,
    })


@upload_bp.route("/delete-table", methods=["POST"])
def delete_table():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        table_key = body.get("tableKey")

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not table_key:
            return jsonify({"error": "tableKey required"}), 400

        conn = get_session_db(session_id)
        delete_table_from_session(conn, table_key)

        inventory = build_inventory(conn)
        previews = build_preview(conn)
        set_meta(conn, "inventory", inventory)

        table_keys = [inv["table_key"] for inv in inventory]
        columns = collect_column_info(conn, table_keys)
        set_meta(conn, "columns", columns)

        conn.close()
        return jsonify({"inventory": inventory, "previews": previews})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@upload_bp.route("/get-raw-preview", methods=["POST"])
def raw_preview():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        table_key = body.get("tableKey")

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not table_key:
            return jsonify({"error": "tableKey required"}), 400

        conn = get_session_db(session_id)
        preview = get_raw_preview(conn, table_key)
        conn.close()
        return jsonify({"rawPreview": preview})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@upload_bp.route("/set-header-row", methods=["POST"])
def set_header_row():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        table_key = body.get("tableKey")
        header_row_index = body.get("headerRowIndex")
        custom_column_names = body.get("customColumnNames")

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not table_key:
            return jsonify({"error": "tableKey required"}), 400
        if header_row_index is None:
            return jsonify({"error": "headerRowIndex required"}), 400

        custom_names = None
        if custom_column_names and isinstance(custom_column_names, dict):
            custom_names = {int(k): v for k, v in custom_column_names.items()}

        conn = get_session_db(session_id)
        set_header_row_for_table(conn, table_key, header_row_index, custom_names)

        inventory = build_inventory(conn)
        previews = build_preview(conn)
        set_meta(conn, "inventory", inventory)

        table_keys = [inv["table_key"] for inv in inventory]
        columns = collect_column_info(conn, table_keys)
        set_meta(conn, "columns", columns)

        conn.close()
        return jsonify({
            "inventory": inventory,
            "previews": previews,
            "columns": columns,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@upload_bp.route("/delete-rows", methods=["POST"])
def delete_rows():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        table_key = body.get("tableKey")
        row_ids = body.get("rowIds", [])

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not table_key:
            return jsonify({"error": "tableKey required"}), 400
        if not row_ids:
            return jsonify({"error": "rowIds required"}), 400

        conn = get_session_db(session_id)
        deleted_count = delete_rows_from_table(conn, table_key, row_ids)

        inventory = build_inventory(conn)
        previews = build_preview(conn)
        set_meta(conn, "inventory", inventory)

        inventory_row = next((inv for inv in inventory if inv["table_key"] == table_key), None)
        preview = previews.get(table_key)

        conn.close()
        return jsonify({
            "deletedCount": deleted_count,
            "preview": preview,
            "inventoryRow": inventory_row,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
