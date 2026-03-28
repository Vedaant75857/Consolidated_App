import io
import random
import time
import zipfile

from flask import Blueprint, jsonify, request

from shared.db import get_session_db, set_meta, get_meta, session_exists
from services.file_loader import (
    load_zip_to_session,
    load_single_file,
    detect_column_types,
    build_inventory,
    build_preview,
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
            table_names, warnings = load_zip_to_session(conn, file_data)
        else:
            table_names, warnings = load_single_file(conn, filename, file_data)

        if not table_names:
            return jsonify({"error": "No valid data files found in upload."}), 400

        columns = detect_column_types(conn, table_names)
        inventory = build_inventory(conn, table_names)
        preview = build_preview(conn, table_names)

        set_meta(conn, "table_names", table_names)
        set_meta(conn, "inventory", inventory)
        set_meta(conn, "columns", columns)
        set_meta(conn, "step", 2)

        conn.close()
        return jsonify({
            "sessionId": session_id,
            "columns": columns,
            "fileInventory": inventory,
            "preview": preview,
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
    inventory = get_meta(conn, "inventory")
    mapping = get_meta(conn, "mapping")
    cast_report = get_meta(conn, "cast_report")
    view_results = get_meta(conn, "view_results")
    conn.close()
    return jsonify({
        "step": step,
        "columns": columns,
        "fileInventory": inventory,
        "mapping": mapping,
        "castReport": cast_report,
        "viewResults": view_results,
    })
