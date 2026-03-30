import logging

from flask import Blueprint, jsonify, request

from shared.db import get_session_db, get_meta, set_meta, session_exists
from services.column_mapper import (
    STANDARD_FIELDS,
    ai_map_columns,
    build_typed_table,
)

logger = logging.getLogger(__name__)
mapping_bp = Blueprint("mapping", __name__)


@mapping_bp.route("/map-columns", methods=["POST"])
def map_columns():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        api_key = body.get("apiKey")

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not api_key:
            return jsonify({"error": "API key required"}), 400

        conn = get_session_db(session_id)
        columns = get_meta(conn, "columns")
        if not columns:
            return jsonify({"error": "No columns found. Upload data first."}), 400

        mappings = ai_map_columns(columns, api_key)
        logger.info("AI returned %d mapping(s): %s",
                     len(mappings),
                     [m.get("fieldKey") for m in mappings] if mappings else "EMPTY")
        set_meta(conn, "ai_mappings", mappings)
        set_meta(conn, "step", 3)
        conn.close()

        return jsonify({
            "mappings": mappings,
            "standardFields": STANDARD_FIELDS,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@mapping_bp.route("/confirm-mapping", methods=["POST"])
def confirm_mapping():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        mapping = body.get("mapping")  # {fieldKey: sourceColumnName | null}

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not mapping:
            return jsonify({"error": "Mapping required"}), 400

        conn = get_session_db(session_id)
        set_meta(conn, "mapping", mapping)

        cast_report = build_typed_table(conn, mapping)
        set_meta(conn, "step", 4)
        conn.close()

        return jsonify({"castReport": cast_report})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
