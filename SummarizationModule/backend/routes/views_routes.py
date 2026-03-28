from flask import Blueprint, jsonify, request

from shared.db import get_session_db, get_meta, set_meta, session_exists
from services.view_engine import get_available_views, compute_views
from services.ai_summary import generate_summaries

views_bp = Blueprint("views", __name__)


@views_bp.route("/available-views", methods=["POST"])
def available_views():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400

        conn = get_session_db(session_id)
        mapping = get_meta(conn, "mapping")
        conn.close()

        if not mapping:
            return jsonify({"error": "No mapping confirmed yet."}), 400

        views = get_available_views(mapping)
        return jsonify({"views": views})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@views_bp.route("/compute-views", methods=["POST"])
def compute():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        selected_views = body.get("selectedViews", [])
        config = body.get("config", {})
        api_key = body.get("apiKey")

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not selected_views:
            return jsonify({"error": "No views selected"}), 400

        conn = get_session_db(session_id)
        mapping = get_meta(conn, "mapping")
        if not mapping:
            return jsonify({"error": "No mapping confirmed yet."}), 400

        results = compute_views(conn, selected_views, config, mapping)

        if api_key:
            results = generate_summaries(results, api_key)

        set_meta(conn, "view_results", results)
        set_meta(conn, "step", 4)
        conn.close()

        return jsonify({"views": results})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
