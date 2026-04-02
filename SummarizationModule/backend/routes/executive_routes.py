from flask import Blueprint, jsonify, request

from shared.db import get_session_db, session_exists
from services.executive_summary import compute_executive_summary

executive_bp = Blueprint("executive", __name__)


@executive_bp.route("/executive-summary", methods=["POST"])
def executive_summary():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400

        conn = get_session_db(session_id)
        rows = compute_executive_summary(conn)
        conn.close()

        return jsonify({"rows": rows})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
