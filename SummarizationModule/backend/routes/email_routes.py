import logging

from flask import Blueprint, jsonify, request

from shared.db import get_meta, session_exists, get_session_db
from services.email.email_generator import generate_email, build_fallback_email

logger = logging.getLogger(__name__)

email_bp = Blueprint("email", __name__)


@email_bp.route("/generate-email", methods=["POST"])
def gen_email():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        api_key = (body.get("apiKey") or "").strip()
        context = body.get("context", {})

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not api_key:
            return jsonify({"error": "apiKey required"}), 400

        conn = get_session_db(session_id)
        view_results = get_meta(conn, "view_results") or []

        if not view_results:
            return jsonify({"error": "No view results found. Run analysis first."}), 400

        try:
            result = generate_email(view_results, context, api_key)
            return jsonify({
                "email": result["email"],
                "subject": result["subject"],
            })
        except Exception as ai_exc:
            logger.error("AI email generation failed: %s", ai_exc, exc_info=True)
            fallback = build_fallback_email(view_results, context)
            return jsonify({
                "email": None,
                "error": str(ai_exc),
                "fallback": fallback,
            })

    except Exception as exc:
        logger.error("generate-email route failed: %s", exc, exc_info=True)
        return jsonify({"error": str(exc)}), 500
