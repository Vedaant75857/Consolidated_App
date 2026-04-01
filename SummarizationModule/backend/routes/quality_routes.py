"""Routes for the quality-analysis feature (dedicated blueprint)."""

import logging

from flask import Blueprint, jsonify, request

from shared.db import get_session_db, get_meta, session_exists
from services.quality_analysis import (
    compute_quality_metrics,
    generate_quality_analysis_summary,
)

logger = logging.getLogger(__name__)
quality_bp = Blueprint("quality", __name__)


@quality_bp.route("/quality-analysis", methods=["POST"])
def quality_analysis():
    """Compute description + supplier quality metrics (no AI call)."""
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400

        conn = get_session_db(session_id)
        mapping = get_meta(conn, "mapping")
        if not mapping:
            conn.close()
            return jsonify({"error": "No mapping found. Complete column mapping first."}), 400

        metrics = compute_quality_metrics(conn, mapping)
        conn.close()

        return jsonify({"metrics": metrics})
    except Exception as exc:
        logger.exception("quality-analysis failed")
        return jsonify({"error": str(exc)}), 500


@quality_bp.route("/quality-analysis-summary", methods=["POST"])
def quality_analysis_summary():
    """Generate an AI narrative summary from pre-computed metrics."""
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        metrics = body.get("metrics")
        api_key = body.get("apiKey")

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not metrics:
            return jsonify({"error": "metrics payload required"}), 400
        if not api_key:
            return jsonify({"error": "API key required"}), 400

        summary = generate_quality_analysis_summary(metrics, api_key)
        return jsonify({"summary": summary})
    except Exception as exc:
        logger.exception("quality-analysis-summary failed")
        return jsonify({"error": str(exc)}), 500
