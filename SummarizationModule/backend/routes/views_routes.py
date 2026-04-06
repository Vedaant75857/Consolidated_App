import logging
import threading

from flask import Blueprint, jsonify, request

from shared.db import get_session_db, get_meta, set_meta, session_exists
from services.view_engine import get_available_views, compute_views
from services.ai_summary import generate_summary_for_view
from services.data_quality import run_executive_summary

logger = logging.getLogger(__name__)

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

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not selected_views:
            return jsonify({"error": "No views selected"}), 400

        conn = get_session_db(session_id)
        mapping = get_meta(conn, "mapping")
        if not mapping:
            return jsonify({"error": "No mapping confirmed yet."}), 400

        logger.info("Computing %d view(s) for session %s", len(selected_views), session_id)
        results = compute_views(conn, selected_views, config, mapping)

        set_meta(conn, "view_results", results)
        set_meta(conn, "step", 6)
        conn.close()

        return jsonify({"views": results})
    except Exception as exc:
        logger.error("compute-views failed: %s", exc, exc_info=True)
        return jsonify({"error": str(exc)}), 500


@views_bp.route("/recompute-view", methods=["POST"])
def recompute_view():
    """Recompute a single view with updated config (e.g. slider change)."""
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        view_id = body.get("viewId")
        config = body.get("config", {})

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not view_id:
            return jsonify({"error": "viewId required"}), 400

        conn = get_session_db(session_id)
        mapping = get_meta(conn, "mapping")
        if not mapping:
            return jsonify({"error": "No mapping confirmed yet."}), 400

        results = compute_views(conn, [view_id], config, mapping)
        if not results:
            conn.close()
            return jsonify({"error": f"View {view_id} not found or could not be computed"}), 404

        view_results = get_meta(conn, "view_results") or []
        new_view = results[0]
        view_results = [new_view if v.get("viewId") == view_id else v for v in view_results]
        if not any(v.get("viewId") == view_id for v in view_results):
            view_results.append(new_view)
        set_meta(conn, "view_results", view_results)
        conn.close()

        return jsonify({"view": new_view})
    except Exception as exc:
        logger.error("recompute-view failed: %s", exc, exc_info=True)
        return jsonify({"error": str(exc)}), 500


@views_bp.route("/generate-summary", methods=["POST"])
def generate_summary():
    """Generate AI summary for a single view (called async per-view)."""
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        view_id = body.get("viewId")
        api_key = body.get("apiKey")

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not view_id:
            return jsonify({"error": "viewId required"}), 400
        if not api_key or not api_key.strip():
            return jsonify({"error": "apiKey required"}), 400

        conn = get_session_db(session_id)
        view_results = get_meta(conn, "view_results") or []
        view = next((v for v in view_results if v.get("viewId") == view_id), None)
        if not view:
            conn.close()
            return jsonify({"error": f"View {view_id} not found"}), 404

        summary = generate_summary_for_view(view, api_key.strip())

        for v in view_results:
            if v.get("viewId") == view_id:
                v["aiSummary"] = summary
                break
        set_meta(conn, "view_results", view_results)
        conn.close()

        return jsonify({"viewId": view_id, "summary": summary})
    except Exception as exc:
        logger.error("generate-summary failed: %s", exc, exc_info=True)
        return jsonify({"error": str(exc)}), 500


# ── Executive Summary (DQA) ──────────────────────────────────────────────

_ES_LOCK_GUARD = threading.Lock()
_ES_LOCKS: dict[str, threading.RLock] = {}


def _es_lock(session_id: str) -> threading.RLock:
    with _ES_LOCK_GUARD:
        lock = _ES_LOCKS.get(session_id)
        if lock is None:
            lock = threading.RLock()
            _ES_LOCKS[session_id] = lock
        return lock


@views_bp.route("/executive-summary", methods=["POST"])
def executive_summary():
    """Run the Executive Summary DQA on the analysis_data table.

    Request JSON:
        sessionId – active session identifier (required)
        apiKey    – Portkey / OpenAI key for AI insights (required)
        force     – if true, bypass cache and recompute (optional)
    """
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        api_key = body.get("apiKey")
        force = body.get("force", False)

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not api_key or not str(api_key).strip():
            return jsonify({"error": "Missing API key"}), 400

        conn = get_session_db(str(session_id))
        lock = _es_lock(str(session_id))

        with lock:
            # Return cached result unless force-rerun requested
            if not force:
                cached = get_meta(conn, "executive_summary")
                if cached:
                    conn.close()
                    return jsonify(cached)

            result = run_executive_summary(conn, str(api_key).strip())
            set_meta(conn, "executive_summary", result)
            conn.close()

        return jsonify(result)

    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logger.error("executive-summary failed: %s", exc, exc_info=True)
        return jsonify({"error": str(exc)}), 500
