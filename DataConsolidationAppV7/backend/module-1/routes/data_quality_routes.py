"""Flask blueprint for the Data Quality Assessment endpoint."""

from __future__ import annotations

import threading

from flask import Blueprint, jsonify, request

from shared.db import get_session_db

from data_quality_assessment.service import run_data_quality_assessment

data_quality_bp = Blueprint("data_quality_bp", __name__)

_SESSION_LOCK_GUARD = threading.Lock()
_SESSION_LOCKS: dict[str, threading.RLock] = {}


def _session_lock(session_id: str) -> threading.RLock:
    """Per-session reentrant lock to serialise DQA requests."""
    with _SESSION_LOCK_GUARD:
        lock = _SESSION_LOCKS.get(session_id)
        if lock is None:
            lock = threading.RLock()
            _SESSION_LOCKS[session_id] = lock
        return lock


@data_quality_bp.route("/data-quality-assessment", methods=["POST"])
def data_quality_assessment():
    """Run the Data Quality Assessment on a specified merge output table.

    Request JSON:
        sessionId  – active session identifier (required)
        apiKey     – Portkey / OpenAI key for AI insights (required)
        tableName  – SQLite table to analyse, e.g. ``final_merged_v1`` (required)

    Returns:
        JSON with ``totalRows`` and ``parameters`` list (see service.py).
    """
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        api_key = body.get("apiKey")
        table_name = body.get("tableName")

        if not session_id:
            return jsonify({"error": "Missing sessionId"}), 400
        if not (api_key and str(api_key).strip()):
            return jsonify({"error": "Missing API key"}), 400
        if not table_name:
            return jsonify({"error": "Missing tableName"}), 400

        conn = get_session_db(str(session_id))
        lock = _session_lock(str(session_id))

        with lock:
            result = run_data_quality_assessment(
                conn, str(table_name), str(api_key).strip()
            )

        return jsonify(result)

    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:  # noqa: BLE001
        return jsonify({"error": str(exc)}), 500
