"""Flask blueprint for the redesigned Data Quality Assessment.

Exposes five independent endpoints — one per analysis panel — so the frontend
can call them in parallel and render results as they arrive.
"""

from __future__ import annotations

import threading

from flask import Blueprint, jsonify, request

from shared.db import get_session_db, lookup_sql_name

from data_quality_assessment.service import (
    run_dqa_country_region,
    run_dqa_currency,
    run_dqa_date,
    run_dqa_payment_terms,
    run_dqa_supplier,
)

data_quality_bp = Blueprint("data_quality_bp", __name__)

_SESSION_LOCK_GUARD = threading.Lock()
_SESSION_LOCKS: dict[str, threading.RLock] = {}


def _session_lock(session_id: str) -> threading.RLock:
    """Per-session reentrant lock to serialise requests."""
    with _SESSION_LOCK_GUARD:
        lock = _SESSION_LOCKS.get(session_id)
        if lock is None:
            lock = threading.RLock()
            _SESSION_LOCKS[session_id] = lock
        return lock


def _parse_common_body() -> tuple[str, str, str]:
    """Extract and validate ``sessionId``, ``apiKey``, and resolved
    ``tableName`` from the request JSON.

    Returns:
        (session_id, api_key, table_name)

    Raises:
        ValueError: On missing / invalid fields.
    """
    body = request.get_json(force=True, silent=True) or {}
    session_id = body.get("sessionId")
    api_key = body.get("apiKey")
    table_name = body.get("tableName")
    table_key = body.get("tableKey")

    if not session_id:
        raise ValueError("Missing sessionId")
    if not (api_key and str(api_key).strip()):
        raise ValueError("Missing API key")

    conn = get_session_db(str(session_id))

    if not table_name and table_key:
        table_name = lookup_sql_name(conn, str(table_key))
        if not table_name:
            raise ValueError(f"No table found for key: {table_key}")

    if not table_name:
        raise ValueError("Missing tableName or tableKey")

    return str(session_id), str(api_key).strip(), str(table_name)


# ── Date ──────────────────────────────────────────────────────────────────

@data_quality_bp.route("/dqa/date", methods=["POST"])
def dqa_date():
    """Date format detection + spend pivot + AI insight.

    Extra request field:
        ``dateColumn`` – optional; auto-selects if omitted.
    """
    try:
        session_id, api_key, table_name = _parse_common_body()
        body = request.get_json(force=True, silent=True) or {}
        date_column = body.get("dateColumn")

        conn = get_session_db(session_id)
        with _session_lock(session_id):
            result = run_dqa_date(conn, table_name, api_key, date_column)

        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ── Currency ──────────────────────────────────────────────────────────────

@data_quality_bp.route("/dqa/currency", methods=["POST"])
def dqa_currency():
    """Currency quality table + AI insight."""
    try:
        session_id, api_key, table_name = _parse_common_body()

        conn = get_session_db(session_id)
        with _session_lock(session_id):
            result = run_dqa_currency(conn, table_name, api_key)

        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ── Payment Terms ─────────────────────────────────────────────────────────

@data_quality_bp.route("/dqa/payment-terms", methods=["POST"])
def dqa_payment_terms():
    """Payment terms spend breakdown + AI insight."""
    try:
        session_id, api_key, table_name = _parse_common_body()

        conn = get_session_db(session_id)
        with _session_lock(session_id):
            result = run_dqa_payment_terms(conn, table_name, api_key)

        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ── Country / Region ──────────────────────────────────────────────────────

@data_quality_bp.route("/dqa/country-region", methods=["POST"])
def dqa_country_region():
    """Country / Region unique values + AI standardisation insight."""
    try:
        session_id, api_key, table_name = _parse_common_body()

        conn = get_session_db(session_id)
        with _session_lock(session_id):
            result = run_dqa_country_region(conn, table_name, api_key)

        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ── Supplier ──────────────────────────────────────────────────────────────

@data_quality_bp.route("/dqa/supplier", methods=["POST"])
def dqa_supplier():
    """Supplier name list + AI normalisation insight."""
    try:
        session_id, api_key, table_name = _parse_common_body()

        conn = get_session_db(session_id)
        with _session_lock(session_id):
            result = run_dqa_supplier(conn, table_name, api_key)

        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
