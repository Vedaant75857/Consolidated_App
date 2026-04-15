"""Flask blueprint for the redesigned Data Quality Assessment.

Exposes five independent endpoints — one per analysis panel — so the frontend
can call them in parallel and render results as they arrive.
"""

from __future__ import annotations

import threading

from flask import Blueprint, jsonify, request

from shared.db import get_session_db, get_session_lock, lookup_sql_name

from data_quality_assessment.service import (
    TableMissingError,
    run_dqa_country_region,
    run_dqa_currency,
    run_dqa_date,
    run_dqa_fill_rate,
    run_dqa_payment_terms,
    run_dqa_spend_bifurcation,
    run_dqa_supplier,
)

data_quality_bp = Blueprint("data_quality_bp", __name__)

def _session_lock(session_id: str) -> "threading.RLock":
    """Per-session reentrant lock — delegates to the centralized lock in session_db."""
    return get_session_lock(session_id)


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
    except TableMissingError as exc:
        return jsonify({"error": str(exc), "code": "TABLE_MISSING"}), 404
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
    except TableMissingError as exc:
        return jsonify({"error": str(exc), "code": "TABLE_MISSING"}), 404
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
    except TableMissingError as exc:
        return jsonify({"error": str(exc), "code": "TABLE_MISSING"}), 404
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
    except TableMissingError as exc:
        return jsonify({"error": str(exc), "code": "TABLE_MISSING"}), 404
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
    except TableMissingError as exc:
        return jsonify({"error": str(exc), "code": "TABLE_MISSING"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ── Fill Rate Summary ─────────────────────────────────────────────────────

@data_quality_bp.route("/dqa/fill-rate", methods=["POST"])
def dqa_fill_rate():
    """Per-column fill rate with spend coverage (no AI key required)."""
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        table_name = body.get("tableName")
        table_key = body.get("tableKey")

        if not session_id:
            raise ValueError("Missing sessionId")

        conn = get_session_db(str(session_id))

        if not table_name and table_key:
            table_name = lookup_sql_name(conn, str(table_key))
            if not table_name:
                raise ValueError(f"No table found for key: {table_key}")
        if not table_name:
            raise ValueError("Missing tableName or tableKey")

        with _session_lock(str(session_id)):
            result = run_dqa_fill_rate(conn, str(table_name))

        return jsonify(result)
    except TableMissingError as exc:
        return jsonify({"error": str(exc), "code": "TABLE_MISSING"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


# ── Spend Bifurcation ─────────────────────────────────────────────────────

@data_quality_bp.route("/dqa/spend-bifurcation", methods=["POST"])
def dqa_spend_bifurcation():
    """Positive vs negative spend split (no AI key required)."""
    try:
        body = request.get_json(force=True, silent=True) or {}
        session_id = body.get("sessionId")
        table_name = body.get("tableName")
        table_key = body.get("tableKey")

        if not session_id:
            raise ValueError("Missing sessionId")

        conn = get_session_db(str(session_id))

        if not table_name and table_key:
            table_name = lookup_sql_name(conn, str(table_key))
            if not table_name:
                raise ValueError(f"No table found for key: {table_key}")
        if not table_name:
            raise ValueError("Missing tableName or tableKey")

        with _session_lock(str(session_id)):
            result = run_dqa_spend_bifurcation(conn, str(table_name))

        return jsonify(result)
    except TableMissingError as exc:
        return jsonify({"error": str(exc), "code": "TABLE_MISSING"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
