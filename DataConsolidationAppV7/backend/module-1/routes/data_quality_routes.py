"""Flask blueprint for the redesigned Data Quality Assessment.

Exposes seven independent endpoints — one per analysis panel — so the
frontend can call them in parallel and render results as they arrive.

AI calls run OUTSIDE the per-session lock so they don't block other panels
from accessing the database.
"""

from __future__ import annotations

import logging
import threading

from flask import Blueprint, jsonify, request

from shared.db import get_session_db, get_session_lock, lookup_sql_name

from data_quality_assessment.service import (
    TableMissingError,
    run_dqa_country_region_sql,
    run_dqa_country_region_ai,
    run_dqa_currency_sql,
    run_dqa_currency_ai,
    run_dqa_date_sql,
    run_dqa_date_ai,
    run_dqa_fill_rate,
    run_dqa_payment_terms_sql,
    run_dqa_payment_terms_ai,
    run_dqa_spend_bifurcation,
    run_dqa_supplier_sql,
    run_dqa_supplier_ai,
)

logger = logging.getLogger(__name__)

data_quality_bp = Blueprint("data_quality_bp", __name__)


def _session_lock(session_id: str) -> "threading.RLock":
    """Per-session reentrant lock — delegates to the centralized lock in session_db."""
    return get_session_lock(session_id)


def _parse_request_fields(require_api_key: bool = True) -> dict:
    """Extract common fields from the request JSON without touching the DB.

    Returns:
        Dict with ``session_id``, ``api_key`` (may be None), ``table_name``,
        and ``table_key`` — all plain strings or None.

    Raises:
        ValueError: On missing required fields.
    """
    body = request.get_json(force=True, silent=True) or {}
    session_id = body.get("sessionId")
    api_key = body.get("apiKey")
    table_name = body.get("tableName") or None
    table_key = body.get("tableKey") or None

    if not session_id:
        raise ValueError("Missing sessionId")
    if require_api_key and not (api_key and str(api_key).strip()):
        raise ValueError("Missing API key")

    return {
        "session_id": str(session_id),
        "api_key": str(api_key).strip() if api_key else None,
        "table_name": str(table_name) if table_name else None,
        "table_key": str(table_key) if table_key else None,
    }


def _resolve_table_under_lock(
    session_id: str,
    table_name: str | None,
    table_key: str | None,
):
    """Acquire the session lock, resolve the table name, and return (conn, lock, table_name).

    The caller MUST use this inside a ``with`` block on the returned lock.
    All DB access happens under the lock.

    Raises:
        ValueError: If neither table_name nor table_key resolves.
    """
    conn = get_session_db(session_id)
    lock = _session_lock(session_id)

    if not table_name and table_key:
        with lock:
            resolved = lookup_sql_name(conn, table_key)
        if not resolved:
            raise ValueError(f"No table found for key: {table_key}")
        table_name = resolved
    elif not table_name:
        raise ValueError("Missing tableName or tableKey")

    return conn, lock, table_name


# ── Date ──────────────────────────────────────────────────────────────────

@data_quality_bp.route("/dqa/date", methods=["POST"])
def dqa_date():
    """Date format detection + spend pivot + AI insight."""
    try:
        fields = _parse_request_fields()
        body = request.get_json(force=True, silent=True) or {}
        date_column = body.get("dateColumn")

        conn, lock, table_name = _resolve_table_under_lock(
            fields["session_id"], fields["table_name"], fields["table_key"],
        )

        with lock:
            sql_data = run_dqa_date_sql(conn, table_name, date_column)

        result = run_dqa_date_ai(sql_data, fields["api_key"])
        return jsonify(result)
    except TableMissingError as exc:
        return jsonify({"error": str(exc), "code": "TABLE_MISSING"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logger.exception("DQA date failed")
        return jsonify({"error": str(exc)}), 500


# ── Currency ──────────────────────────────────────────────────────────────

@data_quality_bp.route("/dqa/currency", methods=["POST"])
def dqa_currency():
    """Currency quality table + AI insight."""
    try:
        fields = _parse_request_fields()
        conn, lock, table_name = _resolve_table_under_lock(
            fields["session_id"], fields["table_name"], fields["table_key"],
        )

        with lock:
            sql_data = run_dqa_currency_sql(conn, table_name)

        result = run_dqa_currency_ai(sql_data, fields["api_key"])
        return jsonify(result)
    except TableMissingError as exc:
        return jsonify({"error": str(exc), "code": "TABLE_MISSING"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logger.exception("DQA currency failed")
        return jsonify({"error": str(exc)}), 500


# ── Payment Terms ─────────────────────────────────────────────────────────

@data_quality_bp.route("/dqa/payment-terms", methods=["POST"])
def dqa_payment_terms():
    """Payment terms spend breakdown + AI insight."""
    try:
        fields = _parse_request_fields()
        conn, lock, table_name = _resolve_table_under_lock(
            fields["session_id"], fields["table_name"], fields["table_key"],
        )

        with lock:
            sql_data = run_dqa_payment_terms_sql(conn, table_name)

        result = run_dqa_payment_terms_ai(sql_data, fields["api_key"])
        return jsonify(result)
    except TableMissingError as exc:
        return jsonify({"error": str(exc), "code": "TABLE_MISSING"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logger.exception("DQA payment terms failed")
        return jsonify({"error": str(exc)}), 500


# ── Country / Region ──────────────────────────────────────────────────────

@data_quality_bp.route("/dqa/country-region", methods=["POST"])
def dqa_country_region():
    """Country / Region unique values + AI standardisation insight."""
    try:
        fields = _parse_request_fields()
        conn, lock, table_name = _resolve_table_under_lock(
            fields["session_id"], fields["table_name"], fields["table_key"],
        )

        with lock:
            sql_data = run_dqa_country_region_sql(conn, table_name)

        result = run_dqa_country_region_ai(sql_data, fields["api_key"])
        return jsonify(result)
    except TableMissingError as exc:
        return jsonify({"error": str(exc), "code": "TABLE_MISSING"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logger.exception("DQA country/region failed")
        return jsonify({"error": str(exc)}), 500


# ── Supplier ──────────────────────────────────────────────────────────────

@data_quality_bp.route("/dqa/supplier", methods=["POST"])
def dqa_supplier():
    """Supplier name list + AI normalisation insight."""
    try:
        fields = _parse_request_fields()
        conn, lock, table_name = _resolve_table_under_lock(
            fields["session_id"], fields["table_name"], fields["table_key"],
        )

        with lock:
            sql_data = run_dqa_supplier_sql(conn, table_name)

        result = run_dqa_supplier_ai(sql_data, fields["api_key"])
        return jsonify(result)
    except TableMissingError as exc:
        return jsonify({"error": str(exc), "code": "TABLE_MISSING"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logger.exception("DQA supplier failed")
        return jsonify({"error": str(exc)}), 500


# ── Fill Rate Summary ─────────────────────────────────────────────────────

@data_quality_bp.route("/dqa/fill-rate", methods=["POST"])
def dqa_fill_rate():
    """Per-column fill rate with spend coverage (no AI key required)."""
    try:
        fields = _parse_request_fields(require_api_key=False)
        conn, lock, table_name = _resolve_table_under_lock(
            fields["session_id"], fields["table_name"], fields["table_key"],
        )

        with lock:
            result = run_dqa_fill_rate(conn, table_name)

        return jsonify(result)
    except TableMissingError as exc:
        return jsonify({"error": str(exc), "code": "TABLE_MISSING"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logger.exception("DQA fill rate failed")
        return jsonify({"error": str(exc)}), 500


# ── Spend Bifurcation ─────────────────────────────────────────────────────

@data_quality_bp.route("/dqa/spend-bifurcation", methods=["POST"])
def dqa_spend_bifurcation():
    """Positive vs negative spend split (no AI key required)."""
    try:
        fields = _parse_request_fields(require_api_key=False)
        conn, lock, table_name = _resolve_table_under_lock(
            fields["session_id"], fields["table_name"], fields["table_key"],
        )

        with lock:
            result = run_dqa_spend_bifurcation(conn, table_name)

        return jsonify(result)
    except TableMissingError as exc:
        return jsonify({"error": str(exc), "code": "TABLE_MISSING"}), 404
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        logger.exception("DQA spend bifurcation failed")
        return jsonify({"error": str(exc)}), 500
