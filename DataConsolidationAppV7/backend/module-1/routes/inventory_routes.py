"""Data Preview routes: clean-table, clean-group, delete-rows."""

from __future__ import annotations

from flask import Blueprint, jsonify, request

from shared.db import get_session_db
from inventory.service import (
    clean_table_sql,
    clean_group_sql,
    delete_rows_sql,
    dedup_preview_stats,
    dedup_apply_group,
    analyze_column_format,
    apply_column_standardize,
    concat_columns_apply,
    delete_concat_column,
)
from inventory.dtype_defaults import STANDARD_FIELD_DTYPES

inventory_bp = Blueprint("inventory_bp", __name__)


@inventory_bp.route("/standard-field-dtypes", methods=["GET"])
def standard_field_dtypes():
    return jsonify(STANDARD_FIELD_DTYPES)


@inventory_bp.route("/clean-table", methods=["POST"])
def clean_table():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        table_key = body.get("tableKey")
        config = body.get("config")
        if not session_id or not table_key or not config:
            return jsonify({"error": "Missing required fields."}), 400

        conn = get_session_db(session_id)
        result = clean_table_sql(conn, table_key, config)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc) or "An error occurred during cleaning."}), 500


@inventory_bp.route("/clean-group", methods=["POST"])
def clean_group():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        group_id = body.get("groupId")
        config = body.get("config")
        if not session_id or not group_id or not config:
            return jsonify({"error": "Missing required fields."}), 400

        conn = get_session_db(session_id)
        result = clean_group_sql(conn, group_id, config)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc) or "An error occurred during cleaning."}), 500


@inventory_bp.route("/delete-rows", methods=["POST"])
def delete_rows():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        table_key = body.get("tableKey")
        row_ids = body.get("rowIds")
        if not session_id or not table_key or not row_ids:
            return jsonify({"error": "Missing required fields."}), 400

        conn = get_session_db(session_id)
        result = delete_rows_sql(conn, table_key, row_ids)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc) or "An error occurred."}), 500


@inventory_bp.route("/dedup-preview", methods=["POST"])
def dedup_preview():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        group_id = body.get("groupId")
        dedup_columns = body.get("deduplicateColumns") or []
        if not session_id or not group_id or not dedup_columns:
            return jsonify({"error": "Missing sessionId, groupId, or deduplicateColumns."}), 400

        conn = get_session_db(session_id)
        result = dedup_preview_stats(conn, group_id, dedup_columns)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@inventory_bp.route("/dedup-apply", methods=["POST"])
def dedup_apply():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        group_id = body.get("groupId")
        dedup_columns = body.get("deduplicateColumns") or []
        if not session_id or not group_id or not dedup_columns:
            return jsonify({"error": "Missing sessionId, groupId, or deduplicateColumns."}), 400

        conn = get_session_db(session_id)
        result = dedup_apply_group(conn, group_id, dedup_columns)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@inventory_bp.route("/analyze-column-format", methods=["POST"])
def analyze_column_format_route():
    """Auto-detect leading-zero patterns and numeric format for selected columns."""
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        group_id = body.get("groupId")
        columns = body.get("columns") or []
        if not session_id or not group_id or not columns:
            return jsonify({"error": "Missing sessionId, groupId, or columns."}), 400

        conn = get_session_db(session_id)
        results = analyze_column_format(conn, group_id, columns)
        return jsonify({"results": results})
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@inventory_bp.route("/apply-column-standardize", methods=["POST"])
def apply_column_standardize_route():
    """Apply strip/pad zero operations to selected columns."""
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        group_id = body.get("groupId")
        actions = body.get("actions") or []
        if not session_id or not group_id or not actions:
            return jsonify({"error": "Missing sessionId, groupId, or actions."}), 400

        conn = get_session_db(session_id)
        result = apply_column_standardize(conn, group_id, actions)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@inventory_bp.route("/concat-columns-apply", methods=["POST"])
def concat_columns_apply_route():
    """Concatenate selected columns into a new derived column."""
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        group_id = body.get("groupId")
        columns = body.get("columns") or []
        if not session_id or not group_id or len(columns) < 2:
            return jsonify({"error": "Missing sessionId, groupId, or need at least 2 columns."}), 400

        conn = get_session_db(session_id)
        result = concat_columns_apply(conn, group_id, columns)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@inventory_bp.route("/delete-concat-column", methods=["POST"])
def delete_concat_column_route():
    """Delete a previously created concatenation column."""
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        group_id = body.get("groupId")
        column_name = body.get("columnName")
        if not session_id or not group_id or not column_name:
            return jsonify({"error": "Missing sessionId, groupId, or columnName."}), 400

        conn = get_session_db(session_id)
        result = delete_concat_column(conn, group_id, column_name)
        return jsonify(result)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
