"""Preview routes for Excel-like grid operations."""

from __future__ import annotations

import logging

from flask import Blueprint, request, jsonify

from shared.db import get_session_db, get_session_lock
from shared.db.meta_ops import get_meta
from shared.utils import json_safe
from preview_operations import service as preview_ops
from data_loading.service import (
    build_files_payload_from_db,
    build_inventory_from_db,
    build_previews_from_db,
)

logger = logging.getLogger(__name__)

bp = Blueprint("preview", __name__)


def _json_ok(payload: dict, status: int = 200):
    """Return a JSON-safe Flask response."""
    return jsonify(json_safe(payload)), status


def _handle_preview_error(exc: Exception, route: str):
    """Map preview exceptions to HTTP status codes with logging."""
    if isinstance(exc, ValueError):
        return _json_ok({"error": str(exc), "code": "VALIDATION_ERROR"}, 400)
    logger.exception("%s failed", route)
    return _json_ok({"error": str(exc) or "Internal server error"}, 500)


@bp.route("/preview/state", methods=["POST"])
def preview_state():
    """Get preview state with pagination, filtering, sorting."""
    data = request.get_json() or {}
    session_id = data.get("sessionId")
    table_key = data.get("tableKey")
    offset = data.get("offset", 0)
    limit = data.get("limit", 200)
    search = data.get("search")
    filters = data.get("filters")
    sort = data.get("sort")

    if not session_id:
        return _json_ok({"error": "Missing sessionId"}, 400)
    if not table_key:
        return _json_ok({"error": "Missing tableKey"}, 400)

    try:
        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            result = preview_ops.get_preview_data(
                conn,
                table_key,
                offset=offset,
                limit=limit,
                search=search,
                filters=filters,
                sort=sort,
            )
        return _json_ok(result)
    except Exception as exc:
        return _handle_preview_error(exc, "preview/state")


@bp.route("/preview/column-values", methods=["POST"])
def preview_column_values():
    """Get distinct values for a column filter dropdown."""
    data = request.get_json() or {}
    session_id = data.get("sessionId")
    table_key = data.get("tableKey")
    column = data.get("column")
    search = data.get("search")
    filters = data.get("filters")
    limit = data.get("limit", 500)

    if not session_id:
        return _json_ok({"error": "Missing sessionId"}, 400)
    if not table_key:
        return _json_ok({"error": "Missing tableKey"}, 400)
    if not column:
        return _json_ok({"error": "Missing column"}, 400)

    try:
        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            result = preview_ops.get_column_filter_values(
                conn,
                table_key,
                column,
                search=search,
                filters=filters,
                limit=limit,
            )
        return _json_ok(result)
    except Exception as exc:
        return _handle_preview_error(exc, "preview/column-values")


@bp.route("/preview/operation", methods=["POST"])
def preview_operation():
    """Run a preview operation (cell_edit, column_rename, etc.)."""
    data = request.get_json() or {}
    session_id = data.get("sessionId")
    table_key = data.get("tableKey")
    op = data.get("op")
    params = data.get("params", {})

    if not session_id:
        return _json_ok({"error": "Missing sessionId"}, 400)
    if not table_key:
        return _json_ok({"error": "Missing tableKey"}, 400)
    if not op:
        return _json_ok({"error": "Missing operation"}, 400)

    try:
        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            result = preview_ops.run_operation(conn, table_key, op, params)
        return _json_ok({"ok": True, **result})
    except Exception as exc:
        return _handle_preview_error(exc, "preview/operation")


@bp.route("/preview/undo", methods=["POST"])
def preview_undo():
    """Undo last operation."""
    data = request.get_json() or {}
    session_id = data.get("sessionId")
    table_key = data.get("tableKey")

    if not session_id or not table_key:
        return _json_ok({"error": "Missing sessionId or tableKey"}, 400)

    try:
        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            result = preview_ops.undo_operation(conn, table_key)
        return _json_ok({"ok": True, **result})
    except Exception as exc:
        return _handle_preview_error(exc, "preview/undo")


@bp.route("/preview/redo", methods=["POST"])
def preview_redo():
    """Redo last undone operation."""
    data = request.get_json() or {}
    session_id = data.get("sessionId")
    table_key = data.get("tableKey")

    if not session_id or not table_key:
        return _json_ok({"error": "Missing sessionId or tableKey"}, 400)

    try:
        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            result = preview_ops.redo_operation(conn, table_key)
        return _json_ok({"ok": True, **result})
    except Exception as exc:
        return _handle_preview_error(exc, "preview/redo")


@bp.route("/preview/apply", methods=["POST"])
def preview_apply():
    """Apply preview changes back to pipeline."""
    data = request.get_json() or {}
    session_id = data.get("sessionId")
    table_key = data.get("tableKey")
    target = data.get("target", {})

    if not session_id or not table_key:
        return _json_ok({"error": "Missing sessionId or tableKey"}, 400)

    target_kind = target.get("kind")
    target_id = target.get("id")
    mode = target.get("mode", "replace")

    if not target_kind or not target_id:
        return _json_ok({"error": "Missing target kind or id"}, 400)

    try:
        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            result = preview_ops.apply_to_pipeline(
                conn, table_key, target_kind, target_id, mode
            )

            state_patch: dict = {}
            if target_kind == "raw":
                state_patch["inventory"] = build_inventory_from_db(conn)
                state_patch["filesPayload"] = build_files_payload_from_db(conn)
                state_patch["previews"] = build_previews_from_db(conn)
            elif target_kind == "group":
                state_patch["groupSchema"] = get_meta(conn, "groupSchemaTableRows") or []
                state_patch["previews"] = build_previews_from_db(conn)

            result["statePatch"] = state_patch
        return _json_ok(result)
    except Exception as exc:
        return _handle_preview_error(exc, "preview/apply")


@bp.route("/preview/refresh-inventory", methods=["POST"])
def preview_refresh_inventory():
    """Refresh inventory and previews after operations like pivot that create new tables."""
    data = request.get_json() or {}
    session_id = data.get("sessionId")

    if not session_id:
        return _json_ok({"error": "Missing sessionId"}, 400)

    try:
        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            payload = {
                "ok": True,
                "inventory": build_inventory_from_db(conn),
                "previews": build_previews_from_db(conn),
            }
        return _json_ok(payload)
    except Exception as exc:
        return _handle_preview_error(exc, "preview/refresh-inventory")
