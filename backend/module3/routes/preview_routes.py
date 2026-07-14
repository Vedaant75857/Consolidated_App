from __future__ import annotations

import logging
from typing import Any

from flask import Blueprint, jsonify, request

from services.playground.service import (
    PreviewValidationError,
    get_column_values,
    get_preview_state,
    redo_operation,
    refresh_inventory,
    run_operation,
    undo_operation,
)
from shared.db import get_session_db, get_session_lock, session_exists


logger = logging.getLogger(__name__)

preview_bp = Blueprint("preview", __name__)

_FILTER_OP_ALIASES = {
    "equals": "eq",
    "not_equals": "neq",
    "starts_with": "startswith",
    "ends_with": "endswith",
    "blank": "is_null",
    "not_blank": "is_not_null",
}


@preview_bp.route("/preview/state", methods=["POST"])
def preview_state():
    body = request.get_json(force=True, silent=True) or {}
    try:
        payload = _validate_state_request(body)
    except PreviewValidationError as exc:
        return _error(str(exc), details=exc.details), 400

    session_id = payload.pop("session_id")
    with get_session_lock(session_id):
        if not session_exists(session_id):
            return _error("Invalid session", code="SESSION_NOT_FOUND"), 404
        try:
            conn = get_session_db(session_id)
            result = get_preview_state(conn, **payload)
            return jsonify(result)
        except PreviewValidationError as exc:
            return _error(str(exc), details=exc.details), 400
        except Exception as exc:
            logger.exception("preview/state failed")
            return _error(str(exc) or "Internal server error", code="INTERNAL_ERROR"), 500


@preview_bp.route("/preview/column-values", methods=["POST"])
def preview_column_values():
    body = request.get_json(force=True, silent=True) or {}
    try:
        payload = _validate_column_values_request(body)
    except PreviewValidationError as exc:
        return _error(str(exc), details=exc.details), 400

    session_id = payload.pop("session_id")
    with get_session_lock(session_id):
        if not session_exists(session_id):
            return _error("Invalid session", code="SESSION_NOT_FOUND"), 404
        try:
            conn = get_session_db(session_id)
            result = get_column_values(conn, **payload)
            return jsonify(result)
        except PreviewValidationError as exc:
            return _error(str(exc), details=exc.details), 400
        except Exception as exc:
            logger.exception("preview/column-values failed")
            return _error(str(exc) or "Internal server error", code="INTERNAL_ERROR"), 500


@preview_bp.route("/preview/operation", methods=["POST"])
def preview_operation():
    body = request.get_json(force=True, silent=True) or {}
    try:
        payload = _validate_operation_request(body)
    except PreviewValidationError as exc:
        return _error(str(exc), details=exc.details), 400

    session_id = payload.pop("session_id")
    with get_session_lock(session_id):
        if not session_exists(session_id):
            return _error("Invalid session", code="SESSION_NOT_FOUND"), 404
        try:
            conn = get_session_db(session_id)
            result = run_operation(conn, **payload)
            return jsonify(result)
        except PreviewValidationError as exc:
            return _error(str(exc), details=exc.details), 400
        except Exception as exc:
            logger.exception("preview/operation failed")
            return _error(str(exc) or "Internal server error", code="INTERNAL_ERROR"), 500


@preview_bp.route("/preview/undo", methods=["POST"])
def preview_undo():
    body = request.get_json(force=True, silent=True) or {}
    try:
        payload = _validate_table_request(body)
    except PreviewValidationError as exc:
        return _error(str(exc), details=exc.details), 400

    session_id = payload.pop("session_id")
    with get_session_lock(session_id):
        if not session_exists(session_id):
            return _error("Invalid session", code="SESSION_NOT_FOUND"), 404
        try:
            conn = get_session_db(session_id)
            result = undo_operation(conn, **payload)
            return jsonify(result)
        except PreviewValidationError as exc:
            return _error(str(exc), details=exc.details), 400
        except Exception as exc:
            logger.exception("preview/undo failed")
            return _error(str(exc) or "Internal server error", code="INTERNAL_ERROR"), 500


@preview_bp.route("/preview/redo", methods=["POST"])
def preview_redo():
    body = request.get_json(force=True, silent=True) or {}
    try:
        payload = _validate_table_request(body)
    except PreviewValidationError as exc:
        return _error(str(exc), details=exc.details), 400

    session_id = payload.pop("session_id")
    with get_session_lock(session_id):
        if not session_exists(session_id):
            return _error("Invalid session", code="SESSION_NOT_FOUND"), 404
        try:
            conn = get_session_db(session_id)
            result = redo_operation(conn, **payload)
            return jsonify(result)
        except PreviewValidationError as exc:
            return _error(str(exc), details=exc.details), 400
        except Exception as exc:
            logger.exception("preview/redo failed")
            return _error(str(exc) or "Internal server error", code="INTERNAL_ERROR"), 500


@preview_bp.route("/preview/refresh-inventory", methods=["POST"])
def preview_refresh_inventory():
    body = request.get_json(force=True, silent=True) or {}
    try:
        session_id = _required_string(body, "sessionId")
    except PreviewValidationError as exc:
        return _error(str(exc), details=exc.details), 400

    with get_session_lock(session_id):
        if not session_exists(session_id):
            return _error("Invalid session", code="SESSION_NOT_FOUND"), 404
        try:
            conn = get_session_db(session_id)
            return jsonify(refresh_inventory(conn))
        except PreviewValidationError as exc:
            return _error(str(exc), details=exc.details), 400
        except Exception as exc:
            logger.exception("preview/refresh-inventory failed")
            return _error(str(exc) or "Internal server error", code="INTERNAL_ERROR"), 500


def _validate_state_request(body: dict[str, Any]) -> dict[str, Any]:
    session_id = _required_string(body, "sessionId")
    table_key = _required_string(body, "tableKey")
    offset = _bounded_int(body.get("offset", 0), "offset", minimum=0)
    limit = _bounded_int(body.get("limit", 200), "limit", minimum=1, maximum=1000)

    search = body.get("search")
    if search is not None and not isinstance(search, str):
        raise PreviewValidationError("search must be a string.", {"field": "search"})

    filters = body.get("filters", body.get("filter", []))
    if filters is None:
        filters = []
    if isinstance(filters, dict):
        filters = [filters]
    if not isinstance(filters, list) or not all(isinstance(item, dict) for item in filters):
        raise PreviewValidationError("filters must be an array of objects.", {"field": "filters"})
    filters = [_normalize_filter(item, index) for index, item in enumerate(filters)]

    sort = body.get("sort", [])
    if sort is None:
        sort = []
    if isinstance(sort, dict):
        sort = [sort]
    if not isinstance(sort, list) or not all(isinstance(item, dict) for item in sort):
        raise PreviewValidationError("sort must be an array of objects.", {"field": "sort"})
    sort = [_normalize_sort(item, index) for index, item in enumerate(sort)]

    return {
        "session_id": session_id,
        "table_key": table_key,
        "offset": offset,
        "limit": limit,
        "search": search,
        "filters": filters,
        "sort": sort,
    }


def _validate_column_values_request(body: dict[str, Any]) -> dict[str, Any]:
    payload = _validate_state_request({
        "sessionId": body.get("sessionId"),
        "tableKey": body.get("tableKey"),
        "offset": 0,
        "limit": 1,
        "filters": body.get("filters", []),
        "sort": [],
    })
    search = body.get("search")
    if search is not None and not isinstance(search, str):
        raise PreviewValidationError("search must be a string.", {"field": "search"})
    return {
        "session_id": payload["session_id"],
        "table_key": payload["table_key"],
        "column_key": _required_string(body, "columnKey"),
        "search": search,
        "filters": payload["filters"],
        "limit": _bounded_int(body.get("limit", 500), "limit", minimum=1, maximum=500),
    }


def _validate_operation_request(body: dict[str, Any]) -> dict[str, Any]:
    session_id = _required_string(body, "sessionId")
    table_key = _required_string(body, "tableKey")
    op = _required_string(body, "op")
    params = body.get("params", {})
    if params is None:
        params = {}
    if not isinstance(params, dict):
        raise PreviewValidationError("params must be an object.", {"field": "params"})
    _reject_display_or_physical_refs(params)
    return {
        "session_id": session_id,
        "table_key": table_key,
        "op": op,
        "params": params,
    }


def _validate_table_request(body: dict[str, Any]) -> dict[str, Any]:
    return {
        "session_id": _required_string(body, "sessionId"),
        "table_key": _required_string(body, "tableKey"),
    }


def _required_string(body: dict[str, Any], field: str) -> str:
    value = body.get(field)
    if not isinstance(value, str) or not value.strip():
        raise PreviewValidationError(f"{field} is required.", {"field": field})
    return value.strip()


def _bounded_int(value: Any, field: str, *, minimum: int, maximum: int | None = None) -> int:
    if isinstance(value, bool):
        raise PreviewValidationError(f"{field} must be an integer.", {"field": field})
    if isinstance(value, float) and not value.is_integer():
        raise PreviewValidationError(f"{field} must be an integer.", {"field": field})
    if isinstance(value, str) and not value.strip().lstrip("-").isdigit():
        raise PreviewValidationError(f"{field} must be an integer.", {"field": field})
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        raise PreviewValidationError(f"{field} must be an integer.", {"field": field})
    if parsed < minimum:
        raise PreviewValidationError(f"{field} must be at least {minimum}.", {"field": field})
    if maximum is not None and parsed > maximum:
        raise PreviewValidationError(f"{field} must be at most {maximum}.", {"field": field})
    return parsed


def _normalize_filter(item: dict[str, Any], index: int) -> dict[str, Any]:
    if "columnKey" not in item:
        raise PreviewValidationError(
            "Filter columnKey is required.",
            {"index": index, "field": "columnKey"},
        )
    _reject_display_or_physical_refs(item)
    normalized = dict(item)
    op = normalized.get("op")
    if isinstance(op, str):
        normalized["op"] = _FILTER_OP_ALIASES.get(op, op)
    return normalized


def _normalize_sort(item: dict[str, Any], index: int) -> dict[str, Any]:
    if "columnKey" not in item:
        raise PreviewValidationError(
            "Sort columnKey is required.",
            {"index": index, "field": "columnKey"},
        )
    _reject_display_or_physical_refs(item)
    normalized = dict(item)
    if "dir" in normalized:
        normalized["dir"] = str(normalized["dir"]).lower()
    return normalized


def _reject_display_or_physical_refs(value: Any):
    if isinstance(value, dict):
        forbidden = {
            "column",
            "columns",
            "oldName",
            "newName",
            "display_name",
            "physicalName",
            "physical_name",
        }
        found = forbidden.intersection(value.keys())
        if found:
            raise PreviewValidationError(
                "Preview contracts accept stable columnKey DTOs only.",
                {"fields": sorted(found)},
            )
        for child in value.values():
            _reject_display_or_physical_refs(child)
    elif isinstance(value, list):
        for child in value:
            _reject_display_or_physical_refs(child)


def _error(
    message: str,
    *,
    code: str = "VALIDATION_ERROR",
    details: dict[str, Any] | None = None,
):
    return jsonify({"error": message, "code": code, "details": details or {}})
