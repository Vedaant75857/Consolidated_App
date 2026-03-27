"""Append routes: plan, save groups, mapping, execute."""

from __future__ import annotations

from flask import Blueprint, jsonify, request

from shared.db import get_session_db
from appending.service import save_append_groups
from routes.summary_routes import execute_operation_kernel

appending_bp = Blueprint("appending_bp", __name__)


@appending_bp.route("/append-plan", methods=["POST"])
def append_plan_route():
    try:
        body = request.get_json(force=True)
        payload, status = execute_operation_kernel(
            session_id=body.get("sessionId"),
            operation="append_plan",
            api_key=body.get("apiKey"),
            input_data={
                "filesPayload": body.get("filesPayload"),
                "tableKeys": body.get("tableKeys"),
            },
            options={"mode": "pipeline", "autoPrepare": True, "persist": True},
            request_id=request.headers.get("X-Request-Id"),
        )
        if status != 200:
            return jsonify({"error": payload.get("error"), "missing_requirements": payload.get("missing_requirements")}), status
        return jsonify(payload.get("result") or {})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@appending_bp.route("/save-append-groups", methods=["POST"])
def save_append_groups_route():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        if not session_id:
            return jsonify({"error": "sessionId is required."}), 400
        conn = get_session_db(session_id)
        save_append_groups(conn, body.get("appendGroups"), body.get("unassigned"))
        return jsonify({"ok": True})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@appending_bp.route("/append-mapping", methods=["POST"])
def append_mapping_route():
    try:
        body = request.get_json(force=True)
        payload, status = execute_operation_kernel(
            session_id=body.get("sessionId"),
            operation="append_mapping",
            api_key=body.get("apiKey"),
            input_data={"appendGroups": body.get("appendGroups")},
            options={"mode": "pipeline", "autoPrepare": True, "persist": True},
            request_id=request.headers.get("X-Request-Id"),
        )
        if status != 200:
            return jsonify({"error": payload.get("error"), "missing_requirements": payload.get("missing_requirements")}), status
        return jsonify(payload.get("result") or {})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@appending_bp.route("/append-execute", methods=["POST"])
def append_execute_route():
    try:
        body = request.get_json(force=True)
        payload, status = execute_operation_kernel(
            session_id=body.get("sessionId"),
            operation="append_execute",
            api_key=body.get("apiKey"),
            input_data={
                "appendGroupMappings": body.get("appendGroupMappings"),
                "unassignedTables": body.get("unassignedTables"),
            },
            options={"mode": "pipeline", "autoPrepare": True, "persist": True},
            request_id=request.headers.get("X-Request-Id"),
        )
        if status != 200:
            return jsonify({"error": payload.get("error"), "missing_requirements": payload.get("missing_requirements")}), status
        return jsonify(payload.get("result") or {})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
