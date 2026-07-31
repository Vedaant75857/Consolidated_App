import logging
from copy import deepcopy

from flask import Blueprint, Response, jsonify, request

from shared.db import get_session_db, get_meta, get_session_lock, session_exists
from services.email.export_service import generate_csv
from services.spend_quality_assessment.excel_export import (
    build_complete_analysis_workbook,
)

export_bp = Blueprint("export", __name__)
logger = logging.getLogger(__name__)


@export_bp.route("/export/csv/<view_id>", methods=["POST"])
def export_csv(view_id: str):
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400

        conn = get_session_db(session_id)
        view_results = get_meta(conn, "view_results") or []

        view = next((v for v in view_results if v.get("viewId") == view_id), None)
        if not view:
            return jsonify({"error": f"View {view_id} not found"}), 404

        csv_data = generate_csv(view)
        if not csv_data:
            return jsonify({"error": "No table data for this view"}), 400

        return Response(
            csv_data,
            mimetype="text/csv",
            headers={
                "Content-Disposition": f'attachment; filename="{view_id}.csv"'
            },
        )
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


def _has_completed_assessment(value) -> bool:
    """Check that a cached Executive Summary has all public export sections."""
    if not isinstance(value, dict):
        return False
    required_sections = (
        "executiveSummary",
        "columnFillRate",
        "spendBifurcation",
        "datePivot",
        "paretoAnalysis",
    )
    return all(isinstance(value.get(section), dict) for section in required_sections)


@export_bp.route("/export/xlsx/complete-analysis", methods=["POST"])
def export_complete_analysis():
    """Download the saved assessment plus the generated dashboard views as XLSX."""
    body = request.get_json(force=True, silent=True) or {}
    raw_session_id = body.get("sessionId")
    session_id = raw_session_id.strip() if isinstance(raw_session_id, str) else ""
    if not session_id:
        return jsonify({
            "error": "A valid session is required to export the analysis.",
            "code": "INVALID_SESSION",
        }), 400

    try:
        # Snapshot the two cached results together. Workbook creation happens after
        # releasing the DuckDB/session lock, so a slow download cannot block users.
        with get_session_lock(session_id):
            if not session_exists(session_id):
                return jsonify({
                    "error": "The analysis session is no longer available.",
                    "code": "SESSION_NOT_FOUND",
                }), 404
            conn = get_session_db(session_id)
            assessment = deepcopy(get_meta(conn, "executive_summary"))
            views = deepcopy(get_meta(conn, "view_results"))

        if not _has_completed_assessment(assessment):
            return jsonify({
                "error": "Complete the Spend Quality Assessment before exporting.",
                "code": "ASSESSMENT_NOT_READY",
            }), 409
        if not isinstance(views, list) or not views:
            return jsonify({
                "error": "Generate at least one dashboard view before exporting.",
                "code": "VIEWS_NOT_READY",
            }), 409

        workbook_data = build_complete_analysis_workbook(assessment, views)
        return Response(
            workbook_data,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": 'attachment; filename="spend-quality-and-views.xlsx"',
                "Cache-Control": "no-store",
                "X-Content-Type-Options": "nosniff",
            },
        )
    except ValueError as exc:
        return jsonify({
            "error": str(exc),
            "code": "EXPORT_GENERATION_FAILED",
        }), 422
    except Exception:
        logger.exception("complete analysis XLSX export failed")
        return jsonify({
            "error": "The Excel export could not be generated.",
            "code": "EXPORT_GENERATION_FAILED",
        }), 500
