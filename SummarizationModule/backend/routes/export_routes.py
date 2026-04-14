from flask import Blueprint, Response, jsonify, request

from shared.db import get_session_db, get_meta, session_exists
from services.email.export_service import generate_csv

export_bp = Blueprint("export", __name__)


@export_bp.route("/export/csv/<view_id>", methods=["POST"])
def export_csv(view_id: str):
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400

        conn = get_session_db(session_id)
        try:
            view_results = get_meta(conn, "view_results") or []
        finally:
            try:
                conn.close()
            except Exception:
                pass

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
