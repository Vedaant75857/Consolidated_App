import logging

from flask import Blueprint, jsonify, request

from shared.db import get_session_db, get_session_lock, get_meta, set_meta, session_exists
from services.mapping.column_mapper import (
    STANDARD_FIELDS,
    deterministic_match,
    ai_map_columns,
    build_typed_table,
)
from services.procurement_views.procurement_views import get_procurement_view_availability

logger = logging.getLogger(__name__)
mapping_bp = Blueprint("mapping", __name__)


@mapping_bp.route("/map-columns", methods=["POST"])
def map_columns():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        api_key = body.get("apiKey")

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not api_key:
            return jsonify({"error": "API key required"}), 400

        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            columns = get_meta(conn, "columns")
            if not columns:
                return jsonify({"error": "No columns found. Upload data first."}), 400

            exact_matched, unmatched_fields, unmatched_cols = deterministic_match(columns)
            logger.info(
                "Deterministic pass: %d exact matches, %d fields remaining",
                len(exact_matched),
                len(unmatched_fields),
            )

            ai_results = ai_map_columns(unmatched_fields, unmatched_cols, api_key)
            logger.info("AI pass returned %d mapping(s)", len(ai_results))

            mappings: list[dict] = []
            for field in STANDARD_FIELDS:
                fk = field["fieldKey"]
                if fk in exact_matched:
                    mappings.append({
                        "fieldKey": fk,
                        "bestMatch": exact_matched[fk],
                        "alternatives": [],
                        "reasoning": "Exact name match",
                        "expectedType": field["expectedType"],
                    })
                else:
                    ai_entry = next((r for r in ai_results if r["fieldKey"] == fk), None)
                    if ai_entry:
                        mappings.append(ai_entry)
                    else:
                        mappings.append({
                            "fieldKey": fk,
                            "bestMatch": None,
                            "alternatives": [],
                            "reasoning": "No match found",
                            "expectedType": field["expectedType"],
                        })

            set_meta(conn, "ai_mappings", mappings)
            set_meta(conn, "step", 3)

        return jsonify({
            "mappings": mappings,
            "standardFields": STANDARD_FIELDS,
        })
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500


@mapping_bp.route("/procurement-views", methods=["POST"])
def procurement_views():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400

        conn = get_session_db(session_id)

        # Read precomputed result (cached during /confirm-mapping)
        views = get_meta(conn, "procurement_views")
        if views:
            return jsonify({"views": views})

        # Fallback: compute on the fly if cache is missing
        mapping = get_meta(conn, "mapping")
        if not mapping:
            return jsonify({"error": "No mapping found. Complete column mapping first."}), 400

        views = get_procurement_view_availability(mapping)
        return jsonify({"views": views})
    except Exception as exc:
        logger.exception("procurement-views failed for session %s", body.get("sessionId", "?"))
        return jsonify({"error": str(exc)}), 500


@mapping_bp.route("/confirm-mapping", methods=["POST"])
def confirm_mapping():
    try:
        body = request.get_json(force=True)
        session_id = body.get("sessionId")
        mapping = body.get("mapping")  # {fieldKey: sourceColumnName | null}

        if not session_id or not session_exists(session_id):
            return jsonify({"error": "Invalid session"}), 400
        if not mapping:
            return jsonify({"error": "Mapping required"}), 400

        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            set_meta(conn, "mapping", mapping)

            cast_report = build_typed_table(conn, mapping)
            set_meta(conn, "step", 4)

            # Precompute procurement view feasibility so step 7 is instant
            proc_views = get_procurement_view_availability(mapping)
            set_meta(conn, "procurement_views", proc_views)

        return jsonify({"castReport": cast_report, "procurementViews": proc_views})
    except Exception as exc:
        return jsonify({"error": str(exc)}), 500
