"""Debug helper: inspect analysis_data for a session DB."""
import json
import os
import sys

BACKEND = os.path.dirname(os.path.dirname(__file__))
sys.path.insert(0, BACKEND)

from shared.db import SESSIONS_DIR, get_session_db, get_meta
from services.spend_quality_assessment.analysis_data import (
    analysis_table_exists,
    column_has_data,
    list_analysis_columns,
)
from services.spend_quality_assessment.not_procurable import get_searchable_columns
from services.spend_quality_assessment.intercompany import get_vendor_searchable_columns
from services.spend_quality_assessment.capex_opex import get_candidate_columns, resolve_indicator_column


def inspect(session_id: str) -> None:
    print("SESSIONS_DIR:", SESSIONS_DIR)
    print("session:", session_id)
    conn = get_session_db(session_id)
    print("analysis_table_exists:", analysis_table_exists(conn))
    cols = list_analysis_columns(conn)
    print("columns count:", len(cols))
    print("sample cols:", cols[:10])
    cast = get_meta(conn, "cast_report") or {}
    fields = cast.get("fields", {})
    for fk in ["description", "supplier", "total_spend", "capex_opex_indicator", "gl_account"]:
        info = fields.get(fk, {})
        print(
            f"cast {fk}:",
            "mapped=", info.get("mapped"),
            "validRows=", info.get("validRows"),
            "source=", info.get("sourceColumn"),
            "has_data_sql=", column_has_data(conn, fk) if fk in cols else "N/A",
        )
    print("searchable:", get_searchable_columns(conn))
    print("vendor:", get_vendor_searchable_columns(conn))
    print("capex candidates:", get_candidate_columns(conn))
    print("capex resolved:", resolve_indicator_column(conn))
    row = conn.execute('SELECT COUNT(*) AS n FROM "analysis_data"').fetchone()
    print("row count:", row[0] if row else None)
    if "description" in cols:
        sample = conn.execute(
            'SELECT description FROM "analysis_data" '
            "WHERE description IS NOT NULL AND TRIM(CAST(description AS VARCHAR)) != '' "
            "LIMIT 3"
        ).fetchall()
        print("description samples:", sample)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        files = sorted(
            [f for f in os.listdir(SESSIONS_DIR) if f.endswith(".duckdb")],
            key=lambda f: os.path.getmtime(os.path.join(SESSIONS_DIR, f)),
            reverse=True,
        )
        print("Recent session DBs:")
        for f in files[:5]:
            print(" ", f)
        sys.exit(0)
    inspect(sys.argv[1].removesuffix(".duckdb"))
