import atexit
import json
import logging
import math
import os
import sys as _sys
import io
import inspect
import re as _re
import uuid
import warnings
from urllib.parse import urlsplit

_sys.dont_write_bytecode = True
os.environ.setdefault("PYTHONDONTWRITEBYTECODE", "1")

from flask import Flask, current_app, request, jsonify, send_file, g
from flask.json.provider import DefaultJSONProvider
from flask_cors import CORS
import pandas as pd
import numpy as np
from dotenv import load_dotenv

if not getattr(_sys, "frozen", False):
    load_dotenv(os.path.join(os.path.dirname(os.path.abspath(__file__)), ".env"))

from db import (
    get_session_db,
    get_session_lock,
    delete_session_db,
    safe_table_name,
    register_table,
    unregister_table,
    lookup_sql_name,
    all_registered_tables,
    cleanup_all_sessions,
    cleanup_stale_sessions,
    read_table,
    read_table_columns,
    table_exists,
    drop_table,
    table_row_count,
    set_meta,
    get_meta,
    quote_id,
    DB_DIR,
)
from db.bridge import sqlite_to_df, df_to_sqlite, PREVIEW_POOL, pick_best_df_rows
try:
    # Optional compatibility boundary.  It is disabled by default and keeps
    # the legacy loader as the rollback path until parity gates pass.
    from backend.module2.ingestion_adapter import load_source_file_to_session as _adapter_load_source_file
except ImportError:  # pragma: no cover - standalone module2 hosting
    from ingestion_adapter import load_source_file_to_session as _adapter_load_source_file


def _nan_to_none(obj):
    """Recursively replace float NaN/Infinity with None for JSON safety."""
    if isinstance(obj, float) and (math.isnan(obj) or math.isinf(obj)):
        return None
    if isinstance(obj, dict):
        return {k: _nan_to_none(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_nan_to_none(v) for v in obj]
    return obj


class SafeJSONProvider(DefaultJSONProvider):
    def dumps(self, obj, **kwargs):
        kwargs.setdefault("default", self.default)
        return json.dumps(_nan_to_none(obj), allow_nan=False, **kwargs)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s: %(message)s",
)
logger = logging.getLogger("module2")

from agents.normalization import (
    normalize_supplier_name_agent,
    normalize_supplier_country_agent,
    date_normalization_agent,
    payment_terms_agent,
    normalize_region_agent,
    normalize_plant_agent,
    normalize_spend_agent,
    assess_supplier_country,
    assess_region,
    assess_currency_conversion,
)
from agents.fx_rates import load_fx_table

warnings.filterwarnings('ignore')

app = Flask(__name__)
app.json = SafeJSONProvider(app)
CORS(app)


def create_app() -> Flask:
    """Return the Module 2 application for the unified backend host."""
    return app

AGENT_MAPPING = {
    "date": date_normalization_agent,
    "payment_terms": payment_terms_agent,
    "supplier_name": normalize_supplier_name_agent,
    "supplier_country": normalize_supplier_country_agent,
    "region": normalize_region_agent,
    "plant": normalize_plant_agent,
    "currency_conversion": normalize_spend_agent
}

import zipfile
import requests as _requests

SOURCE_EXCEL_EXTS = ('.xls', '.xlsx', '.xlsm', '.xlsb', '.xltx', '.xltm', '.ods')
# Shared ingestion supports the common delimited aliases while the legacy
# loader remains the default rollback path.  Keeping these in the route's
# allow-list lets an explicitly gated shared upload handle TSV/PSV/TAB members.
SOURCE_DATA_EXTS = SOURCE_EXCEL_EXTS + ('.csv', '.tsv', '.psv', '.tab')

# ── Session startup / shutdown cleanup ─────────────────────────────────────────

def _on_exit():
    cleaned = cleanup_all_sessions()
    logger.info("[Module-2] Shutdown: deleted %d session(s).", cleaned)


atexit.register(_on_exit)


# ── Helpers ────────────────────────────────────────────────────────────────────

def _load_source_file_to_session(conn, name: str, data: bytes, inventory: list[dict]) -> None:
    """Load one supported source file into the normalizer session."""
    lower = name.lower()
    if lower.endswith(SOURCE_EXCEL_EXTS):
        excel_file = pd.ExcelFile(io.BytesIO(data), engine='calamine')
        try:
            for sheet in excel_file.sheet_names:
                key = f"{name}::{sheet}"
                df = pd.read_excel(excel_file, sheet_name=sheet, header=None)
                if not df.empty:
                    raw_sql = safe_table_name("raw", key)
                    data_sql = safe_table_name("data", key)
                    df_to_sqlite(conn, raw_sql, df, commit=False)
                    conn.execute(f"DROP TABLE IF EXISTS {quote_id(data_sql)}")
                    conn.execute(f"CREATE TABLE {quote_id(data_sql)} AS SELECT * FROM {quote_id(raw_sql)}")
                    register_table(conn, key, data_sql, commit=False)
                    inventory.append({"table_key": key, "rows": len(df), "cols": len(df.columns)})
        finally:
            excel_file.close()
    elif lower.endswith('.csv'):
        key = f"{name}::"
        df = pd.read_csv(io.BytesIO(data), header=None)
        if not df.empty:
            raw_sql = safe_table_name("raw", key)
            data_sql = safe_table_name("data", key)
            df_to_sqlite(conn, raw_sql, df, commit=False)
            conn.execute(f"DROP TABLE IF EXISTS {quote_id(data_sql)}")
            conn.execute(f"CREATE TABLE {quote_id(data_sql)} AS SELECT * FROM {quote_id(raw_sql)}")
            register_table(conn, key, data_sql, commit=False)
            inventory.append({"table_key": key, "rows": len(df), "cols": len(df.columns)})


def _get_session_id() -> str:
    """Extract sessionId from the request (JSON body, form data, query param, or header)."""
    # JSON body
    data = request.get_json(silent=True)
    if data and data.get("sessionId"):
        return data["sessionId"]
    # Form data
    if request.form.get("sessionId"):
        return request.form["sessionId"]
    # Query parameter
    if request.args.get("sessionId"):
        return request.args["sessionId"]
    # Header
    hdr = request.headers.get("X-Session-Id")
    if hdr:
        return hdr
    raise ValueError("Missing sessionId in request")


def get_api_key():
    """Read API key from JSON body or environment.

    Fallback order: request body → PORTKEY_API_KEY → OPENAI_API_KEY.
    PORTKEY_API_KEY is checked first because the default AI_PROVIDER is 'portkey'.
    """
    try:
        data = request.get_json(silent=True) or {}
        key = data.get('apiKey')
        if key:
            return key
    except Exception:
        pass
    return os.getenv('PORTKEY_API_KEY') or os.getenv('OPENAI_API_KEY', '')


# ── Column suggestion heuristic (unchanged) ───────────────────────────────────

def _suggest_columns(df: pd.DataFrame, sample_size: int = 100) -> dict:
    """Analyze the first `sample_size` rows and return candidate columns for date,
    currency code, and spend amount. Returns None for each field when no confident
    match is found.
    """
    sample = df.head(sample_size)
    cols = [str(c) for c in df.columns]

    def col_lower(c):
        return str(c).lower().strip()

    def pct_populated(series):
        s = series.astype(str).str.strip()
        valid = (series.notna()) & (s != "") & (s != "nan") & (s != "None") & (s != "<NA>")
        return valid.sum() / len(series) if len(series) > 0 else 0.0

    # DATE
    DATE_KEYWORDS = ("date", "dob", "time")
    date_candidates = [
        c for c in cols
        if any(kw in col_lower(c) for kw in DATE_KEYWORDS)
        and not str(c).startswith("Norm_Date_")
    ]

    best_date, best_date_pct = None, 0.0
    for c in date_candidates:
        s = sample[c]
        pop = pct_populated(s)
        if pop == 0:
            continue
        s_clean = s.dropna()
        s_clean = s_clean[s_clean.astype(str).str.strip().isin(["", "nan", "None", "<NA>"]) == False]
        if len(s_clean) == 0:
            continue
        parsed = pd.to_datetime(s_clean, errors="coerce", dayfirst=False)
        pct = parsed.notna().sum() / len(s_clean)
        if pct < 0.60:
            parsed2 = pd.to_datetime(s_clean, errors="coerce", dayfirst=True)
            pct = max(pct, parsed2.notna().sum() / len(s_clean))
        if pct >= 0.60 and pct > best_date_pct:
            best_date_pct = pct
            best_date = c

    # CURRENCY CODE
    CCY_HEADER_WEIGHTS = {
        "currency code": 1.0, "currency_code": 1.0,
        "currency": 0.95,
        "curr code": 0.90, "curr_code": 0.90,
        "ccy code": 0.90, "ccy_code": 0.90,
        "curr": 0.85, "ccy": 0.85,
        "fx code": 0.70, "fx_code": 0.70,
        "iso code": 0.65, "iso_code": 0.65,
        "iso": 0.55,
        "fx": 0.50,
    }
    CCY_CODE_RE = _re.compile(r'^[A-Z]{3}$')

    best_ccy, best_ccy_score = None, 0.0
    for c in cols:
        cl = col_lower(c)
        header_w = 0.0
        for kw, wt in CCY_HEADER_WEIGHTS.items():
            if kw in cl:
                header_w = max(header_w, wt)
        if header_w == 0.0:
            continue
        s = sample[c].dropna().astype(str).str.strip().str.upper()
        s = s[s.isin(["", "NAN", "NONE", "<NA>"]) == False].head(20)
        if len(s) == 0:
            continue
        pct_valid_codes = s.apply(lambda v: bool(CCY_CODE_RE.match(v))).mean()
        pop = pct_populated(sample[c])
        score = header_w * pct_valid_codes * pop
        if pct_valid_codes >= 0.70 and score > best_ccy_score:
            best_ccy_score = score
            best_ccy = c

    # SPEND AMOUNT
    SPEND_HEADER_WEIGHTS = {
        "spend":    1.00,
        "amount":   0.90,
        "cost":     0.85,
        "price":    0.80,
        "payment":  0.75,
        "invoice":  0.75,
        "charge":   0.70,
        "fee":      0.65,
        "total":    0.60,
        "value":    0.55,
    }
    STRIP_RE   = _re.compile(r"[$€£,\s]")
    PARENS_RE  = _re.compile(r"^\((.+)\)$")

    best_spend, best_spend_score = None, 0.0
    for c in cols:
        cl = col_lower(c)
        header_w = 0.0
        for kw, wt in SPEND_HEADER_WEIGHTS.items():
            if kw in cl:
                header_w = max(header_w, wt)
        if header_w == 0.0:
            continue
        s_str = sample[c].astype(str).str.strip()
        total = len(s_str)
        pop = pct_populated(sample[c])
        cleaned = s_str.str.replace(STRIP_RE, "", regex=True)
        paren_mask = cleaned.str.match(r"^\(.+\)$")
        cleaned = cleaned.where(~paren_mask, "-" + cleaned.str[1:-1])
        numeric = pd.to_numeric(cleaned, errors="coerce")
        pct_numeric = numeric.notna().sum() / total if total > 0 else 0.0
        n_valid = numeric.notna().sum()
        pct_positive = float((numeric > 0).sum()) / n_valid if n_valid > 0 else 0.0
        monetary_bonus = 1.2 if pct_positive >= 0.50 else 1.0
        score = header_w * pct_numeric * pop * monetary_bonus
        if pct_numeric >= 0.70 and score > best_spend_score:
            best_spend_score = score
            best_spend = c

    return {
        "date_col":     best_date,
        "currency_col": best_ccy,
        "spend_col":    best_spend,
        "scores": {
            "date_pct":    round(best_date_pct,   3) if best_date    else None,
            "ccy_score":   round(best_ccy_score,  3) if best_ccy     else None,
            "spend_score": round(best_spend_score, 3) if best_spend  else None,
        },
    }


# ── Helpers for preview formatting ─────────────────────────────────────────────

_PREVIEW_CLEAN_VALUES = {"<NA>", "nan", "NaT", "None"}


def _clean_preview_rows(rows: list[dict]) -> list[dict]:
    """Replace NaN-like string values with empty strings for frontend display."""
    for row in rows:
        for k, v in row.items():
            if v is None or v in _PREVIEW_CLEAN_VALUES:
                row[k] = ""
    return rows


# ══════════════════════════════════════════════════════════════════════════════
#  ROUTES
# ══════════════════════════════════════════════════════════════════════════════

@app.route('/api/status', methods=['GET'])
@app.route('/api/health', methods=['GET'])
def health_check():
    """Lightweight liveness check used by the launcher before opening the browser."""
    return jsonify({"status": "ok"})


# ── Upload ─────────────────────────────────────────────────────────────────────

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    session_id = request.form.get("sessionId")
    if not session_id:
        return jsonify({"error": "Missing sessionId"}), 400

    file = request.files['file']
    filename = file.filename.lower()
    buffer = file.read()

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        for entry in all_registered_tables(conn):
            drop_table(conn, entry["sql_name"], commit=False)
            unregister_table(conn, entry["table_key"], commit=False)
        drop_table(conn, "active", commit=False)
        conn.commit()

        inventory = []

        try:
            if filename.endswith('.zip') or file.content_type in ['application/zip', 'application/x-zip-compressed']:
                with zipfile.ZipFile(io.BytesIO(buffer)) as zf:
                    for entry in zf.infolist():
                        if entry.is_dir():
                            continue
                        name = entry.filename
                        lower = name.lower()
                        try:
                            if lower.endswith('.zip'):
                                with zipfile.ZipFile(io.BytesIO(zf.read(name))) as nested_zf:
                                    for nested_entry in nested_zf.infolist():
                                        if nested_entry.is_dir():
                                            continue
                                        nested_name = nested_entry.filename
                                        nested_lower = nested_name.lower()
                                        if nested_lower.endswith(SOURCE_DATA_EXTS):
                                            _adapter_load_source_file(
                                                conn,
                                                f"{name}/{nested_name}",
                                                nested_zf.read(nested_name),
                                                inventory,
                                                legacy_loader=_load_source_file_to_session,
                                                session_id=session_id,
                                            )
                            elif lower.endswith(SOURCE_DATA_EXTS):
                                _adapter_load_source_file(
                                    conn, name, zf.read(name), inventory,
                                    legacy_loader=_load_source_file_to_session,
                                    session_id=session_id,
                                )
                        except Exception as e:
                            logger.error("Failed parsing %s: %s", name, e, exc_info=True)
            else:
                try:
                    if filename.endswith(SOURCE_DATA_EXTS):
                        _adapter_load_source_file(
                            conn, file.filename, buffer, inventory,
                            legacy_loader=_load_source_file_to_session,
                            session_id=session_id,
                        )
                except Exception as e:
                    logger.error("Failed parsing directly uploaded file %s: %s", file.filename, e, exc_info=True)

            conn.commit()

            if not inventory:
                return jsonify({"error": "No valid raw data tables found. Ensure files have valid columns and rows."}), 400

            return jsonify({
                "message": "Files extracted successfully",
                "inventory": inventory
            })
        except Exception as e:
            logger.error("Failed to ingest package: %s", e, exc_info=True)
            return jsonify({"error": f"Failed to ingest package: {str(e)}"}), 500


# ── Preview routes ─────────────────────────────────────────────────────────────

@app.route('/api/get-raw-preview', methods=['POST', 'GET'])
def get_raw_preview():
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    data = request.get_json(silent=True) or {}
    table_key = data.get('tableKey') or request.args.get('tableKey')

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        raw_sql = safe_table_name("raw", table_key)

        if not table_exists(conn, raw_sql):
            return jsonify({"error": "Table not found"}), 404

        df = sqlite_to_df(conn, raw_sql, limit=PREVIEW_POOL)
        if df is None:
            return jsonify({"error": "Table not found"}), 404

    df = pick_best_df_rows(df, 50)
    preview_df = df.fillna("").astype(str).replace(_PREVIEW_CLEAN_VALUES, "")
    raw_list = preview_df.values.tolist()
    return jsonify({"rawPreview": raw_list})


@app.route('/api/get-preview', methods=['GET'])
def get_preview():
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    table_key = request.args.get('tableKey')

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        data_sql = lookup_sql_name(conn, table_key)

        if not data_sql or not table_exists(conn, data_sql):
            return jsonify({"error": "Table not found"}), 404

        df = sqlite_to_df(conn, data_sql, limit=PREVIEW_POOL)
        if df is None:
            return jsonify({"error": "Table not found"}), 404

    df = pick_best_df_rows(df, 50)
    preview_df = df.fillna("").astype(str).replace(_PREVIEW_CLEAN_VALUES, "")
    return jsonify({
        "columns": [str(c) for c in df.columns],
        "rows": preview_df.to_dict(orient="records")
    })


@app.route('/api/current-preview', methods=['GET'])
def get_current_preview():
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        if not table_exists(conn, "active"):
            return jsonify({"error": "No active dataset loaded"}), 400

        df = sqlite_to_df(conn, "active", limit=PREVIEW_POOL)
        columns = read_table_columns(conn, "active")

    df = pick_best_df_rows(df, 50)
    preview_df = df.fillna("").astype(str).replace(_PREVIEW_CLEAN_VALUES, "")
    return jsonify({
        "columns": columns,
        "rows": preview_df.to_dict(orient="records")
    })


@app.route('/api/suggest-columns', methods=['GET'])
def suggest_columns():
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        df = sqlite_to_df(conn, "active", limit=200)
        if df is None:
            return jsonify({"error": "No active dataset loaded"}), 400

    try:
        result = _suggest_columns(df, sample_size=100)
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/current-inventory', methods=['GET'])
def current_inventory():
    """Return the current tables as an inventory list."""
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        entries = all_registered_tables(conn)
        if not entries:
            return jsonify({"inventory": []})

        sql_names = [e["sql_name"] for e in entries]

        # Batch: column counts
        col_rows = conn.execute(
            "SELECT table_name, COUNT(*) "
            "FROM information_schema.columns "
            "WHERE table_name = ANY(?) "
            "GROUP BY table_name",
            (sql_names,),
        ).fetchall()
        col_map = {r[0]: r[1] for r in col_rows}

        # Batch: row counts
        existing = [t for t in sql_names if t in col_map]
        row_map: dict[str, int] = {}
        if existing:
            parts = [
                f"SELECT '{t}' AS tbl, COUNT(*) AS cnt FROM {quote_id(t)}"
                for t in existing
            ]
            for r in conn.execute(" UNION ALL ".join(parts)).fetchall():
                row_map[r[0]] = r[1]

        inventory = []
        for entry in entries:
            sn = entry["sql_name"]
            if sn not in col_map:
                continue
            inventory.append({
                "table_key": entry["table_key"],
                "rows": row_map.get(sn, 0),
                "cols": col_map[sn],
            })

    return jsonify({"inventory": inventory})


# ── Table management ───────────────────────────────────────────────────────────

@app.route('/api/set-header-row', methods=['POST'])
def set_header_row():
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    data = request.json
    table_key = data.get('tableKey')
    row_index = data.get('rowIndex')
    custom_names = data.get('customNames', {})

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        raw_sql = safe_table_name("raw", table_key)

        raw_df = sqlite_to_df(conn, raw_sql)
        if raw_df is None:
            return jsonify({"error": "Table not found"}), 404

        if row_index is None or not (0 <= row_index < len(raw_df)):
            return jsonify({"error": "Invalid row index"}), 400

        new_headers = raw_df.iloc[row_index].fillna("").astype(str).tolist()
        for col_idx_str, custom_name in custom_names.items():
            try:
                col_idx = int(col_idx_str)
                if 0 <= col_idx < len(new_headers) and custom_name.strip():
                    new_headers[col_idx] = custom_name.strip()
            except (ValueError, IndexError):
                pass

        final_headers = []
        seen = set()
        for i, h in enumerate(new_headers):
            h = h.strip()
            if not h:
                h = f"Unnamed_{i}"
            original = h
            counter = 1
            while h in seen:
                h = f"{original}_{counter}"
                counter += 1
            seen.add(h)
            final_headers.append(h)

        processed = raw_df.iloc[row_index + 1:].copy().reset_index(drop=True)
        processed.columns = final_headers

        data_sql = lookup_sql_name(conn, table_key)
        if not data_sql:
            data_sql = safe_table_name("data", table_key)
            register_table(conn, table_key, data_sql, commit=False)

        df_to_sqlite(conn, data_sql, processed)

    return jsonify({"message": "Header row updated successfully", "rows": len(processed), "columns": final_headers})


@app.route('/api/delete-rows', methods=['POST'])
def delete_rows():
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    data = request.json
    table_key = data.get('tableKey')
    row_ids = data.get('rowIds', [])

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        data_sql = lookup_sql_name(conn, table_key)
        if not data_sql or not table_exists(conn, data_sql):
            return jsonify({"error": "Table not found"}), 404

        tbl = quote_id(data_sql)
        row_set = {int(i) for i in row_ids}
        cols = read_table_columns(conn, data_sql)
        col_select = ", ".join(quote_id(c) for c in cols)
        placeholders = ",".join(str(r) for r in row_set)

        tmp = quote_id("_tmp_del")
        conn.execute(f"DROP TABLE IF EXISTS {tmp}")
        conn.execute(
            f"CREATE TABLE {tmp} AS "
            f"SELECT {col_select} FROM ("
            f"  SELECT *, ROW_NUMBER() OVER () - 1 AS _rn FROM {tbl}"
            f") sub WHERE _rn NOT IN ({placeholders})"
        )
        conn.execute(f"DROP TABLE IF EXISTS {tbl}")
        conn.execute(f"ALTER TABLE {tmp} RENAME TO {tbl}")
        conn.commit()
        new_count = conn.execute(f"SELECT COUNT(*) FROM {tbl}").fetchone()[0]

    return jsonify({"message": f"Deleted {len(row_ids)} rows successfully", "rows": new_count})


@app.route('/api/delete-table', methods=['POST'])
def delete_table():
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    table_key = request.json.get('tableKey')

    with get_session_lock(session_id):
        conn = get_session_db(session_id)

        data_sql = lookup_sql_name(conn, table_key)
        if data_sql:
            drop_table(conn, data_sql, commit=False)
        raw_sql = safe_table_name("raw", table_key)
        drop_table(conn, raw_sql, commit=False)
        unregister_table(conn, table_key, commit=False)
        conn.commit()

    return jsonify({"message": "Table deleted"})


@app.route('/api/select-table', methods=['POST'])
def select_table():
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    table_key = request.json.get('tableKey')

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        data_sql = lookup_sql_name(conn, table_key)

        if not data_sql or not table_exists(conn, data_sql):
            return jsonify({"error": "Table not found"}), 404

        tbl_src = quote_id(data_sql)
        tbl_dst = quote_id("active")
        conn.execute(f"DROP TABLE IF EXISTS {tbl_dst}")
        conn.execute(f"CREATE TABLE {tbl_dst} AS SELECT * FROM {tbl_src}")
        conn.commit()
        set_meta(conn, "active_table_key", table_key)
        fname = table_key.split('::')[0]
        set_meta(conn, "filename", fname)

        row_count = conn.execute(f"SELECT COUNT(*) FROM {tbl_dst}").fetchone()[0]
        columns = read_table_columns(conn, "active")
        preview_df = sqlite_to_df(conn, "active", limit=PREVIEW_POOL)

    preview_df = pick_best_df_rows(preview_df, 10).fillna("").astype(str)
    return jsonify({
        "columns": columns,
        "rows": row_count,
        "previewData": preview_df.to_dict(orient='records')
    })


@app.route('/api/reset-normalization', methods=['POST'])
def reset_normalization():
    """Re-copy df from data table, discarding all normalization changes."""
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    try:
        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            body = request.get_json(silent=True) or {}
            table_key = body.get('tableKey') or get_meta(conn, "active_table_key")

            source_sql = None
            if table_key:
                data_sql = lookup_sql_name(conn, table_key)
                if data_sql and table_exists(conn, data_sql):
                    source_sql = data_sql

            if not source_sql:
                entries = all_registered_tables(conn)
                if entries:
                    source_sql = entries[0]["sql_name"]

            if source_sql:
                tbl_dst = quote_id("active")
                conn.execute(f"DROP TABLE IF EXISTS {tbl_dst}")
                conn.execute(f"CREATE TABLE {tbl_dst} AS SELECT * FROM {quote_id(source_sql)}")
                conn.commit()
                row_count = conn.execute(f"SELECT COUNT(*) FROM {tbl_dst}").fetchone()[0]
                return jsonify({"ok": True, "rows": row_count})

        return jsonify({"ok": True, "rows": 0})
    except Exception as e:
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/api/reset-state', methods=['POST'])
def reset_state():
    """Full state reset — deletes the entire session database."""
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    delete_session_db(session_id)
    return jsonify({"ok": True})


# ── Agent routes ───────────────────────────────────────────────────────────────

@app.route('/api/assess-supplier-country', methods=['POST'])
def assess_supplier_country_api():
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        df = sqlite_to_df(conn, "active")
        if df is None:
            return jsonify({"error": "No file loaded"}), 400

    data = request.json
    kwargs = data.get('kwargs', {})

    try:
        result = assess_supplier_country(df, **kwargs)
        return jsonify(result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/assess-region', methods=['POST'])
def assess_region_api():
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        df = sqlite_to_df(conn, "active")
        if df is None:
            return jsonify({"error": "No file loaded"}), 400

    data = request.json
    kwargs = data.get('kwargs', {})

    try:
        result = assess_region(df, **kwargs)
        return jsonify(result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/assess-currency-conversion', methods=['POST'])
def assess_currency_conversion_api():
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        df = sqlite_to_df(conn, "active")
        if df is None:
            return jsonify({"error": "No file loaded"}), 400

    data = request.json
    kwargs = data.get('kwargs', {})

    try:
        result = assess_currency_conversion(df, **kwargs)
        return jsonify(result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/run-normalization', methods=['POST'])
def run_normalization():
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    data = request.json
    agent_id = data.get('agent_id')

    if agent_id not in AGENT_MAPPING:
        return jsonify({"error": f"Invalid agent ID: {agent_id}"}), 400

    agent_func = AGENT_MAPPING[agent_id]
    api_key = get_api_key()
    kwargs = data.get('kwargs', {})

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        df = sqlite_to_df(conn, "active")
        if df is None:
            return jsonify({"error": "No file loaded"}), 400

        try:
            result = agent_func(df, api_key=api_key, **kwargs)

            if isinstance(result, tuple) and len(result) >= 2:
                modified_df, message = result[0], result[1]
                df_to_sqlite(conn, "active", modified_df)
                columns = [str(c) for c in modified_df.columns]
                response = {"message": message, "columns": columns}
                if len(result) >= 4 and result[3] is not None:
                    if agent_id == "currency_conversion":
                        response["conversion_metrics"] = result[3]
                    elif agent_id == "supplier_country":
                        response["country_norm_metrics"] = result[3]
                    elif agent_id == "region":
                        response["region_norm_metrics"] = result[3]
                return jsonify(response)
            else:
                return jsonify({"error": "Unexpected return format from agent"}), 500

        except Exception as e:
            return jsonify({"error": str(e)}), 500


# ── Import from DataStitcher ──────────────────────────────────────────────────

def _configured_backend_url(key: str) -> str | None:
    raw = current_app.config.get(key)
    if raw is None or not str(raw).strip():
        raw = os.environ.get(key)
    if raw is None or not str(raw).strip():
        return None
    value = str(raw).strip().rstrip("/")
    parsed = urlsplit(value)
    try:
        parsed.port
        valid_port = True
    except ValueError:
        valid_port = False
    if (
        parsed.scheme not in ("http", "https")
        or not parsed.netloc
        or not parsed.hostname
        or any(char.isspace() for char in parsed.netloc)
        or not valid_port
        or parsed.query
        or parsed.fragment
    ):
        raise RuntimeError(f"{key} must be an absolute http(s) URL with a valid host")
    return value


def _analyzer_be() -> str:
    """Resolve the Module 3 namespace on the one-process backend host."""
    configured = _configured_backend_url("UNIFIED_BACKEND_URL")
    if configured:
        return f"{configured}/api/module3"
    port = current_app.config.get("UNIFIED_BACKEND_PORT") or os.environ.get("UNIFIED_BACKEND_PORT", "8000")
    return f"http://127.0.0.1:{int(port)}/api/module3"


def _typed_artifact_module():
    """Load the optional typed transport implementation package-qualified."""
    for module_name in ("backend.ingestion.typed_artifact", "ingestion.typed_artifact"):
        try:
            return __import__(module_name, fromlist=["*"])
        except (ImportError, ModuleNotFoundError):
            continue
    return None


def _manifest_json_bytes(upload) -> bytes | None:
    if upload is not None:
        return _read_bounded_upload(upload, 2 * 1024 * 1024)
    value = request.form.get("manifest")
    if value:
        return value.encode("utf-8")
    return None


def _read_bounded_upload(upload, max_bytes: int) -> bytes:
    declared = getattr(upload, "content_length", None)
    if declared is not None and declared > max_bytes:
        raise ValueError("upload exceeds configured size limit")
    chunks: list[bytes] = []
    total = 0
    while True:
        chunk = upload.stream.read(min(1024 * 1024, max_bytes - total + 1))
        if not chunk:
            break
        total += len(chunk)
        if total > max_bytes:
            raise ValueError("upload exceeds configured size limit")
        chunks.append(chunk)
    return b"".join(chunks)


def _parse_transport_manifest(raw_manifest: bytes, payload: bytes):
    """Parse and byte-verify a typed manifest before touching a session DB."""
    api = _typed_artifact_module()
    if api is not None and callable(getattr(api, "parse_transport_manifest", None)):
        return api.parse_transport_manifest(raw_manifest, payload)

    raise ValueError("typed transport parser is unavailable")


def _call_typed_api(fn, **kwargs):
    """Pass only parameters supported by the in-flight typed_artifact API."""
    try:
        params = inspect.signature(fn).parameters
    except (TypeError, ValueError):
        return fn(**kwargs)
    return fn(**{key: value for key, value in kwargs.items() if key in params})


def _typed_result_field(result, *names):
    if isinstance(result, dict):
        for name in names:
            if name in result:
                return result[name]
    for name in names:
        if hasattr(result, name):
            return getattr(result, name)
    return None


@app.route('/api/import-from-stitcher', methods=['POST'])
def import_from_stitcher():
    """Import a verified typed artifact, with explicitly lossy CSV fallback."""
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files['file']
    try:
        buffer = _read_bounded_upload(file, 512 * 1024 * 1024)
    except ValueError as exc:
        return jsonify({"error": str(exc), "code": "typed_payload_too_large"}), 413
    fname = file.filename or "imported.csv"

    manifest_upload = request.files.get("manifest")
    try:
        manifest_bytes = _manifest_json_bytes(manifest_upload)
    except ValueError as exc:
        return jsonify({"error": str(exc), "code": "typed_manifest_too_large"}), 413
    is_typed = bool(manifest_bytes is not None or fname.lower().endswith((".parquet", ".arrow")))

    # Verify all untrusted bytes and manifest metadata before creating a session
    # or acquiring its lock.  A malformed/tampered artifact is side-effect free.
    manifest = None
    if is_typed:
        if not manifest_bytes:
            return jsonify({"error": "Typed transfer manifest is required", "code": "typed_manifest_required"}), 400
        try:
            manifest = _parse_transport_manifest(manifest_bytes, buffer)
            if getattr(manifest, "destination_module", "module2") not in ("module2", "module-2", ""):
                return jsonify({"error": "Typed transfer destination does not match Module 2", "code": "typed_destination_mismatch"}), 400
            if getattr(manifest, "source_module", "") not in ("module1", "module-1"):
                return jsonify({"error": "Typed transfer source is not Module 1", "code": "typed_source_mismatch"}), 400
            if len(getattr(manifest, "tables", ())) != 1:
                return jsonify({"error": "Module 2 typed import requires one table", "code": "typed_table_count_invalid"}), 400
            table = manifest.tables[0]
            keys = [str(column.key) for column in table.columns]
            physical = [str(column.physical_name or column.key) for column in table.columns]
            reserved = {"__row_id", "record_id", "recordid"}
            if len(keys) != len(set(keys)) or len(physical) != len(set(physical)) or any(name.casefold() in reserved for name in physical):
                return jsonify({"error": "Typed transfer schema contains duplicate or reserved columns", "code": "typed_schema_invalid"}), 422
        except Exception as exc:
            logger.warning("Rejected typed Module 2 import before session mutation: %s", exc)
            return jsonify({"error": "Invalid typed transfer artifact", "code": "typed_manifest_invalid"}), 400
        typed_api = _typed_artifact_module()
        if not typed_api or not callable(getattr(typed_api, "import_duckdb_parquet", None)):
            return jsonify({"error": "Typed transfer import is unavailable", "code": "UNSUPPORTED_TYPED_ARTIFACT"}), 415

    if manifest is not None:
        import hashlib
        import_id = f"typed_{hashlib.sha256(str(manifest.artifact_id).encode('utf-8')).hexdigest()[:40]}"
    else:
        import_id = str(uuid.uuid4())
    session_id = import_id

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        if manifest is not None:
            previous = get_meta(conn, "typed_transfer")
            if previous:
                if previous.get("payload_checksum") != manifest.payload_checksum:
                    return jsonify({"error": "Typed artifact ID already exists with a different payload", "code": "typed_artifact_collision"}), 409
                existing = all_registered_tables(conn)
                inventory = []
                for entry in existing:
                    inventory.append({
                        "table_key": entry["table_key"],
                        "rows": table_row_count(conn, entry["sql_name"]),
                        "cols": len(read_table_columns(conn, entry["sql_name"])),
                    })
                return jsonify({"inventory": inventory, "imported": True, "sessionId": session_id, "transport": "typed", "idempotent": True})
        for entry in all_registered_tables(conn):
            drop_table(conn, entry["sql_name"], commit=False)
            unregister_table(conn, entry["table_key"], commit=False)
        conn.commit()

        try:
            key = f"{fname}::"
            if manifest is not None:
                key = str(manifest.tables[0].table_key)
                api = _typed_artifact_module()
                importer = getattr(api, "import_duckdb_parquet", None) if api else None
                if not callable(importer):
                    return jsonify({"error": "Typed transfer import is unavailable", "code": "UNSUPPORTED_TYPED_ARTIFACT"}), 415

                imported = _call_typed_api(
                    importer, conn=conn, connection=conn, payload=buffer,
                    manifest=manifest, table_name=safe_table_name("typed_import", import_id),
                    session_id=import_id,
                )
                raw_source = _typed_result_field(imported, "raw_table", "rawTable")
                typed_source = _typed_result_field(imported, "typed_table", "typedTable")
                # The shared importer currently materializes one requested
                # staging table and returns TransferTable metadata.  Keep
                # accepting richer raw/typed result objects when available.
                staging_source = safe_table_name("typed_import", import_id)
                if not raw_source and not typed_source:
                    raw_source = typed_source = staging_source
                typed_source = typed_source or raw_source
                raw_source = raw_source or typed_source
                data_sql = safe_table_name("data", key)
                raw_sql = safe_table_name("raw", key)
                conn.execute(f"DROP TABLE IF EXISTS {quote_id(data_sql)}")
                conn.execute(f"CREATE TABLE {quote_id(data_sql)} AS SELECT * FROM {quote_id(str(typed_source))}")
                source_columns = read_table_columns(conn, str(raw_source))
                if not source_columns:
                    source_columns = read_table_columns(conn, str(typed_source))
                raw_projection = ", ".join(
                    f"CAST({quote_id(column)} AS VARCHAR) AS {quote_id(column)}"
                    for column in source_columns
                )
                conn.execute(f"DROP TABLE IF EXISTS {quote_id(raw_sql)}")
                conn.execute(f"CREATE TABLE {quote_id(raw_sql)} AS SELECT {raw_projection} FROM {quote_id(str(raw_source))}")
                register_table(conn, key, data_sql, commit=False)
                conn.execute(f"DROP TABLE IF EXISTS {quote_id('active')}")
                # Module 2 agents historically operate on VARCHAR active data;
                # keep native typed data in the registered source table while
                # exposing a compatibility active projection.
                conn.execute(f"CREATE TABLE {quote_id('active')} AS SELECT * FROM {quote_id(raw_sql)}")
                set_meta(conn, "active_table_key", key)
                set_meta(conn, "filename", fname)
                set_meta(conn, "import_id", import_id)
                set_meta(conn, "typed_transfer", {
                    "artifact_id": manifest.artifact_id,
                    "source_module": manifest.source_module,
                    "destination_module": manifest.destination_module,
                    "contract_version": manifest.contract_version,
                    "payload_checksum": manifest.payload_checksum,
                    "raw_hash": manifest.tables[0].raw_hash,
                    "typed_hash": manifest.tables[0].typed_hash,
                    "provenance": dict(manifest.tables[0].provenance),
                    "raw_representation": "VARCHAR compatibility projection",
                })
                row_count = int(conn.execute(f"SELECT COUNT(*) FROM {quote_id(data_sql)}").fetchone()[0])
                cols = len(read_table_columns(conn, data_sql))
                if (
                    lookup_sql_name(conn, key) != data_sql
                    or not table_exists(conn, raw_sql)
                    or not table_exists(conn, data_sql)
                    or not table_exists(conn, "active")
                    or get_meta(conn, "active_table_key") != key
                ):
                    raise RuntimeError("typed import publication postcondition failed")
                inventory = [{"table_key": key, "rows": row_count, "cols": cols}]
                conn.commit()
                return jsonify({"inventory": inventory, "imported": True, "sessionId": import_id, "transport": "typed"})

            df = pd.read_csv(io.BytesIO(buffer))
            if df.empty:
                return jsonify({"error": "Imported file contains no data"}), 400

            data_sql = safe_table_name("data", key)
            df_to_sqlite(conn, data_sql, df, commit=False)
            register_table(conn, key, data_sql, commit=False)
            set_meta(conn, "import_id", import_id)
            conn.commit()

            inventory = [{"table_key": key, "rows": len(df), "cols": len(df.columns)}]
            return jsonify({"inventory": inventory, "imported": True, "sessionId": import_id,
                            "transport": "csv", "warnings": [{
                                "code": "LOSSY_CSV_FALLBACK",
                                "message": "CSV import is a lossy compatibility fallback; typed values and provenance may not survive.",
                            }]})
        except Exception as e:
            try:
                conn.rollback()
                for table_name in (
                    safe_table_name("typed_import", import_id),
                    safe_table_name("raw", key),
                    safe_table_name("data", key),
                    "active",
                ):
                    conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
                conn.commit()
            except Exception:
                logger.exception("Failed to clean up partial Module 2 typed import")
            logger.error("Failed to import data: %s", e, exc_info=True)
            return jsonify({"error": f"Failed to import data: {str(e)}"}), 500


# ── Download / Transfer ────────────────────────────────────────────────────────


class _TypedTransportUnavailable(RuntimeError):
    pass


def _export_typed_transfer(conn, *, filename: str, session_id: str):
    api = _typed_artifact_module()
    exporter = getattr(api, "export_duckdb_parquet", None) if api else None
    if not callable(exporter):
        raise _TypedTransportUnavailable("typed transport exporter is unavailable")
    from backend.ingestion.models import OrderedColumnSchema, ValueType
    from backend.ingestion.transfer import TransferManifest, TransferTable
    schema_rows = conn.execute('PRAGMA table_info("active")').fetchall()
    type_map = {
        "BOOL": ValueType.BOOLEAN, "INTEGER": ValueType.INTEGER, "BIGINT": ValueType.INTEGER,
        "DECIMAL": ValueType.DECIMAL, "DOUBLE": ValueType.FLOAT, "FLOAT": ValueType.FLOAT,
        "DATE": ValueType.DATE, "TIME": ValueType.TIME, "TIMESTAMP": ValueType.TIMESTAMP,
    }
    columns = tuple(
        OrderedColumnSchema(
            key=str(row[1]), display_name=str(row[1]), ordinal=index,
            value_type=next((value for token, value in type_map.items() if token in str(row[2]).upper()), ValueType.TEXT),
            physical_name=str(row[1]),
        )
        for index, row in enumerate(schema_rows)
    )
    row_count = int(conn.execute('SELECT COUNT(*) FROM "active"').fetchone()[0])
    manifest = TransferManifest(
        f"module2:{session_id}", "module2", "module3",
        (TransferTable("active", columns, row_count),),
    )
    result = _call_typed_api(
        exporter, conn=conn, connection=conn, table_name="active",
        filename=filename, session_id=session_id, manifest=manifest,
    )
    payload = _typed_result_field(result, "payload", "bytes", "artifact")
    manifest = _typed_result_field(result, "manifest", "transport_manifest")
    if isinstance(result, (tuple, list)) and len(result) >= 2:
        if hasattr(result[0], "verify_payload"):
            manifest, payload = result[0], result[1]
        else:
            payload, manifest = result[0], result[1]
    if not isinstance(payload, (bytes, bytearray, memoryview)) or manifest is None:
        raise _TypedTransportUnavailable("typed exporter returned an incomplete artifact")
    if isinstance(manifest, (bytes, bytearray, memoryview, str)):
        raise _TypedTransportUnavailable("typed exporter returned an unbound manifest")
    # Return the bound manifest object; serialization is an HTTP transport
    # concern and happens immediately before multipart submission.
    return manifest, bytes(payload)


def _typed_destination_unsupported(resp) -> bool:
    try:
        body = resp.json() or {}
    except Exception:
        return False
    code = str(body.get("code", body.get("error_code", ""))).lower()
    return code in {"unsupported_typed_artifact", "typed_unsupported", "typed_import_unsupported", "unsupported_typed_transport"}

@app.route('/api/transfer-to-analyzer', methods=['POST'])
def transfer_to_analyzer():
    """Send the current normalised DataFrame to the Summarization Module (Module 3)."""
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        if not table_exists(conn, "active"):
            return jsonify({"ok": False, "error": "No active dataset to transfer"}), 400
        filename = get_meta(conn, "filename") or "normalized_data"

    try:
        base_name = filename.rsplit('.', 1)[0] if filename else "normalized_data"
        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            typed_manifest, typed_payload = _export_typed_transfer(
                conn, filename=f"{base_name}_normalized.parquet", session_id=session_id,
            )
            typed_api = _typed_artifact_module()
            manifest_bytes = typed_api.serialize_transport(typed_manifest)

        typed_resp = _requests.post(
            f"{_analyzer_be()}/import",
            files={
                "file": (f"{base_name}_normalized.parquet", io.BytesIO(typed_payload), "application/vnd.apache.parquet"),
                "manifest": (f"{base_name}_normalized.manifest.json", io.BytesIO(manifest_bytes), "application/json"),
            },
            timeout=120,
        )
        if typed_resp.status_code == 200:
            data = typed_resp.json()
            if not data.get("sessionId"):
                return jsonify({"ok": False, "error": "Analyzer returned no session for typed transfer", "code": "typed_destination_invalid"}), 502
            return jsonify({"ok": True, "analyzerSessionId": data.get("sessionId"), "transport": "typed"})
        if not _typed_destination_unsupported(typed_resp):
            err = typed_resp.json().get("error", typed_resp.text) if typed_resp.headers.get("content-type", "").startswith("application/json") else typed_resp.text
            return jsonify({"ok": False, "error": f"Analyzer typed upload failed: {err}", "code": "typed_destination_failed"}), 502

        # CSV is permitted only for an explicit destination capability refusal.
        with get_session_lock(session_id):
            conn = get_session_db(session_id)
            df = sqlite_to_df(conn, "active")
            if df is None:
                return jsonify({"ok": False, "error": "No active dataset to transfer"}), 400
        csv_str = df.to_csv(index=False, na_rep="")
        csv_bytes = csv_str.encode("utf-8")
        fname = f"{base_name}_normalized.csv"
        resp = _requests.post(
            f"{_analyzer_be()}/import",
            files={"file": (fname, io.BytesIO(csv_bytes), "text/csv")},
            timeout=120,
        )
        if resp.status_code != 200:
            err = resp.json().get("error", resp.text) if resp.headers.get("content-type", "").startswith("application/json") else resp.text
            return jsonify({"ok": False, "error": f"Analyzer upload failed: {err}"}), 502
        data = resp.json()
        return jsonify({"ok": True, "analyzerSessionId": data.get("sessionId"), "transport": "csv",
                        "warnings": [{"code": "LOSSY_CSV_FALLBACK",
                                      "message": "Destination does not support typed transfer; CSV fallback may lose types and provenance."}]})
    except Exception as e:
        import traceback
        traceback.print_exc()
        err_str = str(e).lower()
        if "connection" in err_str or "refused" in err_str or "httpconnectionpool" in err_str:
            return jsonify({"ok": False, "error": "Cannot reach the Data Analyzer backend. Is it running?"}), 502
        if isinstance(e, _TypedTransportUnavailable):
            return jsonify({"ok": False, "error": str(e), "code": "typed_export_unavailable"}), 503
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/api/download', methods=['GET'])
def download():
    """Download the active table as CSV."""
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        if not table_exists(conn, "active"):
            return jsonify({"error": "No file loaded"}), 400

        filename = get_meta(conn, "filename") or "normalized_data"
        base = filename.rsplit('.', 1)[0] if '.' in filename else filename

        df = sqlite_to_df(conn, "active")
        if df is None:
            return jsonify({"error": "No data available"}), 400

    buf = io.BytesIO()
    df.to_csv(buf, index=False, na_rep="")
    buf.seek(0)

    return send_file(
        buf,
        download_name=f"{base}_normalized.csv",
        as_attachment=True,
        mimetype="text/csv",
    )


@app.route('/api/supported-currencies', methods=['GET'])
def supported_currencies_api():
    """Return the list of currency codes available in the FX rates table."""
    try:
        fx_data = load_fx_table()
        currencies = sorted(set(fx_data[2]) | {"USD"})
        return jsonify({"currencies": currencies})
    except Exception as e:
        return jsonify({"error": str(e), "currencies": ["USD"]}), 200


if __name__ == '__main__':
    port = int(os.environ.get("FLASK_PORT", "5000"))
    app.run(host='0.0.0.0', port=port, debug=os.environ.get("FLASK_DEBUG", "0") == "1")
