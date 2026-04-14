import atexit
import json
import logging
import math
import os
import io
import re as _re
import threading
import uuid
import warnings
from flask import Flask, request, jsonify, send_file, g
from flask.json.provider import DefaultJSONProvider
from flask_cors import CORS
from openpyxl import Workbook
import pandas as pd
import numpy as np
from dotenv import load_dotenv

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
    DB_DIR,
)
from db.bridge import sqlite_to_df, df_to_sqlite, PREVIEW_POOL, pick_best_df_rows


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

import sys as _sys
warnings.filterwarnings('ignore')
if not getattr(_sys, "frozen", False):
    load_dotenv()

app = Flask(__name__)
app.json = SafeJSONProvider(app)
CORS(app)

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

# ── Session startup / shutdown cleanup ─────────────────────────────────────────

_startup_cleaned = cleanup_all_sessions()
if _startup_cleaned:
    logger.info("[Module-2] Startup: cleared %d leftover session(s).", _startup_cleaned)


def _on_exit():
    cleaned = cleanup_all_sessions()
    logger.info("[Module-2] Shutdown: deleted %d session(s).", cleaned)


atexit.register(_on_exit)


# ── Helpers ────────────────────────────────────────────────────────────────────

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
    """Read API key from JSON body or environment."""
    try:
        data = request.get_json(silent=True) or {}
        key = data.get('apiKey')
        if key:
            return key
    except Exception:
        pass
    return os.getenv('OPENAI_API_KEY', '')


# ── Fast XLSX writer + per-session disk cache ──────────────────────────────────

def _build_xlsx_buf(df: pd.DataFrame) -> io.BytesIO:
    """Build XLSX in memory using openpyxl write_only."""
    CHUNK = 5000
    wb = Workbook(write_only=True)
    ws = wb.create_sheet("Normalized Data")
    ws.append([str(c) for c in df.columns])
    for start in range(0, len(df), CHUNK):
        chunk = df.iloc[start:start + CHUNK]
        for row in chunk.itertuples(index=False, name=None):
            ws.append([None if (isinstance(v, float) and pd.isna(v)) else v for v in row])
    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)
    return buf


def _xlsx_cache_path(session_id: str) -> str:
    """Return the file path for a session's cached XLSX."""
    return os.path.join(DB_DIR, f"{session_id}.xlsx")


_xlsx_gen: dict[str, int] = {}
_xlsx_threads: dict[str, threading.Thread] = {}


def _pregenerate_xlsx_to_disk(session_id: str, df_snapshot: pd.DataFrame, gen_id: int):
    """Background thread target: build XLSX and write to disk."""
    try:
        buf = _build_xlsx_buf(df_snapshot)
        if _xlsx_gen.get(session_id) == gen_id:
            path = _xlsx_cache_path(session_id)
            with open(path, "wb") as f:
                f.write(buf.getvalue())
            logger.info("XLSX cache written for session %s (gen=%s, %s bytes)", session_id, gen_id, len(buf.getvalue()))
    except Exception as e:
        logger.error("XLSX pre-generation failed for session %s: %s", session_id, e)


def _trigger_xlsx_cache(conn, session_id: str):
    """Snapshot the active table and start background XLSX generation to disk."""
    df = sqlite_to_df(conn, "active")
    if df is None or df.empty:
        return
    gen = _xlsx_gen.get(session_id, 0) + 1
    _xlsx_gen[session_id] = gen
    # Remove stale cache file
    try:
        os.unlink(_xlsx_cache_path(session_id))
    except OSError:
        pass
    t = threading.Thread(target=_pregenerate_xlsx_to_disk, args=(session_id, df, gen), daemon=True)
    _xlsx_threads[session_id] = t
    t.start()


def _get_xlsx_bytes(conn, session_id: str) -> io.BytesIO:
    """Return XLSX bytes — waits for background thread or builds on the fly."""
    t = _xlsx_threads.get(session_id)
    if t is not None and t.is_alive():
        logger.info("Download requested — waiting for background XLSX generation…")
        t.join()
    path = _xlsx_cache_path(session_id)
    if os.path.exists(path):
        with open(path, "rb") as f:
            return io.BytesIO(f.read())
    df = sqlite_to_df(conn, "active")
    if df is None:
        return io.BytesIO()
    return _build_xlsx_buf(df)


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
def health_check():
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
                            if lower.endswith(('.xlsx', '.xlsm', '.xltx')):
                                data = zf.read(name)
                                excel_file = pd.ExcelFile(io.BytesIO(data), engine='calamine')
                                for sheet in excel_file.sheet_names:
                                    key = f"{name}::{sheet}"
                                    df = pd.read_excel(excel_file, sheet_name=sheet, header=None)
                                    if not df.empty:
                                        raw_sql = safe_table_name("raw", key)
                                        data_sql = safe_table_name("data", key)
                                        df_to_sqlite(conn, raw_sql, df, commit=False)
                                        df_to_sqlite(conn, data_sql, df, commit=False)
                                        register_table(conn, key, data_sql, commit=False)
                                        inventory.append({"table_key": key, "rows": len(df), "cols": len(df.columns)})
                            elif lower.endswith('.csv'):
                                data = zf.read(name)
                                key = f"{name}::"
                                df = pd.read_csv(io.BytesIO(data), header=None)
                                if not df.empty:
                                    raw_sql = safe_table_name("raw", key)
                                    data_sql = safe_table_name("data", key)
                                    df_to_sqlite(conn, raw_sql, df, commit=False)
                                    df_to_sqlite(conn, data_sql, df, commit=False)
                                    register_table(conn, key, data_sql, commit=False)
                                    inventory.append({"table_key": key, "rows": len(df), "cols": len(df.columns)})
                        except Exception as e:
                            logger.error("Failed parsing %s: %s", name, e, exc_info=True)
            else:
                try:
                    if filename.endswith(('.xlsx', '.xlsm', '.xltx')):
                        excel_file = pd.ExcelFile(io.BytesIO(buffer), engine='calamine')
                        for sheet in excel_file.sheet_names:
                            key = f"{file.filename}::{sheet}"
                            df = pd.read_excel(excel_file, sheet_name=sheet, header=None)
                            if not df.empty:
                                raw_sql = safe_table_name("raw", key)
                                data_sql = safe_table_name("data", key)
                                df_to_sqlite(conn, raw_sql, df, commit=False)
                                df_to_sqlite(conn, data_sql, df, commit=False)
                                register_table(conn, key, data_sql, commit=False)
                                inventory.append({"table_key": key, "rows": len(df), "cols": len(df.columns)})
                    elif filename.endswith('.csv'):
                        key = f"{file.filename}::"
                        df = pd.read_csv(io.BytesIO(buffer), header=None)
                        if not df.empty:
                            raw_sql = safe_table_name("raw", key)
                            data_sql = safe_table_name("data", key)
                            df_to_sqlite(conn, raw_sql, df, commit=False)
                            df_to_sqlite(conn, data_sql, df, commit=False)
                            register_table(conn, key, data_sql, commit=False)
                            inventory.append({"table_key": key, "rows": len(df), "cols": len(df.columns)})
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
        df = sqlite_to_df(conn, "active")
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
        inventory = []
        for entry in entries:
            rows = table_row_count(conn, entry["sql_name"])
            cols_list = read_table_columns(conn, entry["sql_name"])
            inventory.append({"table_key": entry["table_key"], "rows": rows, "cols": len(cols_list)})

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

        df = sqlite_to_df(conn, data_sql)
        indices_to_drop = [int(i) for i in row_ids if int(i) in df.index]
        df = df.drop(indices_to_drop).reset_index(drop=True)
        df_to_sqlite(conn, data_sql, df)

    return jsonify({"message": f"Deleted {len(row_ids)} rows successfully", "rows": len(df)})


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

        df = sqlite_to_df(conn, data_sql)
        df_to_sqlite(conn, "active", df)
        set_meta(conn, "active_table_key", table_key)
        fname = table_key.split('::')[0]
        set_meta(conn, "filename", fname)

        _trigger_xlsx_cache(conn, session_id)

    preview_df = pick_best_df_rows(df, 10).fillna("").astype(str)
    return jsonify({
        "columns": [str(c) for c in df.columns],
        "rows": len(df),
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

            if table_key:
                data_sql = lookup_sql_name(conn, table_key)
                if data_sql and table_exists(conn, data_sql):
                    df = sqlite_to_df(conn, data_sql)
                    df_to_sqlite(conn, "active", df)
                    _trigger_xlsx_cache(conn, session_id)
                    return jsonify({"ok": True, "rows": len(df) if df is not None else 0})

            entries = all_registered_tables(conn)
            if entries:
                first_sql = entries[0]["sql_name"]
                df = sqlite_to_df(conn, first_sql)
                df_to_sqlite(conn, "active", df)
                _trigger_xlsx_cache(conn, session_id)
                return jsonify({"ok": True, "rows": len(df) if df is not None else 0})

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
    # Also remove XLSX cache
    try:
        os.unlink(_xlsx_cache_path(session_id))
    except OSError:
        pass
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
                _trigger_xlsx_cache(conn, session_id)
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

ANALYZER_BE = os.environ.get("ANALYZER_BACKEND_URL", "http://localhost:3005")


@app.route('/api/import-from-stitcher', methods=['POST'])
def import_from_stitcher():
    """Accept a CSV file from DataStitcher (Module 1) with headers already in row 0."""
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files['file']
    buffer = file.read()
    fname = file.filename or "imported.csv"

    import_id = str(uuid.uuid4())
    session_id = import_id

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        for entry in all_registered_tables(conn):
            drop_table(conn, entry["sql_name"], commit=False)
            unregister_table(conn, entry["table_key"], commit=False)
        conn.commit()

        try:
            key = f"{fname}::"
            df = pd.read_csv(io.BytesIO(buffer))
            if df.empty:
                return jsonify({"error": "Imported file contains no data"}), 400

            data_sql = safe_table_name("data", key)
            df_to_sqlite(conn, data_sql, df)
            register_table(conn, key, data_sql)
            set_meta(conn, "import_id", import_id)

            inventory = [{"table_key": key, "rows": len(df), "cols": len(df.columns)}]
            return jsonify({"inventory": inventory, "imported": True, "sessionId": import_id})
        except Exception as e:
            logger.error("Failed to import data: %s", e, exc_info=True)
            return jsonify({"error": f"Failed to import data: {str(e)}"}), 500


# ── Download / Transfer ────────────────────────────────────────────────────────

@app.route('/api/transfer-to-analyzer', methods=['POST'])
def transfer_to_analyzer():
    """Send the current normalised DataFrame to the Summarization Module (Module 3)."""
    try:
        session_id = _get_session_id()
    except ValueError as e:
        return jsonify({"error": str(e)}), 400

    with get_session_lock(session_id):
        conn = get_session_db(session_id)
        df = sqlite_to_df(conn, "active")
        if df is None:
            return jsonify({"ok": False, "error": "No active dataset to transfer"}), 400
        filename = get_meta(conn, "filename") or "normalized_data"

    try:
        csv_str = df.to_csv(index=False, na_rep="")
        csv_bytes = csv_str.encode("utf-8")
        fname = (filename.rsplit('.', 1)[0] + "_normalized.csv") if filename else "normalized_data.csv"

        resp = _requests.post(
            f"{ANALYZER_BE}/api/import",
            files={"file": (fname, io.BytesIO(csv_bytes), "text/csv")},
            timeout=120,
        )
        if resp.status_code != 200:
            err = resp.json().get("error", resp.text) if resp.headers.get("content-type", "").startswith("application/json") else resp.text
            return jsonify({"ok": False, "error": f"Analyzer upload failed: {err}"}), 502

        data = resp.json()
        return jsonify({"ok": True, "analyzerSessionId": data.get("sessionId")})
    except _requests.exceptions.ConnectionError:
        return jsonify({"ok": False, "error": "Cannot reach the Data Analyzer backend. Ensure it is running on port 3005."}), 502
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"ok": False, "error": str(e)}), 500


@app.route('/api/download', methods=['GET'])
def download():
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

        buf = _get_xlsx_bytes(conn, session_id)

    return send_file(
        buf,
        download_name=f"{base}_normalized.xlsx",
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
    )


if __name__ == '__main__':
    port = int(os.environ.get("FLASK_PORT", "5000"))
    app.run(host='0.0.0.0', port=port, debug=True)
