import os
import io
import re as _re
import warnings
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
import numpy as np
from dotenv import load_dotenv

from agents.normalization import (
    normalize_supplier_name_agent,
    normalize_supplier_country_agent,
    date_normalization_agent,
    payment_terms_agent,
    normalize_region_agent,
    normalize_plant_agent,
    normalize_spend_agent
)

warnings.filterwarnings('ignore')
load_dotenv()

app = Flask(__name__)
CORS(app)
app.config['MAX_CONTENT_LENGTH'] = 500 * 1024 * 1024

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

class AppState:
    def __init__(self):
        self.df = None
        self.filename = None
        self.data_vault = {}

state = AppState()

def get_api_key():
    try:
        data = request.get_json(silent=True) or {}
        key = data.get('apiKey')
        if key: return key
    except Exception:
        pass
    return os.getenv('OPENAI_API_KEY', '')

@app.route('/api/status', methods=['GET'])
def health_check():
    return jsonify({"status": "ok"})

@app.route('/api/upload', methods=['POST'])
def upload_file():
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400
    
    file = request.files['file']
    filename = file.filename.lower()
    buffer = file.read()
    
    state.data_vault = {} 
    inventory = []
    
    try:
        # Check if the file is a ZIP archive
        if filename.endswith('.zip') or file.content_type in ['application/zip', 'application/x-zip-compressed']:
            with zipfile.ZipFile(io.BytesIO(buffer)) as zf:
                for entry in zf.infolist():
                    if entry.is_dir(): continue
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
                                    state.data_vault[key] = df
                                    inventory.append({"table_key": key, "rows": len(df), "cols": len(df.columns)})
                        elif lower.endswith('.csv'):
                            data = zf.read(name)
                            key = f"{name}::"
                            df = pd.read_csv(io.BytesIO(data), header=None)
                            if not df.empty:
                                state.data_vault[key] = df
                                inventory.append({"table_key": key, "rows": len(df), "cols": len(df.columns)})
                    except Exception as e:
                        print(f"Failed parsing {name}: {e}")
        else:
            # Fallback natively handles bare individual Excel or CSV uploads effortlessly
            try:
                if filename.endswith(('.xlsx', '.xlsm', '.xltx')):
                    excel_file = pd.ExcelFile(io.BytesIO(buffer), engine='calamine')
                    for sheet in excel_file.sheet_names:
                        key = f"{file.filename}::{sheet}"
                        df = pd.read_excel(excel_file, sheet_name=sheet, header=None)
                        if not df.empty:
                            state.data_vault[key] = df
                            inventory.append({"table_key": key, "rows": len(df), "cols": len(df.columns)})
                elif filename.endswith('.csv'):
                    key = f"{file.filename}::"
                    df = pd.read_csv(io.BytesIO(buffer), header=None)
                    if not df.empty:
                        state.data_vault[key] = df
                        inventory.append({"table_key": key, "rows": len(df), "cols": len(df.columns)})
            except Exception as e:
                print(f"Failed parsing directly uploaded file {file.filename}: {e}")
                
        if not inventory:
            return jsonify({"error": "No valid raw data tables found. Ensure files have valid columns and rows."}), 400
            
        return jsonify({
            "message": "Files extracted successfully",
            "inventory": inventory
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Failed to ingest package: {str(e)}"}), 500

@app.route('/api/get-raw-preview', methods=['POST', 'GET'])
def get_raw_preview():
    data = request.get_json(silent=True) or {}
    table_key = data.get('tableKey') or request.args.get('tableKey')
    if table_key not in state.data_vault:
        return jsonify({"error": "Table not found"}), 404
    
    df = state.data_vault[table_key]
    preview_df = df.head(50).astype(str).replace(["<NA>", "nan", "NaT", "None"], "")
    raw_list = preview_df.values.tolist()
    
    # If columns were explicitly set, inject them at row 0 so the editor can preview them natively
    columns = list(df.columns)
    if any(isinstance(c, str) for c in columns):
        raw_list.insert(0, columns)
        
    return jsonify({"rawPreview": raw_list})

@app.route('/api/get-preview', methods=['GET'])
def get_preview():
    table_key = request.args.get('tableKey')
    if table_key not in state.data_vault:
        return jsonify({"error": "Table not found"}), 404
        
    df = state.data_vault[table_key]
    return jsonify({
        "columns": list(df.columns),
        "rows": df.head(50).astype(str).replace(["<NA>", "nan", "NaT", "None"], "").to_dict(orient="records")
    })

@app.route('/api/current-preview', methods=['GET'])
def get_current_preview():
    if state.df is None:
        return jsonify({"error": "No active dataset loaded"}), 400

    preview_df = state.df.head(50).astype(str).replace(["<NA>", "nan", "NaT", "None"], "")
    return jsonify({
        "columns": [str(c) for c in state.df.columns],
        "rows": preview_df.to_dict(orient="records")
    })

def _suggest_columns(df: pd.DataFrame, sample_size: int = 100) -> dict:
    """
    Analyze the first `sample_size` rows of df and return the best candidate
    columns for date, currency code, and spend amount.
    Returns None for each field when no confident match is found.
    """
    sample = df.head(sample_size)
    cols = list(df.columns)

    # ── Helpers ──────────────────────────────────────────────────────────────
    def col_lower(c):
        return str(c).lower().strip()

    def pct_populated(series):
        """Fraction of non-null, non-empty string rows."""
        s = series.astype(str).str.strip()
        valid = (series.notna()) & (s != "") & (s != "nan") & (s != "None") & (s != "<NA>")
        return valid.sum() / len(series) if len(series) > 0 else 0.0

    # ── DATE ─────────────────────────────────────────────────────────────────
    # Same detection keywords as date_normalization_agent
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
        # Retry with dayfirst=True for DD/MM/YYYY formats
        if pct < 0.60:
            parsed2 = pd.to_datetime(s_clean, errors="coerce", dayfirst=True)
            pct = max(pct, parsed2.notna().sum() / len(s_clean))
        if pct >= 0.60 and pct > best_date_pct:
            best_date_pct = pct
            best_date = c

    # ── CURRENCY CODE ────────────────────────────────────────────────────────
    # Header keywords with weights (higher = stronger signal)
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
        # Find best matching keyword weight
        header_w = 0.0
        for kw, wt in CCY_HEADER_WEIGHTS.items():
            if kw in cl:
                header_w = max(header_w, wt)
        if header_w == 0.0:
            continue
        # Sample up to 20 non-null values, check for 3-letter uppercase codes
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

    # ── SPEND AMOUNT ─────────────────────────────────────────────────────────
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

        # Strip currency symbols and handle parentheses negatives
        cleaned = s_str.str.replace(STRIP_RE, "", regex=True)
        paren_mask = cleaned.str.match(r"^\(.+\)$")
        cleaned = cleaned.where(~paren_mask, "-" + cleaned.str[1:-1])
        numeric = pd.to_numeric(cleaned, errors="coerce")

        pct_numeric = numeric.notna().sum() / total if total > 0 else 0.0
        # Monetary bonus: most values positive (not a ratio or index column)
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


@app.route('/api/suggest-columns', methods=['GET'])
def suggest_columns():
    if state.df is None:
        return jsonify({"error": "No active dataset loaded"}), 400
    try:
        result = _suggest_columns(state.df, sample_size=100)
        return jsonify(result)
    except Exception as e:
        import traceback; traceback.print_exc()
        return jsonify({"error": str(e)}), 500


@app.route('/api/set-header-row', methods=['POST'])
def set_header_row():
    data = request.json
    table_key = data.get('tableKey')
    row_index = data.get('rowIndex')
    custom_names = data.get('customNames', {})
    
    if table_key not in state.data_vault: return jsonify({"error": "Table not found"}), 404
    df = state.data_vault[table_key]
    
    if row_index is None or not (0 <= row_index < len(df)):
        return jsonify({"error": "Invalid row index"}), 400
        
    new_headers = df.iloc[row_index].fillna("").astype(str).tolist()
    for col_idx_str, custom_name in custom_names.items():
        try:
            col_idx = int(col_idx_str)
            if custom_name.strip(): new_headers[col_idx] = custom_name.strip()
        except ValueError:
            pass
            
    final_headers = []
    seen = set()
    for i, h in enumerate(new_headers):
        h = h.strip()
        if not h: h = f"Unnamed_{i}"
        
        original = h
        counter = 1
        while h in seen:
            h = f"{original}_{counter}"
            counter += 1
            
        seen.add(h)
        final_headers.append(h)
        
    df.columns = final_headers
    state.data_vault[table_key] = df.iloc[row_index + 1:].reset_index(drop=True)
    
    return jsonify({"message": "Header row updated successfully", "rows": len(state.data_vault[table_key]), "columns": list(state.data_vault[table_key].columns)})

@app.route('/api/delete-rows', methods=['POST'])
def delete_rows():
    data = request.json
    table_key = data.get('tableKey')
    row_ids = data.get('rowIds', [])
    
    if table_key not in state.data_vault: return jsonify({"error": "Table not found"}), 404
    df = state.data_vault[table_key]
    
    indices_to_drop = [int(i) for i in row_ids if i in df.index]
    state.data_vault[table_key] = df.drop(indices_to_drop).reset_index(drop=True)
    
    return jsonify({"message": f"Deleted {len(row_ids)} rows successfully", "rows": len(state.data_vault[table_key])})

@app.route('/api/delete-table', methods=['POST'])
def delete_table():
    table_key = request.json.get('tableKey')
    if table_key in state.data_vault: del state.data_vault[table_key]
    return jsonify({"message": "Table deleted"})

@app.route('/api/select-table', methods=['POST'])
def select_table():
    table_key = request.json.get('tableKey')
    if table_key not in state.data_vault: return jsonify({"error": "Table not found"}), 404
        
    state.df = state.data_vault[table_key].copy()
    state.filename = table_key.split('::')[0]
    
    # Return 10-row preview for UI
    preview_df = state.df.head(10).fillna("").astype(str)
    
    return jsonify({
        "columns": [str(c) for c in state.df.columns],
        "rows": len(state.df),
        "previewData": preview_df.to_dict(orient='records')
    })

@app.route('/api/assess-supplier-country', methods=['POST'])
def assess_supplier_country_api():
    if state.df is None:
        return jsonify({"error": "No file loaded"}), 400

    data = request.json
    kwargs = data.get('kwargs', {})

    try:
        from agents.normalization import assess_supplier_country
        result = assess_supplier_country(state.df, **kwargs)
        return jsonify(result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/assess-currency-conversion', methods=['POST'])
def assess_currency_conversion_api():
    if state.df is None:
        return jsonify({"error": "No file loaded"}), 400
        
    data = request.json
    kwargs = data.get('kwargs', {})
    
    try:
        from agents.normalization import assess_currency_conversion
        result = assess_currency_conversion(state.df, **kwargs)
        return jsonify(result)
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": str(e)}), 500

@app.route('/api/run-normalization', methods=['POST'])
def run_normalization():
    if state.df is None:
        return jsonify({"error": "No file loaded"}), 400
        
    data = request.json
    agent_id = data.get('agent_id')
    
    if agent_id not in AGENT_MAPPING:
        return jsonify({"error": f"Invalid agent ID: {agent_id}"}), 400
        
    agent_func = AGENT_MAPPING[agent_id]
    api_key = get_api_key()
    
    kwargs = data.get('kwargs', {})
    
    try:
        result = agent_func(state.df, api_key=api_key, **kwargs)
        
        if isinstance(result, tuple) and len(result) >= 2:
            modified_df, message = result[0], result[1]
            state.df = modified_df
            response = {"message": message, "columns": list(state.df.columns)}
            if len(result) >= 4 and result[3] is not None:
                if agent_id == "currency_conversion":
                    response["conversion_metrics"] = result[3]
                elif agent_id == "supplier_country":
                    response["country_norm_metrics"] = result[3]
            return jsonify(response)
        else:
            return jsonify({"error": "Unexpected return format from agent"}), 500
            
    except Exception as e:
         return jsonify({"error": str(e)}), 500

ANALYZER_BE = os.environ.get("ANALYZER_BACKEND_URL", "http://localhost:3005")


@app.route('/api/import-from-stitcher', methods=['POST'])
def import_from_stitcher():
    """Accept a CSV file from DataStitcher (Module 1) with headers already in row 0."""
    if 'file' not in request.files:
        return jsonify({"error": "No file provided"}), 400

    file = request.files['file']
    buffer = file.read()
    fname = file.filename or "imported.csv"

    state.data_vault = {}
    inventory = []

    try:
        key = f"{fname}::"
        df = pd.read_csv(io.BytesIO(buffer))
        if df.empty:
            return jsonify({"error": "Imported file contains no data"}), 400
        state.data_vault[key] = df
        inventory.append({"table_key": key, "rows": len(df), "cols": len(df.columns)})

        return jsonify({"inventory": inventory, "imported": True})
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({"error": f"Failed to import data: {str(e)}"}), 500


@app.route('/api/current-inventory', methods=['GET'])
def current_inventory():
    """Return the current data_vault as an inventory list (used after external import)."""
    inventory = []
    for key, df in state.data_vault.items():
        inventory.append({"table_key": key, "rows": len(df), "cols": len(df.columns)})
    return jsonify({"inventory": inventory})


@app.route('/api/transfer-to-analyzer', methods=['POST'])
def transfer_to_analyzer():
    """Send the current normalised DataFrame to the Summarization Module (Module 3)."""
    if state.df is None:
        return jsonify({"ok": False, "error": "No active dataset to transfer"}), 400

    try:
        csv_str = state.df.to_csv(index=False)
        csv_buf = io.BytesIO(csv_str.encode("utf-8"))

        fname = (state.filename.rsplit('.', 1)[0] + "_normalized.csv") if state.filename else "normalized_data.csv"

        resp = _requests.post(
            f"{ANALYZER_BE}/api/upload",
            files={"file": (fname, csv_buf, "text/csv")},
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
    if state.df is None:
        return jsonify({"error": "No file loaded"}), 400
        
    buffer = io.BytesIO()
    with pd.ExcelWriter(buffer, engine='xlsxwriter') as writer:
        state.df.to_excel(writer, index=False, sheet_name='Normalized Data')
        
    buffer.seek(0)
    
    download_name = state.filename.rsplit('.', 1)[0] + "_normalized.xlsx" if state.filename else "normalized_data.xlsx"
    
    return send_file(
        buffer,
        download_name=download_name,
        as_attachment=True,
        mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
    )

if __name__ == '__main__':
    port = int(os.environ.get("FLASK_PORT", "5000"))
    app.run(host='0.0.0.0', port=port, debug=True)
