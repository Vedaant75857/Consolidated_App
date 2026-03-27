import os
import io
import warnings
from flask import Flask, request, jsonify, send_file
from flask_cors import CORS
import pandas as pd
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
                            excel_file = pd.ExcelFile(io.BytesIO(data))
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
                    excel_file = pd.ExcelFile(io.BytesIO(buffer))
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
    preview_df = df.head(50).fillna("").astype(str)
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
        "rows": df.head(50).fillna("").astype(str).to_dict(orient="records")
    })

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
            return jsonify({
                "message": message,
                "columns": list(state.df.columns)
            })
        else:
            return jsonify({"error": "Unexpected return format from agent"}), 500
            
    except Exception as e:
         return jsonify({"error": str(e)}), 500

@app.route('/api/run-pipeline', methods=['POST'])
def run_pipeline():
    if state.df is None:
        return jsonify({"error": "No file loaded"}), 400
        
    data = request.json
    agent_ids = data.get('agent_ids', [])
    api_key = get_api_key()
    
    results = []
    
    for agent_id in agent_ids:
        if agent_id in AGENT_MAPPING:
            agent_func = AGENT_MAPPING[agent_id]
            kwargs = data.get('kwargs', {}).get(agent_id, {})
            try:
                result = agent_func(state.df, api_key=api_key, **kwargs)
                if isinstance(result, tuple) and len(result) >= 2:
                    state.df = result[0]
                    results.append({"agent": agent_id, "message": result[1]})
            except Exception as e:
                results.append({"agent": agent_id, "error": str(e)})
                
    return jsonify({
        "message": "Pipeline completed",
        "results": results,
        "columns": list(state.df.columns)
    })

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
