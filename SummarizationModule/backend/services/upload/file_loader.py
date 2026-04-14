import csv
import io
import os
import re
import sqlite3
import zipfile
from typing import Any

import pandas as pd


PREVIEW_POOL = 1000


def pick_best_rows(rows: list[dict], limit: int) -> list[dict]:
    """Return up to *limit* rows ranked by number of populated columns."""
    if len(rows) <= limit:
        return rows

    def _score(row: dict) -> int:
        return sum(
            1 for v in row.values()
            if v is not None and str(v).strip() != ""
        )

    return sorted(rows, key=_score, reverse=True)[:limit]


def pick_best_raw_rows(rows: list[list], limit: int) -> list[list]:
    """Same as pick_best_rows but for list-of-lists (raw preview) format."""
    if len(rows) <= limit:
        return rows

    def _score(row: list) -> int:
        return sum(
            1 for v in row
            if v is not None and str(v).strip() != ""
        )

    return sorted(rows, key=_score, reverse=True)[:limit]


# ──────────────────────────────────────────────
# Internal helpers
# ──────────────────────────────────────────────

def _safe_sql_name(raw: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_]", "_", raw)
    return cleaned[:120]


def _clean_header(h: str) -> str:
    return str(h).strip().upper() if h else "UNNAMED"


def _dedupe_headers(headers: list[str]) -> list[str]:
    seen: dict[str, int] = {}
    out: list[str] = []
    for h in headers:
        if h in seen:
            seen[h] += 1
            out.append(f"{h}_{seen[h]}")
        else:
            seen[h] = 0
            out.append(h)
    return out


def _ensure_registry(conn: sqlite3.Connection):
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _table_registry "
        "(table_key TEXT PRIMARY KEY, data_table TEXT, raw_table TEXT)"
    )
    conn.commit()


def _register_table(conn: sqlite3.Connection, table_key: str, data_table: str, raw_table: str):
    conn.execute(
        "INSERT OR REPLACE INTO _table_registry (table_key, data_table, raw_table) VALUES (?, ?, ?)",
        (table_key, data_table, raw_table),
    )


def _get_registry(conn: sqlite3.Connection) -> list[dict[str, str]]:
    _ensure_registry(conn)
    rows = conn.execute("SELECT table_key, data_table, raw_table FROM _table_registry ORDER BY rowid").fetchall()
    return [{"table_key": r[0], "data_table": r[1], "raw_table": r[2]} for r in rows]


def _unregister_table(conn: sqlite3.Connection, table_key: str):
    conn.execute("DELETE FROM _table_registry WHERE table_key = ?", (table_key,))


# ──────────────────────────────────────────────
# Parsing
# ──────────────────────────────────────────────

def _parse_csv_bytes(data: bytes, filename: str) -> tuple[list[list[Any]], list[str]]:
    """Returns (raw_grid, header_names). raw_grid includes the header row."""
    text = data.decode("utf-8", errors="replace")
    if text.startswith("\ufeff"):
        text = text[1:]
    reader = csv.reader(io.StringIO(text))
    raw_grid: list[list[Any]] = []
    for row in reader:
        raw_grid.append(row)
    if not raw_grid:
        return [], []
    raw_headers = raw_grid[0]
    headers = _dedupe_headers([_clean_header(h) for h in raw_headers])
    return raw_grid, headers


def _parse_excel_bytes(data: bytes, filename: str) -> dict[str, tuple[list[list[Any]], list[str]]]:
    """Returns {sheet_name: (raw_grid, header_names)}. Uses calamine (Rust) for speed."""
    excel_file = pd.ExcelFile(io.BytesIO(data), engine="calamine")
    result: dict[str, tuple[list[list[Any]], list[str]]] = {}
    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
        if df.empty:
            continue
        raw_grid = []
        for row in df.itertuples(index=False, name=None):
            raw_grid.append([str(c) if not pd.isna(c) else "" for c in row])
        raw_headers = raw_grid[0]
        headers = _dedupe_headers([_clean_header(h) for h in raw_headers])
        if headers:
            result[sheet_name] = (raw_grid, headers)
    excel_file.close()
    return result


# ──────────────────────────────────────────────
# Storage: raw + data tables
# ──────────────────────────────────────────────

_BATCH_SIZE = 5000


def _store_raw_table(conn: sqlite3.Connection, table_name: str, raw_grid: list[list[Any]]):
    """Store the raw grid as RAW_0, RAW_1, ... columns (all TEXT)."""
    if not raw_grid:
        return
    max_cols = max(len(r) for r in raw_grid)
    col_defs = ", ".join(f'"RAW_{i}" TEXT' for i in range(max_cols))
    conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    conn.execute(f'CREATE TABLE "{table_name}" ({col_defs})')
    placeholders = ", ".join("?" for _ in range(max_cols))
    sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
    batch: list[tuple] = []
    for row in raw_grid:
        padded = list(row) + [""] * (max_cols - len(row))
        batch.append(tuple(padded))
        if len(batch) >= _BATCH_SIZE:
            conn.executemany(sql, batch)
            batch.clear()
    if batch:
        conn.executemany(sql, batch)


def _store_data_table(conn: sqlite3.Connection, table_name: str, headers: list[str], data_rows: list[list[Any]]):
    """Store parsed data with proper column names (all TEXT). Adds RECORD_ID."""
    if not headers:
        return
    # Strip any existing RECORD_ID column to avoid duplicates (cross-module imports)
    rid_indices = [i for i, h in enumerate(headers) if h == "RECORD_ID"]
    if rid_indices:
        headers = [h for i, h in enumerate(headers) if i not in rid_indices]
        data_rows = [
            [v for i, v in enumerate(row) if i not in rid_indices]
            for row in data_rows
        ]
    all_headers = ["RECORD_ID"] + headers
    col_defs = ", ".join(f'"{h}" TEXT' for h in all_headers)
    conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
    conn.execute(f'CREATE TABLE "{table_name}" ({col_defs})')
    placeholders = ", ".join("?" for _ in all_headers)
    sql = f'INSERT INTO "{table_name}" VALUES ({placeholders})'
    num_headers = len(headers)
    record_id = 0
    batch: list[tuple] = []
    for row in data_rows:
        vals = [str(v).strip() if v is not None and str(v).strip() else "" for v in row]
        if not any(v for v in vals):
            continue
        padded = vals + [""] * (num_headers - len(vals))
        record_id += 1
        batch.append(tuple([str(record_id)] + padded[:num_headers]))
        if len(batch) >= _BATCH_SIZE:
            conn.executemany(sql, batch)
            batch.clear()
    if batch:
        conn.executemany(sql, batch)


def _build_table_key(zip_path: str, sheet_name: str | None) -> str:
    name = os.path.basename(zip_path) if zip_path else "file"
    if sheet_name:
        return f"{name}::{sheet_name}"
    return f"{name}::"


# ──────────────────────────────────────────────
# Load ZIP / single file
# ──────────────────────────────────────────────

def _set_bulk_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA synchronous = OFF")
    conn.execute("PRAGMA cache_size = -64000")
    conn.execute("PRAGMA temp_store = MEMORY")


def _restore_pragmas(conn: sqlite3.Connection) -> None:
    conn.execute("PRAGMA synchronous = NORMAL")


def load_zip_to_session(
    conn: sqlite3.Connection, file_data: bytes
) -> tuple[list[str], list[dict[str, str]]]:
    """Extract ZIP, parse files, store raw + data tables. Returns (table_keys, warnings)."""
    _ensure_registry(conn)
    _set_bulk_pragmas(conn)
    warnings: list[dict[str, str]] = []
    table_keys: list[str] = []

    with zipfile.ZipFile(io.BytesIO(file_data)) as zf:
        for entry in zf.namelist():
            if entry.startswith("__MACOSX") or entry.startswith("."):
                continue
            basename = os.path.basename(entry)
            if not basename:
                continue
            ext = os.path.splitext(basename)[1].lower()

            try:
                raw = zf.read(entry)
                if ext == ".csv":
                    raw_grid, headers = _parse_csv_bytes(raw, basename)
                    if not headers:
                        continue
                    table_key = _build_table_key(entry, None)
                    safe = _safe_sql_name(os.path.splitext(basename)[0])
                    data_tbl = f"data__{safe}"
                    raw_tbl = f"raw__{safe}"
                    _store_raw_table(conn, raw_tbl, raw_grid)
                    _store_data_table(conn, data_tbl, headers, raw_grid[1:])
                    _register_table(conn, table_key, data_tbl, raw_tbl)
                    table_keys.append(table_key)

                elif ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
                    sheets = _parse_excel_bytes(raw, basename)
                    for sheet_name, (raw_grid, headers) in sheets.items():
                        table_key = _build_table_key(entry, sheet_name)
                        safe = _safe_sql_name(f"{os.path.splitext(basename)[0]}__{sheet_name}")
                        data_tbl = f"data__{safe}"
                        raw_tbl = f"raw__{safe}"
                        _store_raw_table(conn, raw_tbl, raw_grid)
                        _store_data_table(conn, data_tbl, headers, raw_grid[1:])
                        _register_table(conn, table_key, data_tbl, raw_tbl)
                        table_keys.append(table_key)
            except Exception as exc:
                warnings.append({"file": basename, "message": str(exc)})

    conn.commit()
    _restore_pragmas(conn)
    return table_keys, warnings


def load_single_file(
    conn: sqlite3.Connection, filename: str, file_data: bytes
) -> tuple[list[str], list[dict[str, str]]]:
    """Parse a single CSV/Excel file. Returns (table_keys, warnings)."""
    _ensure_registry(conn)
    _set_bulk_pragmas(conn)
    warnings: list[dict[str, str]] = []
    table_keys: list[str] = []
    ext = os.path.splitext(filename)[1].lower()

    try:
        if ext == ".csv":
            raw_grid, headers = _parse_csv_bytes(file_data, filename)
            if headers:
                table_key = _build_table_key(filename, None)
                safe = _safe_sql_name(os.path.splitext(filename)[0])
                data_tbl = f"data__{safe}"
                raw_tbl = f"raw__{safe}"
                _store_raw_table(conn, raw_tbl, raw_grid)
                _store_data_table(conn, data_tbl, headers, raw_grid[1:])
                _register_table(conn, table_key, data_tbl, raw_tbl)
                table_keys.append(table_key)

        elif ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
            sheets = _parse_excel_bytes(file_data, filename)
            for sheet_name, (raw_grid, headers) in sheets.items():
                table_key = _build_table_key(filename, sheet_name)
                safe = _safe_sql_name(f"{os.path.splitext(filename)[0]}__{sheet_name}")
                data_tbl = f"data__{safe}"
                raw_tbl = f"raw__{safe}"
                _store_raw_table(conn, raw_tbl, raw_grid)
                _store_data_table(conn, data_tbl, headers, raw_grid[1:])
                _register_table(conn, table_key, data_tbl, raw_tbl)
                table_keys.append(table_key)
    except Exception as exc:
        warnings.append({"file": filename, "message": str(exc)})

    conn.commit()
    _restore_pragmas(conn)
    return table_keys, warnings


# ──────────────────────────────────────────────
# Column info (lightweight — no type inference)
# ──────────────────────────────────────────────

def collect_column_info(
    conn: sqlite3.Connection, table_keys: list[str]
) -> list[dict[str, Any]]:
    """Gather column names and sample values across all session tables.

    Returns [{"name": "COL", "sampleValues": [...]}].
    No type detection or parsing — just raw string samples so the AI
    can infer types from the values themselves.
    """
    registry = _get_registry(conn)
    data_tables = [r["data_table"] for r in registry if r["table_key"] in table_keys]

    all_columns: dict[str, list[str]] = {}
    for tname in data_tables:
        try:
            cursor = conn.execute(f'PRAGMA table_info("{tname}")')
        except Exception:
            continue
        cols = [row[1] for row in cursor.fetchall()]
        for col in cols:
            if col == "RECORD_ID":
                continue
            if col not in all_columns:
                all_columns[col] = []
            rows = conn.execute(
                f'SELECT DISTINCT "{col}" FROM "{tname}" '
                f'WHERE "{col}" IS NOT NULL AND TRIM("{col}") != "" '
                f"LIMIT 50"
            ).fetchall()
            all_columns[col].extend(r[0] for r in rows)

    results: list[dict[str, Any]] = []
    for col_name, values in all_columns.items():
        unique_vals = list(dict.fromkeys(values))[:50]
        results.append({
            "name": col_name,
            "sampleValues": unique_vals,
        })

    return results


# ──────────────────────────────────────────────
# Inventory & Preview (new format)
# ──────────────────────────────────────────────

def build_inventory(conn: sqlite3.Connection) -> list[dict[str, Any]]:
    """Returns [{table_key, rows, cols}, ...]"""
    registry = _get_registry(conn)
    inventory = []
    for entry in registry:
        tname = entry["data_table"]
        try:
            row_count = conn.execute(f'SELECT COUNT(*) FROM "{tname}"').fetchone()[0]
            cols = conn.execute(f'PRAGMA table_info("{tname}")').fetchall()
            col_names = [c[1] for c in cols if c[1] != "RECORD_ID"]
            inventory.append({
                "table_key": entry["table_key"],
                "rows": row_count,
                "cols": len(col_names),
            })
        except Exception:
            pass
    return inventory


def _safe_value(v: Any) -> Any:
    """Convert None or float NaN/Infinity to empty string for JSON safety."""
    if v is None:
        return ""
    if isinstance(v, float):
        import math
        if math.isnan(v) or math.isinf(v):
            return ""
    return v


def build_preview(conn: sqlite3.Connection, limit: int = 50) -> dict[str, dict[str, Any]]:
    """Returns {table_key: {columns: [...], rows: [{...}, ...]}}"""
    registry = _get_registry(conn)
    previews: dict[str, dict[str, Any]] = {}
    for entry in registry:
        tname = entry["data_table"]
        try:
            cursor = conn.execute(f'SELECT * FROM "{tname}" LIMIT {PREVIEW_POOL}')
            col_names = [desc[0] for desc in cursor.description]
            rows = cursor.fetchall()
            all_rows = [
                {c: _safe_value(v) for c, v in zip(col_names, row)}
                for row in rows
            ]
            previews[entry["table_key"]] = {
                "columns": col_names,
                "rows": pick_best_rows(all_rows, limit),
            }
        except Exception:
            pass
    return previews


def build_single_preview(conn: sqlite3.Connection, table_key: str, limit: int = 50) -> dict[str, Any] | None:
    """Build a preview for a single table_key. Returns None if not found."""
    registry = _get_registry(conn)
    entry = next((r for r in registry if r["table_key"] == table_key), None)
    if not entry:
        return None
    tname = entry["data_table"]
    try:
        cursor = conn.execute(f'SELECT * FROM "{tname}" LIMIT {PREVIEW_POOL}')
        col_names = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        all_rows = [
            {c: _safe_value(v) for c, v in zip(col_names, row)}
            for row in rows
        ]
        return {
            "columns": col_names,
            "rows": pick_best_rows(all_rows, limit),
        }
    except Exception:
        return None


# ──────────────────────────────────────────────
# Table operations (inventory management)
# ──────────────────────────────────────────────

def delete_table_from_session(conn: sqlite3.Connection, table_key: str):
    registry = _get_registry(conn)
    entry = next((r for r in registry if r["table_key"] == table_key), None)
    if not entry:
        return
    for tbl in [entry["data_table"], entry["raw_table"]]:
        try:
            conn.execute(f'DROP TABLE IF EXISTS "{tbl}"')
        except Exception:
            pass
    _unregister_table(conn, table_key)
    conn.commit()


def get_raw_preview(conn: sqlite3.Connection, table_key: str, limit: int = 50) -> list[list[Any]]:
    registry = _get_registry(conn)
    entry = next((r for r in registry if r["table_key"] == table_key), None)
    if not entry:
        return []
    raw_tbl = entry["raw_table"]
    try:
        cursor = conn.execute(f'SELECT * FROM "{raw_tbl}" LIMIT {PREVIEW_POOL}')
        rows = cursor.fetchall()
        all_rows = [list(r) for r in rows]
        return pick_best_raw_rows(all_rows, limit)
    except Exception:
        return []


def set_header_row_for_table(
    conn: sqlite3.Connection,
    table_key: str,
    header_row_index: int,
    custom_names: dict[int, str] | None = None,
):
    """Rebuild the data table using a different row from the raw table as headers."""
    registry = _get_registry(conn)
    entry = next((r for r in registry if r["table_key"] == table_key), None)
    if not entry:
        raise ValueError(f"Table key not found: {table_key}")

    raw_tbl = entry["raw_table"]
    data_tbl = entry["data_table"]

    cursor = conn.execute(f'SELECT * FROM "{raw_tbl}"')
    all_rows = cursor.fetchall()
    if header_row_index >= len(all_rows):
        raise ValueError(f"Row index {header_row_index} out of range (table has {len(all_rows)} rows)")

    raw_header_row = list(all_rows[header_row_index])
    headers = []
    for i, cell in enumerate(raw_header_row):
        if custom_names and i in custom_names and custom_names[i].strip():
            headers.append(_clean_header(custom_names[i]))
        elif cell is not None and str(cell).strip():
            headers.append(_clean_header(str(cell)))
        else:
            headers.append(f"COL_{i}")
    headers = _dedupe_headers(headers)

    data_rows = [list(r) for idx, r in enumerate(all_rows) if idx != header_row_index]

    _store_data_table(conn, data_tbl, headers, data_rows)
    conn.commit()


def delete_rows_from_table(
    conn: sqlite3.Connection,
    table_key: str,
    row_ids: list[str | int],
) -> int:
    """Delete rows by RECORD_ID from the data table. Returns count deleted."""
    registry = _get_registry(conn)
    entry = next((r for r in registry if r["table_key"] == table_key), None)
    if not entry:
        return 0
    data_tbl = entry["data_table"]
    placeholders = ", ".join("?" for _ in row_ids)
    str_ids = [str(rid) for rid in row_ids]
    try:
        cursor = conn.execute(
            f'DELETE FROM "{data_tbl}" WHERE RECORD_ID IN ({placeholders})', str_ids
        )
        conn.commit()
        return cursor.rowcount
    except Exception:
        return 0
