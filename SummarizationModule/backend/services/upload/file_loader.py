import csv
import io
import logging
import os
import re
import zipfile
from typing import Any

import pandas as pd

from shared.duckdb_compat import DuckDBConnection

logger = logging.getLogger(__name__)


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


def _ensure_registry(conn: DuckDBConnection):
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _table_registry "
        "(table_key VARCHAR PRIMARY KEY, data_table VARCHAR, raw_table VARCHAR)"
    )
    conn.commit()


def _register_table(conn: DuckDBConnection, table_key: str, data_table: str, raw_table: str):
    conn.execute(
        "INSERT OR REPLACE INTO _table_registry (table_key, data_table, raw_table) VALUES (?, ?, ?)",
        (table_key, data_table, raw_table),
    )


def _get_registry(conn: DuckDBConnection) -> list[dict[str, str]]:
    _ensure_registry(conn)
    rows = conn.execute("SELECT table_key, data_table, raw_table FROM _table_registry ORDER BY table_key").fetchall()
    return [{"table_key": r[0], "data_table": r[1], "raw_table": r[2]} for r in rows]


def _unregister_table(conn: DuckDBConnection, table_key: str):
    conn.execute("DELETE FROM _table_registry WHERE table_key = ?", (table_key,))


# ──────────────────────────────────────────────
# Parsing
# ──────────────────────────────────────────────

def _parse_csv_bytes(data: bytes, filename: str) -> tuple[pd.DataFrame | None, list[str]]:
    """Returns (df, header_names). First row is kept in the DataFrame as raw data."""
    try:
        df = pd.read_csv(io.BytesIO(data), header=None, dtype=str,
                         keep_default_na=False, encoding_errors="replace")
    except Exception:
        return None, []
    if df.empty:
        return None, []
    raw_headers = [str(c) for c in df.iloc[0]]
    headers = _dedupe_headers([_clean_header(h) for h in raw_headers])
    return df, headers


def _parse_excel_bytes(data: bytes, filename: str) -> dict[str, tuple[pd.DataFrame, list[str]]]:
    """Returns {sheet_name: (df, header_names)}. Uses calamine (Rust) for speed.

    Returns DataFrames instead of Python grids for native DuckDB ingestion.
    """
    excel_file = pd.ExcelFile(io.BytesIO(data), engine="calamine")
    result: dict[str, tuple[pd.DataFrame, list[str]]] = {}
    for sheet_name in excel_file.sheet_names:
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
        if df.empty:
            continue
        raw_headers = [str(c) if not pd.isna(c) else "" for c in df.iloc[0]]
        headers = _dedupe_headers([_clean_header(h) for h in raw_headers])
        if headers:
            result[sheet_name] = (df, headers)
    excel_file.close()
    return result


# ──────────────────────────────────────────────
# Storage: raw + data tables
# ──────────────────────────────────────────────

def _store_df_native(conn: DuckDBConnection, table_name: str, df: pd.DataFrame):
    """Persist a DataFrame using DuckDB's zero-copy register path.

    All columns are cast to VARCHAR.
    """
    if df is None or df.empty:
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        return

    view_name = f"_tmp_df_{id(df)}"
    raw_conn = conn._conn
    raw_conn.register(view_name, df)
    try:
        conn.execute(f'DROP TABLE IF EXISTS "{table_name}"')
        cols = ", ".join(
            f'CAST("{c}" AS VARCHAR) AS "{c}"' for c in df.columns
        )
        conn.execute(f'CREATE TABLE "{table_name}" AS SELECT {cols} FROM "{view_name}"')
    finally:
        raw_conn.unregister(view_name)


def _store_raw_table(conn: DuckDBConnection, table_name: str, df: pd.DataFrame):
    """Store the raw DataFrame as RAW_0, RAW_1, ... columns using native ingestion."""
    if df is None or df.empty:
        return
    raw_df = df.copy()
    raw_df.columns = [f"RAW_{i}" for i in range(len(raw_df.columns))]
    _store_df_native(conn, table_name, raw_df)


def _store_data_table_from_raw(conn: DuckDBConnection, data_table: str, raw_table: str, headers: list[str]):
    """Build the data table from the raw table via SQL. Adds RECORD_ID.

    Skips the header row (row 1) and empty rows. No Python iteration.
    """
    if not headers:
        return
    # Strip RECORD_ID from headers AND corresponding raw columns in lockstep
    keep_indices = [i for i, h in enumerate(headers) if h != "RECORD_ID"]
    headers = [headers[i] for i in keep_indices]
    raw_cols = [f"RAW_{i}" for i in keep_indices]
    select_parts = ", ".join(
        f'CAST("{rc}" AS VARCHAR) AS "{hdr}"'
        for rc, hdr in zip(raw_cols, headers)
    )
    null_check = " AND ".join(
        f'("{rc}" IS NULL OR TRIM(CAST("{rc}" AS VARCHAR)) = \'\')'
        for rc in raw_cols
    )
    conn.execute(f'DROP TABLE IF EXISTS "{data_table}"')
    conn.execute(
        f'CREATE TABLE "{data_table}" AS '
        f"SELECT "
        f"  CAST(ROW_NUMBER() OVER () AS VARCHAR) AS \"RECORD_ID\", "
        f"  {select_parts} "
        f"FROM ("
        f'  SELECT *, ROW_NUMBER() OVER () AS _rn FROM "{raw_table}"'
        f") _sub "
        f"WHERE _rn > 1 "
        f"AND NOT ({null_check})"
    )


def _build_table_key(zip_path: str, sheet_name: str | None) -> str:
    name = os.path.basename(zip_path) if zip_path else "file"
    if sheet_name:
        return f"{name}::{sheet_name}"
    return f"{name}::"


# ──────────────────────────────────────────────
# Load ZIP / single file
# ──────────────────────────────────────────────

def load_zip_to_session(
    conn: DuckDBConnection, file_data: bytes
) -> tuple[list[str], list[dict[str, str]]]:
    """Extract ZIP, parse files, store raw + data tables. Returns (table_keys, warnings)."""
    _ensure_registry(conn)
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
                    df, headers = _parse_csv_bytes(raw, basename)
                    if df is None or not headers:
                        continue
                    table_key = _build_table_key(entry, None)
                    safe = _safe_sql_name(os.path.splitext(basename)[0])
                    data_tbl = f"data__{safe}"
                    raw_tbl = f"raw__{safe}"
                    _store_raw_table(conn, raw_tbl, df)
                    _store_data_table_from_raw(conn, data_tbl, raw_tbl, headers)
                    _register_table(conn, table_key, data_tbl, raw_tbl)
                    table_keys.append(table_key)

                elif ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
                    sheets = _parse_excel_bytes(raw, basename)
                    for sheet_name, (df, headers) in sheets.items():
                        table_key = _build_table_key(entry, sheet_name)
                        safe = _safe_sql_name(f"{os.path.splitext(basename)[0]}__{sheet_name}")
                        data_tbl = f"data__{safe}"
                        raw_tbl = f"raw__{safe}"
                        _store_raw_table(conn, raw_tbl, df)
                        _store_data_table_from_raw(conn, data_tbl, raw_tbl, headers)
                        _register_table(conn, table_key, data_tbl, raw_tbl)
                        table_keys.append(table_key)
            except Exception as exc:
                warnings.append({"file": basename, "message": str(exc)})

    conn.commit()
    return table_keys, warnings


def load_single_file(
    conn: DuckDBConnection, filename: str, file_data: bytes
) -> tuple[list[str], list[dict[str, str]]]:
    """Parse a single CSV/Excel file. Returns (table_keys, warnings)."""
    _ensure_registry(conn)
    warnings: list[dict[str, str]] = []
    table_keys: list[str] = []
    ext = os.path.splitext(filename)[1].lower()

    try:
        if ext == ".csv":
            df, headers = _parse_csv_bytes(file_data, filename)
            if df is not None and headers:
                table_key = _build_table_key(filename, None)
                safe = _safe_sql_name(os.path.splitext(filename)[0])
                data_tbl = f"data__{safe}"
                raw_tbl = f"raw__{safe}"
                _store_raw_table(conn, raw_tbl, df)
                _store_data_table_from_raw(conn, data_tbl, raw_tbl, headers)
                _register_table(conn, table_key, data_tbl, raw_tbl)
                table_keys.append(table_key)

        elif ext in (".xlsx", ".xlsm", ".xltx", ".xltm"):
            sheets = _parse_excel_bytes(file_data, filename)
            for sheet_name, (df, headers) in sheets.items():
                table_key = _build_table_key(filename, sheet_name)
                safe = _safe_sql_name(f"{os.path.splitext(filename)[0]}__{sheet_name}")
                data_tbl = f"data__{safe}"
                raw_tbl = f"raw__{safe}"
                _store_raw_table(conn, raw_tbl, df)
                _store_data_table_from_raw(conn, data_tbl, raw_tbl, headers)
                _register_table(conn, table_key, data_tbl, raw_tbl)
                table_keys.append(table_key)
    except Exception as exc:
        warnings.append({"file": filename, "message": str(exc)})

    conn.commit()
    return table_keys, warnings


# ──────────────────────────────────────────────
# Column info (lightweight — no type inference)
# ──────────────────────────────────────────────

def collect_column_info(
    conn: DuckDBConnection, table_keys: list[str]
) -> list[dict[str, Any]]:
    """Gather column names, sample values, and statistics across all session tables.

    Returns [{"name": "COL", "sampleValues": [...], "totalRows": int,
              "nullCount": int, "nullRate": float, "distinctCount": int,
              "inferredType": "numeric"|"datetime"|"string"}].
    """
    registry = _get_registry(conn)
    data_tables = [r["data_table"] for r in registry if r["table_key"] in table_keys]
    if not data_tables:
        return []

    col_rows = conn.execute(
        "SELECT table_name, column_name FROM information_schema.columns "
        "WHERE table_name = ANY(?) AND column_name != 'RECORD_ID' "
        "ORDER BY table_name, ordinal_position",
        (data_tables,),
    ).fetchall()

    table_cols: dict[str, list[str]] = {}
    for tname, cname in col_rows:
        table_cols.setdefault(tname, []).append(cname)

    all_columns: dict[str, list[str]] = {}
    col_stats: dict[str, dict[str, int | float]] = {}

    for tname, cols in table_cols.items():
        for col in cols:
            if col not in all_columns:
                all_columns[col] = []
                col_stats[col] = {
                    "totalRows": 0, "nullCount": 0,
                    "distinctCount": 0, "numericCount": 0, "dateCount": 0,
                }
            try:
                rows = conn.execute(
                    f'SELECT DISTINCT "{col}" FROM "{tname}" '
                    f'WHERE "{col}" IS NOT NULL AND TRIM("{col}") != \'\' '
                    f"LIMIT 50"
                ).fetchall()
                all_columns[col].extend(r[0] for r in rows)
            except Exception:
                logger.warning("Failed to sample column %s from %s", col, tname)
                continue

            try:
                stats_row = conn.execute(
                    f'SELECT '
                    f'  COUNT(*) AS total, '
                    f'  SUM(CASE WHEN "{col}" IS NULL OR TRIM("{col}") = \'\' '
                    f'       THEN 1 ELSE 0 END) AS nulls, '
                    f'  COUNT(DISTINCT CASE WHEN "{col}" IS NOT NULL '
                    f'       AND TRIM("{col}") != \'\' THEN "{col}" END) AS dist, '
                    f'  SUM(CASE WHEN TRY_CAST("{col}" AS DOUBLE) IS NOT NULL '
                    f'       AND TRIM("{col}") != \'\' THEN 1 ELSE 0 END) AS nums, '
                    f'  SUM(CASE WHEN TRY_CAST("{col}" AS TIMESTAMP) IS NOT NULL '
                    f'       AND TRIM("{col}") != \'\' THEN 1 ELSE 0 END) AS dates '
                    f'FROM "{tname}"'
                ).fetchone()
                if stats_row:
                    col_stats[col]["totalRows"] += int(stats_row[0] or 0)
                    col_stats[col]["nullCount"] += int(stats_row[1] or 0)
                    col_stats[col]["distinctCount"] = max(
                        col_stats[col]["distinctCount"], int(stats_row[2] or 0)
                    )
                    col_stats[col]["numericCount"] += int(stats_row[3] or 0)
                    col_stats[col]["dateCount"] += int(stats_row[4] or 0)
            except Exception:
                logger.warning("Failed to compute stats for column %s from %s", col, tname)

    results: list[dict[str, Any]] = []
    for col_name, values in all_columns.items():
        unique_vals = list(dict.fromkeys(values))[:50]
        st = col_stats.get(col_name, {})
        total = st.get("totalRows", 0)
        null_count = st.get("nullCount", 0)
        non_null = total - null_count
        null_rate = round(null_count / total, 3) if total > 0 else 0.0
        distinct = st.get("distinctCount", 0)

        # Infer type: numeric if >80% of non-null values parse as numbers,
        # datetime if >80% parse as dates (and not also numeric), else string
        inferred = "string"
        if non_null > 0:
            num_ratio = st.get("numericCount", 0) / non_null
            date_ratio = st.get("dateCount", 0) / non_null
            if num_ratio > 0.8:
                inferred = "numeric"
            elif date_ratio > 0.8 and num_ratio < 0.5:
                inferred = "datetime"

        results.append({
            "name": col_name,
            "sampleValues": unique_vals,
            "totalRows": total,
            "nullCount": null_count,
            "nullRate": null_rate,
            "distinctCount": distinct,
            "inferredType": inferred,
        })

    return results


# ──────────────────────────────────────────────
# Inventory & Preview (new format)
# ──────────────────────────────────────────────

def build_inventory(conn: DuckDBConnection) -> list[dict[str, Any]]:
    """Returns [{table_key, rows, cols}, ...]. Uses batched SQL queries."""
    registry = _get_registry(conn)
    if not registry:
        return []

    data_tables = [e["data_table"] for e in registry]

    # Batch: column counts (excluding RECORD_ID)
    col_rows = conn.execute(
        "SELECT table_name, COUNT(*) "
        "FROM information_schema.columns "
        "WHERE table_name = ANY(?) AND column_name != 'RECORD_ID' "
        "GROUP BY table_name",
        (data_tables,),
    ).fetchall()
    col_counts = {r[0]: r[1] for r in col_rows}

    # Batch: row counts via UNION ALL
    existing = [t for t in data_tables if t in col_counts]
    row_counts: dict[str, int] = {}
    if existing:
        parts = [
            f"SELECT '{t}' AS tbl, COUNT(*) AS cnt FROM \"{t}\""
            for t in existing
        ]
        for r in conn.execute(" UNION ALL ".join(parts)).fetchall():
            row_counts[r[0]] = r[1]

    inventory = []
    for entry in registry:
        tname = entry["data_table"]
        if tname not in col_counts:
            continue
        inventory.append({
            "table_key": entry["table_key"],
            "rows": row_counts.get(tname, 0),
            "cols": col_counts[tname],
        })
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


def build_preview(conn: DuckDBConnection, limit: int = 50) -> dict[str, dict[str, Any]]:
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


def build_single_preview(conn: DuckDBConnection, table_key: str, limit: int = 50) -> dict[str, Any] | None:
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

def delete_table_from_session(conn: DuckDBConnection, table_key: str):
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


def get_raw_preview(conn: DuckDBConnection, table_key: str, limit: int = 50) -> list[list[Any]]:
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
    conn: DuckDBConnection,
    table_key: str,
    header_row_index: int,
    custom_names: dict[int, str] | None = None,
):
    """Rebuild the data table using a different row from the raw table as headers.

    Uses SQL with ROW_NUMBER() to avoid loading the entire table into Python.
    Only the chosen header row is fetched to determine column names.
    """
    registry = _get_registry(conn)
    entry = next((r for r in registry if r["table_key"] == table_key), None)
    if not entry:
        raise ValueError(f"Table key not found: {table_key}")

    raw_tbl = entry["raw_table"]
    data_tbl = entry["data_table"]

    total_rows = conn.execute(f'SELECT COUNT(*) FROM "{raw_tbl}"').fetchone()[0]
    if header_row_index >= total_rows:
        raise ValueError(f"Row index {header_row_index} out of range (table has {total_rows} rows)")

    # Fetch only the header row via SQL
    raw_cols_info = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name = ? ORDER BY ordinal_position",
        (raw_tbl,),
    ).fetchall()
    raw_cols = [r[0] for r in raw_cols_info]

    col_list = ", ".join(f'"{c}"' for c in raw_cols)
    header_row = conn.execute(
        f"SELECT {col_list} FROM ("
        f'  SELECT *, ROW_NUMBER() OVER () AS _rn FROM "{raw_tbl}"'
        f") _sub WHERE _rn = ?",
        (header_row_index + 1,),
    ).fetchone()
    if not header_row:
        raise ValueError("Header row not found.")

    headers = []
    for i, cell in enumerate(list(header_row)):
        if custom_names and i in custom_names and custom_names[i].strip():
            headers.append(_clean_header(custom_names[i]))
        elif cell is not None and str(cell).strip():
            headers.append(_clean_header(str(cell)))
        else:
            headers.append(f"COL_{i}")
    headers = _dedupe_headers(headers)

    # Rebuild data table from raw via SQL, skipping the header row
    select_parts = ", ".join(
        f'CAST("{rc}" AS VARCHAR) AS "{hdr}"'
        for rc, hdr in zip(raw_cols[:len(headers)], headers)
    )
    null_check = " AND ".join(
        f'("{rc}" IS NULL OR TRIM(CAST("{rc}" AS VARCHAR)) = \'\')'
        for rc in raw_cols[:len(headers)]
    )
    conn.execute(f'DROP TABLE IF EXISTS "{data_tbl}"')
    conn.execute(
        f'CREATE TABLE "{data_tbl}" AS '
        f"SELECT "
        f"  CAST(ROW_NUMBER() OVER () AS VARCHAR) AS \"RECORD_ID\", "
        f"  {select_parts} "
        f"FROM ("
        f'  SELECT *, ROW_NUMBER() OVER () AS _rn FROM "{raw_tbl}"'
        f") _sub "
        f"WHERE _rn != ? "
        f"AND NOT ({null_check})",
        (header_row_index + 1,),
    )
    conn.commit()


def delete_rows_from_table(
    conn: DuckDBConnection,
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
