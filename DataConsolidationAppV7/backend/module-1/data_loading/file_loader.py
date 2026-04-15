"""File loading: ZIP extraction + streaming Excel/CSV to DuckDB."""

from __future__ import annotations

import csv
import io
import logging
import os
import time
import zipfile
from typing import Any, Callable, Iterator

import pandas as pd

logger = logging.getLogger(__name__)

from shared.db import (
    DuckDBConnection,
    safe_table_name,
    register_table,
    store_table_streaming,
    store_df_native,
    table_exists,
    table_row_count,
    read_table_columns,
    quote_id,
)
from shared.utils import make_unique

RAW_META_PREVIEW_ROWS = int(os.getenv("RAW_META_PREVIEW_ROWS", "20"))

_EXCEL_EXTS = (".xlsx", ".xlsm", ".xlsb", ".xltx", ".xltm")

_META_COLUMNS = ["FILE_NAME", "RECORD_ID"]


def _file_name_from_key(table_key: str) -> str:
    """Extract human-readable file name from a table_key like 'path/to/file.xlsx::Sheet1'."""
    path_part = table_key.split("::")[0]
    return path_part.rsplit("/", 1)[-1].rsplit("\\", 1)[-1]


def _clean_header(cells: list) -> list[str]:
    """Clean a header row: strip, uppercase, fill blanks."""
    result = []
    for i, c in enumerate(cells):
        if c is None or str(c).strip() == "":
            result.append(f"Column_{i + 1}")
        else:
            result.append(str(c).strip().upper())
    return make_unique(result)


def _to_str(val: Any) -> str | None:
    """Convert a value to string without cleaning. Preserves original casing and whitespace."""
    if val is None:
        return None
    s = str(val)
    return s if s.strip() else None


def _is_empty_row(vals: list) -> bool:
    return all(v is None or str(v).strip() == "" for v in vals)


def bulk_clean_table(conn: DuckDBConnection, table_name: str) -> None:
    """Apply UPPER(TRIM(...)) to all VARCHAR columns in a table via a single SQL UPDATE.

    This replaces per-cell Python cleaning with a single DuckDB operation
    that processes the entire table at columnar speed. Empty strings are
    set to NULL for consistency.
    """
    cols = read_table_columns(conn, table_name)
    if not cols:
        return
    set_clauses = ", ".join(
        f'{quote_id(c)} = CASE WHEN TRIM({quote_id(c)}) = \'\' THEN NULL '
        f'ELSE UPPER(TRIM({quote_id(c)})) END'
        for c in cols
    )
    conn.execute(f"UPDATE {quote_id(table_name)} SET {set_clauses}")
    conn.commit()


def _sql_all_null_condition(columns: list[str]) -> str:
    """Build a SQL condition that is true when ALL columns are NULL or empty-string."""
    parts = [
        f"({quote_id(c)} IS NULL OR TRIM(CAST({quote_id(c)} AS VARCHAR)) = '')"
        for c in columns
    ]
    return " AND ".join(parts)


def _sql_literal(val: str) -> str:
    """Escape a string for use as a SQL literal."""
    return "'" + val.replace("'", "''") + "'"


def _iter_csv_rows(text: str) -> Iterator[list[str]]:
    reader = csv.reader(io.StringIO(text))
    for row in reader:
        yield list(row)


def _pad_row(row: list, max_cols: int) -> list:
    """Pad or truncate a row to exactly max_cols."""
    if len(row) < max_cols:
        return list(row) + [None] * (max_cols - len(row))
    if len(row) > max_cols:
        return list(row)[:max_cols]
    return list(row)


def _df_row_to_list(row: tuple) -> list:
    """Convert a DataFrame row tuple, replacing NaN with None."""
    return [None if pd.isna(v) else v for v in row]


def _load_excel_sheet(
    conn: DuckDBConnection,
    table_key: str,
    excel_file: pd.ExcelFile,
    sheet_name: str,
    warnings: list[dict],
    commit: bool = True,
) -> None:
    """Load a single Excel sheet via calamine (fast Rust parser).

    Uses DuckDB native DataFrame ingestion for the raw table, then builds
    the data table from the raw table via SQL — no Python row iteration.
    """
    try:
        df = pd.read_excel(excel_file, sheet_name=sheet_name, header=None)
        if df.empty:
            return

        max_cols = len(df.columns)
        header_raw = _df_row_to_list(next(df.itertuples(index=False, name=None)))

        # Raw table: native DataFrame ingestion (zero Python iteration)
        raw_cols = [f"RAW_{i + 1}" for i in range(max_cols)]
        df.columns = raw_cols
        raw_name = safe_table_name("raw", table_key)
        store_df_native(conn, raw_name, df, commit=commit)

        # Data table: build from raw table via SQL (skip header row)
        base_header = _clean_header(header_raw)
        file_name = _file_name_from_key(table_key)
        tbl_name = safe_table_name("tbl", table_key)

        raw_select = ", ".join(
            f"CAST({quote_id(raw_cols[i])} AS VARCHAR) AS {quote_id(base_header[i])}"
            for i in range(len(base_header))
        )
        fname_lit = _sql_literal(file_name)
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(tbl_name)}")
        conn.execute(
            f"CREATE TABLE {quote_id(tbl_name)} AS "
            f"SELECT "
            f"  {fname_lit} AS \"FILE_NAME\", "
            f"  CAST(ROW_NUMBER() OVER () AS VARCHAR) AS \"RECORD_ID\", "
            f"  {raw_select} "
            f"FROM ("
            f"  SELECT *, ROW_NUMBER() OVER () AS _rn FROM {quote_id(raw_name)}"
            f") _sub "
            f"WHERE _rn > 1 "
            f"AND NOT ({_sql_all_null_condition(raw_cols)})"
        )
        if commit:
            conn.commit()
        register_table(conn, table_key, tbl_name, commit=commit)
    except Exception as e:
        warnings.append({"file": table_key, "message": str(e)})


def _load_csv(
    conn: DuckDBConnection,
    table_key: str,
    csv_bytes: bytes,
    warnings: list[dict],
    commit: bool = True,
) -> None:
    """Load a CSV file using native DataFrame ingestion + SQL.

    Reads CSV into a DataFrame once, ingests the raw table natively, then
    builds the data table from the raw table via SQL — no Python row loops.
    """
    try:
        df = pd.read_csv(io.BytesIO(csv_bytes), header=None, dtype=str,
                         keep_default_na=False, encoding_errors="replace")
        if df.empty:
            return

        max_cols = len(df.columns)
        header_raw = list(df.iloc[0])

        # Raw table: native ingestion
        raw_cols = [f"RAW_{i + 1}" for i in range(max_cols)]
        df.columns = raw_cols
        raw_name = safe_table_name("raw", table_key)
        store_df_native(conn, raw_name, df, commit=commit)

        # Data table: build from raw table via SQL (skip header row)
        base_header = _clean_header(header_raw)
        file_name = _file_name_from_key(table_key)
        tbl_name = safe_table_name("tbl", table_key)

        raw_select = ", ".join(
            f"CAST({quote_id(raw_cols[i])} AS VARCHAR) AS {quote_id(base_header[i])}"
            for i in range(len(base_header))
        )
        fname_lit = _sql_literal(file_name)
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(tbl_name)}")
        conn.execute(
            f"CREATE TABLE {quote_id(tbl_name)} AS "
            f"SELECT "
            f"  {fname_lit} AS \"FILE_NAME\", "
            f"  CAST(ROW_NUMBER() OVER () AS VARCHAR) AS \"RECORD_ID\", "
            f"  {raw_select} "
            f"FROM ("
            f"  SELECT *, ROW_NUMBER() OVER () AS _rn FROM {quote_id(raw_name)}"
            f") _sub "
            f"WHERE _rn > 1 "
            f"AND NOT ({_sql_all_null_condition(raw_cols)})"
        )
        if commit:
            conn.commit()
        register_table(conn, table_key, tbl_name, commit=commit)
    except Exception as e:
        warnings.append({"file": table_key, "message": str(e)})


def load_zip_to_session(
    conn: DuckDBConnection,
    file_data: bytes,
    on_progress: Callable[[dict], None] | None = None,
) -> tuple[dict[str, list], list[dict]]:
    """Parse a ZIP archive and stream all files into the session DuckDB.

    Opens each Excel workbook exactly once and parses each file in a single
    pass.  All DB writes are batched into one commit at the end.

    Args:
        conn: DuckDB session connection.
        file_data: Raw bytes of the uploaded file (ZIP or single file).
        on_progress: Optional callback invoked with progress dicts at key
            milestones (zip opened, each file loaded, commit done).

    Returns:
        ({}, warnings_list).  The first element is kept for interface
        compatibility but is always empty (raw data lives in raw__* tables).
    """
    warnings: list[dict] = []
    file_count = 0

    def _emit(payload: dict) -> None:
        if on_progress is not None:
            on_progress(payload)

    def _process_excel(data: bytes, name: str) -> None:
        """Load all sheets from an in-memory Excel workbook."""
        t = time.perf_counter()
        excel_file = pd.ExcelFile(io.BytesIO(data), engine="calamine")
        try:
            for sheet in excel_file.sheet_names:
                key = f"{name}::{sheet}"
                _load_excel_sheet(
                    conn, key, excel_file, sheet, warnings, commit=False,
                )
        finally:
            excel_file.close()
        elapsed = time.perf_counter() - t
        logger.info("  Loaded Excel %s (%d sheets) in %.1fs", name, len(excel_file.sheet_names), elapsed)
        nonlocal file_count
        file_count += 1
        _emit({"stage": "file_loaded", "name": name, "current": file_count, "elapsed": round(elapsed, 1)})

    with zipfile.ZipFile(io.BytesIO(file_data)) as zf:
        entries = [e for e in zf.infolist() if not e.is_dir()]
        # Count only loadable entries to get an accurate total
        loadable = [
            e for e in entries
            if e.filename.lower().endswith(_EXCEL_EXTS + (".csv", ".zip"))
        ]
        total = len(loadable)
        logger.info("ZIP contains %d file(s)", len(entries))
        _emit({"stage": "zip_info", "total": total})

        for entry in entries:
            name = entry.filename
            lower = name.lower()

            if lower.endswith(_EXCEL_EXTS):
                _process_excel(zf.read(name), name)

            elif lower.endswith(".csv"):
                t = time.perf_counter()
                key = f"{name}::"
                _load_csv(conn, key, zf.read(name), warnings, commit=False)
                elapsed = time.perf_counter() - t
                logger.info("  Loaded CSV %s in %.1fs", name, elapsed)
                file_count += 1
                _emit({"stage": "file_loaded", "name": name, "current": file_count, "elapsed": round(elapsed, 1)})

            elif lower.endswith(".zip"):
                try:
                    nested_data = zf.read(name)
                    with zipfile.ZipFile(io.BytesIO(nested_data)) as nested_zf:
                        for nested_entry in nested_zf.infolist():
                            if nested_entry.is_dir():
                                continue
                            nested_name = nested_entry.filename
                            nested_lower = nested_name.lower()
                            full_key_prefix = f"{name}/{nested_name}"

                            if nested_lower.endswith(_EXCEL_EXTS):
                                _process_excel(nested_zf.read(nested_name), full_key_prefix)

                            elif nested_lower.endswith(".csv"):
                                t = time.perf_counter()
                                key = f"{full_key_prefix}::"
                                _load_csv(conn, key, nested_zf.read(nested_name), warnings, commit=False)
                                elapsed = time.perf_counter() - t
                                logger.info("  Loaded nested CSV %s in %.1fs", nested_name, elapsed)
                                file_count += 1
                                _emit({"stage": "file_loaded", "name": nested_name, "current": file_count, "elapsed": round(elapsed, 1)})
                except Exception as e:
                    warnings.append({"file": name, "message": f"Failed to extract nested ZIP: {e}"})

    t_commit = time.perf_counter()
    conn.commit()
    logger.info("Committed %d file(s) to DuckDB in %.1fs", file_count, time.perf_counter() - t_commit)
    _emit({"stage": "committed", "file_count": file_count})
    return {}, warnings


def get_raw_array_from_table(
    conn: DuckDBConnection,
    table_key: str,
    limit: int | None = None,
) -> list[list[Any]]:
    """Read raw table data as a list-of-lists.

    Args:
        conn: DuckDB session connection.
        table_key: Logical table key.
        limit: Optional max rows.

    Returns:
        2D array of raw values.
    """
    raw_name = safe_table_name("raw", table_key)
    if not table_exists(conn, raw_name):
        return []
    cols = read_table_columns(conn, raw_name)
    if not cols:
        return []
    select_cols = ", ".join(quote_id(c) for c in cols)
    sql = f"SELECT {select_cols} FROM {quote_id(raw_name)}"
    params: tuple[Any, ...] = ()
    if limit is not None:
        sql += " LIMIT ?"
        params = (int(limit),)
    rows = conn.execute(sql, params).fetchall()
    return [[r[c] for c in cols] for r in rows]


def _build_columns_from_header(
    header_row: list[Any],
    custom_column_names: dict[int, str] | None = None,
) -> list[str]:
    columns: list[str] = []
    for i, cell in enumerate(header_row):
        if custom_column_names and i in custom_column_names:
            columns.append(str(custom_column_names[i]))
        elif cell is None or str(cell).strip() == "":
            columns.append(f"Column_{i + 1}")
        else:
            columns.append(str(cell).strip())
    normalized = [
        str(c).strip().upper() if str(c).strip() else f"COLUMN_{i + 1}"
        for i, c in enumerate(columns)
    ]
    return make_unique(normalized)


def rebuild_table_from_raw_table(
    conn: DuckDBConnection,
    table_key: str,
    header_row_index: int,
    custom_column_names: dict[int, str] | None = None,
) -> None:
    """Rebuild a data table from raw data using a new header row.

    Uses ROW_NUMBER() OVER() instead of rowid for row addressing, since
    DuckDB does not have SQLite's implicit rowid.
    """
    raw_name = safe_table_name("raw", table_key)
    if not table_exists(conn, raw_name):
        raise ValueError("Raw table data not found for this table. Please re-upload.")

    raw_cols = read_table_columns(conn, raw_name)
    if not raw_cols:
        raise ValueError("Raw table has no columns.")

    total_rows = table_row_count(conn, raw_name)
    if header_row_index < 0 or header_row_index >= total_rows:
        raise ValueError(f"headerRowIndex {header_row_index} is out of range.")

    select_cols = ", ".join(quote_id(c) for c in raw_cols)
    # Use ROW_NUMBER() to create a 1-based row index (replaces SQLite rowid)
    numbered_cte = (
        f"WITH numbered AS ("
        f"  SELECT {select_cols}, ROW_NUMBER() OVER() AS _rn "
        f"  FROM {quote_id(raw_name)}"
        f")"
    )

    header_row = conn.execute(
        f"{numbered_cte} SELECT {select_cols} FROM numbered WHERE _rn = ?",
        (header_row_index + 1,),
    ).fetchone()
    if not header_row:
        raise ValueError("Header row not found in raw table.")

    header_values = [header_row[c] for c in raw_cols]
    final_columns = _build_columns_from_header(header_values, custom_column_names)
    if not final_columns:
        raise ValueError("Could not infer any columns from selected header row.")

    # Pass 1: detect columns that are entirely empty below the selected header row.
    has_value = [False] * len(final_columns)
    row_cursor = conn.execute(
        f"{numbered_cte} SELECT {select_cols} FROM numbered WHERE _rn > ?",
        (header_row_index + 1,),
    )
    for row in row_cursor:
        for i in range(len(final_columns)):
            raw_val = row[raw_cols[i]] if i < len(raw_cols) else None
            if raw_val is not None and str(raw_val).strip() != "":
                has_value[i] = True

    valid_idx = [i for i, ok in enumerate(has_value) if ok]
    if not valid_idx:
        valid_idx = list(range(len(final_columns)))
    base_output_columns = [final_columns[i] for i in valid_idx]
    file_name = _file_name_from_key(table_key)
    output_columns = _META_COLUMNS + base_output_columns

    def _data_gen():
        record_id = 0
        cur = conn.execute(
            f"{numbered_cte} SELECT {select_cols} FROM numbered WHERE _rn > ?",
            (header_row_index + 1,),
        )
        for row in cur:
            out_row: list[str | None] = []
            non_empty = False
            for i in valid_idx:
                raw_val = row[raw_cols[i]] if i < len(raw_cols) else None
                val = _to_str(raw_val)
                out_row.append(val)
                if val is not None and val.strip() != "":
                    non_empty = True
            if non_empty:
                record_id += 1
                yield [file_name, str(record_id)] + out_row

    tbl_name = safe_table_name("tbl", table_key)
    store_table_streaming(conn, tbl_name, output_columns, _data_gen())
    bulk_clean_table(conn, tbl_name)
    register_table(conn, table_key, tbl_name)


def array_to_objects(
    raw_arr: list[list],
    header_row_index: int,
    custom_column_names: dict[int, str] | None = None,
) -> list[dict]:
    """Convert a 2D raw array into a list of row dicts, given a header row index."""
    if not raw_arr or header_row_index >= len(raw_arr):
        return []

    header_row = raw_arr[header_row_index]
    data_rows = raw_arr[header_row_index + 1:]

    columns: list[str] = []
    for i, cell in enumerate(header_row):
        if custom_column_names and i in custom_column_names:
            columns.append(custom_column_names[i])
        elif cell is None or str(cell).strip() == "":
            columns.append(f"Column_{i + 1}")
        else:
            columns.append(str(cell).strip())

    return [
        {columns[i]: (row[i] if i < len(row) else None) for i in range(len(columns))}
        for row in data_rows
    ]


