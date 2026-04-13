"""Data-loading service: file parsing, data preview building, previews."""

from __future__ import annotations

import os
import sqlite3
from typing import Any

from shared.db import (
    all_registered_tables,
    read_table,
    read_table_columns,
    table_row_count,
    table_exists,
)
from shared.db.stats_ops import distinct_values_by_column_sql

PREVIEW_ROWS = 50
MAX_COLUMNS_IN_FILES_PAYLOAD = int(os.getenv("MAX_COLUMNS_IN_FILES_PAYLOAD", "300"))
MAX_COLUMNS_FOR_DISTINCT_EXAMPLES = int(os.getenv("MAX_COLUMNS_FOR_DISTINCT_EXAMPLES", "40"))
MAX_DISTINCT_VALUES_PER_COLUMN = int(os.getenv("MAX_DISTINCT_VALUES_PER_COLUMN", "50"))


def infer_file_type(table_key: str) -> str:
    path = table_key.split("::")[0].lower()
    if path.endswith(".csv"):
        return "csv"
    if any(path.endswith(ext) for ext in (".xlsx", ".xlsm", ".xltx", ".xltm")):
        return "excel"
    return "unknown"


def file_display_name(internal_path: str, sheet: str | None) -> str:
    return f"{internal_path} :: {sheet}" if sheet else internal_path


def build_inventory_from_db(conn: sqlite3.Connection) -> list[dict]:
    registered = all_registered_tables(conn)
    rows: list[dict] = []
    for entry in registered:
        sql_name = entry["sql_name"]
        table_key = entry["table_key"]
        if not sql_name.startswith(("tbl__", "hn__")):
            continue
        if not table_exists(conn, sql_name):
            continue
        parts = table_key.split("::", 1)
        internal = parts[0]
        sheet = parts[1] if len(parts) > 1 and parts[1] else None
        cols = read_table_columns(conn, sql_name)
        rows.append({
            "table_key": table_key,
            "internal_path": internal,
            "sheet": sheet,
            "rows": table_row_count(conn, sql_name),
            "cols": len(cols),
        })
    rows.sort(key=lambda r: r["internal_path"])
    return rows


def build_files_payload_from_db(
    conn: sqlite3.Connection,
    skip_distinct: bool = False,
) -> list[dict]:
    registered = all_registered_tables(conn)
    files: list[dict] = []
    for entry in registered:
        sql_name = entry["sql_name"]
        table_key = entry["table_key"]
        if not sql_name.startswith(("tbl__", "hn__")):
            continue
        if not table_exists(conn, sql_name):
            continue
        parts = table_key.split("::", 1)
        internal_path = parts[0]
        sheet = parts[1] if len(parts) > 1 and parts[1] else None
        ftype = infer_file_type(table_key)
        file_name = file_display_name(internal_path, sheet)
        cols = read_table_columns(conn, sql_name)
        n_rows = table_row_count(conn, sql_name)

        if n_rows == 0 or not cols:
            files.append({
                "table_key": table_key,
                "file_name": file_name,
                "internal_path": internal_path,
                "sheet": sheet,
                "file_type": ftype,
                "n_rows": 0,
                "n_cols": 0,
                "columns": [],
                "distinct_examples_by_column": {},
                "empty": True,
            })
            continue

        if skip_distinct:
            distinct_map: dict = {}
        else:
            distinct_columns = cols[:MAX_COLUMNS_FOR_DISTINCT_EXAMPLES]
            if n_rows > 200_000:
                distinct_columns = cols[: max(8, MAX_COLUMNS_FOR_DISTINCT_EXAMPLES // 3)]
            elif n_rows > 50_000:
                distinct_columns = cols[: max(16, MAX_COLUMNS_FOR_DISTINCT_EXAMPLES // 2)]
            distinct_map = distinct_values_by_column_sql(
                conn,
                sql_name,
                max_per_col=MAX_DISTINCT_VALUES_PER_COLUMN,
                columns=distinct_columns,
            )

        files.append({
            "table_key": table_key,
            "file_name": file_name,
            "internal_path": internal_path,
            "sheet": sheet,
            "file_type": ftype,
            "n_rows": n_rows,
            "n_cols": len(cols),
            "columns": cols[:MAX_COLUMNS_IN_FILES_PAYLOAD],
            "distinct_examples_by_column": distinct_map,
            "empty": False,
        })
    return files


def build_previews_from_db(conn: sqlite3.Connection) -> dict[str, dict]:
    registered = all_registered_tables(conn)
    previews: dict[str, dict] = {}
    for entry in registered:
        sql_name = entry["sql_name"]
        table_key = entry["table_key"]
        if not sql_name.startswith(("tbl__", "hn__")):
            continue
        if not table_exists(conn, sql_name):
            continue
        cols = read_table_columns(conn, sql_name)
        if not cols:
            previews[table_key] = {"columns": [], "rows": []}
        else:
            previews[table_key] = {
                "columns": cols,
                "rows": read_table(conn, sql_name, PREVIEW_ROWS),
            }
    return previews


def build_single_preview(conn: sqlite3.Connection, table_key: str) -> dict | None:
    """Build a preview for a single table_key. Returns None if not found."""
    from shared.db import lookup_sql_name

    sql_name = lookup_sql_name(conn, table_key)
    if not sql_name or not table_exists(conn, sql_name):
        return None
    cols = read_table_columns(conn, sql_name)
    if not cols:
        return {"columns": [], "rows": []}
    return {
        "columns": cols,
        "rows": read_table(conn, sql_name, PREVIEW_ROWS),
    }
