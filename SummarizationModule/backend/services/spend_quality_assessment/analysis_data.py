"""Shared helpers for reading ``analysis_data`` in spend-quality tabs."""

from __future__ import annotations

from shared.db import get_meta
from shared.duckdb_compat import DuckDBConnection


def analysis_table_exists(conn: DuckDBConnection) -> bool:
    try:
        conn.execute('SELECT 1 FROM "analysis_data" LIMIT 1').fetchone()
        return True
    except Exception:
        pass
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE lower(table_name) = 'analysis_data'"
    ).fetchone()
    return int(row[0] or 0) > 0


def list_analysis_columns(conn: DuckDBConnection) -> list[str]:
    """Return column names for ``analysis_data``, or an empty list."""
    if not analysis_table_exists(conn):
        return []
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE lower(table_name) = 'analysis_data' "
        "ORDER BY ordinal_position"
    ).fetchall()
    if rows:
        return [str(r[0]) for r in rows]
    try:
        pragma = conn.execute('PRAGMA table_info("analysis_data")').fetchall()
        return [str(r[1]) for r in pragma]
    except Exception:
        return []


def get_cast_report_fields(conn: DuckDBConnection) -> dict[str, dict]:
    """Return per-field cast stats saved at confirm-mapping time."""
    cast = get_meta(conn, "cast_report") or {}
    fields = cast.get("fields")
    return fields if isinstance(fields, dict) else {}


def field_mapped_with_data(conn: DuckDBConnection, field_key: str) -> bool:
    """True when Step 3 mapping recorded non-empty values for *field_key*."""
    info = get_cast_report_fields(conn).get(field_key, {})
    if info.get("mapped") and int(info.get("validRows") or 0) > 0:
        return True
    return column_has_data(conn, field_key)


def column_has_data(conn: DuckDBConnection, column: str) -> bool:
    """Fast existence check — uses LIMIT 1 instead of COUNT(*)."""
    if column not in set(list_analysis_columns(conn)):
        return False
    qc = quote_id(column)
    try:
        row = conn.execute(
            f"""
            SELECT 1 FROM "analysis_data"
            WHERE {qc} IS NOT NULL
              AND TRIM(CAST({qc} AS VARCHAR)) != ''
            LIMIT 1
            """
        ).fetchone()
        return row is not None
    except Exception:
        return False


def get_mapped_field_keys(
    conn: DuckDBConnection,
    field_keys: list[str],
) -> list[str]:
    """Return *field_keys* that are mapped with data (cast report preferred)."""
    present = set(list_analysis_columns(conn))
    result: list[str] = []
    cast_fields = get_cast_report_fields(conn)
    for fk in field_keys:
        if fk not in present:
            continue
        info = cast_fields.get(fk, {})
        if info.get("mapped") and int(info.get("validRows") or 0) > 0:
            result.append(fk)
        elif column_has_data(conn, fk):
            result.append(fk)
    return result


def quote_id(name: str) -> str:
    return f'"{name}"'
