"""Preview operations service: DuckDB-backed Excel-like operations.

This module provides operations for the Excel-like preview grid:
- Cell editing
- Column operations (rename, delete, reorder)
- Calculated columns
- Filtering and sorting
- Pivot tables
- Undo/redo
"""

from __future__ import annotations

import re
import uuid
from datetime import datetime
from typing import Any, Mapping
from collections import defaultdict

from shared.db.duckdb_compat import DuckDBConnection
from shared.db.meta_ops import get_meta, set_meta
from shared.db.table_ops import (
    drop_table,
    quote_id,
    read_table_columns,
    read_table_column_types,
    table_exists,
    table_row_count,
)
from shared.db.session_db import lookup_sql_name, register_table, safe_table_name
from shared.invalidation import invalidate_session_downstream


# Export lookup_sql_name for use in routes
__all__ = [
    'run_operation',
    'get_preview_data',
    'get_column_filter_values',
    'apply_to_pipeline',
    'lookup_sql_name',
]

# Constants
ROW_ID_COL = "__row_id"
PREVIEW_META_PREFIX = "preview_ops_"
DEFAULT_PAGE_SIZE = 200
MAX_PIVOT_COLUMNS = 200

# Filter operators mapping
_FILTER_OPS = {
    "eq": "=",
    "neq": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
    "contains": "LIKE",
    "startswith": "LIKE",
    "endswith": "LIKE",
    "is_null": "IS NULL",
    "is_not_null": "IS NOT NULL",
}

# Aggregation functions for pivot
_AGG_FUNCS = {
    "sum": "SUM",
    "count": "COUNT",
    "avg": "AVG",
    "min": "MIN",
    "max": "MAX",
    "count_distinct": "COUNT",
}

_NUMERIC_AGG_KEYS = frozenset({"sum", "avg", "min", "max"})

# UI column types
_UI_TYPES = frozenset({"TEXT", "INTEGER", "DOUBLE", "DATE", "BOOLEAN"})

_NUMERIC_NAME_PATTERNS = re.compile(
    r"(AMOUNT|QTY|QUANTITY|PRICE|COST|SPEND|NO$|_NO$|_ID$|RECORD_ID|COUNT|NUM|NUMBER|RATE|VALUE|TOTAL)",
    re.IGNORECASE,
)
_DATE_NAME_PATTERNS = re.compile(
    r"(DATE|_DT$|_AT$|TIMESTAMP|CREATED|UPDATED|APPROVED)",
    re.IGNORECASE,
)

# Allowed functions in calculated columns
_ALLOWED_CALC_FUNCTIONS = {
    "UPPER", "LOWER", "TRIM", "SUBSTRING", "LEFT", "RIGHT", "LEN", "LENGTH",
    "ROUND", "ABS", "FLOOR", "CEIL", "POWER", "SQRT",
    "COALESCE", "NULLIF",
    "CAST",
    "YEAR", "MONTH", "DAY", "DATE",
}


def _now_iso() -> str:
    """Return current timestamp in ISO format."""
    return datetime.now().isoformat()


def _get_table_meta_key(table_key: str) -> str:
    """Generate meta key for table preview operations."""
    return f"{PREVIEW_META_PREFIX}{table_key}"


def _get_preview_state(conn: DuckDBConnection, table_key: str) -> dict[str, Any]:
    """Get preview state for a table."""
    key = _get_table_meta_key(table_key)
    state = get_meta(conn, key) or {}
    if not isinstance(state, dict):
        state = {}
    return state


def _save_preview_state(conn: DuckDBConnection, table_key: str, state: dict[str, Any]) -> None:
    """Save preview state for a table."""
    key = _get_table_meta_key(table_key)
    set_meta(conn, key, state)


def _data_columns(cols: list[str]) -> list[str]:
    """Return display columns excluding the internal row-id column."""
    return [c for c in cols if c != ROW_ID_COL]


def _ordered_display_columns(
    conn: DuckDBConnection,
    table_key: str,
    display_cols: list[str],
) -> list[str]:
    """Apply persisted column_order from preview meta when valid."""
    state = _get_preview_state(conn, table_key)
    saved_order = state.get("column_order")
    if not isinstance(saved_order, list) or not saved_order:
        return display_cols
    col_set = set(display_cols)
    ordered = [c for c in saved_order if c in col_set]
    for c in display_cols:
        if c not in ordered:
            ordered.append(c)
    return ordered if ordered else display_cols


def _looks_numeric_value(value: Any) -> bool:
    """Return True if value parses as a number."""
    if value is None:
        return False
    s = str(value).strip()
    if not s:
        return False
    try:
        float(s)
        return True
    except ValueError:
        return False


def _looks_date_value(value: Any) -> bool:
    """Return True if value looks like a date/datetime string."""
    if value is None:
        return False
    s = str(value).strip()
    if not s:
        return False
    if re.match(r"^\d{4}-\d{2}-\d{2}", s):
        return True
    if re.match(r"^\d{1,2}[/-]\d{1,2}[/-]\d{2,4}", s):
        return True
    return False


def _infer_column_type(col_name: str, sample_values: list[Any]) -> str:
    """Infer UI column type from name heuristics and sample values."""
    if _DATE_NAME_PATTERNS.search(col_name):
        return "DATE"
    if _NUMERIC_NAME_PATTERNS.search(col_name):
        return "DOUBLE"

    non_null = [v for v in sample_values if v is not None and str(v).strip() != ""]
    if not non_null:
        return "TEXT"

    date_hits = sum(1 for v in non_null if _looks_date_value(v))
    num_hits = sum(1 for v in non_null if _looks_numeric_value(v))
    n = len(non_null)
    if date_hits / n >= 0.8:
        return "DATE"
    if num_hits / n >= 0.8:
        return "DOUBLE"
    return "TEXT"


def _get_column_type_overrides(conn: DuckDBConnection, table_key: str) -> dict[str, str]:
    """Load user-persisted column type overrides from preview meta."""
    state = _get_preview_state(conn, table_key)
    raw = state.get("column_types") or {}
    if not isinstance(raw, dict):
        return {}
    return {k: v for k, v in raw.items() if v in _UI_TYPES}


def _save_column_type_override(
    conn: DuckDBConnection,
    table_key: str,
    column: str,
    ui_type: str,
) -> None:
    """Persist a single column type override in preview meta."""
    state = _get_preview_state(conn, table_key)
    overrides = dict(_get_column_type_overrides(conn, table_key))
    overrides[column] = ui_type
    state["column_types"] = overrides
    _save_preview_state(conn, table_key, state)


def _resolve_effective_column_types(
    conn: DuckDBConnection,
    sql_table: str,
    table_key: str,
    display_cols: list[str],
    sample_rows: list[dict[str, Any]] | None = None,
) -> dict[str, str]:
    """Resolve effective UI types: meta override > DuckDB schema > inference."""
    schema_types = read_table_column_types(conn, sql_table)
    overrides = _get_column_type_overrides(conn, table_key)
    result: dict[str, str] = {}

    for col in display_cols:
        if col in overrides:
            result[col] = overrides[col]
            continue
        schema_type = schema_types.get(col, "TEXT")
        if schema_type != "TEXT":
            result[col] = schema_type
            continue
        samples: list[Any] = []
        if sample_rows:
            samples = [r.get(col) for r in sample_rows[:20]]
        result[col] = _infer_column_type(col, samples)

    return result


def _sort_expr_for_type(col: str, ui_type: str) -> str:
    """Build ORDER BY expression for a column based on effective UI type."""
    quoted = quote_id(col)
    if ui_type in ("INTEGER", "DOUBLE"):
        return f"TRY_CAST({quoted} AS DOUBLE)"
    if ui_type == "DATE":
        return f"TRY_CAST({quoted} AS TIMESTAMP)"
    if ui_type == "BOOLEAN":
        return f"TRY_CAST({quoted} AS BOOLEAN)"
    return quoted


def _pivot_agg_expr(field: str, agg_key: str) -> str:
    """Build a pivot aggregation expression, casting VARCHAR for numeric aggs."""
    quoted = quote_id(field)
    if agg_key == "count_distinct":
        return f"COUNT(DISTINCT {quoted})"
    if agg_key in _NUMERIC_AGG_KEYS:
        cast_field = f"TRY_CAST({quoted} AS DOUBLE)"
        agg = _AGG_FUNCS.get(agg_key, "SUM")
        return f"{agg}({cast_field})"
    agg = _AGG_FUNCS.get(agg_key, "COUNT")
    return f"{agg}({quoted})"


def _rebuild_row_id_table(
    conn: DuckDBConnection,
    sql_table: str,
    display_cols: list[str],
) -> None:
    """Recreate a table with sequential __row_id values after row deletes."""
    quoted_table = quote_id(sql_table)
    quoted_row_id = quote_id(ROW_ID_COL)
    temp_table = f"temp_{sql_table}_{uuid.uuid4().hex[:8]}"
    quoted_temp = quote_id(temp_table)

    if display_cols:
        col_list = ", ".join(quote_id(c) for c in display_cols)
        conn.execute(f"""
            CREATE TABLE {quoted_temp} AS
            SELECT {col_list}, ROW_NUMBER() OVER () AS {quoted_row_id}
            FROM {quoted_table}
        """)
    else:
        conn.execute(f"""
            CREATE TABLE {quoted_temp} AS
            SELECT ROW_NUMBER() OVER () AS {quoted_row_id}
            FROM {quoted_table}
        """)

    drop_table(conn, sql_table, commit=False)
    conn.execute(f"ALTER TABLE {quoted_temp} RENAME TO {quoted_table}")
    conn.commit()


def _ensure_row_id(conn: DuckDBConnection, sql_table: str) -> None:
    """Ensure table has __row_id column for stable row identification."""
    if not table_exists(conn, sql_table):
        raise ValueError(f"Table {sql_table} does not exist")

    cols = read_table_columns(conn, sql_table)
    if ROW_ID_COL in cols:
        return

    data_cols = _data_columns(cols)
    if not data_cols:
        raise ValueError(f"Table has no data columns: {sql_table}")

    # Add row_id column
    quoted_table = quote_id(sql_table)
    quoted_row_id = quote_id(ROW_ID_COL)

    # Create temp table with row_id
    temp_table = f"temp_{sql_table}_{uuid.uuid4().hex[:8]}"
    quoted_temp = quote_id(temp_table)

    col_list = ", ".join(quote_id(c) for c in data_cols)

    conn.execute(f"""
        CREATE TABLE {quoted_temp} AS
        SELECT {col_list}, ROW_NUMBER() OVER () AS {quoted_row_id}
        FROM {quoted_table}
    """)

    # Replace original table
    drop_table(conn, sql_table, commit=False)
    conn.execute(f"ALTER TABLE {quoted_temp} RENAME TO {quoted_table}")
    conn.commit()


def _build_where_clause(
    filters: list[dict[str, Any]] | None,
    search: str | None,
    display_cols: list[str],
) -> tuple[str, list[Any]]:
    """Build WHERE clause from filters and search."""
    conditions: list[str] = []
    params: list[Any] = []

    # Search across all columns
    if search:
        search_conds = []
        for col in display_cols:
            search_conds.append(f"CAST({quote_id(col)} AS VARCHAR) ILIKE ?")
            params.append(f"%{search}%")
        if search_conds:
            conditions.append(f"({' OR '.join(search_conds)})")

    # Filter conditions
    if filters:
        for f in filters:
            col = f.get("column", "")
            op = f.get("op", "eq")
            value = f.get("value")

            if not col:
                continue

            quoted_col = quote_id(col)
            sql_op = _FILTER_OPS.get(op, "=")

            if op in ("is_null", "is_not_null"):
                conditions.append(f"{quoted_col} {sql_op}")
            elif op == "in":
                selected = f.get("values") or []
                include_blanks = bool(f.get("includeBlanks"))
                in_parts: list[str] = []
                if selected:
                    placeholders = ", ".join("?" for _ in selected)
                    in_parts.append(f"CAST({quoted_col} AS VARCHAR) IN ({placeholders})")
                    params.extend(selected)
                if include_blanks:
                    in_parts.append(
                        f"({quoted_col} IS NULL OR TRIM(CAST({quoted_col} AS VARCHAR)) = '')"
                    )
                if in_parts:
                    conditions.append(f"({' OR '.join(in_parts)})")
            elif op == "not_in":
                selected = f.get("values") or []
                include_blanks = bool(f.get("includeBlanks"))
                not_parts: list[str] = []
                if selected:
                    placeholders = ", ".join("?" for _ in selected)
                    not_parts.append(f"CAST({quoted_col} AS VARCHAR) NOT IN ({placeholders})")
                    params.extend(selected)
                if include_blanks:
                    not_parts.append(
                        f"({quoted_col} IS NOT NULL AND TRIM(CAST({quoted_col} AS VARCHAR)) != '')"
                    )
                if not_parts:
                    conditions.append(f"({' AND '.join(not_parts)})")
            elif op in ("contains", "startswith", "endswith"):
                if op == "contains":
                    params.append(f"%{value}%")
                elif op == "startswith":
                    params.append(f"{value}%")
                else:  # endswith
                    params.append(f"%{value}")
                conditions.append(f"CAST({quoted_col} AS VARCHAR) ILIKE ?")
            else:
                params.append(value)
                conditions.append(f"{quoted_col} {sql_op} ?")

    if conditions:
        return " WHERE " + " AND ".join(conditions), params
    return "", []


def _build_order_clause(
    sort: list[dict[str, str]] | None,
    column_types: dict[str, str] | None = None,
) -> str:
    """Build ORDER BY clause from sort configuration with type-aware casts."""
    if not sort:
        return f" ORDER BY {quote_id(ROW_ID_COL)}"

    types = column_types or {}
    order_parts = []
    for s in sort:
        col = s.get("column", "")
        dir = s.get("dir", "asc").upper()
        if col:
            expr = _sort_expr_for_type(col, types.get(col, "TEXT"))
            order_parts.append(f"{expr} {dir} NULLS LAST")

    if order_parts:
        return " ORDER BY " + ", ".join(order_parts)
    return f" ORDER BY {quote_id(ROW_ID_COL)}"


def _parse_calc_expression(expression: str, columns: list[str]) -> str:
    """Parse and validate calculated column expression.

    Converts [Column Name] references to SQL identifiers.
    Validates only allowed functions and operators.
    """
    # Replace [Column Name] with quoted identifiers
    def replace_column_ref(match: re.Match) -> str:
        col_name = match.group(1).strip()
        if col_name not in columns:
            raise ValueError(f"Unknown column: {col_name}")
        return quote_id(col_name)

    # Replace column references
    parsed = re.sub(r'\[([^\]]+)\]', replace_column_ref, expression)

    # Validate no SQL injection
    # Only allow: function names, numbers, operators, parentheses, quoted identifiers, whitespace
    allowed_pattern = re.compile(
        r'^(?:\s*|\w+|\d+\.?\d*|'  # words and numbers
        r"\"[^\"]*\"|"  # double-quoted identifiers
        r'[+\-*/(),.=<>]|'  # operators and punctuation
        r"'[^']*'|"  # string literals
        r'\s+)+$',  # whitespace
        re.IGNORECASE
    )

    # Check for disallowed SQL keywords
    disallowed = {'SELECT', 'INSERT', 'UPDATE', 'DELETE', 'DROP', 'CREATE',
                  'ALTER', 'FROM', 'WHERE', 'JOIN', 'UNION', 'EXEC', 'EXECUTE'}

    # Extract words and check
    words = set(re.findall(r'\b[A-Z_]+\b', parsed.upper()))
    if words - _ALLOWED_CALC_FUNCTIONS - {'AS', 'NULL', 'AND', 'OR', 'NOT', 'CASE', 'WHEN', 'THEN', 'ELSE', 'END'}:
        # Check if any remaining are disallowed SQL
        if words & disallowed:
            raise ValueError("Expression contains disallowed SQL keywords")

    return parsed


# --- Public API ---


def get_preview_data(
    conn: DuckDBConnection,
    table_key: str,
    offset: int = 0,
    limit: int = DEFAULT_PAGE_SIZE,
    search: str | None = None,
    filters: list[dict[str, Any]] | None = None,
    sort: list[dict[str, str]] | None = None,
) -> dict[str, Any]:
    """Get paginated preview data with filtering, sorting, and search."""
    sql_table = lookup_sql_name(conn, table_key)
    if not sql_table or not table_exists(conn, sql_table):
        raise ValueError(f"Table not found: {table_key}")

    # Ensure row_id exists
    _ensure_row_id(conn, sql_table)

    cols = read_table_columns(conn, sql_table)
    if not cols:
        raise ValueError(f"Table has no columns: {table_key}")

    display_cols = _data_columns(cols)
    display_cols = _ordered_display_columns(conn, table_key, display_cols)

    # Sample rows for type inference (lightweight)
    quoted_table = quote_id(sql_table)
    sample_sql = f"SELECT * FROM {quoted_table} LIMIT 20"
    sample_raw = conn.execute(sample_sql).fetchall()
    sample_rows = [dict(zip(r.keys(), r)) for r in sample_raw]
    column_types = _resolve_effective_column_types(
        conn, sql_table, table_key, display_cols, sample_rows
    )

    # Build query
    where_clause, where_params = _build_where_clause(filters, search, display_cols)
    order_clause = _build_order_clause(sort, column_types)

    # Get total count
    count_sql = f"SELECT COUNT(*) FROM {quoted_table}{where_clause}"
    total_rows = conn.execute(count_sql, where_params).fetchone()[0]

    # Get data
    col_list = ", ".join(quote_id(c) for c in cols)
    if not col_list:
        raise ValueError(f"Table has no columns: {table_key}")
    data_sql = f"""
        SELECT {col_list}
        FROM {quoted_table}
        {where_clause}
        {order_clause}
        LIMIT ? OFFSET ?
    """
    params = where_params + [limit, offset]
    raw_rows = conn.execute(data_sql, params).fetchall()
    rows = [dict(zip(r.keys(), r)) for r in raw_rows]

    return {
        "columns": display_cols,
        "columnTypes": column_types,
        "rows": rows,
        "totalRows": total_rows,
        "offset": offset,
        "limit": limit,
    }


def get_column_filter_values(
    conn: DuckDBConnection,
    table_key: str,
    column: str,
    search: str | None = None,
    filters: list[dict[str, Any]] | None = None,
    limit: int = 500,
) -> dict[str, Any]:
    """Return distinct values for a column, respecting other active column filters."""
    sql_table = lookup_sql_name(conn, table_key)
    if not sql_table or not table_exists(conn, sql_table):
        raise ValueError(f"Table not found: {table_key}")

    _ensure_row_id(conn, sql_table)

    cols = read_table_columns(conn, sql_table)
    display_cols = _data_columns(cols)
    if column not in display_cols:
        raise ValueError(f"Column not found: {column}")

    quoted_table = quote_id(sql_table)
    quoted_col = quote_id(column)

    other_filters = [f for f in (filters or []) if f.get("column") != column]
    base_where, base_params = _build_where_clause(other_filters, None, display_cols)

    blank_cond = f"({quoted_col} IS NULL OR TRIM(CAST({quoted_col} AS VARCHAR)) = '')"
    blank_sql = f"SELECT 1 FROM {quoted_table}{base_where}"
    if base_where:
        blank_sql += f" AND {blank_cond}"
    else:
        blank_sql += f" WHERE {blank_cond}"
    blank_sql += " LIMIT 1"
    has_blanks = conn.execute(blank_sql, base_params).fetchone() is not None

    non_blank_cond = f"{quoted_col} IS NOT NULL AND TRIM(CAST({quoted_col} AS VARCHAR)) != ''"
    count_sql = f"SELECT COUNT(DISTINCT CAST({quoted_col} AS VARCHAR)) FROM {quoted_table}{base_where}"
    if base_where:
        count_sql += f" AND {non_blank_cond}"
    else:
        count_sql += f" WHERE {non_blank_cond}"
    total_distinct = conn.execute(count_sql, base_params).fetchone()[0]

    value_where = base_where
    value_params = list(base_params)
    extra_conds = [non_blank_cond]
    if search:
        extra_conds.append(f"CAST({quoted_col} AS VARCHAR) ILIKE ?")
        value_params.append(f"%{search}%")
    extra_clause = " AND ".join(extra_conds)
    if value_where:
        value_where = f"{value_where} AND {extra_clause}"
    else:
        value_where = f" WHERE {extra_clause}"

    value_sql = (
        f"SELECT DISTINCT CAST({quoted_col} AS VARCHAR) AS v "
        f"FROM {quoted_table}{value_where} ORDER BY v LIMIT ?"
    )
    value_params.append(limit)
    rows = conn.execute(value_sql, value_params).fetchall()
    values = [str(r["v"]) for r in rows]

    return {
        "values": values,
        "hasBlanks": has_blanks,
        "totalDistinct": total_distinct,
    }


def cell_edit(
    conn: DuckDBConnection,
    table_key: str,
    row_id: int,
    column: str,
    value: Any,
) -> dict[str, Any]:
    """Edit a single cell value."""
    sql_table = lookup_sql_name(conn, table_key)
    if not sql_table or not table_exists(conn, sql_table):
        raise ValueError(f"Table not found: {table_key}")

    _ensure_row_id(conn, sql_table)

    cols = read_table_columns(conn, sql_table)
    if column not in cols:
        raise ValueError(f"Column not found: {column}")

    quoted_table = quote_id(sql_table)
    quoted_col = quote_id(column)
    quoted_row_id = quote_id(ROW_ID_COL)

    old_row = conn.execute(
        f"SELECT {quoted_col} FROM {quoted_table} WHERE {quoted_row_id} = ?",
        [row_id],
    ).fetchone()
    old_value = old_row[column] if old_row else None

    # Save operation for undo
    state = _get_preview_state(conn, table_key)
    undo_stack = state.get("undo_stack", [])
    undo_stack.append({
        "op": "cell_edit",
        "row_id": row_id,
        "column": column,
        "old_value": old_value,
        "new_value": value,
        "timestamp": _now_iso(),
    })
    state["undo_stack"] = undo_stack[-100:]  # Keep last 100
    _save_preview_state(conn, table_key, state)

    # Execute update
    conn.execute(
        f"UPDATE {quoted_table} SET {quoted_col} = ? WHERE {quoted_row_id} = ?",
        [value, row_id],
    )
    conn.commit()

    return get_preview_data(conn, table_key)


def column_rename(
    conn: DuckDBConnection,
    table_key: str,
    old_name: str,
    new_name: str,
) -> dict[str, Any]:
    """Rename a column."""
    if not new_name or new_name == old_name:
        raise ValueError("Invalid column name")

    sql_table = lookup_sql_name(conn, table_key)
    if not sql_table or not table_exists(conn, sql_table):
        raise ValueError(f"Table not found: {table_key}")

    cols = read_table_columns(conn, sql_table)
    if old_name not in cols:
        raise ValueError(f"Column not found: {old_name}")
    if new_name in cols:
        raise ValueError(f"Column already exists: {new_name}")

    quoted_table = quote_id(sql_table)
    quoted_old = quote_id(old_name)
    quoted_new = quote_id(new_name)

    conn.execute(f"ALTER TABLE {quoted_table} RENAME COLUMN {quoted_old} TO {quoted_new}")
    conn.commit()

    return get_preview_data(conn, table_key)


def column_delete(
    conn: DuckDBConnection,
    table_key: str,
    column: str,
) -> dict[str, Any]:
    """Delete a column."""
    sql_table = lookup_sql_name(conn, table_key)
    if not sql_table or not table_exists(conn, sql_table):
        raise ValueError(f"Table not found: {table_key}")

    cols = read_table_columns(conn, sql_table)
    if column not in cols:
        raise ValueError(f"Column not found: {column}")

    data_cols = _data_columns(cols)
    if column not in data_cols:
        raise ValueError(f"Cannot delete internal column: {column}")
    if len(data_cols) <= 1:
        raise ValueError("Cannot delete the last column")

    # DuckDB doesn't support DROP COLUMN directly, so recreate table
    quoted_table = quote_id(sql_table)
    temp_table = f"temp_{sql_table}_{uuid.uuid4().hex[:8]}"
    quoted_temp = quote_id(temp_table)

    new_cols = [c for c in cols if c != column]
    if not new_cols:
        raise ValueError("Cannot delete the last column")
    col_list = ", ".join(quote_id(c) for c in new_cols)
    if not col_list:
        raise ValueError("Cannot delete the last column")

    conn.execute(f"""
        CREATE TABLE {quoted_temp} AS
        SELECT {col_list}
        FROM {quoted_table}
    """)

    drop_table(conn, sql_table, commit=False)
    conn.execute(f"ALTER TABLE {quoted_temp} RENAME TO {quoted_table}")
    conn.commit()

    return get_preview_data(conn, table_key)


def column_reorder(
    conn: DuckDBConnection,
    table_key: str,
    new_order: list[str],
) -> dict[str, Any]:
    """Store column order preference (virtual reorder)."""
    sql_table = lookup_sql_name(conn, table_key)
    if not sql_table or not table_exists(conn, sql_table):
        raise ValueError(f"Table not found: {table_key}")

    cols = read_table_columns(conn, sql_table)
    # Validate all columns exist
    for col in new_order:
        if col not in cols:
            raise ValueError(f"Unknown column: {col}")

    # Store in state
    state = _get_preview_state(conn, table_key)
    state["column_order"] = new_order
    _save_preview_state(conn, table_key, state)

    return get_preview_data(conn, table_key)


def column_change_type(
    conn: DuckDBConnection,
    table_key: str,
    column: str,
    new_type: str,
) -> dict[str, Any]:
    """Change the data type of a column.

    Supported types: TEXT, INTEGER, DOUBLE, DATE, BOOLEAN.
    Uses ALTER TABLE ALTER COLUMN with CAST for data conversion.
    """
    # Validate supported types
    valid_types = {"TEXT", "INTEGER", "DOUBLE", "DATE", "BOOLEAN"}
    if new_type not in valid_types:
        raise ValueError(f"Unsupported data type: {new_type}. Use: {', '.join(valid_types)}")

    sql_table = lookup_sql_name(conn, table_key)
    if not sql_table or not table_exists(conn, sql_table):
        raise ValueError(f"Table not found: {table_key}")

    cols = read_table_columns(conn, sql_table)
    if column not in cols:
        raise ValueError(f"Column not found: {column}")

    quoted_table = quote_id(sql_table)
    quoted_col = quote_id(column)

    # DuckDB: ALTER TABLE ... ALTER COLUMN ... TYPE ...
    # This requires recreating the column with CAST
    temp_table = f"temp_{sql_table}_{uuid.uuid4().hex[:8]}"
    quoted_temp = quote_id(temp_table)

    # Build column list with the changed type
    col_defs = []
    for col in cols:
        if col == column:
            col_defs.append(f"CAST({quote_id(col)} AS {new_type}) AS {quote_id(col)}")
        else:
            col_defs.append(quote_id(col))

    # Create new table with updated column type
    conn.execute(f"""
        CREATE TABLE {quoted_temp} AS
        SELECT {', '.join(col_defs)}
        FROM {quoted_table}
    """)

    # Drop old table and rename new one
    drop_table(conn, sql_table, commit=False)
    conn.execute(f"ALTER TABLE {quoted_temp} RENAME TO {quoted_table}")
    conn.commit()

    _save_column_type_override(conn, table_key, column, new_type)

    return get_preview_data(conn, table_key)


def rows_delete(
    conn: DuckDBConnection,
    table_key: str,
    row_ids: list[int],
) -> dict[str, Any]:
    """Delete specific rows by their __row_id values."""
    if not row_ids:
        raise ValueError("No rows specified for deletion")

    sql_table = lookup_sql_name(conn, table_key)
    if not sql_table or not table_exists(conn, sql_table):
        raise ValueError(f"Table not found: {table_key}")

    _ensure_row_id(conn, sql_table)

    quoted_table = quote_id(sql_table)
    quoted_row_id = quote_id(ROW_ID_COL)

    # Build placeholders for IN clause
    placeholders = ', '.join('?' for _ in row_ids)

    # Delete rows using DELETE with WHERE __row_id IN (...)
    conn.execute(f"""
        DELETE FROM {quoted_table}
        WHERE {quoted_row_id} IN ({placeholders})
    """, row_ids)

    conn.commit()

    # Rebuild __row_id to maintain sequential numbering
    cols = read_table_columns(conn, sql_table)
    display_cols = _data_columns(cols)
    _rebuild_row_id_table(conn, sql_table, display_cols)

    return get_preview_data(conn, table_key)


def add_calculated_column(
    conn: DuckDBConnection,
    table_key: str,
    name: str,
    expression: str,
    data_type: str = "TEXT",
) -> dict[str, Any]:
    """Add a calculated column."""
    sql_table = lookup_sql_name(conn, table_key)
    if not sql_table or not table_exists(conn, sql_table):
        raise ValueError(f"Table not found: {table_key}")

    cols = read_table_columns(conn, sql_table)
    if name in cols:
        raise ValueError(f"Column already exists: {name}")

    # Parse and validate expression
    parsed_expr = _parse_calc_expression(expression, cols)

    quoted_table = quote_id(sql_table)
    quoted_name = quote_id(name)

    # Add column with default expression
    conn.execute(f"""
        ALTER TABLE {quoted_table}
        ADD COLUMN {quoted_name} {data_type}
        DEFAULT ({parsed_expr})
    """)

    # Update existing rows
    conn.execute(f"""
        UPDATE {quoted_table}
        SET {quoted_name} = {parsed_expr}
    """)

    conn.commit()

    return get_preview_data(conn, table_key)


def apply_filter_sort(
    conn: DuckDBConnection,
    table_key: str,
    filters: list[dict[str, Any]] | None = None,
    sort: list[dict[str, str]] | None = None,
    search: str | None = None,
) -> dict[str, Any]:
    """Apply filter and sort (virtual operations that affect preview but not base table)."""
    # These are just parameters to get_preview_data
    return get_preview_data(conn, table_key, filters=filters, sort=sort, search=search)


def create_pivot(
    conn: DuckDBConnection,
    table_key: str,
    row_fields: list[str],
    column_fields: list[str],
    value_fields: list[dict[str, str]],
) -> dict[str, Any]:
    """Create a pivot table.

    Creates a new SQL table with the pivot result and registers it for use.
    Returns the new table key for the pivot result.
    """
    sql_table = lookup_sql_name(conn, table_key)
    if not sql_table or not table_exists(conn, sql_table):
        raise ValueError(f"Table not found: {table_key}")

    cols = read_table_columns(conn, sql_table)

    # Validate fields
    for f in row_fields + column_fields:
        if f not in cols:
            raise ValueError(f"Unknown column: {f}")

    for vf in value_fields:
        field = vf.get("field", "")
        if field not in cols:
            raise ValueError(f"Unknown value field: {field}")

    if not value_fields:
        raise ValueError("Pivot requires at least one value field")

    if column_fields and not row_fields:
        raise ValueError("Pivot requires at least one row field when column fields are specified")

    quoted_table = quote_id(sql_table)

    # Get unique values for column fields
    column_values = []
    for col_field in column_fields:
        result = conn.execute(
            f"SELECT DISTINCT {quote_id(col_field)} FROM {quoted_table} ORDER BY {quote_id(col_field)} LIMIT {MAX_PIVOT_COLUMNS}"
        ).fetchall()
        column_values.append([r[0] for r in result])

    if len(column_values) > 0 and len(column_values[0]) * len(value_fields) > MAX_PIVOT_COLUMNS:
        raise ValueError(f"Too many pivot columns. Maximum is {MAX_PIVOT_COLUMNS}")

    # Build pivot query using conditional aggregation
    select_parts = [quote_id(f) for f in row_fields]
    params: list[Any] = []

    if not column_fields and value_fields:
        for vf in value_fields:
            field = vf.get("field", "")
            agg_key = vf.get("aggregation", "sum")
            alias = f"{field}_{agg_key}"
            select_parts.append(
                f"{_pivot_agg_expr(field, agg_key)} AS {quote_id(alias)}"
            )
    else:
        for vf in value_fields:
            field = vf.get("field", "")
            agg_key = vf.get("aggregation", "sum")
            for i, col_vals in enumerate(column_values):
                for val in col_vals:
                    col_name = f"{val}_{field}_{_AGG_FUNCS.get(agg_key, 'SUM')}"
                    inner_expr = _pivot_agg_expr(field, agg_key)
                    # Wrap conditional aggregation around the inner expression
                    if agg_key in _NUMERIC_AGG_KEYS:
                        cast_field = f"TRY_CAST({quote_id(field)} AS DOUBLE)"
                        agg = _AGG_FUNCS.get(agg_key, "SUM")
                        inner_expr = f"{agg}(CASE WHEN {quote_id(column_fields[i])} = ? THEN {cast_field} END)"
                    elif agg_key == "count_distinct":
                        inner_expr = (
                            f"COUNT(DISTINCT CASE WHEN {quote_id(column_fields[i])} = ? "
                            f"THEN {quote_id(field)} END)"
                        )
                    else:
                        agg = _AGG_FUNCS.get(agg_key, "COUNT")
                        inner_expr = (
                            f"{agg}(CASE WHEN {quote_id(column_fields[i])} = ? "
                            f"THEN {quote_id(field)} END)"
                        )
                    select_parts.append(
                        f"{inner_expr} AS {quote_id(col_name)}"
                    )
                    params.append(val)

    group_by = ", ".join(quote_id(f) for f in row_fields)

    # Create new pivot table (pg__ prefix so invalidation preserves playground tables)
    pivot_id = uuid.uuid4().hex[:8]
    pivot_table_name = safe_table_name("pg", f"pivot_{pivot_id}")
    quoted_pivot = quote_id(pivot_table_name)

    # Build and execute CREATE TABLE AS SELECT
    pivot_sql = f"""
        CREATE TABLE {quoted_pivot} AS
        SELECT {', '.join(select_parts)}
        FROM {quoted_table}
        {f"GROUP BY {group_by}" if row_fields else ""}
    """

    conn.execute(pivot_sql, params)
    conn.commit()

    # Get the new table's columns and row count
    pivot_cols = read_table_columns(conn, pivot_table_name)
    pivot_row_count = table_row_count(conn, pivot_table_name)

    # Register as a new table key for the playground preview overlay.
    new_table_key = f"playground::pivot_{pivot_id}"

    register_table(conn, new_table_key, pivot_table_name)

    return {
        "success": True,
        "message": "Pivot table created successfully",
        "columns": pivot_cols,
        "totalRows": pivot_row_count,
        "newTableKey": new_table_key,
        "tableName": pivot_table_name,
    }


def undo_operation(conn: DuckDBConnection, table_key: str) -> dict[str, Any]:
    """Undo last operation."""
    state = _get_preview_state(conn, table_key)
    undo_stack = state.get("undo_stack", [])

    if not undo_stack:
        raise ValueError("Nothing to undo")

    last_op = undo_stack.pop()

    # Restore old value for cell edit
    if last_op.get("op") == "cell_edit":
        sql_table = lookup_sql_name(conn, table_key)
        if sql_table and table_exists(conn, sql_table):
            quoted_table = quote_id(sql_table)
            quoted_col = quote_id(last_op["column"])
            quoted_row_id = quote_id(ROW_ID_COL)
            conn.execute(
                f"UPDATE {quoted_table} SET {quoted_col} = ? WHERE {quoted_row_id} = ?",
                [last_op.get("old_value"), last_op["row_id"]],
            )
            conn.commit()

    state["undo_stack"] = undo_stack
    state["redo_stack"] = state.get("redo_stack", []) + [last_op]
    _save_preview_state(conn, table_key, state)

    return get_preview_data(conn, table_key)


def redo_operation(conn: DuckDBConnection, table_key: str) -> dict[str, Any]:
    """Redo last undone operation."""
    state = _get_preview_state(conn, table_key)
    redo_stack = state.get("redo_stack", [])

    if not redo_stack:
        raise ValueError("Nothing to redo")

    next_op = redo_stack.pop()

    # Re-apply operation
    if next_op.get("op") == "cell_edit":
        sql_table = lookup_sql_name(conn, table_key)
        if sql_table and table_exists(conn, sql_table):
            quoted_table = quote_id(sql_table)
            quoted_col = quote_id(next_op["column"])
            quoted_row_id = quote_id(ROW_ID_COL)
            conn.execute(
                f"UPDATE {quoted_table} SET {quoted_col} = ? WHERE {quoted_row_id} = ?",
                [next_op.get("new_value"), next_op["row_id"]],
            )
            conn.commit()

    state["redo_stack"] = redo_stack
    state["undo_stack"] = state.get("undo_stack", []) + [next_op]
    _save_preview_state(conn, table_key, state)

    return get_preview_data(conn, table_key)


def apply_to_pipeline(
    conn: DuckDBConnection,
    table_key: str,
    target_kind: str,
    target_id: str,
    mode: str = "replace",
) -> dict[str, Any]:
    """Apply preview changes back to a raw table or appended group table."""
    sql_table = lookup_sql_name(conn, table_key)
    if not sql_table or not table_exists(conn, sql_table):
        raise ValueError(f"Table not found: {table_key}")

    if target_kind in ("merge_version", "merge_group"):
        raise ValueError("Applying preview changes to merge outputs is not supported yet")

    if target_kind not in ("raw", "group"):
        raise ValueError(f"Unsupported apply target: {target_kind}")

    reset_step = 2 if target_kind == "raw" else 5
    target_sql = lookup_sql_name(conn, target_id)
    if not target_sql or not table_exists(conn, target_sql):
        raise ValueError(f"Target not found: {target_id}")

    source_cols = read_table_columns(conn, sql_table)
    data_cols = _data_columns(source_cols)
    if not data_cols:
        raise ValueError("Preview table has no data columns to apply")

    col_list = ", ".join(quote_id(c) for c in data_cols)
    quoted_target = quote_id(target_sql)
    quoted_source = quote_id(sql_table)

    if mode == "add":
        conn.execute(
            f"INSERT INTO {quoted_target} SELECT {col_list} FROM {quoted_source}"
        )
    else:
        drop_table(conn, target_sql, commit=False)
        conn.execute(
            f"CREATE TABLE {quoted_target} AS SELECT {col_list} FROM {quoted_source}"
        )
    conn.commit()
    invalidate_session_downstream(conn, reset_step)

    return {
        "ok": True,
        "resetStep": reset_step,
        "message": f"Applied to {target_kind}: {target_id}",
        "row_count": table_row_count(conn, target_sql),
    }


# --- Unified operation handler ---


def run_operation(
    conn: DuckDBConnection,
    table_key: str,
    op: str,
    params: dict[str, Any],
) -> dict[str, Any]:
    """Run a preview operation and return updated preview data."""
    if op == "cell_edit":
        return cell_edit(
            conn,
            table_key,
            params.get("rowId"),
            params.get("column", ""),
            params.get("value"),
        )
    elif op == "column_rename":
        return column_rename(
            conn,
            table_key,
            params.get("oldName", ""),
            params.get("newName", ""),
        )
    elif op == "column_delete":
        return column_delete(conn, table_key, params.get("column", ""))
    elif op == "column_reorder":
        return column_reorder(conn, table_key, params.get("order", []))
    elif op == "calculated_column":
        return add_calculated_column(
            conn,
            table_key,
            params.get("name", ""),
            params.get("expression", ""),
            params.get("dataType", "TEXT"),
        )
    elif op == "filter":
        return apply_filter_sort(
            conn,
            table_key,
            filters=params.get("filters"),
            sort=params.get("sort"),
            search=params.get("search"),
        )
    elif op == "sort":
        return apply_filter_sort(
            conn,
            table_key,
            sort=params.get("sort"),
        )
    elif op == "pivot":
        return create_pivot(
            conn,
            table_key,
            params.get("rowFields", []),
            params.get("columnFields", []),
            params.get("valueFields", []),
        )
    elif op == "column_change_type":
        return column_change_type(
            conn,
            table_key,
            params.get("column", ""),
            params.get("newType", "TEXT"),
        )
    elif op == "rows_delete":
        return rows_delete(
            conn,
            table_key,
            params.get("rowIds", []),
        )
    elif op == "undo":
        return undo_operation(conn, table_key)
    elif op == "redo":
        return redo_operation(conn, table_key)
    else:
        raise ValueError(f"Unknown operation: {op}")
