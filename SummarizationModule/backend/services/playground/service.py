from __future__ import annotations

import json
import re
import uuid
from hashlib import sha256
from typing import Any

from shared.db import delete_meta, get_meta, set_meta
from shared.duckdb_compat import DuckDBConnection
from services.upload.file_loader import (
    build_preview,
    collect_column_info,
    ensure_column_metadata_for_table,
)


class PreviewValidationError(ValueError):
    def __init__(self, message: str, details: dict[str, Any] | None = None):
        super().__init__(message)
        self.details = details or {}


ROW_ID_COL = "RECORD_ID"
DEFAULT_LIMIT = 200
MAX_LIMIT = 1000
MAX_COLUMN_VALUES = 500
MAX_FILTER_VALUES = 500
MAX_ROW_DELETE = 5000
MAX_UNDO_DEPTH = 5
MAX_CALC_EXPRESSION = 500
MAX_PIVOT_COLUMNS = 200
MAX_PIVOT_ROW_FIELDS = 5
MAX_PIVOT_VALUE_FIELDS = 5
MAX_PIVOT_OUTPUT_ROWS = 100_000
MAX_PIVOT_OUTPUT_CELLS = 1_000_000
MAX_READ_CACHE_ENTRIES = 64
LARGE_TABLE_SEARCH_ROW_THRESHOLD = 100_000
LARGE_TABLE_MIN_SEARCH_LENGTH = 3
LARGE_TABLE_SEARCHABLE_COLUMNS = 50

_STATE_PREFIX = "playground_state::"
_READ_CACHE_PREFIX = "playground_read_cache::"
_VALID_FILTER_OPS = {
    "eq",
    "neq",
    "gt",
    "gte",
    "lt",
    "lte",
    "contains",
    "startswith",
    "endswith",
    "is_null",
    "is_not_null",
    "in",
    "not_in",
}
_FILTER_SQL = {
    "eq": "=",
    "neq": "!=",
    "gt": ">",
    "gte": ">=",
    "lt": "<",
    "lte": "<=",
}
_VALID_TYPES = {"TEXT", "INTEGER", "DOUBLE", "DATE", "BOOLEAN"}
_AGGREGATIONS = {
    "sum": "SUM",
    "count": "COUNT",
    "avg": "AVG",
    "min": "MIN",
    "max": "MAX",
    "count_distinct": "COUNT_DISTINCT",
}
_NUMERIC_AGGS = {"sum", "avg", "min", "max"}
_ALLOWED_CALC_FUNCTIONS = {
    "ABS",
    "CAST",
    "CEIL",
    "COALESCE",
    "CONCAT",
    "DATE",
    "DAY",
    "DOUBLE",
    "FLOOR",
    "LEFT",
    "LENGTH",
    "LOWER",
    "MONTH",
    "NULLIF",
    "POWER",
    "ROUND",
    "RIGHT",
    "SQRT",
    "SUBSTRING",
    "TRIM",
    "TRY_CAST",
    "UPPER",
    "VARCHAR",
    "YEAR",
}
_DISALLOWED_SQL_WORDS = {
    "ALTER",
    "ATTACH",
    "COPY",
    "CREATE",
    "DELETE",
    "DETACH",
    "DROP",
    "EXEC",
    "EXECUTE",
    "FROM",
    "INSERT",
    "JOIN",
    "PRAGMA",
    "SELECT",
    "UNION",
    "UPDATE",
    "WHERE",
}


def _quote_id(identifier: str) -> str:
    return '"' + identifier.replace('"', '""') + '"'


def _safe_table_suffix(value: str) -> str:
    cleaned = re.sub(r"[^A-Za-z0-9_]+", "_", value).strip("_").lower()
    return (cleaned or "table")[:48]


def _safe_physical_name(prefix: str, existing: set[str]) -> str:
    base = re.sub(r"[^A-Za-z0-9_]+", "_", prefix.upper()).strip("_") or "COLUMN"
    base = base[:80]
    candidate = base
    index = 1
    while candidate in existing or candidate == ROW_ID_COL:
        candidate = f"{base}_{index}"
        index += 1
    existing.add(candidate)
    return candidate


def _state_key(table_key: str) -> str:
    return f"{_STATE_PREFIX}{table_key}"


def _read_cache_key(table_key: str) -> str:
    return f"{_READ_CACHE_PREFIX}{table_key}"


def _json_clone(value: Any) -> Any:
    return json.loads(json.dumps(value))


def _cache_digest(parts: dict[str, Any]) -> str:
    serialized = json.dumps(parts, sort_keys=True, separators=(",", ":"), default=str)
    return sha256(serialized.encode("utf-8")).hexdigest()


def _load_read_cache(conn: DuckDBConnection, table_key: str) -> dict[str, Any]:
    cache = get_meta(conn, _read_cache_key(table_key))
    if not isinstance(cache, dict) or not isinstance(cache.get("entries"), list):
        return {"entries": []}
    return cache


def _cache_get(conn: DuckDBConnection, table_key: str, digest: str) -> Any | None:
    cache = _load_read_cache(conn, table_key)
    entries = list(cache.get("entries") or [])
    for index, entry in enumerate(entries):
        if isinstance(entry, dict) and entry.get("key") == digest:
            entries.append(entries.pop(index))
            set_meta(conn, _read_cache_key(table_key), {"entries": entries[-MAX_READ_CACHE_ENTRIES:]})
            return _json_clone(entry.get("value"))
    return None


def _cache_set(conn: DuckDBConnection, table_key: str, digest: str, value: Any) -> None:
    cache = _load_read_cache(conn, table_key)
    entries = [
        entry
        for entry in list(cache.get("entries") or [])
        if isinstance(entry, dict) and entry.get("key") != digest
    ]
    entries.append({"key": digest, "value": _json_clone(value)})
    set_meta(conn, _read_cache_key(table_key), {"entries": entries[-MAX_READ_CACHE_ENTRIES:]})


def _load_state(conn: DuckDBConnection, table_key: str) -> dict[str, Any]:
    raw = get_meta(conn, _state_key(table_key))
    if not isinstance(raw, dict):
        raw = {}
    raw.setdefault("tableVersion", 0)
    raw.setdefault("dirty", False)
    raw.setdefault("undoStack", [])
    raw.setdefault("redoStack", [])
    raw.setdefault("sourceTableKey", table_key)
    return raw


def _save_state(conn: DuckDBConnection, table_key: str, state: dict[str, Any]) -> None:
    set_meta(conn, _state_key(table_key), state)


def _ensure_playground_schema(conn: DuckDBConnection) -> None:
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _playground_registry ("
        "table_key VARCHAR PRIMARY KEY, "
        "data_table VARCHAR, "
        "source_table_key VARCHAR, "
        "kind VARCHAR"
        ")"
    )
    conn.execute(
        "CREATE TABLE IF NOT EXISTS _playground_columns ("
        "table_key VARCHAR, "
        "column_key VARCHAR, "
        "physical_name VARCHAR, "
        "display_name VARCHAR, "
        "data_type VARCHAR, "
        "column_order INTEGER, "
        "hidden BOOLEAN, "
        "PRIMARY KEY (table_key, column_key)"
        ")"
    )
    conn.commit()


def _table_exists(conn: DuckDBConnection, table_name: str | None) -> bool:
    if not table_name:
        return False
    row = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema = 'main' AND table_name = ?",
        (table_name,),
    ).fetchone()
    return bool(row and int(row[0] or 0) > 0)


def _read_table_columns(conn: DuckDBConnection, table_name: str) -> list[str]:
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_schema = 'main' AND table_name = ? "
        "ORDER BY ordinal_position",
        (table_name,),
    ).fetchall()
    return [row[0] for row in rows]


def _preview_data_type(duckdb_type: str) -> str:
    upper = str(duckdb_type or "").upper()
    if any(token in upper for token in ("INT", "BIGINT", "SMALLINT", "TINYINT")):
        return "INTEGER"
    if any(token in upper for token in ("DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC")):
        return "DOUBLE"
    if "DATE" in upper or "TIME" in upper:
        return "DATE"
    if "BOOL" in upper:
        return "BOOLEAN"
    return "TEXT"


def _read_table_types(conn: DuckDBConnection, table_name: str) -> dict[str, str]:
    rows = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE table_schema = 'main' AND table_name = ?",
        (table_name,),
    ).fetchall()
    return {row[0]: _preview_data_type(row[1]) for row in rows}


def _get_source_registry_entry(conn: DuckDBConnection, table_key: str) -> dict[str, Any] | None:
    try:
        row = conn.execute(
            "SELECT table_key, data_table, raw_table FROM _table_registry WHERE table_key = ?",
            (table_key,),
        ).fetchone()
    except Exception:
        row = None
    if not row:
        return None
    return {
        "tableKey": row[0],
        "dataTable": row[1],
        "rawTable": row[2],
        "kind": "source",
        "sourceTableKey": row[0],
    }


def _get_playground_registry_entry(conn: DuckDBConnection, table_key: str) -> dict[str, Any] | None:
    _ensure_playground_schema(conn)
    row = conn.execute(
        "SELECT table_key, data_table, source_table_key, kind "
        "FROM _playground_registry WHERE table_key = ?",
        (table_key,),
    ).fetchone()
    if not row:
        return None
    return {
        "tableKey": row[0],
        "dataTable": row[1],
        "rawTable": None,
        "kind": row[3] or "playground",
        "sourceTableKey": row[2] or row[0],
    }


def _get_registry_entry(conn: DuckDBConnection, table_key: str) -> dict[str, Any]:
    entry = _get_source_registry_entry(conn, table_key)
    if entry is None:
        entry = _get_playground_registry_entry(conn, table_key)
    if entry is None:
        raise PreviewValidationError("Unknown tableKey.", {"field": "tableKey"})
    if not _table_exists(conn, entry["dataTable"]):
        raise PreviewValidationError("Preview table no longer exists.", {"field": "tableKey"})
    return entry


def _source_metadata(conn: DuckDBConnection, table_key: str) -> list[dict[str, Any]]:
    metadata = ensure_column_metadata_for_table(conn, table_key)
    return [dict(item) for item in metadata]


def _playground_metadata(conn: DuckDBConnection, table_key: str) -> list[dict[str, Any]]:
    _ensure_playground_schema(conn)
    rows = conn.execute(
        "SELECT column_key, physical_name, display_name, data_type, column_order, hidden "
        "FROM _playground_columns WHERE table_key = ? "
        "ORDER BY column_order, column_key",
        (table_key,),
    ).fetchall()
    return [
        {
            "key": row[0],
            "physicalName": row[1],
            "displayName": row[2],
            "dataType": row[3],
            "order": int(row[4] or 0),
            "hidden": bool(row[5]),
        }
        for row in rows
    ]


def _replace_playground_metadata(
    conn: DuckDBConnection,
    table_key: str,
    metadata: list[dict[str, Any]],
) -> None:
    _ensure_playground_schema(conn)
    conn.execute("DELETE FROM _playground_columns WHERE table_key = ?", (table_key,))
    rows = [
        (
            table_key,
            col["key"],
            col["physicalName"],
            col["displayName"],
            col["dataType"],
            int(col.get("order", 0)),
            bool(col.get("hidden", False)),
        )
        for col in metadata
    ]
    if rows:
        conn.executemany(
            "INSERT OR REPLACE INTO _playground_columns "
            "(table_key, column_key, physical_name, display_name, data_type, column_order, hidden) "
            "VALUES (?, ?, ?, ?, ?, ?, ?)",
            rows,
        )
    conn.commit()


def _clear_playground_metadata(conn: DuckDBConnection, table_key: str) -> None:
    _ensure_playground_schema(conn)
    conn.execute("DELETE FROM _playground_columns WHERE table_key = ?", (table_key,))
    conn.commit()


def _metadata_for_table(conn: DuckDBConnection, table_key: str) -> list[dict[str, Any]]:
    overlay = _playground_metadata(conn, table_key)
    if overlay:
        return overlay
    entry = _get_source_registry_entry(conn, table_key)
    if entry:
        return _source_metadata(conn, table_key)
    return []


def _active_table(conn: DuckDBConnection, table_key: str) -> str:
    state = _load_state(conn, table_key)
    table_name = state.get("playgroundTableName")
    if table_name and _table_exists(conn, table_name):
        return table_name
    return _get_registry_entry(conn, table_key)["dataTable"]


def _visible_metadata(conn: DuckDBConnection, table_key: str) -> list[dict[str, Any]]:
    active_columns = set(_read_table_columns(conn, _active_table(conn, table_key)))
    result = []
    for col in _metadata_for_table(conn, table_key):
        if col["hidden"]:
            continue
        if col["physicalName"] not in active_columns:
            continue
        result.append(col)
    return sorted(result, key=lambda item: (int(item.get("order", 0)), item["key"]))


def _column_by_key(
    conn: DuckDBConnection,
    table_key: str,
    column_key: str,
    *,
    include_hidden: bool = False,
) -> dict[str, Any]:
    for col in _metadata_for_table(conn, table_key):
        if col["key"] == column_key and (include_hidden or not col["hidden"]):
            return col
    raise PreviewValidationError("Unknown columnKey.", {"field": "columnKey", "columnKey": column_key})


def _normalize_type(data_type: Any) -> str:
    normalized = str(data_type or "TEXT").upper()
    if normalized not in _VALID_TYPES:
        raise PreviewValidationError("Unsupported dataType.", {"dataType": data_type})
    return normalized


def _sort_expr(col: dict[str, Any]) -> str:
    quoted = _quote_id(col["physicalName"])
    data_type = str(col.get("dataType") or "TEXT").upper()
    if data_type in {"INTEGER", "DOUBLE"}:
        return f"TRY_CAST(REPLACE(REPLACE(TRIM(CAST({quoted} AS VARCHAR)), ',', ''), ' ', '') AS DOUBLE)"
    if data_type == "DATE":
        return f"TRY_CAST({quoted} AS TIMESTAMP)"
    if data_type == "BOOLEAN":
        return f"TRY_CAST({quoted} AS BOOLEAN)"
    return quoted


def _filter_values(item: dict[str, Any]) -> list[Any]:
    if "values" in item:
        values = item.get("values")
    else:
        values = item.get("value")
    if values is None:
        values = []
    if not isinstance(values, list):
        values = [values]
    if len(values) > MAX_FILTER_VALUES:
        raise PreviewValidationError("Filter values are capped at 500.", {"field": "filters"})
    return values


def _build_where_clause(
    conn: DuckDBConnection,
    table_key: str,
    filters: list[dict[str, Any]] | None,
    search: str | None,
    *,
    exclude_column_key: str | None = None,
    search_columns: list[dict[str, Any]] | None = None,
) -> tuple[str, list[Any]]:
    visible = _visible_metadata(conn, table_key)
    visible_by_key = {col["key"]: col for col in visible}
    conditions: list[str] = []
    params: list[Any] = []

    if search:
        parts = []
        for col in search_columns if search_columns is not None else visible:
            parts.append(f"CAST({_quote_id(col['physicalName'])} AS VARCHAR) ILIKE ?")
            params.append(f"%{search}%")
        if parts:
            conditions.append("(" + " OR ".join(parts) + ")")

    for item in filters or []:
        column_key = item.get("columnKey")
        if column_key == exclude_column_key:
            continue
        if column_key not in visible_by_key:
            raise PreviewValidationError("Unknown columnKey.", {"field": "filters", "columnKey": column_key})
        op = item.get("op", "eq")
        if op not in _VALID_FILTER_OPS:
            raise PreviewValidationError("Unsupported filter op.", {"field": "filters", "op": op})

        col = visible_by_key[column_key]
        quoted = _quote_id(col["physicalName"])
        if op in {"is_null", "is_not_null"}:
            blank_expr = f"({quoted} IS NULL OR TRIM(CAST({quoted} AS VARCHAR)) = '')"
            conditions.append(blank_expr if op == "is_null" else f"NOT {blank_expr}")
        elif op in {"contains", "startswith", "endswith"}:
            raw_value = "" if item.get("value") is None else str(item.get("value"))
            if op == "contains":
                value = f"%{raw_value}%"
            elif op == "startswith":
                value = f"{raw_value}%"
            else:
                value = f"%{raw_value}"
            conditions.append(f"CAST({quoted} AS VARCHAR) ILIKE ?")
            params.append(value)
        elif op in {"in", "not_in"}:
            selected = _filter_values(item)
            include_blanks = bool(item.get("includeBlanks"))
            blank_expr = f"({quoted} IS NULL OR TRIM(CAST({quoted} AS VARCHAR)) = '')"
            if op == "in":
                parts = []
                if selected:
                    placeholders = ", ".join("?" for _ in selected)
                    parts.append(f"CAST({quoted} AS VARCHAR) IN ({placeholders})")
                    params.extend(str(value) for value in selected)
                if include_blanks:
                    parts.append(blank_expr)
                conditions.append("(" + " OR ".join(parts) + ")" if parts else "1 = 0")
            else:
                parts = []
                if selected:
                    placeholders = ", ".join("?" for _ in selected)
                    parts.append(f"CAST({quoted} AS VARCHAR) NOT IN ({placeholders})")
                    params.extend(str(value) for value in selected)
                if include_blanks:
                    parts.append(f"NOT {blank_expr}")
                conditions.append("(" + " AND ".join(parts) + ")" if parts else "1 = 1")
        else:
            if "value" not in item:
                raise PreviewValidationError("Filter value is required.", {"field": "filters"})
            conditions.append(f"{quoted} {_FILTER_SQL[op]} ?")
            params.append(item.get("value"))

    return (" WHERE " + " AND ".join(conditions), params) if conditions else ("", [])


def _large_table_row_count(conn: DuckDBConnection, table_name: str) -> int:
    row = conn.execute(f"SELECT COUNT(*) FROM {_quote_id(table_name)}").fetchone()
    return int(row[0] or 0) if row else 0


def _search_columns_for_table(
    conn: DuckDBConnection,
    table_name: str,
    visible: list[dict[str, Any]],
    search: str | None,
) -> list[dict[str, Any]] | None:
    if not search:
        return None
    row_count = _large_table_row_count(conn, table_name)
    if row_count < LARGE_TABLE_SEARCH_ROW_THRESHOLD:
        return None

    if len(search.strip()) < LARGE_TABLE_MIN_SEARCH_LENGTH:
        raise PreviewValidationError(
            f"Search must be at least {LARGE_TABLE_MIN_SEARCH_LENGTH} characters for large tables.",
            {
                "field": "search",
                "minLength": LARGE_TABLE_MIN_SEARCH_LENGTH,
                "rowThreshold": LARGE_TABLE_SEARCH_ROW_THRESHOLD,
            },
        )
    return visible[:LARGE_TABLE_SEARCHABLE_COLUMNS]


def _build_order_clause(
    conn: DuckDBConnection,
    table_key: str,
    sort: list[dict[str, Any]] | None,
) -> str:
    if not sort:
        return f" ORDER BY {_quote_id(ROW_ID_COL)}"
    visible_by_key = {col["key"]: col for col in _visible_metadata(conn, table_key)}
    parts = []
    for item in sort:
        column_key = item.get("columnKey")
        col = visible_by_key.get(column_key)
        if not col:
            raise PreviewValidationError("Unknown columnKey.", {"field": "sort", "columnKey": column_key})
        direction = str(item.get("dir", "asc")).lower()
        if direction not in {"asc", "desc"}:
            raise PreviewValidationError("Sort dir must be asc or desc.", {"field": "sort"})
        parts.append(f"{_sort_expr(col)} {direction.upper()} NULLS LAST")
    return " ORDER BY " + ", ".join(parts) if parts else f" ORDER BY {_quote_id(ROW_ID_COL)}"


def _state_depths(state: dict[str, Any]) -> dict[str, Any]:
    return {
        "tableVersion": int(state.get("tableVersion") or 0),
        "dirty": bool(state.get("dirty")),
        "undoDepth": len(state.get("undoStack") or []),
        "redoDepth": len(state.get("redoStack") or []),
        "sourceTableKey": state.get("sourceTableKey"),
        "playgroundTableName": state.get("playgroundTableName"),
    }


def _json_safe(value: Any) -> Any:
    if value is None:
        return ""
    if isinstance(value, float):
        if value != value or value in (float("inf"), float("-inf")):
            return ""
    return value


def get_preview_state(
    conn: DuckDBConnection,
    table_key: str,
    offset: int = 0,
    limit: int = DEFAULT_LIMIT,
    search: str | None = None,
    filters: list[dict[str, Any]] | None = None,
    sort: list[dict[str, Any]] | None = None,
) -> dict[str, Any]:
    if offset < 0:
        raise PreviewValidationError("offset must be at least 0.", {"field": "offset"})
    if limit < 1 or limit > MAX_LIMIT:
        raise PreviewValidationError("limit must be between 1 and 1000.", {"field": "limit"})

    _get_registry_entry(conn, table_key)
    table_name = _active_table(conn, table_key)
    visible = _visible_metadata(conn, table_key)
    search_columns = _search_columns_for_table(conn, table_name, visible, search)
    where_sql, params = _build_where_clause(
        conn,
        table_key,
        filters,
        search,
        search_columns=search_columns,
    )
    order_sql = _build_order_clause(conn, table_key, sort)
    state = _load_state(conn, table_key)
    cache_key = _cache_digest(
        {
            "kind": "preview_state",
            "tableVersion": int(state.get("tableVersion") or 0),
            "tableKey": table_key,
            "offset": offset,
            "limit": limit,
            "search": search or "",
            "searchColumnKeys": [col["key"] for col in search_columns] if search_columns is not None else None,
            "filters": filters or [],
            "sort": sort or [],
        }
    )
    cached = _cache_get(conn, table_key, cache_key)
    if cached is not None:
        return cached

    total_row = conn.execute(
        f"SELECT COUNT(*) FROM {_quote_id(table_name)}{where_sql}",
        params,
    ).fetchone()
    total_rows = int(total_row[0] or 0) if total_row else 0

    select_parts = []
    has_record_id = ROW_ID_COL in _read_table_columns(conn, table_name)
    if not has_record_id:
        raise PreviewValidationError(
            "Preview table is missing persistent row identity.",
            {"tableKey": table_key},
        )
    if has_record_id:
        select_parts.append(_quote_id(ROW_ID_COL))
    for col in visible:
        select_parts.append(_quote_id(col["physicalName"]))
    if not select_parts:
        select_parts.append("NULL AS __empty")

    rows = conn.execute(
        f"SELECT {', '.join(select_parts)} FROM {_quote_id(table_name)}"
        f"{where_sql}{order_sql} LIMIT ? OFFSET ?",
        [*params, limit, offset],
    ).fetchall()

    out_rows = []
    for row in rows:
        values = {
            col["key"]: _json_safe(row[col["physicalName"]])
            for col in visible
        }
        out_rows.append({"__row_id": _json_safe(row[ROW_ID_COL]), "values": values})

    result = {
        "tableKey": table_key,
        "columns": [
            {
                "key": col["key"],
                "displayName": col["displayName"],
                "dataType": col.get("dataType") or "TEXT",
            }
            for col in visible
        ],
        "rows": out_rows,
        "totalRows": total_rows,
        "offset": offset,
        "limit": limit,
        **_state_depths(state),
    }
    _cache_set(conn, table_key, cache_key, result)
    return result


def get_column_values(
    conn: DuckDBConnection,
    table_key: str,
    column_key: str,
    search: str | None = None,
    filters: list[dict[str, Any]] | None = None,
    limit: int = MAX_COLUMN_VALUES,
) -> dict[str, Any]:
    if limit < 1 or limit > MAX_COLUMN_VALUES:
        raise PreviewValidationError("limit must be between 1 and 500.", {"field": "limit"})
    _get_registry_entry(conn, table_key)
    column = _column_by_key(conn, table_key, column_key)
    table_name = _active_table(conn, table_key)
    quoted = _quote_id(column["physicalName"])
    filters_without_self = [
        item
        for item in (filters or [])
        if not isinstance(item, dict) or item.get("columnKey") != column_key
    ]
    where_sql, params = _build_where_clause(
        conn,
        table_key,
        filters,
        None,
        exclude_column_key=column_key,
    )
    state = _load_state(conn, table_key)
    cache_key = _cache_digest(
        {
            "kind": "column_values",
            "tableVersion": int(state.get("tableVersion") or 0),
            "tableKey": table_key,
            "columnKey": column_key,
            "filtersWithoutSelf": filters_without_self,
            "search": search or "",
            "limit": limit,
        }
    )
    cached = _cache_get(conn, table_key, cache_key)
    if cached is not None:
        return cached
    conditions = []
    if where_sql:
        conditions.append(where_sql[7:])
    if search:
        conditions.append(f"CAST({quoted} AS VARCHAR) ILIKE ?")
        params.append(f"%{search}%")
    base_where = " WHERE " + " AND ".join(conditions) if conditions else ""
    blank_expr = f"({quoted} IS NULL OR TRIM(CAST({quoted} AS VARCHAR)) = '')"

    values = [
        row[0]
        for row in conn.execute(
            f"SELECT DISTINCT CAST({quoted} AS VARCHAR) AS value "
            f"FROM {_quote_id(table_name)}{base_where}"
            f"{' AND ' if base_where else ' WHERE '}NOT {blank_expr} "
            f"ORDER BY value LIMIT ?",
            [*params, limit],
        ).fetchall()
    ]
    blank_row = conn.execute(
        f"SELECT COUNT(*) FROM {_quote_id(table_name)}{base_where}"
        f"{' AND ' if base_where else ' WHERE '}{blank_expr}",
        params,
    ).fetchone()
    distinct_row = conn.execute(
        f"SELECT COUNT(DISTINCT CAST({quoted} AS VARCHAR)) FROM {_quote_id(table_name)}"
        f"{base_where}{' AND ' if base_where else ' WHERE '}NOT {blank_expr}",
        params,
    ).fetchone()
    result = {
        "columnKey": column_key,
        "values": [_json_safe(value) for value in values],
        "hasBlanks": bool(blank_row and int(blank_row[0] or 0) > 0),
        "totalDistinct": int(distinct_row[0] or 0) if distinct_row else 0,
    }
    _cache_set(conn, table_key, cache_key, result)
    return result


def _copy_metadata_for_working_table(conn: DuckDBConnection, table_key: str) -> None:
    if _playground_metadata(conn, table_key):
        return
    _replace_playground_metadata(conn, table_key, _source_metadata(conn, table_key))


def _ensure_working_table(conn: DuckDBConnection, table_key: str) -> str:
    entry = _get_registry_entry(conn, table_key)
    state = _load_state(conn, table_key)
    existing = state.get("playgroundTableName")
    if existing and _table_exists(conn, existing):
        _copy_metadata_for_working_table(conn, table_key)
        return existing

    if entry["kind"] != "source":
        state["playgroundTableName"] = entry["dataTable"]
        state["sourceTableKey"] = entry.get("sourceTableKey") or table_key
        _save_state(conn, table_key, state)
        return entry["dataTable"]

    table_name = f"pg__{_safe_table_suffix(table_key)}__{uuid.uuid4().hex[:8]}"
    conn.execute(
        f"CREATE TABLE {_quote_id(table_name)} AS "
        f"SELECT * FROM {_quote_id(entry['dataTable'])}"
    )
    _copy_metadata_for_working_table(conn, table_key)
    state["sourceTableKey"] = table_key
    state["sourceDataTable"] = entry["dataTable"]
    state["playgroundTableName"] = table_name
    state.setdefault("tableVersion", 0)
    state.setdefault("dirty", False)
    state.setdefault("undoStack", [])
    state.setdefault("redoStack", [])
    _save_state(conn, table_key, state)
    conn.commit()
    return table_name


def _snapshot_table(conn: DuckDBConnection, table_name: str) -> str:
    snapshot = f"pg__snap__{uuid.uuid4().hex[:12]}"
    conn.execute(
        f"CREATE TABLE {_quote_id(snapshot)} AS SELECT * FROM {_quote_id(table_name)}"
    )
    conn.commit()
    return snapshot


def _drop_table(conn: DuckDBConnection, table_name: str | None) -> None:
    if table_name:
        conn.execute(f"DROP TABLE IF EXISTS {_quote_id(table_name)}")
        conn.commit()


def _begin_mutation(conn: DuckDBConnection, table_key: str, op: str) -> tuple[str, dict[str, Any]]:
    pre_state = _load_state(conn, table_key)
    pre_playground_table = pre_state.get("playgroundTableName")
    entry = _get_registry_entry(conn, table_key)
    table_name = _ensure_working_table(conn, table_key)
    state = _load_state(conn, table_key)
    snapshot = _snapshot_table(conn, table_name)
    undo_item = {
        "kind": "snapshot",
        "op": op,
        "snapshotTable": snapshot,
        "metadata": _metadata_for_table(conn, table_key),
        "tableVersion": int(state.get("tableVersion") or 0),
        "dirty": bool(state.get("dirty")),
        "playgroundTableName": table_name,
        "createdWorkingTable": bool(entry.get("kind") == "source" and not pre_playground_table),
        "prePlaygroundTableName": pre_playground_table,
    }
    undo_stack = list(state.get("undoStack") or [])
    undo_stack.append(undo_item)
    state["undoStack"] = undo_stack
    _save_state(conn, table_key, state)
    return table_name, state


def _finish_mutation(conn: DuckDBConnection, table_key: str, state: dict[str, Any]) -> None:
    undo_stack = list(state.get("undoStack") or [])
    while len(undo_stack) > MAX_UNDO_DEPTH:
        old = undo_stack.pop(0)
        _drop_table(conn, old.get("snapshotTable"))
    state["undoStack"] = undo_stack
    for redo in state.get("redoStack") or []:
        _drop_table(conn, redo.get("snapshotTable"))
    state["redoStack"] = []
    state["tableVersion"] = int(state.get("tableVersion") or 0) + 1
    state["dirty"] = True
    _save_state(conn, table_key, state)
    conn.commit()


def _abort_mutation(
    conn: DuckDBConnection,
    table_key: str,
    table_name: str,
    state: dict[str, Any],
) -> None:
    undo_stack = list(state.get("undoStack") or [])
    if not undo_stack:
        return
    item = undo_stack.pop()
    if item.get("createdWorkingTable") and not item.get("prePlaygroundTableName"):
        _drop_table(conn, table_name)
        _clear_playground_metadata(conn, table_key)
        state["playgroundTableName"] = None
    else:
        _replace_table_from_snapshot(conn, table_name, item.get("snapshotTable"))
        _replace_playground_metadata(conn, table_key, item.get("metadata") or [])
        state["playgroundTableName"] = item.get("playgroundTableName")
    _drop_table(conn, item.get("snapshotTable"))
    state["undoStack"] = undo_stack
    state["tableVersion"] = int(item.get("tableVersion") or 0)
    state["dirty"] = bool(item.get("dirty"))
    _save_state(conn, table_key, state)
    conn.commit()


def _replace_table_from_snapshot(
    conn: DuckDBConnection,
    target_table: str,
    snapshot_table: str,
) -> None:
    _drop_table(conn, target_table)
    conn.execute(
        f"CREATE TABLE {_quote_id(target_table)} AS "
        f"SELECT * FROM {_quote_id(snapshot_table)}"
    )
    conn.commit()


def _next_column_key(metadata: list[dict[str, Any]]) -> str:
    max_index = -1
    for col in metadata:
        match = re.fullmatch(r"COL_(\d+)", str(col.get("key") or ""))
        if match:
            max_index = max(max_index, int(match.group(1)))
    return f"COL_{max_index + 1}"


def _next_order(metadata: list[dict[str, Any]]) -> int:
    orders = [int(col.get("order") or 0) for col in metadata if not col.get("hidden")]
    return max(orders, default=-1) + 1


def _parse_calc_expression(
    conn: DuckDBConnection,
    table_key: str,
    expression: str,
    data_type: str,
) -> str:
    if not isinstance(expression, str) or not expression.strip():
        raise PreviewValidationError("Calculated expression is required.", {"field": "expression"})
    if len(expression) > MAX_CALC_EXPRESSION:
        raise PreviewValidationError("Calculated expression is too long.", {"field": "expression"})
    if '"' in expression:
        raise PreviewValidationError(
            "Calculated expressions must reference columns as [COL_n].",
            {"field": "expression"},
        )

    metadata_by_key = {col["key"]: col for col in _visible_metadata(conn, table_key)}

    def replace_ref(match: re.Match) -> str:
        key = match.group(1).strip()
        col = metadata_by_key.get(key)
        if not col:
            raise PreviewValidationError(
                "Calculated expressions must reference stable column keys.",
                {"field": "expression", "columnKey": key},
            )
        quoted = _quote_id(col["physicalName"])
        if data_type in {"INTEGER", "DOUBLE"}:
            return f"TRY_CAST({quoted} AS DOUBLE)"
        return quoted

    parsed = re.sub(r"\[([A-Za-z0-9_]+)\]", replace_ref, expression)
    allowed = re.compile(r'^[\s\w\d+\-*/().,<>=!|"\'_]+$')
    if not allowed.fullmatch(parsed):
        raise PreviewValidationError("Calculated expression contains unsupported characters.", {"field": "expression"})

    keyword_source = re.sub(r'"[^"]*"', " ", parsed)
    keyword_source = re.sub(r"'[^']*'", " ", keyword_source)
    words = set(re.findall(r"\b[A-Z_]+\b", keyword_source.upper()))
    unsupported = words - _ALLOWED_CALC_FUNCTIONS - {"AS", "AND", "CASE", "ELSE", "END", "NOT", "NULL", "OR", "THEN", "WHEN"}
    if unsupported & _DISALLOWED_SQL_WORDS:
        raise PreviewValidationError("Calculated expression contains disallowed SQL.", {"field": "expression"})
    bare_cols = unsupported - _DISALLOWED_SQL_WORDS
    if bare_cols:
        raise PreviewValidationError(
            "Calculated expressions must reference columns as [COL_n].",
            {"field": "expression"},
        )
    return parsed


def run_operation(
    conn: DuckDBConnection,
    table_key: str,
    op: str,
    params: dict[str, Any] | None,
) -> dict[str, Any]:
    params = params or {}
    if op == "cell_edit":
        result = _cell_edit(conn, table_key, params)
    elif op == "rows_delete":
        result = _rows_delete(conn, table_key, params)
    elif op == "column_rename":
        result = _column_rename(conn, table_key, params)
    elif op == "column_delete":
        result = _column_delete(conn, table_key, params)
    elif op == "column_reorder":
        result = _column_reorder(conn, table_key, params)
    elif op == "column_change_type":
        result = _column_change_type(conn, table_key, params)
    elif op == "calculated_column":
        result = _calculated_column(conn, table_key, params)
    elif op == "pivot":
        return _pivot(conn, table_key, params)
    else:
        raise PreviewValidationError("Unsupported preview operation.", {"op": op})
    result["ok"] = True
    return result


def _cell_edit(conn: DuckDBConnection, table_key: str, params: dict[str, Any]) -> dict[str, Any]:
    row_id = params.get("rowId")
    if row_id is None or str(row_id) == "":
        raise PreviewValidationError("rowId is required.", {"field": "rowId"})
    column = _column_by_key(conn, table_key, str(params.get("columnKey") or ""))
    active_table = _active_table(conn, table_key)
    exists = conn.execute(
        f"SELECT COUNT(*) FROM {_quote_id(active_table)} WHERE {_quote_id(ROW_ID_COL)} = ?",
        (str(row_id),),
    ).fetchone()
    if not exists or int(exists[0] or 0) == 0:
        raise PreviewValidationError("rowId was not found.", {"field": "rowId"})
    table_name, state = _begin_mutation(conn, table_key, "cell_edit")
    conn.execute(
        f"UPDATE {_quote_id(table_name)} SET {_quote_id(column['physicalName'])} = ? "
        f"WHERE {_quote_id(ROW_ID_COL)} = ?",
        (params.get("value"), str(row_id)),
    )
    _finish_mutation(conn, table_key, state)
    return get_preview_state(conn, table_key)


def _rows_delete(conn: DuckDBConnection, table_key: str, params: dict[str, Any]) -> dict[str, Any]:
    row_ids = params.get("rowIds")
    if not isinstance(row_ids, list) or not row_ids:
        raise PreviewValidationError("rowIds is required.", {"field": "rowIds"})
    if len(row_ids) > MAX_ROW_DELETE:
        raise PreviewValidationError("rowIds is capped at 5000.", {"field": "rowIds"})
    normalized_row_ids = [str(row_id) for row_id in row_ids]
    unique_row_ids = sorted(set(normalized_row_ids))
    active_table = _active_table(conn, table_key)
    preflight_placeholders = ", ".join("?" for _ in unique_row_ids)
    matched = conn.execute(
        f"SELECT COUNT(DISTINCT {_quote_id(ROW_ID_COL)}) FROM {_quote_id(active_table)} "
        f"WHERE {_quote_id(ROW_ID_COL)} IN ({preflight_placeholders})",
        unique_row_ids,
    ).fetchone()
    if not matched or int(matched[0] or 0) != len(unique_row_ids):
        raise PreviewValidationError("One or more rowIds were not found.", {"field": "rowIds"})
    table_name, state = _begin_mutation(conn, table_key, "rows_delete")
    placeholders = ", ".join("?" for _ in normalized_row_ids)
    conn.execute(
        f"DELETE FROM {_quote_id(table_name)} "
        f"WHERE {_quote_id(ROW_ID_COL)} IN ({placeholders})",
        normalized_row_ids,
    )
    _finish_mutation(conn, table_key, state)
    return get_preview_state(conn, table_key)


def _column_rename(conn: DuckDBConnection, table_key: str, params: dict[str, Any]) -> dict[str, Any]:
    column = _column_by_key(conn, table_key, str(params.get("columnKey") or ""))
    display_name = params.get("displayName")
    if not isinstance(display_name, str):
        raise PreviewValidationError("displayName is required.", {"field": "displayName"})
    _table_name, state = _begin_mutation(conn, table_key, "column_rename")
    metadata = _metadata_for_table(conn, table_key)
    for col in metadata:
        if col["key"] == column["key"]:
            col["displayName"] = display_name
    _replace_playground_metadata(conn, table_key, metadata)
    _finish_mutation(conn, table_key, state)
    return get_preview_state(conn, table_key)


def _column_delete(conn: DuckDBConnection, table_key: str, params: dict[str, Any]) -> dict[str, Any]:
    column = _column_by_key(conn, table_key, str(params.get("columnKey") or ""))
    visible = _visible_metadata(conn, table_key)
    if len(visible) <= 1:
        raise PreviewValidationError("Cannot delete the last visible column.", {"field": "columnKey"})
    _table_name, state = _begin_mutation(conn, table_key, "column_delete")
    metadata = _metadata_for_table(conn, table_key)
    for col in metadata:
        if col["key"] == column["key"]:
            col["hidden"] = True
    _replace_playground_metadata(conn, table_key, metadata)
    _finish_mutation(conn, table_key, state)
    return get_preview_state(conn, table_key)


def _column_reorder(conn: DuckDBConnection, table_key: str, params: dict[str, Any]) -> dict[str, Any]:
    column_keys = params.get("columnKeys")
    if not isinstance(column_keys, list) or not all(isinstance(key, str) for key in column_keys):
        raise PreviewValidationError("columnKeys must be an array of keys.", {"field": "columnKeys"})
    visible_keys = [col["key"] for col in _visible_metadata(conn, table_key)]
    if set(column_keys) != set(visible_keys) or len(column_keys) != len(visible_keys):
        raise PreviewValidationError("columnKeys must include every visible column exactly once.", {"field": "columnKeys"})
    _table_name, state = _begin_mutation(conn, table_key, "column_reorder")
    order_by_key = {key: index for index, key in enumerate(column_keys)}
    metadata = _metadata_for_table(conn, table_key)
    for col in metadata:
        if col["key"] in order_by_key:
            col["order"] = order_by_key[col["key"]]
    _replace_playground_metadata(conn, table_key, metadata)
    _finish_mutation(conn, table_key, state)
    return get_preview_state(conn, table_key)


def _column_change_type(conn: DuckDBConnection, table_key: str, params: dict[str, Any]) -> dict[str, Any]:
    column = _column_by_key(conn, table_key, str(params.get("columnKey") or ""))
    new_type = _normalize_type(params.get("newType"))
    _table_name, state = _begin_mutation(conn, table_key, "column_change_type")
    metadata = _metadata_for_table(conn, table_key)
    for col in metadata:
        if col["key"] == column["key"]:
            col["dataType"] = new_type
    _replace_playground_metadata(conn, table_key, metadata)
    _finish_mutation(conn, table_key, state)
    return get_preview_state(conn, table_key)


def _calculated_column(conn: DuckDBConnection, table_key: str, params: dict[str, Any]) -> dict[str, Any]:
    name = params.get("name")
    if not isinstance(name, str) or not name.strip():
        raise PreviewValidationError("name is required.", {"field": "name"})
    data_type = _normalize_type(params.get("dataType"))
    expression = params.get("expression")
    parsed = _parse_calc_expression(conn, table_key, expression, data_type)
    active_table = _active_table(conn, table_key)
    try:
        conn.execute(f"SELECT {parsed} FROM {_quote_id(active_table)} LIMIT 1").fetchone()
    except Exception as exc:
        raise PreviewValidationError(
            f"Invalid calculated column expression: {str(exc).splitlines()[0]}",
            {"field": "expression"},
        ) from exc
    table_name, state = _begin_mutation(conn, table_key, "calculated_column")
    metadata = _metadata_for_table(conn, table_key)
    existing_physical = set(_read_table_columns(conn, table_name))
    physical = _safe_physical_name(name, existing_physical)
    column_key = _next_column_key(metadata)
    sql_type = "BIGINT" if data_type == "INTEGER" else ("DOUBLE" if data_type == "DOUBLE" else "VARCHAR")
    if data_type == "DATE":
        sql_type = "DATE"
    elif data_type == "BOOLEAN":
        sql_type = "BOOLEAN"
    try:
        conn.execute(f"ALTER TABLE {_quote_id(table_name)} ADD COLUMN {_quote_id(physical)} {sql_type}")
        conn.execute(f"UPDATE {_quote_id(table_name)} SET {_quote_id(physical)} = {parsed}")
    except PreviewValidationError:
        _abort_mutation(conn, table_key, table_name, state)
        raise
    except Exception as exc:
        _abort_mutation(conn, table_key, table_name, state)
        raise PreviewValidationError(
            f"Invalid calculated column expression: {str(exc).splitlines()[0]}",
            {"field": "expression"},
        ) from exc
    metadata.append(
        {
            "key": column_key,
            "physicalName": physical,
            "displayName": name.strip(),
            "dataType": data_type,
            "order": _next_order(metadata),
            "hidden": False,
        }
    )
    _replace_playground_metadata(conn, table_key, metadata)
    _finish_mutation(conn, table_key, state)
    return get_preview_state(conn, table_key)


def _aggregation_sql(field: str, aggregation: str) -> str:
    quoted = _quote_id(field)
    if aggregation == "count":
        return f"COUNT({quoted})"
    if aggregation == "count_distinct":
        return f"COUNT(DISTINCT {quoted})"
    if aggregation in _NUMERIC_AGGS:
        return f"{_AGGREGATIONS[aggregation]}(TRY_CAST({quoted} AS DOUBLE))"
    raise PreviewValidationError("Unsupported pivot aggregation.", {"field": "valueFields"})


def _pivot_value_fields(
    conn: DuckDBConnection,
    table_key: str,
    value_fields: Any,
) -> list[dict[str, Any]]:
    if not isinstance(value_fields, list) or not value_fields:
        raise PreviewValidationError("valueFields is required.", {"field": "valueFields"})
    if len(value_fields) > MAX_PIVOT_VALUE_FIELDS:
        raise PreviewValidationError("Pivot valueFields are capped at 5.", {"field": "valueFields"})
    result = []
    for item in value_fields:
        if not isinstance(item, dict):
            raise PreviewValidationError("valueFields must contain objects.", {"field": "valueFields"})
        if "field" in item:
            raise PreviewValidationError("valueFields must use columnKey.", {"field": "valueFields"})
        key = item.get("columnKey")
        col = _column_by_key(conn, table_key, str(key or ""))
        aggregation = str(item.get("aggregation") or "sum").lower()
        if aggregation not in _AGGREGATIONS:
            raise PreviewValidationError("Unsupported pivot aggregation.", {"field": "valueFields"})
        result.append({"column": col, "aggregation": aggregation})
    return result


def _estimate_pivot_rows(
    conn: DuckDBConnection,
    source_table: str,
    row_cols: list[dict[str, Any]],
) -> int:
    if not row_cols:
        return 1
    select_cols = ", ".join(_quote_id(col["physicalName"]) for col in row_cols)
    row = conn.execute(
        f"SELECT COUNT(*) FROM ("
        f"SELECT DISTINCT {select_cols} FROM {_quote_id(source_table)} LIMIT ?"
        f") AS pivot_rows",
        (MAX_PIVOT_OUTPUT_ROWS + 1,),
    ).fetchone()
    return int(row[0] or 0) if row else 0


def _preflight_pivot_shape(
    conn: DuckDBConnection,
    source_table: str,
    row_cols: list[dict[str, Any]],
    generated_value_columns: int,
) -> None:
    estimated_rows = _estimate_pivot_rows(conn, source_table, row_cols)
    if estimated_rows > MAX_PIVOT_OUTPUT_ROWS:
        raise PreviewValidationError(
            "Pivot would generate too many rows.",
            {
                "estimatedRows": estimated_rows,
                "maxRows": MAX_PIVOT_OUTPUT_ROWS,
            },
        )
    estimated_columns = len(row_cols) + generated_value_columns
    estimated_cells = estimated_rows * max(estimated_columns, 1)
    if estimated_cells > MAX_PIVOT_OUTPUT_CELLS:
        raise PreviewValidationError(
            "Pivot output would be too large.",
            {
                "estimatedRows": estimated_rows,
                "estimatedColumns": estimated_columns,
                "estimatedCells": estimated_cells,
                "maxCells": MAX_PIVOT_OUTPUT_CELLS,
            },
        )


def _pivot(conn: DuckDBConnection, table_key: str, params: dict[str, Any]) -> dict[str, Any]:
    _get_registry_entry(conn, table_key)
    source_table = _active_table(conn, table_key)
    row_keys = params.get("rowFields") or []
    column_keys = params.get("columnFields") or []
    if not isinstance(row_keys, list) or not isinstance(column_keys, list):
        raise PreviewValidationError("rowFields and columnFields must be arrays.")
    if len(row_keys) > MAX_PIVOT_ROW_FIELDS:
        raise PreviewValidationError("Pivot rowFields are capped at 5.", {"field": "rowFields"})
    row_cols = [_column_by_key(conn, table_key, str(key)) for key in row_keys]
    column_cols = [_column_by_key(conn, table_key, str(key)) for key in column_keys]
    value_fields = _pivot_value_fields(conn, table_key, params.get("valueFields"))
    if column_cols and not row_cols:
        raise PreviewValidationError("Pivot requires at least one row field when column fields are present.")

    distinct_tuples: list[tuple[Any, ...]] = []
    generated_value_columns = len(value_fields)
    if column_cols:
        select_cols = ", ".join(_quote_id(col["physicalName"]) for col in column_cols)
        distinct_tuples = [
            tuple(row)
            for row in conn.execute(
                f"SELECT DISTINCT {select_cols} FROM {_quote_id(source_table)} "
                f"LIMIT ?",
                (MAX_PIVOT_COLUMNS + 1,),
            ).fetchall()
        ]
        distinct_tuples = sorted(
            distinct_tuples,
            key=lambda values: tuple("" if value is None else str(value) for value in values),
        )
        generated_count = len(distinct_tuples) * len(value_fields)
        if generated_count > MAX_PIVOT_COLUMNS:
            raise PreviewValidationError("Pivot would generate too many columns.", {"maxColumns": MAX_PIVOT_COLUMNS})
        generated_value_columns = generated_count

    _preflight_pivot_shape(conn, source_table, row_cols, generated_value_columns)

    new_table_key = f"playground::pivot::{uuid.uuid4().hex[:8]}"
    temp_table = f"pg__pivot_tmp__{uuid.uuid4().hex[:8]}"
    pivot_table = f"pg__pivot__{uuid.uuid4().hex[:8]}"
    existing_names: set[str] = set()
    select_parts: list[str] = []
    group_parts: list[str] = []
    metadata: list[dict[str, Any]] = []
    order = 0

    for col in row_cols:
        physical = _safe_physical_name(col["displayName"] or col["key"], existing_names)
        select_parts.append(f"{_quote_id(col['physicalName'])} AS {_quote_id(physical)}")
        group_parts.append(_quote_id(col["physicalName"]))
        metadata.append(
            {
                "key": f"COL_{order}",
                "physicalName": physical,
                "displayName": col["displayName"],
                "dataType": col.get("dataType") or "TEXT",
                "order": order,
                "hidden": False,
            }
        )
        order += 1

    params_out: list[Any] = []
    if column_cols:
        for values in distinct_tuples:
            label = " | ".join("" if value is None else str(value) for value in values)
            for value_field in value_fields:
                conditions = []
                for col, value in zip(column_cols, values):
                    if value is None:
                        conditions.append(f"{_quote_id(col['physicalName'])} IS NULL")
                    else:
                        conditions.append(f"{_quote_id(col['physicalName'])} = ?")
                        params_out.append(value)
                condition_sql = " AND ".join(conditions) or "1 = 1"
                source = value_field["column"]
                agg = value_field["aggregation"]
                physical = _safe_physical_name(f"{label}_{source['displayName']}_{agg}", existing_names)
                if agg == "count":
                    expr = f"COUNT(CASE WHEN {condition_sql} THEN 1 END)"
                elif agg == "count_distinct":
                    expr = (
                        f"COUNT(DISTINCT CASE WHEN {condition_sql} "
                        f"THEN {_quote_id(source['physicalName'])} END)"
                    )
                else:
                    expr = (
                        f"{_AGGREGATIONS[agg]}(CASE WHEN {condition_sql} "
                        f"THEN TRY_CAST({_quote_id(source['physicalName'])} AS DOUBLE) END)"
                    )
                select_parts.append(f"{expr} AS {_quote_id(physical)}")
                metadata.append(
                    {
                        "key": f"COL_{order}",
                        "physicalName": physical,
                        "displayName": f"{label} {source['displayName']} {agg}",
                        "dataType": "DOUBLE" if agg in _NUMERIC_AGGS else "INTEGER",
                        "order": order,
                        "hidden": False,
                    }
                )
                order += 1
    else:
        for value_field in value_fields:
            source = value_field["column"]
            agg = value_field["aggregation"]
            physical = _safe_physical_name(f"{source['displayName']}_{agg}", existing_names)
            select_parts.append(f"{_aggregation_sql(source['physicalName'], agg)} AS {_quote_id(physical)}")
            metadata.append(
                {
                    "key": f"COL_{order}",
                    "physicalName": physical,
                    "displayName": f"{source['displayName']} {agg}",
                    "dataType": "DOUBLE" if agg in _NUMERIC_AGGS else "INTEGER",
                    "order": order,
                    "hidden": False,
                }
            )
            order += 1

    if not select_parts:
        raise PreviewValidationError("Pivot has no output columns.")
    group_sql = f" GROUP BY {', '.join(group_parts)}" if group_parts else ""
    try:
        conn.execute(
            f"CREATE TABLE {_quote_id(temp_table)} AS SELECT {', '.join(select_parts)} "
            f"FROM {_quote_id(source_table)}{group_sql}",
            params_out,
        )
        conn.execute(
            f"CREATE TABLE {_quote_id(pivot_table)} AS "
            f"SELECT CAST(ROW_NUMBER() OVER () AS VARCHAR) AS {_quote_id(ROW_ID_COL)}, * "
            f"FROM {_quote_id(temp_table)}"
        )
        _drop_table(conn, temp_table)
        temp_table = ""
        _ensure_playground_schema(conn)
        conn.execute(
            "INSERT OR REPLACE INTO _playground_registry "
            "(table_key, data_table, source_table_key, kind) VALUES (?, ?, ?, ?)",
            (new_table_key, pivot_table, table_key, "pivot"),
        )
        metadata.insert(
            0,
            {
                "key": ROW_ID_COL,
                "physicalName": ROW_ID_COL,
                "displayName": ROW_ID_COL,
                "dataType": "TEXT",
                "order": -1,
                "hidden": True,
            },
        )
        _replace_playground_metadata(conn, new_table_key, metadata)
        state = _load_state(conn, new_table_key)
        state["sourceTableKey"] = table_key
        state["playgroundTableName"] = pivot_table
        state["tableVersion"] = 1
        state["dirty"] = True
        state["undoStack"] = [
            {
                "kind": "drop_created_table",
                "op": "pivot",
                "snapshotTable": _snapshot_table(conn, pivot_table),
                "metadata": metadata,
                "playgroundTableName": pivot_table,
            }
        ]
        state["redoStack"] = []
        _save_state(conn, new_table_key, state)
        result = get_preview_state(conn, new_table_key)
        result.update({"ok": True, "newTableKey": new_table_key})
        return result
    except Exception:
        _drop_table(conn, temp_table)
        _drop_table(conn, pivot_table)
        _ensure_playground_schema(conn)
        conn.execute("DELETE FROM _playground_registry WHERE table_key = ?", (new_table_key,))
        conn.execute("DELETE FROM _playground_columns WHERE table_key = ?", (new_table_key,))
        delete_meta(conn, _state_key(new_table_key))
        delete_meta(conn, _read_cache_key(new_table_key))
        conn.commit()
        raise


def undo_operation(conn: DuckDBConnection, table_key: str) -> dict[str, Any]:
    state = _load_state(conn, table_key)
    undo_stack = list(state.get("undoStack") or [])
    if not undo_stack:
        raise PreviewValidationError("Nothing to undo.")
    item = undo_stack.pop()
    if item.get("kind") == "drop_created_table":
        table_name = state.get("playgroundTableName")
        redo_snapshot = _snapshot_table(conn, table_name) if _table_exists(conn, table_name) else item.get("snapshotTable")
        redo_item = dict(item)
        redo_item["snapshotTable"] = redo_snapshot
        _drop_table(conn, table_name)
        conn.execute("DELETE FROM _playground_registry WHERE table_key = ?", (table_key,))
        conn.execute("DELETE FROM _playground_columns WHERE table_key = ?", (table_key,))
        state["undoStack"] = undo_stack
        state["redoStack"] = list(state.get("redoStack") or []) + [redo_item]
        state["playgroundTableName"] = None
        state["tableVersion"] = int(state.get("tableVersion") or 0) + 1
        _save_state(conn, table_key, state)
        return {
            "ok": True,
            "tableKey": table_key,
            "columns": [],
            "rows": [],
            "totalRows": 0,
            "offset": 0,
            "limit": DEFAULT_LIMIT,
            "dropped": True,
            **_state_depths(state),
        }

    current_table = _ensure_working_table(conn, table_key)
    redo_item = {
        "kind": "snapshot",
        "op": item.get("op"),
        "snapshotTable": _snapshot_table(conn, current_table),
        "metadata": _metadata_for_table(conn, table_key),
        "tableVersion": int(state.get("tableVersion") or 0),
        "dirty": bool(state.get("dirty")),
        "playgroundTableName": current_table,
    }
    _replace_table_from_snapshot(conn, current_table, item["snapshotTable"])
    _drop_table(conn, item.get("snapshotTable"))
    _replace_playground_metadata(conn, table_key, item.get("metadata") or [])
    state["undoStack"] = undo_stack
    state["redoStack"] = list(state.get("redoStack") or []) + [redo_item]
    state["tableVersion"] = int(state.get("tableVersion") or 0) + 1
    state["dirty"] = bool(item.get("dirty"))
    state["playgroundTableName"] = current_table
    _save_state(conn, table_key, state)
    result = get_preview_state(conn, table_key)
    result["ok"] = True
    return result


def redo_operation(conn: DuckDBConnection, table_key: str) -> dict[str, Any]:
    state = _load_state(conn, table_key)
    redo_stack = list(state.get("redoStack") or [])
    if not redo_stack:
        raise PreviewValidationError("Nothing to redo.")
    item = redo_stack.pop()
    if item.get("kind") == "drop_created_table":
        table_name = item.get("playgroundTableName")
        if not table_name:
            raise PreviewValidationError("Cannot redo pivot creation.")
        _replace_table_from_snapshot(conn, table_name, item["snapshotTable"])
        _ensure_playground_schema(conn)
        conn.execute(
            "INSERT OR REPLACE INTO _playground_registry "
            "(table_key, data_table, source_table_key, kind) VALUES (?, ?, ?, ?)",
            (table_key, table_name, state.get("sourceTableKey"), "pivot"),
        )
        _replace_playground_metadata(conn, table_key, item.get("metadata") or [])
        state["playgroundTableName"] = table_name
        state["undoStack"] = list(state.get("undoStack") or []) + [item]
        state["redoStack"] = redo_stack
        state["tableVersion"] = int(state.get("tableVersion") or 0) + 1
        state["dirty"] = True
        _save_state(conn, table_key, state)
        result = get_preview_state(conn, table_key)
        result["ok"] = True
        return result

    current_table = _ensure_working_table(conn, table_key)
    undo_item = {
        "kind": "snapshot",
        "op": item.get("op"),
        "snapshotTable": _snapshot_table(conn, current_table),
        "metadata": _metadata_for_table(conn, table_key),
        "tableVersion": int(state.get("tableVersion") or 0),
        "dirty": bool(state.get("dirty")),
        "playgroundTableName": current_table,
    }
    _replace_table_from_snapshot(conn, current_table, item["snapshotTable"])
    _drop_table(conn, item.get("snapshotTable"))
    _replace_playground_metadata(conn, table_key, item.get("metadata") or [])
    state["undoStack"] = list(state.get("undoStack") or []) + [undo_item]
    state["redoStack"] = redo_stack
    state["tableVersion"] = int(state.get("tableVersion") or 0) + 1
    state["dirty"] = bool(item.get("dirty", True))
    state["playgroundTableName"] = current_table
    _save_state(conn, table_key, state)
    result = get_preview_state(conn, table_key)
    result["ok"] = True
    return result


def refresh_inventory(conn: DuckDBConnection) -> dict[str, Any]:
    file_inventory = []
    try:
        from services.upload.file_loader import build_inventory

        file_inventory.extend(build_inventory(conn))
    except Exception:
        pass
    _ensure_playground_schema(conn)
    rows = conn.execute(
        "SELECT table_key, data_table FROM _playground_registry ORDER BY table_key"
    ).fetchall()
    for table_key, table_name in rows:
        if not _table_exists(conn, table_name):
            continue
        row = conn.execute(f"SELECT COUNT(*) FROM {_quote_id(table_name)}").fetchone()
        visible_cols = [col for col in _playground_metadata(conn, table_key) if not col["hidden"]]
        file_inventory.append(
            {
                "table_key": table_key,
                "rows": int(row[0] or 0) if row else 0,
                "cols": len(visible_cols),
                "playgroundOnly": True,
            }
        )

    source_keys = [item["table_key"] for item in file_inventory if not item.get("playgroundOnly")]
    columns = collect_column_info(conn, source_keys) if source_keys else []
    previews = build_preview(conn)
    for item in file_inventory:
        if not item.get("playgroundOnly"):
            continue
        try:
            state = get_preview_state(conn, item["table_key"], limit=50)
            previews[item["table_key"]] = {
                "columns": [col["displayName"] for col in state["columns"]],
                "rows": [row["values"] for row in state["rows"]],
            }
        except Exception:
            pass
    return {"ok": True, "fileInventory": file_inventory, "columns": columns, "previews": previews}
