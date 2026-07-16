"""
Core table CRUD operations against DuckDB.

All data is stored as VARCHAR columns. Bulk operations use DuckDB's native
DataFrame ingestion when possible; row-streaming is the fallback for generators.
"""

from __future__ import annotations

import json
import re
from typing import Any, Iterator

import pandas as pd

from .duckdb_compat import DuckDBConnection

# Preview-internal columns that must not be profiled or sent to AI payloads.
INTERNAL_COLUMNS = frozenset({"__row_id"})


def normalize_reserved_provenance_name(name: Any) -> str:
    """Normalize a column name for the exact provenance boundary.

    Separator and case variants such as ``__Source-Table`` collapse to the
    same token.  Names containing additional words (for example
    ``Data Source System``) deliberately do not match.
    """
    return re.sub(r"[^a-z0-9]+", "", str(name or "").casefold())


def is_reserved_provenance_column(name: Any) -> bool:
    """Return whether *name* is an exact normalized source-table alias."""
    return normalize_reserved_provenance_name(name) == "sourcetable"


def filter_data_columns(columns: list[str]) -> list[str]:
    """Return user-facing columns, excluding internal system columns."""
    return [
        c for c in columns
        if c not in INTERNAL_COLUMNS and not is_reserved_provenance_column(c)
    ]


def filter_reserved_provenance_columns(columns: list[str]) -> list[str]:
    """Exclude only reserved provenance fields, preserving other internals."""
    return [c for c in columns if not is_reserved_provenance_column(c)]


def public_projection(columns: list[str]) -> str:
    """Return an explicit, safely quoted SQL projection of public columns."""
    return ", ".join(quote_id(c) for c in filter_data_columns(columns))


def quote_id(name: str) -> str:
    """Double-quote a SQL identifier, escaping embedded quotes."""
    return '"' + name.replace('"', '""') + '"'


def normalize_for_match(expr: str) -> str:
    """SQL expression that normalizes a value for join-key matching.

    Handles case folding, whitespace trimming, and numeric format differences
    (e.g. "123", "123.0", "00123" all normalize to the same value).

    Uses DuckDB's TRY_CAST and regexp_matches instead of SQLite GLOB/CAST.
    """
    cast_expr = f"CAST({expr} AS VARCHAR)"
    trimmed = f"TRIM({cast_expr})"
    return (
        f"LOWER(TRIM(CASE "
        f"WHEN regexp_matches({trimmed}, '^[0-9eE.+-]+$') "
        f"THEN CAST(TRY_CAST({trimmed} AS DOUBLE) AS VARCHAR) "
        f"ELSE {cast_expr} END))"
    )


def store_table(
    conn: DuckDBConnection,
    table_name: str,
    rows: list[dict[str, Any]],
    *,
    provision_row_id: bool = False,
) -> None:
    """Store a list of row-dicts as a DuckDB table. Drops any existing table first.

    Args:
        conn: DuckDB session connection.
        table_name: Target table name.
        rows: List of dictionaries, each representing one row.
    """
    if not rows:
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
        conn.commit()
        return

    first = rows[0]
    if not isinstance(first, dict) or not first:
        return

    columns = filter_reserved_provenance_columns(list(first.keys()))
    if not columns:
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
        conn.commit()
        return
    add_row_id = provision_row_id and "__row_id" not in columns
    storage_columns = columns + (["__row_id"] if add_row_id else [])
    col_defs = ", ".join(
        f"{quote_id(c)} {'BIGINT' if c == '__row_id' else 'VARCHAR'}"
        for c in storage_columns
    )
    conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
    conn.execute(f"CREATE TABLE {quote_id(table_name)} ({col_defs})")

    placeholders = ", ".join("?" for _ in storage_columns)
    quoted_cols = ", ".join(quote_id(c) for c in storage_columns)
    sql = f"INSERT INTO {quote_id(table_name)} ({quoted_cols}) VALUES ({placeholders})"

    batch_size = 5000
    for i in range(0, len(rows), batch_size):
        batch = rows[i : i + batch_size]
        conn.executemany(
            sql,
            [
                tuple(
                    None if row.get(c) is None else str(row[c])
                    for c in columns
                ) + ((i + row_index + 1,) if add_row_id else ())
                for row_index, row in enumerate(batch)
            ],
        )
    conn.commit()


def store_table_streaming(
    conn: DuckDBConnection,
    table_name: str,
    columns: list[str],
    row_iterator: Iterator,
    commit: bool = True,
    *,
    provision_row_id: bool = False,
) -> int:
    """Stream rows from an iterator directly into a DuckDB table.

    Never materialises the full dataset in memory. Returns the number of rows inserted.

    Args:
        conn: DuckDB session connection.
        table_name: Target table name.
        columns: Ordered column names.
        row_iterator: Yields rows as dicts, tuples, or lists.
        commit: Whether to commit after all inserts.

    Returns:
        Number of rows inserted.
    """
    original_columns = list(columns)
    columns = filter_reserved_provenance_columns(original_columns)
    if not columns:
        return 0
    kept_indexes = [original_columns.index(c) for c in columns]

    add_row_id = provision_row_id and "__row_id" not in columns
    storage_columns = columns + (["__row_id"] if add_row_id else [])
    col_defs = ", ".join(
        f"{quote_id(c)} {'BIGINT' if c == '__row_id' else 'VARCHAR'}"
        for c in storage_columns
    )
    conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
    conn.execute(f"CREATE TABLE {quote_id(table_name)} ({col_defs})")

    placeholders = ", ".join("?" for _ in storage_columns)
    quoted_cols = ", ".join(quote_id(c) for c in storage_columns)
    sql = f"INSERT INTO {quote_id(table_name)} ({quoted_cols}) VALUES ({placeholders})"

    total = 0
    batch: list[tuple] = []
    num_cols = len(columns)

    for raw_row in row_iterator:
        vals: list
        if isinstance(raw_row, dict):
            vals = [raw_row.get(c) for c in columns]
        else:
            raw_vals = list(raw_row) if not isinstance(raw_row, list) else raw_row
            vals = [raw_vals[i] if i < len(raw_vals) else None for i in kept_indexes]

        if len(vals) < num_cols:
            vals.extend([None] * (num_cols - len(vals)))
        elif len(vals) > num_cols:
            vals = vals[:num_cols]

        stored = tuple(None if v is None else str(v) for v in vals)
        if add_row_id:
            stored += (total + len(batch) + 1,)
        batch.append(stored)
        if len(batch) >= 5000:
            conn.executemany(sql, batch)
            total += len(batch)
            batch.clear()

    if batch:
        conn.executemany(sql, batch)
        total += len(batch)

    if commit:
        conn.commit()
    return total


def store_df_native(
    conn: DuckDBConnection,
    table_name: str,
    df: pd.DataFrame,
    *,
    commit: bool = True,
    as_varchar: bool = True,
) -> int:
    """Persist a DataFrame using DuckDB's zero-copy register path.

    Avoids Python row iteration entirely — orders of magnitude faster than
    store_table_streaming for large DataFrames.

    Args:
        conn: DuckDB session connection.
        table_name: Target table name (will be dropped first if it exists).
        df: The pandas DataFrame to store.
        commit: Whether to commit after the write.
        as_varchar: If True, cast all columns to VARCHAR for consistency.

    Returns:
        Number of rows written.
    """
    if df is None or df.empty:
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
        if commit:
            conn.commit()
        return 0

    public_df = df.loc[:, filter_reserved_provenance_columns([str(c) for c in df.columns])]
    if len(public_df.columns) == 0:
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
        if commit:
            conn.commit()
        return 0

    view_name = f"_tmp_df_{id(df)}"
    raw_conn = conn._conn
    raw_conn.register(view_name, public_df)
    try:
        conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
        if as_varchar:
            cols = ", ".join(
                f"CAST({quote_id(str(c))} AS VARCHAR) AS {quote_id(str(c))}"
                for c in public_df.columns
            )
            conn.execute(
                f"CREATE TABLE {quote_id(table_name)} AS "
                f"SELECT {cols} FROM {quote_id(view_name)}"
            )
        else:
            conn.execute(
                f"CREATE TABLE {quote_id(table_name)} AS "
                f"SELECT * FROM {quote_id(view_name)}"
            )
        if commit:
            conn.commit()
    finally:
        raw_conn.unregister(view_name)
    return len(df)


def read_table(conn: DuckDBConnection, table_name: str, limit: int | None = None) -> list[dict]:
    """Read all (or up to limit) rows from a table as a list of dicts.

    Args:
        conn: DuckDB session connection.
        table_name: Table to read.
        limit: Optional max rows.

    Returns:
        List of row dicts.
    """
    if not table_exists(conn, table_name):
        return []
    tbl = quote_id(table_name)
    safe_columns = filter_reserved_provenance_columns(read_physical_table_columns(conn, table_name))
    projection = ", ".join(quote_id(c) for c in safe_columns)
    if not projection:
        return []
    if limit is not None:
        rows = conn.execute(f"SELECT {projection} FROM {tbl} LIMIT ?", (limit,)).fetchall()
    else:
        rows = conn.execute(f"SELECT {projection} FROM {tbl}").fetchall()
    return [dict(zip(r.keys(), r)) for r in rows]


def _row_get(row: Any, key: str, index: int = 0) -> Any:
    """Read a field from a DuckDB/DictRow result row."""
    if isinstance(row, (list, tuple)):
        return row[index]
    if hasattr(row, "keys") and key in row.keys():
        return row[key]
    return row[index]


def _columns_from_describe(conn: DuckDBConnection, table_name: str) -> list[str]:
    """Fallback column list via DESCRIBE when information_schema returns nothing."""
    tbl = quote_id(table_name)
    try:
        rows = conn.execute(f"DESCRIBE {tbl}").fetchall()
    except Exception:
        return []
    out: list[str] = []
    for r in rows:
        name = _row_get(r, "column_name", 0)
        if name:
            out.append(str(name))
    return out


def _columns_from_sample(conn: DuckDBConnection, table_name: str) -> list[str]:
    """Last-resort column discovery from a sample row's keys."""
    tbl = quote_id(table_name)
    try:
        rows = conn.execute(f"SELECT * FROM {tbl} LIMIT 1").fetchall()
        if rows:
            return list(rows[0].keys())
    except Exception:
        pass
    return []


def read_physical_table_columns(conn: DuckDBConnection, table_name: str) -> list[str]:
    """Return every physically stored column, including legacy internals.

    Uses DuckDB's information_schema. Restricts to the ``main`` schema and
    deduplicates names so catalog quirks (duplicate rows for the same table
    name across schemas) cannot produce more names than the table has columns.
    Falls back to DESCRIBE when information_schema returns no columns.
    """
    if not table_exists(conn, table_name):
        return []
    rows = conn.execute(
        "SELECT column_name FROM information_schema.columns "
        "WHERE LOWER(table_name) = LOWER(?) "
        "AND LOWER(table_schema) = LOWER(?) "
        "ORDER BY ordinal_position",
        (table_name, "main"),
    ).fetchall()
    seen: set[str] = set()
    out: list[str] = []
    for r in rows:
        name = r["column_name"]
        if name in seen:
            continue
        seen.add(name)
        out.append(name)
    if not out:
        rows = conn.execute(
            "SELECT column_name FROM information_schema.columns "
            "WHERE LOWER(table_name) = LOWER(?) "
            "ORDER BY ordinal_position",
            (table_name,),
        ).fetchall()
        for r in rows:
            name = r["column_name"]
            if name in seen:
                continue
            seen.add(name)
            out.append(name)
    if not out:
        out = _columns_from_describe(conn, table_name)
    if not out:
        out = _columns_from_sample(conn, table_name)
    return out


def read_table_columns(conn: DuckDBConnection, table_name: str) -> list[str]:
    """Return ordered columns while enforcing the provenance boundary."""
    return filter_reserved_provenance_columns(read_physical_table_columns(conn, table_name))


def _map_duckdb_type_to_ui(data_type: str) -> str:
    """Map DuckDB information_schema data_type to preview UI type label."""
    upper = (data_type or "").upper()
    if upper in ("VARCHAR", "TEXT", "STRING", "CHAR", "BPCHAR"):
        return "TEXT"
    if upper in ("BIGINT", "HUGEINT", "UBIGINT", "INTEGER", "INT", "SMALLINT", "TINYINT"):
        return "INTEGER"
    if upper in ("DOUBLE", "FLOAT", "REAL", "DECIMAL", "NUMERIC", "BIGNUM"):
        return "DOUBLE"
    if upper in ("DATE", "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMPTZ", "DATETIME"):
        return "DATE"
    if upper == "BOOLEAN":
        return "BOOLEAN"
    return "TEXT"


def read_table_column_types(conn: DuckDBConnection, table_name: str) -> dict[str, str]:
    """Return ordered column name -> UI type mapping from DuckDB schema."""
    if not table_exists(conn, table_name):
        return {}
    rows = conn.execute(
        "SELECT column_name, data_type FROM information_schema.columns "
        "WHERE LOWER(table_name) = LOWER(?) "
        "AND LOWER(table_schema) = LOWER(?) "
        "ORDER BY ordinal_position",
        (table_name, "main"),
    ).fetchall()
    result: dict[str, str] = {}
    for r in rows:
        name = r["column_name"]
        if name not in result and name in filter_reserved_provenance_columns([name]):
            result[name] = _map_duckdb_type_to_ui(r["data_type"])
    if not result:
        rows_any_schema = conn.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE LOWER(table_name) = LOWER(?) "
            "ORDER BY ordinal_position",
            (table_name,),
        ).fetchall()
        for r in rows_any_schema:
            name = r["column_name"]
            if name not in result and name in filter_reserved_provenance_columns([name]):
                result[name] = _map_duckdb_type_to_ui(r["data_type"])
    if not result:
        for name in filter_reserved_provenance_columns(_columns_from_describe(conn, table_name)):
            result[name] = "TEXT"
    if not result:
        for name in filter_reserved_provenance_columns(_columns_from_sample(conn, table_name)):
            result[name] = "TEXT"
    return result


def table_exists(conn: DuckDBConnection, table_name: str) -> bool:
    """Check whether a table exists in the database.

    Uses DuckDB's information_schema instead of sqlite_master.
    """
    row = conn.execute(
        "SELECT 1 FROM information_schema.tables WHERE LOWER(table_name) = LOWER(?)",
        (table_name,),
    ).fetchone()
    return row is not None


def drop_table(conn: DuckDBConnection, table_name: str, *, commit: bool = True) -> None:
    """Drop a table if it exists."""
    conn.execute(f"DROP TABLE IF EXISTS {quote_id(table_name)}")
    if commit:
        conn.commit()


def table_row_count(conn: DuckDBConnection, table_name: str) -> int:
    """Return the number of rows in a table, or 0 if the table does not exist."""
    if not table_exists(conn, table_name):
        return 0
    row = conn.execute(f"SELECT COUNT(*) AS cnt FROM {quote_id(table_name)}").fetchone()
    return row["cnt"] if row else 0


_RESERVED_SANITIZE_PREFIXES = (
    "tbl__", "appended__", "hn__", "merge_step_", "merged_v",
    "merged_output_", "final_merged",
)


def sanitize_reserved_provenance_columns(conn: DuckDBConnection) -> dict[str, Any]:
    """Physically remove reserved provenance aliases from workflow artifacts.

    The rewrite is idempotent and transactional.  A legacy artifact containing
    only reserved columns cannot be represented as a useful zero-column DuckDB
    table, so it is dropped and unregistered.  Audit metadata such as
    ``appendReport`` is intentionally retained; only derived public caches are
    invalidated for their normal rebuild path.
    """
    rows = conn.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE LOWER(table_schema) = 'main' ORDER BY table_name"
    ).fetchall()
    table_names = [str(_row_get(row, "table_name", 0)) for row in rows]
    registry_rows = conn.execute("SELECT table_key, sql_name FROM table_registry").fetchall()
    keys_by_sql: dict[str, list[str]] = {}
    for row in registry_rows:
        keys_by_sql.setdefault(str(row["sql_name"]).casefold(), []).append(str(row["table_key"]))
    targets: list[tuple[str, list[str], list[str]]] = []
    for table_name in table_names:
        if not table_name.casefold().startswith(_RESERVED_SANITIZE_PREFIXES):
            continue
        physical = read_physical_table_columns(conn, table_name)
        reserved = [c for c in physical if is_reserved_provenance_column(c)]
        if reserved:
            targets.append((table_name, physical, reserved))

    if not targets:
        return {"changedTables": [], "droppedTables": [], "removedColumns": 0}

    changed: list[str] = []
    dropped: list[str] = []
    removed = 0
    try:
        conn.execute("BEGIN TRANSACTION")
        for index, (table_name, physical, reserved) in enumerate(targets):
            kept = filter_reserved_provenance_columns(physical)
            removed += len(reserved)
            if not kept:
                conn.execute(f"DROP TABLE {quote_id(table_name)}")
                conn.execute("DELETE FROM table_registry WHERE LOWER(sql_name) = LOWER(?)", (table_name,))
                dropped.append(table_name)
                continue

            temp_name = f"__reserved_cleanup_{index}"
            conn.execute(f"DROP TABLE IF EXISTS {quote_id(temp_name)}")
            conn.execute(
                f"CREATE TABLE {quote_id(temp_name)} AS "
                f"SELECT {', '.join(quote_id(c) for c in kept)} FROM {quote_id(table_name)}"
            )
            conn.execute(f"DROP TABLE {quote_id(table_name)}")
            conn.execute(f"ALTER TABLE {quote_id(temp_name)} RENAME TO {quote_id(table_name)}")
            changed.append(table_name)

        # Preserve restore metadata while removing the same reserved fields from
        # cached schemas, previews and rows in this transaction.
        meta_rows = conn.execute("SELECT key, value FROM meta").fetchall()
        dropped_folded = {name.casefold() for name in dropped}
        dropped_table_keys = {
            key for sql_name in dropped_folded for key in keys_by_sql.get(sql_name, [])
        }
        refreshed_by_key: dict[str, dict[str, Any]] = {}
        for table_name in changed:
            public_columns = filter_data_columns(read_physical_table_columns(conn, table_name))
            row = conn.execute(
                f"SELECT COUNT(*) AS cnt FROM {quote_id(table_name)}"
            ).fetchone()
            metrics = {
                "rows": int(row["cnt"] if row else 0),
                "cols": len(public_columns),
                "columns": public_columns,
            }
            for table_key in keys_by_sql.get(table_name.casefold(), []):
                refreshed_by_key[table_key] = metrics

        def scrub(value: Any, metadata_key: str | None = None) -> Any:
            if isinstance(value, dict):
                table_key = value.get("table_key") or value.get("tableKey")
                if isinstance(table_key, str) and table_key in dropped_table_keys:
                    return None
                if metadata_key == "groupSchemaTableRows":
                    group_id = value.get("group_id")
                    if isinstance(group_id, str) and group_id in dropped_table_keys:
                        return None
                referenced = value.get("sql_name") or value.get("sqlName") or value.get("table_name")
                if isinstance(referenced, str) and referenced.casefold() in dropped_folded:
                    return None
                source_col = value.get("source_col")
                if isinstance(source_col, str) and is_reserved_provenance_column(source_col):
                    return None
                cleaned = {
                    key: scrub(item, metadata_key)
                    for key, item in value.items()
                    if not is_reserved_provenance_column(key) and key not in dropped_table_keys
                }
                cleaned = {key: item for key, item in cleaned.items() if item is not None}
                metrics = refreshed_by_key.get(str(table_key)) if table_key is not None else None
                if metrics:
                    if "rows" in cleaned:
                        cleaned["rows"] = metrics["rows"]
                    if "cols" in cleaned:
                        cleaned["cols"] = metrics["cols"]
                    if "n_cols" in cleaned:
                        cleaned["n_cols"] = metrics["cols"]
                    if "columns" in cleaned:
                        cleaned["columns"] = list(metrics["columns"])
                columns = cleaned.get("columns")
                if isinstance(columns, list):
                    if "cols" in cleaned:
                        cleaned["cols"] = len(columns)
                    if "total_cols" in cleaned:
                        cleaned["total_cols"] = len(columns)
                    if "columns_preview" in cleaned:
                        cleaned["columns_preview"] = ", ".join(str(c) for c in columns[:60])
                return cleaned
            if isinstance(value, list):
                result = []
                for item in value:
                    if isinstance(item, str) and is_reserved_provenance_column(item):
                        continue
                    cleaned_item = scrub(item, metadata_key)
                    if cleaned_item is not None:
                        result.append(cleaned_item)
                return result
            return value

        for row in meta_rows:
            try:
                original = json.loads(row["value"])
            except (TypeError, json.JSONDecodeError):
                continue
            cleaned = scrub(original, str(row["key"]))
            if cleaned != original:
                conn.execute(
                    "UPDATE meta SET value = ? WHERE key = ?",
                    (json.dumps(cleaned), row["key"]),
                )
        conn.execute("COMMIT")
    except Exception:
        try:
            conn.execute("ROLLBACK")
        except Exception:
            pass
        raise

    return {
        "changedTables": changed,
        "droppedTables": dropped,
        "removedColumns": removed,
    }


PREVIEW_POOL = 1000


def pick_best_rows(rows: list[dict], limit: int) -> list[dict]:
    """Return up to *limit* rows ranked by number of populated columns.

    Selects the rows that have the most non-null, non-empty values so that
    data previews show the most informative rows instead of whatever
    happened to be first in insertion order.
    """
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


def iterate_table(conn: DuckDBConnection, table_name: str) -> Iterator[dict]:
    """Iterate rows without loading them all into memory."""
    safe_columns = filter_reserved_provenance_columns(read_physical_table_columns(conn, table_name))
    projection = ", ".join(quote_id(c) for c in safe_columns)
    if not projection:
        return
    cursor = conn.execute(f"SELECT {projection} FROM {quote_id(table_name)}")
    cols = [desc[0] for desc in cursor.description]
    for row in cursor:
        yield dict(zip(cols, row))
