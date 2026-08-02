"""Chunked DuckDB sink for immutable raw and typed working tables."""
from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from decimal import Decimal
import os
import threading
import time
from typing import Any, Callable, Iterable, Sequence

from .models import OrderedColumnSchema, TableArtifact, ValueType


def quote_identifier(value: str) -> str:
    if not value or "\x00" in value:
        raise ValueError("invalid SQL identifier")
    return '"' + value.replace('"', '""') + '"'


def _name(value: str) -> str:
    safe = re.sub(r"[^A-Za-z0-9_]", "_", value)
    return safe.strip("_") or "table"


def _duck_type(value_type: ValueType) -> str:
    return {
        ValueType.INTEGER: "BIGINT",
        ValueType.DECIMAL: "DECIMAL(38,18)",
        ValueType.FLOAT: "DOUBLE",
        ValueType.BOOLEAN: "BOOLEAN",
        ValueType.DATE: "DATE",
        ValueType.TIME: "TIME",
        ValueType.TIMESTAMP: "TIMESTAMP",
    }.get(value_type, "VARCHAR")


@dataclass(frozen=True)
class SinkResult:
    artifact: TableArtifact
    raw_table: str
    typed_table: str
    raw_hash: str
    typed_hash: str


class DuckDBSink:
    """Persist one parsed table using bounded executemany batches.

    Existing table names are never replaced: callers must provide a new
    ``table_key`` for a retry.  This makes the raw representation immutable.
    """

    def __init__(self, connection: Any, *, batch_size: int = 10_000):
        self.connection = connection
        if batch_size <= 0:
            raise ValueError("batch_size must be positive")
        self.batch_size = batch_size

    def write(self, parsed: Any) -> SinkResult:
        return self.write_table(parsed.artifact, parsed.raw_rows, parsed.rows)

    @staticmethod
    def _execute_heartbeat(conn: Any, sql: str, params: Any = None, *, cancel: Callable[[], bool] | None = None,
                           progress: Callable[[dict[str, Any]], None] | None = None, phase: str = "query") -> Any:
        result: list[Any] = []; error: list[BaseException] = []
        def run() -> None:
            try:
                result.append(conn.execute(sql, params) if params is not None else conn.execute(sql))
            except BaseException as exc:
                error.append(exc)
        worker = threading.Thread(target=run, daemon=True); worker.start(); started = time.monotonic()
        callback_error: BaseException | None = None
        while worker.is_alive():
            worker.join(0.25)
            elapsed = time.monotonic() - started
            if progress:
                try:
                    progress({"phase": phase, "heartbeat_seconds": elapsed})
                except BaseException as exc:
                    callback_error = exc
            if cancel and cancel():
                try: conn.interrupt()
                except Exception: pass
                # Do not roll back or reuse the connection while the worker
                # still owns an in-flight query. DuckDB interrupt is expected
                # to terminate promptly; this loop is the safety boundary.
                while worker.is_alive():
                    worker.join(0.1)
                raise InterruptedError("ingestion cancelled")
        if error:
            raise error[0]
        if callback_error:
            raise callback_error
        return result[0] if result else None

    def write_delimited(self, scan: Any, *, source_id: str | None = None,
                        display_name: str | None = None,
                        cancel: Callable[[], bool] | None = None,
                        progress: Callable[[dict[str, Any]], None] | None = None) -> SinkResult:
        """Load a prepared delimited source using DuckDB-native batches.

        The CSV is scanned into a temporary all-VARCHAR relation once. Type
        profiling and publication are SQL operations, so Python never holds a
        second copy of the rows (nor performs per-row ``executemany`` calls).
        """
        conn = self.connection
        delimiter = str(scan.delimiter).replace("'", "''")
        # DuckDB's CSV reader accepts UTF-8/UTF-16 and common ICU encodings. A
        # strict UTF-8 fallback is retained for older DuckDB builds.
        encoding_name = str(scan.encoding).lower().replace("-", "")
        encoding_name = {"utf8sig": "utf-8", "utf8": "utf-8", "cp1252": "latin-1", "latin1": "latin-1"}.get(encoding_name, str(scan.encoding))
        encoding = encoding_name.replace("'", "''")
        conn.execute("BEGIN TRANSACTION")
        stage = f"__ingest_stage_{hashlib.sha256(os.urandom(16)).hexdigest()[:16]}"
        raw_name = typed_name = None
        try:
            if cancel and cancel():
                raise InterruptedError("ingestion cancelled")
            if progress:
                progress({"phase": "reading", "rows": 0})
            read_expr = (f"read_csv(?, header=false, sample_size=-1, all_varchar=true, null_padding=true, "
                         f"strict_mode=false, nullstr='\\\\N', delim='{delimiter}', encoding='{encoding}')")
            self._execute_heartbeat(conn, f"CREATE TEMP TABLE {quote_identifier(stage)} AS SELECT row_number() OVER () - 1 AS __row_id, * FROM {read_expr}", [scan.path], cancel=cancel, progress=progress, phase="reading")
            description = conn.execute(f"SELECT * FROM {quote_identifier(stage)} LIMIT 0").description
            if progress:
                progress({"phase": "profile", "rows": 0})
            names = [item[0] for item in description if item[0] != "__row_id"]
            first = conn.execute(f"SELECT * FROM {quote_identifier(stage)} WHERE __row_id = 0").fetchone()
            headers = list(first[1:]) if first is not None else []
            headers.extend(f"COL_{i + 1}" for i in range(len(headers), len(names)))
            headers = [str(value) if value is not None else f"COL_{i + 1}" for i, value in enumerate(headers)]
            value_types: list[ValueType] = []
            for index, name in enumerate(names):
                if cancel and cancel():
                    raise InterruptedError("ingestion cancelled")
                col = quote_identifier(name)
                stats = self._execute_heartbeat(conn,
                    f"""SELECT
                    count(*) FILTER (WHERE __row_id > 0 AND {col} IS NOT NULL AND {col} <> ''),
                    count(*) FILTER (WHERE __row_id > 0 AND {col} IS NOT NULL AND {col} <> '' AND lower({col}) IN ('true','false')),
                    count(*) FILTER (WHERE __row_id > 0 AND {col} IS NOT NULL AND {col} <> '' AND regexp_matches({col}, '^[+-]?[0-9]+$')
                      AND NOT regexp_matches({col}, '^[+-]?0[0-9]') AND try_cast({col} AS BIGINT) IS NOT NULL),
                    count(*) FILTER (WHERE __row_id > 0 AND {col} IS NOT NULL AND {col} <> '' AND try_cast({col} AS DECIMAL(38,18)) IS NOT NULL
                      AND (position('.' IN {col}) = 0 OR length(split_part({col}, '.', 2)) <= 18)
                      AND length(regexp_replace({col}, '[^0-9]', '', 'g')) <= 38),
                    count(*) FILTER (WHERE __row_id > 0 AND {col} IS NOT NULL AND {col} <> '' AND (contains(lower({col}), '.') OR contains(lower({col}), 'e')))
                    FROM {quote_identifier(stage)}""", cancel=cancel, progress=progress, phase="profile").fetchone()
                nonblank, bool_ok, int_ok, dec_ok, decimal_notation = stats
                if not nonblank:
                    value_types.append(ValueType.NULL)
                elif bool_ok == nonblank:
                    value_types.append(ValueType.BOOLEAN)
                elif int_ok == nonblank:
                    value_types.append(ValueType.INTEGER)
                elif dec_ok == nonblank and decimal_notation:
                    value_types.append(ValueType.DECIMAL)
                else:
                    value_types.append(ValueType.TEXT)
                if progress:
                    progress({"phase": "profile", "rows": 0, "columns": index + 1, "columns_total": len(names)})
            columns = tuple(OrderedColumnSchema(f"col_{i + 1}", headers[i] if i < len(headers) else f"COL_{i + 1}", i, value_types[i], nullable=True, physical_name=f"col_{i + 1}") for i in range(len(names)))
            table_key = source_id or getattr(scan, "source_name", "upload")
            table_key = str(table_key)
            base = f"{_name(table_key)[:48]}_{hashlib.sha256(table_key.encode('utf-8')).hexdigest()[:12]}"
            raw_name, typed_name = f"{base}__raw", f"{base}__typed"
            for name in (raw_name, typed_name):
                if conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [name]).fetchone()[0]:
                    raise ValueError(f"immutable ingestion table already exists: {name}")
            raw_select = ", ".join(f"{quote_identifier(names[i])} AS {quote_identifier(columns[i].physical_name or columns[i].key)}" for i in range(len(names)))
            self._execute_heartbeat(conn, f"CREATE TABLE {quote_identifier(raw_name)} AS SELECT __row_id{', ' if raw_select else ''}{raw_select} FROM {quote_identifier(stage)}", cancel=cancel, progress=progress, phase="raw_write")
            if cancel and cancel():
                raise InterruptedError("ingestion cancelled")
            if progress:
                progress({"phase": "raw_written", "rows": 0, "table": table_key})
            typed_selects = []
            for i, col in enumerate(columns):
                source_col = quote_identifier(names[i])
                target = quote_identifier(col.physical_name or col.key)
                if col.value_type is ValueType.TEXT:
                    expr = source_col
                elif col.value_type is ValueType.NULL:
                    expr = "CAST(NULL AS VARCHAR)"
                else:
                    duck_type = _duck_type(col.value_type)
                    expr = f"CASE WHEN __row_id = 0 THEN NULL ELSE try_cast(NULLIF({source_col}, '') AS {duck_type}) END"
                typed_selects.append(f"{expr} AS {target}")
            typed_select = ", ".join(typed_selects)
            self._execute_heartbeat(conn, f"CREATE TABLE {quote_identifier(typed_name)} AS SELECT __row_id{', ' if typed_select else ''}{typed_select} FROM {quote_identifier(stage)}", cancel=cancel, progress=progress, phase="typed_write")
            row_count = conn.execute(f"SELECT count(*) FROM {quote_identifier(stage)}").fetchone()[0]
            if progress:
                progress({"phase": "written", "rows": row_count, "table": table_key})
            if cancel and cancel():
                raise InterruptedError("ingestion cancelled")
            if cancel and cancel():
                raise InterruptedError("ingestion cancelled")
            if progress:
                progress({"phase": "hashing", "rows": row_count, "table": table_key})
            # Keep the historical row-wise hash representation for bounded
            # compatibility calls; large loads use native JSON aggregation.
            raw_hash, typed_hash = self._hash_tables(conn, raw_name, typed_name, len(columns), row_count, cancel=cancel, progress=progress)
            if cancel and cancel():
                raise InterruptedError("ingestion cancelled")
            artifact = TableArtifact(table_key, display_name or os.path.splitext(os.path.basename(getattr(scan, "source_name", table_key)))[0], columns, row_count, raw_hash=raw_hash, typed_hash=typed_hash)
            conn.execute(f"DROP TABLE {quote_identifier(stage)}")
            conn.execute("COMMIT")
            return SinkResult(artifact, raw_name, typed_name, raw_hash, typed_hash)
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        finally:
            close = getattr(scan, "close", None)
            if close:
                close()

    def _hash_tables(self, conn: Any, raw_name: str, typed_name: str, columns: int, row_count: int, *, cancel: Callable[[], bool] | None = None, progress: Callable[[dict[str, Any]], None] | None = None) -> tuple[str, str]:
        # JSON aggregation stays in DuckDB for large files and avoids creating
        # Python row/cell objects.  The separator mirrors the old incremental
        # digest's no-separator row concatenation.
        def digest(name: str) -> str:
            fields = ", ".join(quote_identifier(f"col_{i + 1}") for i in range(columns))
            if not fields:
                fields = "NULL"
            cursor = self._execute_heartbeat(conn, f"SELECT sha256(coalesce(string_agg(json_array({fields}), '' ORDER BY __row_id), '')) FROM {quote_identifier(name)}", cancel=cancel, progress=progress, phase="hash")
            value = cursor.fetchone()[0]
            return str(value)
        return digest(raw_name), digest(typed_name)

    def write_table(self, artifact: TableArtifact, raw_rows: Iterable[Sequence[Any]], typed_rows: Iterable[Sequence[Any]] | None = None) -> SinkResult:
        columns = tuple(artifact.columns)
        raw_iter = iter(raw_rows)
        typed_iter = iter(typed_rows) if typed_rows is not None else None
        physical_names = [col.physical_name or col.key for col in columns]
        if len(set(physical_names)) != len(physical_names):
            raise ValueError("duplicate physical column names")
        key_digest = hashlib.sha256(artifact.table_key.encode("utf-8")).hexdigest()[:12]
        base = f"{_name(artifact.table_key)[:48]}_{key_digest}"
        raw_name, typed_name = f"{base}__raw", f"{base}__typed"
        conn = self.connection
        # DuckDB's transaction gives all-or-nothing publication.  CREATE TABLE
        # uses the final names only after the input dimensions are validated.
        conn.execute("BEGIN TRANSACTION")
        try:
            for name in (raw_name, typed_name):
                exists = conn.execute("SELECT COUNT(*) FROM information_schema.tables WHERE table_name = ?", [name]).fetchone()[0]
                if exists:
                    raise ValueError(f"immutable ingestion table already exists: {name}")
            fields = ", ".join(f"{quote_identifier(col.physical_name or col.key)} VARCHAR" for col in columns)
            if fields:
                fields = ", " + fields
            conn.execute(f"CREATE TABLE {quote_identifier(raw_name)} (__row_id BIGINT NOT NULL{fields})")
            typed_fields = ", ".join(f"{quote_identifier(col.physical_name or col.key)} {_duck_type(col.value_type)}" for col in columns)
            if typed_fields:
                typed_fields = ", " + typed_fields
            conn.execute(f"CREATE TABLE {quote_identifier(typed_name)} (__row_id BIGINT NOT NULL{typed_fields})")
            raw_hash = hashlib.sha256()
            typed_hash = hashlib.sha256()
            raw_sql = f"INSERT INTO {quote_identifier(raw_name)} VALUES ({', '.join(['?'] * (len(columns) + 1))})"
            typed_sql = f"INSERT INTO {quote_identifier(typed_name)} VALUES ({', '.join(['?'] * (len(columns) + 1))})"
            row_count = 0
            while True:
                raw_batch = []
                typed_batch = []
                for _ in range(self.batch_size):
                    try:
                        raw = next(raw_iter)
                    except StopIteration:
                        raw = None
                        raw_done = True
                    else:
                        raw_done = False
                    if typed_iter is None:
                        typed, typed_done = raw, raw_done
                    else:
                        try:
                            typed = next(typed_iter)
                        except StopIteration:
                            typed = None
                            typed_done = True
                        else:
                            typed_done = False
                    if raw_done and typed_done:
                        break
                    if raw_done != typed_done:
                        raise ValueError("raw and typed row counts differ")
                    raw = tuple(raw)
                    typed = tuple(typed)
                    if len(raw) != len(columns) or len(typed) != len(columns):
                        raise ValueError("row width does not match schema")
                    row_index = row_count
                    row_count += 1
                    raw_values = [None if value is None else str(value) for value in raw]
                    typed_values = list(typed)
                    raw_batch.append([row_index, *raw_values])
                    typed_batch.append([row_index, *typed_values])
                    raw_hash.update(json.dumps(raw_values, ensure_ascii=False, default=str, separators=(",", ":")).encode("utf-8"))
                    typed_hash.update(json.dumps(typed_values, ensure_ascii=False, default=str, separators=(",", ":")).encode("utf-8"))
                if raw_batch:
                    conn.executemany(raw_sql, raw_batch)
                    conn.executemany(typed_sql, typed_batch)
                if not raw_batch:
                    break
            conn.execute("COMMIT")
        except Exception:
            try:
                conn.execute("ROLLBACK")
            except Exception:
                pass
            raise
        updated = TableArtifact(artifact.table_key, artifact.display_name, columns, row_count, artifact.provenance, raw_hash.hexdigest(), typed_hash.hexdigest())
        return SinkResult(updated, raw_name, typed_name, raw_hash.hexdigest(), typed_hash.hexdigest())


def write_table(connection: Any, artifact: TableArtifact, raw_rows: Iterable[Sequence[Any]], typed_rows: Iterable[Sequence[Any]] | None = None, *, batch_size: int = 10_000) -> SinkResult:
    return DuckDBSink(connection, batch_size=batch_size).write_table(artifact, raw_rows, typed_rows)
