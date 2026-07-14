"""DuckDB compatibility layer that mimics the sqlite3 Connection/Row API.

Every module's session_db calls ``duckdb_connect()`` instead of ``sqlite3.connect()``.
The returned ``DuckDBConnection`` quacks like a sqlite3.Connection:
  - ``conn.execute(sql, params)`` returns a cursor-like wrapper
  - ``conn.executemany(sql, seq)`` works
  - ``conn.commit()`` and ``conn.close()`` work
  - ``row["col"]``  dict-style access works (via DictRow)
  - ``cursor.description`` works
  - ``cursor.fetchone()`` / ``cursor.fetchall()`` return DictRow objects
  - ``cursor.rowcount`` works for DML statements

This lets all existing business-logic SQL remain untouched.
"""

from __future__ import annotations

import duckdb
from typing import Any, Sequence


class DictRow:
    """Emulates sqlite3.Row — supports index access, key access, dict(), keys(), len(), iter."""

    __slots__ = ("_data", "_columns")

    def __init__(self, columns: tuple[str, ...], values: tuple):
        self._columns = columns
        self._data = values

    def __getitem__(self, key):
        if isinstance(key, str):
            try:
                idx = self._columns.index(key)
            except ValueError:
                raise KeyError(key)
            return self._data[idx]
        return self._data[key]

    def __len__(self) -> int:
        return len(self._data)

    def __iter__(self):
        return iter(self._data)

    def __repr__(self) -> str:
        return f"DictRow({dict(zip(self._columns, self._data))})"

    def keys(self):
        """Return column names (matches sqlite3.Row.keys())."""
        return self._columns


class DuckCursorWrapper:
    """Wraps a DuckDB result so that .fetchone() / .fetchall() return DictRow objects.

    Also exposes .description and .rowcount for compatibility.
    """

    def __init__(self, result: duckdb.DuckDBPyConnection, connection: DuckDBConnection):
        self._result = result
        self._conn = connection
        self._columns: tuple[str, ...] | None = None
        self._description_cache: list[tuple] | None = None
        self._rowcount: int = -1

    def _ensure_columns(self) -> tuple[str, ...]:
        """Lazily resolve column names from the result description."""
        if self._columns is None:
            desc = self._raw_description()
            if desc:
                self._columns = tuple(d[0] for d in desc)
            else:
                self._columns = ()
        return self._columns

    def _raw_description(self):
        """Safely get the underlying DuckDB description."""
        try:
            return self._result.description
        except (RuntimeError, duckdb.InvalidInputException):
            return None

    @property
    def description(self):
        """Cursor description — list of (name, type_code, ...) 7-tuples."""
        if self._description_cache is not None:
            return self._description_cache
        raw = self._raw_description()
        if raw is None:
            return None
        self._description_cache = [
            (d[0], d[1], None, None, None, None, None) for d in raw
        ]
        return self._description_cache

    @property
    def rowcount(self) -> int:
        """Number of rows affected by the last DML statement."""
        return self._rowcount

    def fetchone(self) -> DictRow | None:
        """Fetch a single row as a DictRow, or None if exhausted."""
        cols = self._ensure_columns()
        try:
            raw = self._result.fetchone()
        except (RuntimeError, duckdb.InvalidInputException):
            return None
        if raw is None:
            return None
        return DictRow(cols, tuple(raw))

    def fetchall(self) -> list[DictRow]:
        """Fetch all remaining rows as a list of DictRow objects."""
        cols = self._ensure_columns()
        try:
            raw_rows = self._result.fetchall()
        except (RuntimeError, duckdb.InvalidInputException):
            return []
        return [DictRow(cols, tuple(r)) for r in raw_rows]

    def fetchmany(self, size: int = 1) -> list[DictRow]:
        """Fetch up to *size* rows."""
        cols = self._ensure_columns()
        try:
            raw_rows = self._result.fetchmany(size)
        except (RuntimeError, duckdb.InvalidInputException):
            return []
        return [DictRow(cols, tuple(r)) for r in raw_rows]

    def close(self):
        """No-op for API compat; DuckDB cursors auto-close."""
        pass

    def __iter__(self):
        """Iterate rows as DictRow objects (used by ``for row in cursor``)."""
        cols = self._ensure_columns()
        while True:
            try:
                raw = self._result.fetchone()
            except (RuntimeError, duckdb.InvalidInputException):
                return
            if raw is None:
                return
            yield DictRow(cols, tuple(raw))


class DuckDBConnection:
    """Drop-in replacement for sqlite3.Connection backed by DuckDB.

    Wraps a duckdb.DuckDBPyConnection and translates sqlite3 conventions
    (``?`` placeholders, ``executemany``, ``commit``/``close``) into DuckDB calls.
    """

    def __init__(self, duck_conn: duckdb.DuckDBPyConnection):
        self._conn = duck_conn

    # ── SQL translation helpers ───────────────────────────────────────────

    @staticmethod
    def _translate_sql(sql: str) -> str:
        """Pass-through for now: DuckDB natively handles ``?`` positional
        params and ``INSERT OR REPLACE`` (on tables with a PRIMARY KEY),
        so no rewriting is required.
        """
        return sql

    @staticmethod
    def _prepare_params(params: Sequence | None) -> list | None:
        """Ensure params are a list (DuckDB prefers lists over tuples for binding)."""
        if params is None:
            return None
        return list(params)

    # ── Core API ──────────────────────────────────────────────────────────

    def execute(self, sql: str, params: Sequence | None = None) -> DuckCursorWrapper:
        """Execute SQL and return a cursor wrapper with DictRow support.

        Args:
            sql: SQL statement (may use ``?`` positional placeholders).
            params: Optional sequence of bind parameters.

        Returns:
            DuckCursorWrapper that supports .fetchone(), .fetchall(), .description, iteration.
        """
        translated = self._translate_sql(sql)
        prepared = self._prepare_params(params)
        if prepared is not None:
            result = self._conn.execute(translated, prepared)
        else:
            result = self._conn.execute(translated)
        wrapper = DuckCursorWrapper(result, self)
        return wrapper

    def executemany(self, sql: str, seq_of_params: Sequence[Sequence]) -> DuckCursorWrapper:
        """Execute SQL with multiple parameter sets.

        DuckDB's native executemany accepts a list of lists/tuples.

        Args:
            sql: SQL statement with ``?`` positional placeholders.
            seq_of_params: Iterable of parameter sequences.

        Returns:
            DuckCursorWrapper (description will be None for DML).
        """
        translated = self._translate_sql(sql)
        params_list = [list(p) for p in seq_of_params]
        if params_list:
            result = self._conn.executemany(translated, params_list)
        else:
            result = self._conn.execute(translated)
        return DuckCursorWrapper(result, self)

    def commit(self) -> None:
        """Commit the current transaction.

        DuckDB in persistent mode supports transactions; this is a real commit.
        """
        try:
            self._conn.commit()
        except duckdb.InvalidInputException as exc:
            # "No active transaction" is benign; anything else should propagate
            msg = str(exc).lower()
            if "no active transaction" not in msg and "no transaction" not in msg:
                raise

    def close(self) -> None:
        """Close the underlying DuckDB connection."""
        try:
            self._conn.close()
        except Exception:
            pass

    def cursor(self) -> DuckCursorWrapper:
        """Return a pseudo-cursor (for API compat with code that calls conn.cursor())."""
        return DuckCursorWrapper(self._conn, self)

    @property
    def row_factory(self):
        """No-op property for sqlite3 compat — DictRow is always used."""
        return DictRow

    @row_factory.setter
    def row_factory(self, value):
        """Silently accept row_factory assignment (sqlite3 compat)."""
        pass

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        return False


def duckdb_connect(db_path: str, **kwargs) -> DuckDBConnection:
    """Open a DuckDB database file and return a DuckDBConnection wrapper.

    This is the single entry point used by all session_db modules.

    Args:
        db_path: Path to the .duckdb file.
        **kwargs: Additional kwargs passed to duckdb.connect() (e.g. read_only).

    Returns:
        A DuckDBConnection that behaves like sqlite3.Connection.
    """
    # check_same_thread is sqlite3-only; strip it if passed
    kwargs.pop("check_same_thread", None)
    duck = duckdb.connect(db_path, **kwargs)
    return DuckDBConnection(duck)
