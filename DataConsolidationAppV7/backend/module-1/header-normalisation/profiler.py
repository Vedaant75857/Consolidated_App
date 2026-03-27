"""Column profiling for header normalisation — all SQL-based."""

from __future__ import annotations

import os
import sys
import sqlite3
from dataclasses import dataclass
from typing import Any

_this_dir = os.path.dirname(os.path.abspath(__file__))
_backend_dir = os.path.normpath(os.path.join(_this_dir, "..", ".."))
if _backend_dir not in sys.path:
    sys.path.insert(0, _backend_dir)

_aliases_mod = None
try:
    import importlib.util

    _aliases_path = os.path.join(_this_dir, "aliases.py")
    _aliases_spec = importlib.util.spec_from_file_location("hn_aliases_profile", _aliases_path)
    _aliases_mod = importlib.util.module_from_spec(_aliases_spec)  # type: ignore[arg-type]
    sys.modules["hn_aliases_profile"] = _aliases_mod
    _aliases_spec.loader.exec_module(_aliases_mod)  # type: ignore[union-attr]
except Exception:
    _aliases_mod = None

from shared.db import read_table_columns, table_row_count
from shared.db.stats_ops import column_stats, column_distinct_values

infer_value_type = getattr(_aliases_mod, "infer_value_type", lambda samples: "text")
semantic_hints = getattr(_aliases_mod, "semantic_hints", lambda samples: [])

_VALUE_TYPE_TO_SQL: dict[str, str] = {
    "number": "REAL",
    "integer": "INTEGER",
    "date": "DATE",
    "currency": "REAL",
    "percentage": "REAL",
}


@dataclass
class ColumnProfile:
    source_name: str
    position: int
    sql_dtype: str
    total_rows: int
    null_pct: float
    distinct_pct: float
    neighbours: list[str]
    sample_values: list[str]
    inferred_value_type: str
    semantic_tags: list[str]

    def to_dict(self) -> dict[str, Any]:
        return {
            "source_name": self.source_name,
            "position": self.position,
            "sql_dtype": self.sql_dtype,
            "total_rows": self.total_rows,
            "null_pct": round(self.null_pct, 4),
            "distinct_pct": round(self.distinct_pct, 4),
            "neighbours": self.neighbours,
            "sample_values": self.sample_values,
            "inferred_value_type": self.inferred_value_type,
            "semantic_tags": self.semantic_tags,
        }


def profile_table_columns(
    conn: sqlite3.Connection,
    sql_name: str,
    max_samples: int = 20,
) -> list[ColumnProfile]:
    """Profile every column in *sql_name* using SQL-only operations."""
    columns = read_table_columns(conn, sql_name)
    if not columns:
        return []

    total_rows = table_row_count(conn, sql_name)
    if total_rows == 0:
        return [
            ColumnProfile(
                source_name=col,
                position=i,
                sql_dtype="TEXT",
                total_rows=0,
                null_pct=1.0,
                distinct_pct=0.0,
                neighbours=_neighbours(columns, i),
                sample_values=[],
                inferred_value_type="text",
                semantic_tags=[],
            )
            for i, col in enumerate(columns)
        ]

    stats = column_stats(conn, sql_name, columns)
    stats_map = {s["column_name"]: s for s in stats}

    profiles: list[ColumnProfile] = []
    for i, col in enumerate(columns):
        s = stats_map.get(col, {})
        non_null = s.get("non_null_count", 0)
        null_pct = 1.0 - (non_null / total_rows) if total_rows else 1.0
        distinct = s.get("distinct_count", 0)
        distinct_pct = distinct / non_null if non_null else 0.0

        samples = column_distinct_values(conn, sql_name, col, max_samples)
        val_type = infer_value_type(samples)

        profiles.append(
            ColumnProfile(
                source_name=col, position=i,
                sql_dtype=_VALUE_TYPE_TO_SQL.get(val_type, "TEXT"),
                total_rows=total_rows, null_pct=null_pct,
                distinct_pct=distinct_pct, neighbours=_neighbours(columns, i),
                sample_values=samples, inferred_value_type=val_type,
                semantic_tags=semantic_hints(samples),
            )
        )
    return profiles


def _neighbours(columns: list[str], idx: int) -> list[str]:
    out: list[str] = []
    if idx > 0:
        out.append(columns[idx - 1])
    if idx < len(columns) - 1:
        out.append(columns[idx + 1])
    return out
