"""Column-level statistics computed via SQL."""

from __future__ import annotations

from typing import Any

from .duckdb_compat import DuckDBConnection
from .table_ops import quote_id, read_table_columns, table_exists, normalize_for_match


def column_stats(
    conn: DuckDBConnection,
    table_name: str,
    columns: list[str] | None = None,
) -> list[dict[str, Any]]:
    """Compute fill-rate and distinct-count stats for each column in one pass.

    Args:
        conn: DuckDB session connection.
        table_name: The table to profile.
        columns: Optional subset of columns; defaults to all.

    Returns:
        List of per-column stat dicts.
    """
    if not table_exists(conn, table_name):
        return []
    cols = columns if columns is not None else read_table_columns(conn, table_name)
    if not cols:
        return []

    tbl = quote_id(table_name)

    # Single-pass: one table scan computes stats for ALL columns at once.
    parts: list[str] = []
    for col in cols:
        qc = quote_id(col)
        parts.append(f"COUNT(CASE WHEN {qc} IS NOT NULL AND TRIM({qc}) != '' THEN 1 END)")
        parts.append(f"COUNT(DISTINCT CASE WHEN {qc} IS NOT NULL AND TRIM({qc}) != '' THEN {qc} END)")

    sql = f"SELECT COUNT(*) AS total, {', '.join(parts)} FROM {tbl}"
    row = conn.execute(sql).fetchone()
    total = row[0]
    results: list[dict[str, Any]] = []
    for i, col in enumerate(cols):
        non_null = row[1 + i * 2]
        distinct_count = row[2 + i * 2]
        results.append({
            "column_name": col,
            "null_count": total - non_null,
            "non_null_count": non_null,
            "fill_rate": non_null / total if total > 0 else 0,
            "distinct_count": distinct_count,
        })
    return results


def column_distinct_values(
    conn: DuckDBConnection,
    table_name: str,
    column: str,
    limit: int = 200,
) -> list[str]:
    """Return up to *limit* distinct non-empty values for a column."""
    if not table_exists(conn, table_name):
        return []
    tbl = quote_id(table_name)
    qc = quote_id(column)
    rows = conn.execute(
        f"SELECT DISTINCT {qc} AS v FROM {tbl} WHERE {qc} IS NOT NULL AND TRIM({qc}) != '' LIMIT ?",
        (limit,),
    ).fetchall()
    return [str(r["v"]) for r in rows]


def column_distinct_count(
    conn: DuckDBConnection, table_name: str, column: str
) -> int:
    """Count distinct normalised non-empty values for a column."""
    if not table_exists(conn, table_name):
        return 0
    tbl = quote_id(table_name)
    qc = quote_id(column)
    norm = normalize_for_match(qc)
    row = conn.execute(
        f"SELECT COUNT(DISTINCT {norm}) AS cnt FROM {tbl} WHERE {qc} IS NOT NULL AND TRIM({qc}) != ''"
    ).fetchone()
    return row["cnt"] if row else 0


def column_null_count(
    conn: DuckDBConnection, table_name: str, column: str
) -> int:
    """Count null or empty values for a column."""
    if not table_exists(conn, table_name):
        return 0
    tbl = quote_id(table_name)
    qc = quote_id(column)
    row = conn.execute(
        f"SELECT COUNT(*) AS cnt FROM {tbl} WHERE {qc} IS NULL OR TRIM({qc}) = ''"
    ).fetchone()
    return row["cnt"] if row else 0


def compute_overlap(
    conn: DuckDBConnection,
    table_a: str,
    col_a: str,
    table_b: str,
    col_b: str,
) -> int:
    """Count the number of distinct normalised values shared by two columns.

    Args:
        conn: DuckDB session connection.
        table_a / col_a: First table and column.
        table_b / col_b: Second table and column.

    Returns:
        Number of overlapping distinct normalised values.
    """
    if not table_exists(conn, table_a) or not table_exists(conn, table_b):
        return 0
    t_a = quote_id(table_a)
    t_b = quote_id(table_b)
    q_a = quote_id(col_a)
    q_b = quote_id(col_b)
    norm_a = normalize_for_match(q_a)
    norm_b = normalize_for_match(q_b)
    row = conn.execute(
        f"""SELECT COUNT(*) AS cnt FROM (
            SELECT DISTINCT {norm_a} AS v FROM {t_a}
                WHERE {q_a} IS NOT NULL AND TRIM({q_a}) != ''
            INTERSECT
            SELECT DISTINCT {norm_b} AS v FROM {t_b}
                WHERE {q_b} IS NOT NULL AND TRIM({q_b}) != ''
        )"""
    ).fetchone()
    return row["cnt"] if row else 0


def distinct_values_by_column_sql(
    conn: DuckDBConnection,
    table_name: str,
    max_per_col: int = 200,
    columns: list[str] | None = None,
) -> dict[str, list[str]]:
    """Return distinct non-empty values for each column in a single-pass query.

    Uses UNION ALL with per-column ROW_NUMBER to cap results, avoiding one
    full table scan per column.
    """
    if not table_exists(conn, table_name):
        return {}
    all_cols = read_table_columns(conn, table_name)
    if columns is None:
        cols = all_cols
    else:
        allowed = set(all_cols)
        cols = [c for c in columns if c in allowed]
    if not cols:
        return {}

    tbl = quote_id(table_name)
    parts = []
    for col in cols:
        qc = quote_id(col)
        lit = "'" + col.replace("'", "''") + "'"
        parts.append(
            f"SELECT {lit} AS col_name, {qc} AS val "
            f"FROM (SELECT DISTINCT {qc} FROM {tbl} "
            f"WHERE {qc} IS NOT NULL AND TRIM({qc}) != '' "
            f"LIMIT {int(max_per_col)}) _d"
        )

    sql = " UNION ALL ".join(parts)
    rows = conn.execute(sql).fetchall()

    result: dict[str, list[str]] = {c: [] for c in cols}
    for r in rows:
        result[r[0]].append(str(r[1]))
    return result


def get_overlap_sql(
    conn: DuckDBConnection,
    table_map: list[dict[str, str]],
) -> dict[str, dict[str, Any]]:
    """Build per-file overlap map using SQL INTERSECT.

    Args:
        table_map: list of {"table_key": ..., "sql_name": ...}

    Returns:
        Nested dict of overlap metrics between each pair of tables.
    """
    non_empty = [
        t for t in table_map if read_table_columns(conn, t["sql_name"])
    ]

    cols_cache: dict[str, list[str]] = {}
    for t in non_empty:
        cols_cache[t["table_key"]] = read_table_columns(conn, t["sql_name"])

    result: dict[str, dict[str, Any]] = {t["table_key"]: {} for t in non_empty}

    for i in range(len(non_empty)):
        a = non_empty[i]
        cols_a = cols_cache[a["table_key"]]
        norm_set_a = {c.strip().lower() for c in cols_a}

        for j in range(i + 1, len(non_empty)):
            b = non_empty[j]
            cols_b = cols_cache[b["table_key"]]
            norm_set_b = {c.strip().lower() for c in cols_b}

            common_norm = norm_set_a & norm_set_b
            common_column_count = len(common_norm)
            min_cols = min(len(cols_a), len(cols_b))
            col_name_overlap = common_column_count / min_cols if min_cols else 0

            value_overlap_avg: float | None = None
            if common_norm:
                norm_to_col_a = {c.strip().lower(): c for c in cols_a}
                norm_to_col_b = {c.strip().lower(): c for c in cols_b}

                sum_ratio = 0.0
                count = 0
                for n in common_norm:
                    c_a = norm_to_col_a.get(n)
                    c_b = norm_to_col_b.get(n)
                    if not c_a or not c_b:
                        continue

                    dist_a = column_distinct_count(conn, a["sql_name"], c_a)
                    dist_b = column_distinct_count(conn, b["sql_name"], c_b)
                    min_distinct = min(dist_a, dist_b)
                    if min_distinct == 0:
                        continue

                    overlap = compute_overlap(conn, a["sql_name"], c_a, b["sql_name"], c_b)
                    sum_ratio += overlap / min_distinct
                    count += 1
                if count > 0:
                    value_overlap_avg = sum_ratio / count

            metrics: dict[str, Any] = {
                "column_name_overlap": col_name_overlap,
                "common_column_count": common_column_count,
            }
            if value_overlap_avg is not None:
                metrics["value_overlap_avg"] = value_overlap_avg

            result[a["table_key"]][b["table_key"]] = metrics
            result[b["table_key"]][a["table_key"]] = metrics

    return result
