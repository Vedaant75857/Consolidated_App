"""Shared step-aware session artifact invalidation for pipeline resets."""

from __future__ import annotations

from shared.db.duckdb_compat import DuckDBConnection
from shared.db.meta_ops import delete_meta, get_all_meta_keys
from shared.db.session_db import all_registered_tables, unregister_table
from shared.db.table_ops import drop_table

# Meta keys and table prefixes cleared when invalidating downstream of each step.
_STEP_ARTIFACTS: dict[int, dict] = {
    3: {
        "meta": ["appendGroups", "appendGroupMappings", "groupSchemaTableRows", "unassigned"],
        "table_prefix": ["appended__"],
    },
    4: {
        "meta": ["headerNormDecisions", "headerNormApplied"],
        "table_prefix": ["hn__"],
    },
    5: {
        "meta": [],
        "table_prefix": [],
    },
    6: {
        "meta": ["mergeBaseGroupId", "mergeApprovedSources", "merge_history"],
        "table_prefix": ["final_merged", "merged_v", "merged_output_"],
    },
}

# Preserved during partial invalidation (from_step > 0).
_PLAYGROUND_META_KEYS = frozenset({"playgroundSheets"})
_PLAYGROUND_TABLE_PREFIX = "pg__"


def _should_drop_table(sql_name: str, prefix: str, from_step: int) -> bool:
    """Return True if a registered table should be dropped for this invalidation."""
    if not sql_name.startswith(prefix):
        return False
    if from_step > 0 and sql_name.startswith(_PLAYGROUND_TABLE_PREFIX):
        return False
    return True


def invalidate_session_downstream(conn: DuckDBConnection, from_step: int) -> None:
    """Clear cached artifacts for all pipeline steps after *from_step*.

    Args:
        conn: DuckDB session connection.
        from_step: Last step that remains valid (0 = full session reset).
    """
    if from_step == 0:
        for key in get_all_meta_keys(conn):
            delete_meta(conn, key)
        for reg in all_registered_tables(conn):
            sql_name = reg.get("sql_name") or reg.get("table_name", "")
            if sql_name:
                drop_table(conn, sql_name, commit=False)
            tk = reg.get("table_key", "")
            if tk:
                unregister_table(conn, tk, commit=False)
        conn.commit()
        return

    for step_num in sorted(_STEP_ARTIFACTS.keys()):
        if step_num <= from_step:
            continue
        spec = _STEP_ARTIFACTS[step_num]
        for key in spec["meta"]:
            if key in _PLAYGROUND_META_KEYS:
                continue
            delete_meta(conn, key)
        for prefix in spec["table_prefix"]:
            for reg in all_registered_tables(conn):
                sql_name = reg.get("sql_name") or reg.get("table_name", "")
                tk = reg.get("table_key", "")
                if sql_name and _should_drop_table(sql_name, prefix, from_step):
                    drop_table(conn, sql_name, commit=False)
                    if tk:
                        unregister_table(conn, tk, commit=False)
    conn.commit()
