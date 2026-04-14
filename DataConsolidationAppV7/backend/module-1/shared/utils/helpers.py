"""General-purpose helpers shared by both modules."""

from __future__ import annotations

from dataclasses import dataclass, field
import json
from typing import Any

from shared.db import DuckDBConnection

from .json_helpers import json_default, json_safe


def chunk_list(items: list, chunk_size: int) -> list[list]:
    return [items[i : i + chunk_size] for i in range(0, len(items), chunk_size)]


@dataclass
class StepSpec:
    """Lightweight runtime contract for a pipeline/modular operation."""

    name: str
    required_tables: list[str] = field(default_factory=list)
    required_meta: list[str] = field(default_factory=list)
    produces_tables: list[str] = field(default_factory=list)
    produces_meta: list[str] = field(default_factory=list)


def resolve_input_table(conn: DuckDBConnection, table_ref: str) -> str | None:
    """Resolve table references across table_key/sql_name and simple aliases."""
    # Lazy import to avoid circular dependency with shared.db at module-load time
    from shared.db import all_registered_tables, lookup_sql_name, table_exists

    ref = str(table_ref or "").strip()
    if not ref:
        return None
    if ref in ("latest", "latest_registered"):
        rows = all_registered_tables(conn)
        if not rows:
            return None
        sql_name = str(rows[-1].get("sql_name") or "")
        return sql_name if sql_name and table_exists(conn, sql_name) else None
    if table_exists(conn, ref):
        return ref
    mapped = lookup_sql_name(conn, ref)
    if mapped and table_exists(conn, mapped):
        return mapped
    rows = all_registered_tables(conn)
    for r in rows:
        key = str(r.get("table_key") or "")
        sql_name = str(r.get("sql_name") or "")
        if ref == key and table_exists(conn, sql_name):
            return sql_name
    return None


def validate_step_inputs(conn: DuckDBConnection, spec: StepSpec) -> tuple[bool, list[str]]:
    """Check whether StepSpec prerequisites exist in the current session."""
    from shared.db import get_meta, table_exists

    missing: list[str] = []
    for t in spec.required_tables:
        if t.startswith("table:"):
            tbl = t.split(":", 1)[1]
            if not table_exists(conn, tbl):
                missing.append(f"table:{tbl}")
            continue
        resolved = resolve_input_table(conn, t)
        if not resolved:
            missing.append(f"table_ref:{t}")
    for k in spec.required_meta:
        val = get_meta(conn, k)
        if val is None:
            missing.append(f"meta:{k}")
        elif isinstance(val, (list, dict, str)) and len(val) == 0:
            missing.append(f"meta:{k}")
        elif isinstance(val, bool) and not val:
            missing.append(f"meta:{k}")
    return (len(missing) == 0, missing)


def make_unique(columns: list[str | None]) -> list[str]:
    """Make column names unique, handling None / NaN / empty values."""
    seen: set[str] = set()
    result: list[str] = []
    for idx, col in enumerate(columns):
        if col is None or str(col).strip().lower() in ("", "nan", "none"):
            c = f"Unnamed_{idx}"
        else:
            c = str(col).strip()
            if not c or c.lower() in ("nan", "none"):
                c = f"Unnamed_{idx}"

        original = c
        i = 1
        while c in seen:
            c = f"{original}_{i}"
            i += 1
        seen.add(c)
        result.append(c)
    return result


def find_column(
    columns: list[str],
    keywords: list[str],
    ai_client: Any = None,
    model: str | None = None,
    ai_description: str | None = None,
) -> str | None:
    """Find a column by keyword matching, with optional AI fallback."""
    for kw in keywords:
        kw_low = kw.lower()
        for c in columns:
            if kw_low in str(c).lower():
                return c

    if not ai_client or not ai_description:
        return None

    prompt = (
        f"Analyze these column headers: {json.dumps(columns)}\n"
        f"Identify the single column that best represents **{ai_description}**.\n"
        f'Return JSON ONLY: {{ "target_column": "Exact Column Name" }} or {{ "target_column": null }}'
    )
    try:
        resp = ai_client.chat.completions.create(
            model=model or "@personal-openai/gpt-5.4",
            messages=[
                {"role": "system", "content": "Output JSON only."},
                {"role": "user", "content": prompt},
            ],
            response_format={"type": "json_object"},
        )
        target = json.loads(resp.choices[0].message.content).get("target_column")
        if target and target in columns:
            return target
    except Exception:
        pass
    return None
