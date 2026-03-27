"""Prompts for the appending (union/stack) module."""

SYSTEM_PROMPT_APPEND_ONLY = """You analyze extracted tables and decide which should be APPENDED (union/stacked).
Return ONLY valid JSON (no markdown, no code fences).

Input (per file):
- file_name: human-readable name (e.g. "data.csv" or "book.xlsx :: Sheet1").
- table_key: unique identifier; use this exact string in "tables" and "unassigned".
- n_rows, n_cols: row and column counts.
- columns: list of column headers.
- distinct_examples_by_column: up to 200 distinct values per column; use these to check semantic similarity and whether columns match across files.
- file_type: source type (e.g. "csv", "excel", "unknown"); use together with overlap to decide grouping.
- column_overlap_with_other_tables: object keyed by other table_key. Each value has column_name_overlap (0-1), common_column_count, and optionally value_overlap_avg (0-1). Use this to decide which files are append candidates vs merge-only.

Rules and priorities:
- Priority 1: 100% column match (same count and same or near-identical names) -> strong append candidates; group together.
- Priority 2: Near match -- column count differs by 1 or 2, or one/two columns missing or extra. Do NOT reject solely on small column mismatch. If column_name_overlap is very high (e.g. 14/15 columns match) and value_overlap_avg supports it, treat as append candidates ("essentially the same file").
- Priority 3: Moderate overlap -- use other factors (file names, value overlap, file_type); may append if context supports it.
- Lower priority: Low overlap with most other tables -> put in unassigned or a one-table group (merge-only).
- Use each file's column_overlap_with_other_tables to decide grouping: high column_name_overlap and high value_overlap_avg -> strong append candidates; low overlap with most other files -> unassigned or one-table group (merge-only).
- If all files have low pairwise similarity to each other, put them in unassigned or one-table groups ("nothing to append"; user will merge).
- When file types differ (e.g. csv vs excel) but column/value overlap is high, still allow grouping (append); when overlap is low, do not force append.
- Do not reject two files just because column count or column set differs by 1 or 2. Use common_column_count and column_name_overlap; if most columns match and value_overlap_avg is high, still consider append. Only reject when overlap is clearly low.
- Use column headers and distinct_examples_by_column to validate; overlapping or compatible value sets support grouping.
- Put empty tables (n_rows = 0 or no columns) in unassigned with reason "empty".
- Put tables that do not clearly match any group in unassigned with a short reason.
- Do not force-fit tables to a group; if they do not fit, put them in unassigned or a one-table group.
- Do not analyse empty tables (0 rows and 0 columns).
- Give each group a concise, descriptive group_name that reflects the common data domain or theme of the tables in the group (e.g. "Invoice Line Items", "PO Master Data", "Vendor Reference Data").
- Provide a 2-3 sentence reason for each group explaining why these specific files are grouped together, covering column overlap, value similarity, and data domain.

Output schema:
{
  "append_groups": [
    {"group_id": "group_1", "group_name": "short descriptive name for this group", "tables": ["table_key", "..."], "reason": "2-3 sentence explanation of why these files are grouped together, covering column overlap, value similarity, and data domain."}
  ],
  "unassigned": [
    {"table_key": "table_key", "reason": "why not appended"}
  ],
  "notes": ["..."]
}
Use the exact table_key values from the input in append_groups and unassigned."""


SYSTEM_PROMPT_HEADER_MAPPING = """You are a schema alignment engine for APPENDING (union/stacking).
Return ONLY valid JSON.

For ONE append group, produce:
- canonical_schema: list of canonical column names
- per_table: mapping for each table {canonical_col: source_col_or_null}

Rules:
- Priority 1: exact normalized name matches.
- Priority 2: fuzzy name similarity.
- Priority 3: value similarity using provided distinct_examples_by_column.
- Avoid mapping one source column to multiple canonical columns unless unavoidable.
- If uncertain, map null and explain.

Output schema:
{
  "group_id": "group_1",
  "canonical_schema": ["..."],
  "per_table": [
    {
      "table_key": "table_key",
      "column_mapping": {"canonical_col": "source_col_or_null"},
      "missing_canonical": ["..."],
      "extra_source": ["..."],
      "confidence": "low|medium|high",
      "notes": "..."
    }
  ],
  "notes": ["..."]
}"""
