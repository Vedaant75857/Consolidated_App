/**
 * Reserved append-provenance names are matched exactly after removing separators.
 * Deliberately do not match business fields such as `source_system` or
 * `Data Source System`.
 */
export function isReservedProvenanceColumn(value: unknown): boolean {
  return typeof value === "string"
    && value.trim().replace(/[^a-z0-9]+/gi, "").toLowerCase() === "sourcetable";
}

export function sanitizeReservedColumns(columns: unknown): string[] {
  if (!Array.isArray(columns)) return [];
  return columns
    .map((column) => String(column))
    .filter((column) => !isReservedProvenanceColumn(column));
}

export function sanitizeReservedRow(row: unknown): Record<string, unknown> {
  if (!row || typeof row !== "object" || Array.isArray(row)) return {};
  return Object.fromEntries(
    Object.entries(row).filter(([column]) => !isReservedProvenanceColumn(column)),
  );
}

export function sanitizeReservedRows(rows: unknown): Record<string, unknown>[] {
  return Array.isArray(rows) ? rows.map(sanitizeReservedRow) : [];
}

export function sanitizePreviewDto<T extends Record<string, any>>(preview: T): T {
  if (!preview || typeof preview !== "object") return preview;
  const next: Record<string, any> = { ...preview };
  if ("columns" in preview) next.columns = sanitizeReservedColumns(preview.columns);
  if ("rows" in preview) next.rows = sanitizeReservedRows(preview.rows);
  if (preview.columnTypes && typeof preview.columnTypes === "object") {
    next.columnTypes = Object.fromEntries(
      Object.entries(preview.columnTypes).filter(([column]) => !isReservedProvenanceColumn(column)),
    );
  }
  return next as T;
}

export function sanitizePreviewMap<T extends Record<string, any>>(previews: T): T {
  if (!previews || typeof previews !== "object") return previews;
  return Object.fromEntries(
    Object.entries(previews).map(([key, preview]) => [key, sanitizePreviewDto(preview)]),
  ) as T;
}

export function pruneReservedColumnTargets<T extends { column: string }>(items: T[]): T[] {
  return items.filter((item) => !isReservedProvenanceColumn(item.column));
}
