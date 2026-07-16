/** @param {string} sourceColumn */
export function normalizeSourceColumn(sourceColumn) {
  return sourceColumn.trim().toLowerCase();
}

/**
 * @param {Record<string, string | null>} confirmedMapping
 * @param {Array<{ fieldKey: string, displayName: string }>} standardFields
 * @returns {Map<string, string[]>}
 */
export function buildMappedHeadersBySource(confirmedMapping, standardFields) {
  const mappedHeaders = new Map();

  for (const field of standardFields) {
    const sourceColumn = confirmedMapping[field.fieldKey]?.trim();
    if (!sourceColumn) continue;
    const canonicalSource = normalizeSourceColumn(sourceColumn);
    const destinations = mappedHeaders.get(canonicalSource) ?? [];
    destinations.push(field.displayName);
    mappedHeaders.set(canonicalSource, destinations);
  }

  return mappedHeaders;
}
