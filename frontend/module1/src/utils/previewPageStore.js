export const PREVIEW_PAGE_SIZE = 200;
export const MAX_PREVIEW_PAGES = 3;

export function previewQueryKey({ tableKey, search = "", filters = [], sort = [] }) {
  return JSON.stringify([tableKey, search, filters, sort]);
}

export function previewPageOffset(rowIndex, pageSize = PREVIEW_PAGE_SIZE) {
  return Math.floor(Math.max(0, rowIndex) / pageSize) * pageSize;
}

export function readPreviewRow(pages, rowIndex, pageSize = PREVIEW_PAGE_SIZE) {
  const offset = previewPageOffset(rowIndex, pageSize);
  return pages.get(offset)?.rows?.[rowIndex - offset];
}

export function storePreviewPage(pages, offset, page, focusOffset = offset, maxPages = MAX_PREVIEW_PAGES) {
  const next = new Map(pages);
  next.set(offset, page);
  if (next.size > maxPages) {
    const retained = [...next.keys()]
      .sort((a, b) => Math.abs(a - focusOffset) - Math.abs(b - focusOffset))
      .slice(0, maxPages);
    const keep = new Set(retained);
    for (const key of next.keys()) if (!keep.has(key)) next.delete(key);
  }
  return next;
}

export function preserveResolvedRowIds(existing, selectedIndices, resolveCachedId) {
  const resolved = new Map();
  const unresolved = [];
  for (const rowIndex of selectedIndices) {
    const existingId = existing.get(rowIndex);
    const rowId = existingId ?? resolveCachedId(rowIndex);
    if (rowId == null) unresolved.push(rowIndex);
    else resolved.set(rowIndex, rowId);
  }
  return { resolved, unresolved };
}
