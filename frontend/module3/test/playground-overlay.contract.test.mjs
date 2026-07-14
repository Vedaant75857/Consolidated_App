import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

async function source(relativePath) {
  return readFile(path.join(root, relativePath), "utf8");
}

function expectSnippets(text, snippets) {
  for (const snippet of snippets) {
    assert.ok(text.includes(snippet), `Expected source to include: ${snippet}`);
  }
}

function rejectSnippets(text, snippets) {
  for (const snippet of snippets) {
    assert.ok(!text.includes(snippet), `Expected source not to include: ${snippet}`);
  }
}

test("App exposes the top Raw Data Preview icon and opens the overlay", async () => {
  const app = await source("src/App.tsx");

  expectSnippets(app, [
    'Table2,',
    'const [showDataPreview, setShowDataPreview] = useState(false);',
    'const openRawDataPreview = useCallback(() => {',
    'if (!sessionId || inventory.length === 0) return;',
    'setShowDataPreview(true);',
    'sessionId && inventory.length > 0',
    'onClick={openRawDataPreview}',
    'title="Raw Data Preview"',
    'aria-label="Raw Data Preview"',
    '<ExcelPreviewOverlay',
    'sessionId={sessionId}',
    'inventory={inventory}',
    'onClose={() => setShowDataPreview(false)}',
    'title="Raw Data Preview"',
  ]);
});

test("overlay renders the expected top-level controls without Apply UI", async () => {
  const overlay = await source("src/components/playground/ExcelPreviewOverlay.tsx");

  expectSnippets(overlay, [
    'import { useVirtualizer } from "@tanstack/react-virtual";',
    'role="dialog"',
    'aria-modal="true"',
    'role="tablist"',
    'aria-label="Preview tables"',
    'placeholder="Search rows"',
    'aria-label={`Filter ${column.displayName}`}',
    'aria-pressed={hasFilter}',
    'Clear all',
    'Blank values',
    'label={`Undo${state?.undoDepth ? ` (${state.undoDepth})` : ""}`}',
    'label={`Redo${state?.redoDepth ? ` (${state.redoDepth})` : ""}`}',
    'aria-label="Refresh inventory"',
    "Showing {visibleStartRow.toLocaleString()}-{visibleEndRow.toLocaleString()} of {totalRows.toLocaleString()} rows",
    '<CalculatedColumnDialog',
    '<PivotDialog',
  ]);

  rejectSnippets(overlay, [
    'POST /api/preview/apply',
    '"/preview/apply"',
    'Apply to',
    'Apply changes',
    'apply target',
    'applyTarget',
    'applyTo',
    'merge_group',
    'merge_version',
    'aria-label="Previous page"',
    'aria-label="Next page"',
  ]);
  assert.doesNotMatch(overlay, />\s*Apply\s*</, "Overlay must not render an Apply button");
});

test("overlay uses virtualized giant scrolling over backend chunks", async () => {
  const overlay = await source("src/components/playground/ExcelPreviewOverlay.tsx");

  expectSnippets(overlay, [
    "const CHUNK_SIZE = 1000;",
    "const MAX_CACHED_CHUNKS = CACHE_CHUNK_RADIUS * 2 + 3;",
    "const rowVirtualizer = useVirtualizer({",
    "const columnVirtualizer = useVirtualizer({",
    "horizontal: true,",
    "getScrollElement: () => gridScrollRef.current,",
    "const [chunkRows, setChunkRows] = useState<Map<number, PreviewRow[]>>(() => new Map());",
    "const loadedChunksRef = useRef<Set<number>>(new Set());",
    "const loadingChunksRef = useRef<Set<number>>(new Set());",
    "const chunkControllersRef = useRef<Map<number, AbortController>>(new Map());",
    "abortChunkRequests();",
    "pruneChunkCache(next, responseOffset, result.totalRows);",
    "offset: chunkOffset,",
    "limit: CHUNK_SIZE,",
    "{ signal: controller.signal }",
    "for (const chunkOffset of chunkOffsetsForRange(start, end, totalRows)) {",
    "void loadChunk(chunkOffset);",
    "virtualColumnItems.map(({ column, virtualColumn })",
    'aria-label="Select all rendered rows"',
    "visibleLoadedRows.forEach((row) => next.add(row.__row_id));",
    "Loading row",
  ]);

  rejectSnippets(overlay, [
    "const PAGE_SIZE = 100;",
    "setOffset(",
    "togglePageSelection",
    "selectedPageRows",
    "rowsByIndex",
  ]);
});

test("API client wrappers post only the supported preview endpoints", async () => {
  const client = await source("src/api/client.ts");

  expectSnippets(client, [
    'export class ApiClientError extends Error {',
    'code?: string;',
    'details?: Record<string, unknown>;',
    'options?: { signal?: AbortSignal }',
    'return post<PreviewTableState>("/preview/state", request, DEFAULT_TIMEOUT_MS, options?.signal);',
    '"/preview/column-values",',
    'options?.signal,',
    'return post<PreviewOperationResult>("/preview/operation", request);',
    'return post<PreviewOperationResult>("/preview/undo", request);',
    'return post<PreviewOperationResult>("/preview/redo", request);',
    'return post<PreviewRefreshInventoryResult>("/preview/refresh-inventory", request);',
  ]);

  rejectSnippets(client, ['"/preview/apply"', "'/preview/apply'"]);
});

test("filter UI uses stable column keys and handles blanks and clear-all", async () => {
  const overlay = await source("src/components/playground/ExcelPreviewOverlay.tsx");

  expectSnippets(overlay, [
    'const clearFilters = () => {',
    'setFilters([]);',
    'const removeFilter = (columnKey: string) => {',
    'current.filter((filter) => filter.columnKey !== columnKey)',
    'filters: excludeColumnFilter(filters, column.key)',
    'columnKey: column.key',
    'aria-pressed={op === "in"}',
    'aria-pressed={op === "not_in"}',
    'const selectAllVisible = () => {',
    'values.forEach((value) => next.add(value));',
    'const hasMoreDistinctValues = totalDistinct > values.length;',
    '{hasMoreDistinctValues ? "Select loaded" : "Select All"}',
    'Select loaded only applies to loaded values.',
    'Select All',
    'if (selected.size === 0 && !includeBlanks) {',
    'onSetFilter(null);',
    'includeBlanks,',
    'checked={includeBlanks}',
    'setIncludeBlanks(event.target.checked)',
    'setIncludeBlanks(false);',
    'OK',
  ]);
});

test("operation wiring sends stable-key payloads and Module 1-like pivot zones", async () => {
  const overlay = await source("src/components/playground/ExcelPreviewOverlay.tsx");

  expectSnippets(overlay, [
    'await runOperation("cell_edit", payload);',
    'rowId: editingCell.rowId',
    'columnKey: editingCell.columnKey',
    'onClick={() => runOperation("rows_delete", { rowIds: [...selectedRows] })}',
    'runOperation("column_rename", { columnKey: columnMenu.columnKey, displayName });',
    'runOperation("column_delete", { columnKey: columnMenu.columnKey });',
    'runOperation("column_change_type", { columnKey: columnMenu.columnKey, newType });',
    'runOperation("column_reorder", { columnKeys: next });',
    'const result = await runOperation("calculated_column", params);',
    'const result = await runOperation("pivot", params);',
    'Available Fields',
    'title="Row Fields"',
    'title="Column Fields"',
    'title="Values"',
    'draggable',
    'onDragStart={(event) => handleDragStart(event, column.key)}',
    'onDrop={(event) => onDrop(event, zone)}',
    'onClick={() => addToZone(column, "rows")}',
    'onClick={() => addToZone(column, "columns")}',
    'onClick={() => addToZone(column, "values")}',
    'columnKey: column.key, aggregation: defaultAggregationForColumn(column)',
    'const canSubmit = valueFields.length > 0 && (rowFields.length > 0 || columnFields.length === 0);',
    'rowFields: string[];',
    'columnFields: string[];',
    'valueFields: PreviewPivotValueField[];',
    'onCreate({ rowFields, columnFields, valueFields })',
    'NUMBER_ONLY_AGGREGATIONS.has(aggregation)',
    '" (number only)"',
  ]);

  assert.doesNotMatch(overlay, /\bfield:\s*column\.displayName\b/, "Pivot values must not send display names");
  assert.doesNotMatch(overlay, /\bcolumnName:\s*column\./, "Operations must use columnKey, not columnName");
  assert.doesNotMatch(overlay, /checked=\{selected\.includes\(column\.key\)\}/, "Pivot creation should not fall back to checkbox-only field selection");
});

test("preview operation types stay stable-key based", async () => {
  const types = await source("src/types/excelPreview.ts");

  expectSnippets(types, [
    "export interface PreviewFilter {",
    "columnKey: string;",
    "includeBlanks?: boolean;",
    "tableVersion: number;",
    "sourceTableKey?: string | null;",
    "playgroundTableName?: string | null;",
    "dropped?: boolean;",
    "export interface PreviewCellEditParams {",
    "rowId: string | number;",
    "export interface PreviewColumnRenameParams {",
    "displayName: string;",
    "export interface PreviewColumnReorderParams {",
    "columnKeys: string[];",
    "export interface PreviewPivotValueField {",
    "columnKey: string;",
  ]);

  rejectSnippets(types, [
    "columnName:",
    "fieldName:",
    "displayName: string[]",
  ]);
});
