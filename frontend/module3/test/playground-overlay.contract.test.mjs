import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import {
  buildMappedHeadersBySource,
  normalizeSourceColumn,
} from "../src/components/spend_quality_assessment/mappingIndicators.js";

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
    'Drag fields from Available Fields to Row Fields, Column Fields, or Values areas.',
    'Drag fields to the areas on the right',
    'className="grid grid-cols-2 gap-4"',
    'const pivotColumns = useMemo(() => columns.filter(isPivotDataColumn), [columns]);',
    'function isPivotDataColumn(column: PreviewColumn)',
    'const validColumnKeys = useMemo(() => new Set(pivotColumns.map((column) => column.key)), [pivotColumns]);',
    'if (!validColumnKeys.has(column.key)) return;',
    'rowFields: validRowFields,',
    'columnFields: validColumnFields,',
    'valueFields: validValueFields,',
    'columnKey: column.key, aggregation: defaultAggregationForColumn(column)',
    'const canSubmit = validValueFields.length > 0 && (validRowFields.length > 0 || validColumnFields.length === 0);',
    'rowFields: string[];',
    'columnFields: string[];',
    'valueFields: PreviewPivotValueField[];',
    'await onCreate({',
    'NUMBER_ONLY_AGGREGATIONS.has(aggregation)',
    '" (number only)"',
    'const MAX_PIVOT_ROW_FIELDS = 5;',
    'const MAX_PIVOT_VALUE_FIELDS = 5;',
    'current.length >= MAX_PIVOT_ROW_FIELDS',
    'current.length >= MAX_PIVOT_VALUE_FIELDS',
    'submitInFlightRef.current',
    'if (!canSubmit || submitting || submitInFlightRef.current) return;',
    'submitAttempted && error',
    'role="alert"',
    'disabled={!canSubmit || submitting}',
    'Drop fields here for rows',
    'Drop fields here for columns',
    'Drop fields here for values',
  ]);

  assert.doesNotMatch(overlay, /\bfield:\s*column\.displayName\b/, "Pivot values must not send display names");
  assert.doesNotMatch(overlay, /\bcolumnName:\s*column\./, "Operations must use columnKey, not columnName");
  assert.doesNotMatch(overlay, /checked=\{selected\.includes\(column\.key\)\}/, "Pivot creation should not fall back to checkbox-only field selection");
  assert.doesNotMatch(overlay, /onClick=\{\(\) => addToZone\(column, "(?:rows|columns|values)"\)\}/, "Pivot fields must stay drag-only like Module 1");
  assert.doesNotMatch(overlay, /\{columns\.length\} fields/, "Available fields must not show the Module 3-only count decoration");
  assert.doesNotMatch(overlay, /use the field buttons/, "Pivot empty states must not mention removed quick-add controls");
  assert.doesNotMatch(overlay, /lg:grid-cols-2/, "Pivot fields and zones must remain a two-column grid at every viewport");
});

test("pivot result activates the returned inventory table and dialogs close only on success", async () => {
  const overlay = await source("src/components/playground/ExcelPreviewOverlay.tsx");

  expectSnippets(overlay, [
    "if (result.newTableKey) {",
    "pendingTableKeyRef.current = result.newTableKey;",
    "await refreshInventory(false);",
    "setActiveTableKey(result.newTableKey);",
    "applyStateFromOperation(result);",
    "if (result) setShowPivotDialog(false);",
  ]);
  assert.match(
    overlay,
    /const result = await runOperation\("pivot", params\);\s*if \(result\) setShowPivotDialog\(false\);/,
  );
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

test("confirmed mappings create case-insensitive ordered fill-rate indicators", () => {
  const standardFields = [
    { fieldKey: "supplier", displayName: "Supplier Name" },
    { fieldKey: "vendor", displayName: "Vendor Name" },
    { fieldKey: "spend", displayName: "Total Spend" },
  ];
  const confirmedMapping = {
    supplier: "  Source Vendor ",
    vendor: "source vendor",
    spend: null,
  };

  const index = buildMappedHeadersBySource(confirmedMapping, standardFields);

  assert.deepEqual(index.get("source vendor"), ["Supplier Name", "Vendor Name"]);
  assert.equal(index.has("total spend"), false);
  assert.equal(index.get("SOURCE VENDOR".toLocaleLowerCase())?.length, 2);
});

test("mapping keys normalize INVOICE DATE and Invoice Date identically", () => {
  const index = buildMappedHeadersBySource(
    { invoice_date: "INVOICE DATE" },
    [{ fieldKey: "invoice_date", displayName: "Invoice Date" }],
  );

  assert.deepEqual(index.get(normalizeSourceColumn("Invoice Date")), ["Invoice Date"]);
});

test("mapping, Mekko, and inventory UI retain their accessibility contracts", async () => {
  const [summary, mekko, overlay] = await Promise.all([
    source("src/components/spend_quality_assessment/ExecutiveSummary.tsx"),
    source("src/components/dashboard/charts/MekkoChart.tsx"),
    source("src/components/playground/ExcelPreviewOverlay.tsx"),
  ]);

  expectSnippets(summary, [
    "buildMappedHeadersBySource(confirmedMapping, standardFields)",
    "normalizeSourceColumn(col.sourceColumn)",
    'aria-hidden="true"',
    'title={destinations.join(", ")}',
    'text-red-600 dark:text-red-400',
  ]);
  assert.doesNotMatch(summary, /className="max-w-full break-words text-violet-700 dark:text-violet-300"/);
  expectSnippets(mekko, [
    '.on("mouseover", showTooltip)',
    '.on("focus", () => {',
    '.attr("tabindex", 0)',
    '"aria-label"',
  ]);
  rejectSnippets(mekko, ['g.append("text")']);
  expectSnippets(overlay, [
    "const [sidebarExpanded, setSidebarExpanded] = useState(true);",
    'aria-expanded={sidebarExpanded}',
    'aria-controls="preview-table-inventory"',
    'id="preview-table-inventory"',
    'hidden={!sidebarExpanded}',
    'sidebarExpanded ? "Collapse table inventory" : "Expand table inventory"',
    '<ChevronLeft className="h-5 w-5" aria-hidden="true" />',
    '<ChevronRight className="h-5 w-5" aria-hidden="true" />',
    'window.requestAnimationFrame(() => columnVirtualizer.measure())',
  ]);
  rejectSnippets(overlay, ["Hide tables", "Show tables", "PanelLeftClose", "PanelLeftOpen"]);
});
