import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const source = (path) => readFile(new URL(`../${path}`, import.meta.url), "utf8");

test("raw preview opens without eager inactive-table hydration", async () => {
  const app = await source("src/App.tsx");
  const handler = app.slice(app.indexOf("const openRawDataPreview"), app.indexOf("const fetchResultsPreviews"));
  assert.match(handler, /setShowDataPreview\(true\)/);
  assert.doesNotMatch(handler, /Promise\.all|fetchPreview|syncSessionFromBackend/);
});

test("overlay has bounded paging, cancellation, and nonblocking loading", async () => {
  const overlay = await source("src/components/module-1/ExcelPreviewOverlay.tsx");
  const api = await source("src/components/module-1/services/stitchingApi.ts");
  for (const snippet of ["pageCacheRef", "inFlightRef", "requestGenerationRef", "controller.abort()", "storePreviewPage", "readPreviewRow", "Loading rows in background...", "loading && pageCacheRef.current.size === 0", "consumeMutationView(result)"]) assert.ok(overlay.includes(snippet), `missing ${snippet}`);
  for (const snippet of ["previewColumnValues(sessionId, tableKey, column", "}, controller.signal)"]) assert.ok(overlay.includes(snippet), `missing ${snippet}`);
  for (const snippet of ["callerSignal?.addEventListener", "callerSignal?.removeEventListener", "PreviewStateResponse", "PreviewOperationResult"]) assert.ok(api.includes(snippet), `missing ${snippet}`);

  const fetchStart = overlay.indexOf("const fetchPreviewData");
  const fetchEnd = overlay.indexOf("// --- Operations ---", fetchStart);
  const fetchSource = overlay.slice(fetchStart, fetchEnd);
  for (const snippet of [
    "const requestKey = `${currentQueryKey}:${offset}`",
    "const duplicate = inFlightRef.current.get(requestKey)",
    "if (duplicate) {",
    "return duplicate.promise.then((result) => {",
    "const page = pageCacheRef.current.get(offset)",
  ]) assert.ok(fetchSource.includes(snippet), `missing dedupe contract: ${snippet}`);

  const successStart = fetchSource.indexOf(".then((result) => {");
  const successEnd = fetchSource.indexOf("}).catch", successStart);
  const successSource = fetchSource.slice(successStart, successEnd);
  assert.match(
    successSource,
    /viewRequest\s*===\s*latestViewRequestRef\.current/,
    "successful reads must reject older same-query page responses before updating the visible view",
  );
});

test("query resets revalidate snapshots and mutations invalidate reads first", async () => {
  const overlay = await source("src/components/module-1/ExcelPreviewOverlay.tsx");
  assert.match(overlay, /if \(!mayRetainInitialSnapshot\) \{\s*pageCacheRef\.current = new Map\(\)/);
  assert.match(overlay, /fetchPreviewData\(0, \{ force: true \}\)/);
  assert.match(overlay, /const invalidateInFlightReads = useCallback[\s\S]*requestGenerationRef\.current \+= 1[\s\S]*request\.controller\.abort\(\)/);
  assert.match(overlay, /invalidateInFlightReads\(\);\s*const result = await previewOperation/);
});

test("foreground reads abort stale work and bound prefetch to the focused window", async () => {
  const overlay = await source("src/components/module-1/ExcelPreviewOverlay.tsx");
  for (const snippet of [
    "foregroundRequestKeyRef",
    "focusOffsetRef",
    "const staleForeground = key === foregroundRequestKeyRef.current && key !== requestKey",
    "const outsideWindow = request.prefetch && Math.abs(request.offset - offset) > PAGE_SIZE",
    "if (Math.abs(offset - focusOffsetRef.current) > PAGE_SIZE) return null",
    "storePreviewPage(pageCacheRef.current, offset, page, focusOffsetRef.current)",
    "inFlightRef.current.set(requestKey, { controller, promise, offset, prefetch: !!options.prefetch })",
    "if (previous >= 0) void fetchPreviewData(previous, { prefetch: true })",
  ]) assert.ok(overlay.includes(snippet), `missing bounded request-window contract: ${snippet}`);
});

test("cross-page selection survives eviction and deletion requires every row ID", async () => {
  const overlay = await source("src/components/module-1/ExcelPreviewOverlay.tsx");
  for (const snippet of [
    "preserveResolvedRowIds(",
    "selectionReadRef.current?.controller.abort()",
    "const missingByPage = new Map<number, number[]>()",
    "for (const [page, indices] of missingByPage)",
    "await previewState(sessionId, activeKey",
    "resolved.size !== selectedIndices.length",
    "selectedRowIds.size !== selectedRowCount || rowIds.length !== selectedRowCount",
    "Reselect unresolved rows and try again.",
  ]) assert.ok(overlay.includes(snippet), `missing cross-page selection contract: ${snippet}`);
  assert.doesNotMatch(
    overlay.slice(overlay.indexOf("const handleGridSelectionChange"), overlay.indexOf("const handleDeleteSelectedRows")),
    /fetchPreviewData\(page, \{ prefetch: true \}\)/,
  );
});

test("deterministic DQA client errors are not retried", async () => {
  const dqa = await source("src/components/module-1/DataQualityAssessment.tsx");
  const api = await source("src/components/module-1/services/stitchingApi.ts");
  assert.match(dqa, /deterministicClientError/);
  assert.doesNotMatch(dqa, /deterministicClientError[\s\S]{0,160}ERROR_CODE_TABLE_MISSING/);
  assert.match(api, /\(err as any\)\.status = res\.status/);
});

test("overlay chrome uses separate identity and wrapping action rows", async () => {
  const overlay = await source("src/components/module-1/ExcelPreviewOverlay.tsx");
  for (const snippet of [
    "Identity header and responsive action row",
    "h-16 min-w-0",
    "flex flex-wrap items-center",
    "gap-x-3 gap-y-2",
    "inline-flex h-9 items-center justify-center",
    "min-w-0 flex-[1_1_16rem]",
    "h-9 w-9",
    'aria-label="Close preview"',
  ]) assert.ok(overlay.includes(snippet), `missing ${snippet}`);
});

test("calculated-column dialog keeps the name-based Module 1 contract", async () => {
  const overlay = await source("src/components/module-1/ExcelPreviewOverlay.tsx");
  const start = overlay.indexOf("function CalcColumnDialog");
  const end = overlay.indexOf("function looksNumericColumn", start);
  const dialog = overlay.slice(start, end);

  for (const snippet of [
    '<Dialog title="Add Calculated Column" onClose={onClose} wide>',
    "md:grid-cols-[minmax(0,1fr)_220px]",
    "rows={7}",
    "max-h-64 space-y-1 overflow-y-auto",
    "name.trim().length > 0",
    "expression.trim().length > 0",
    "await onApply({ name: name.trim(), expression: expression.trim(), dataType })",
    'setExpression((current) => `${current}${current ? " " : ""}[${column}]`)',
    'title={`Insert [${column}]`}',
    'role="alert"',
    'submitting ? "Adding..." : "Add Column"',
  ]) assert.ok(dialog.includes(snippet), `missing calculated-column contract: ${snippet}`);

  assert.doesNotMatch(dialog, /\bCOL_\d+\b/, "Module 1 must insert display names, not Module 3 stable keys");
  assert.doesNotMatch(dialog, /\bcolumnKey\b/, "Module 1 calculated-column payload must remain name based");
});

test("calculated-column close paths are accessible and do not submit", async () => {
  const overlay = await source("src/components/module-1/ExcelPreviewOverlay.tsx");
  const start = overlay.indexOf("function Dialog(");
  const dialog = overlay.slice(start);

  for (const snippet of [
    'role="dialog"',
    'aria-modal="true"',
    "restoreFocusRef.current = document.activeElement",
    "closeRef.current?.focus()",
    "return () => restoreFocusRef.current?.focus()",
    'if (event.key === "Escape")',
    "event.preventDefault()",
    "event.stopPropagation()",
    'if (event.key !== "Tab"',
    "onMouseDown={onClose}",
    "onMouseDown={(event) => event.stopPropagation()}",
    'aria-label={`Close ${title}`}',
  ]) assert.ok(dialog.includes(snippet), `missing dialog accessibility contract: ${snippet}`);

  const calcStart = overlay.indexOf("function CalcColumnDialog");
  const calcEnd = overlay.indexOf("function looksNumericColumn", calcStart);
  const calc = overlay.slice(calcStart, calcEnd);
  assert.match(calc, /<button type="button" onClick=\{onClose\} disabled=\{submitting\}[^>]*>Cancel<\/button>/);
});

test("dialog Escape closes only the nested dialog and restores its opener", async () => {
  const overlay = await source("src/components/module-1/ExcelPreviewOverlay.tsx");
  const overlayShortcutStart = overlay.indexOf("// Keyboard shortcuts");
  const overlayShortcutEnd = overlay.indexOf("// --- Data Fetching ---", overlayShortcutStart);
  const overlayShortcuts = overlay.slice(overlayShortcutStart, overlayShortcutEnd);
  const dialogStart = overlay.indexOf("function Dialog(");
  const dialog = overlay.slice(dialogStart);

  assert.match(
    overlayShortcuts,
    /e\.key === "Escape" && !e\.defaultPrevented[\s\S]*onClose\(\)/,
    "the preview overlay must remain open after a nested dialog consumes Escape",
  );
  assert.match(
    dialog,
    /if \(event\.key === "Escape"\) \{\s*event\.preventDefault\(\);\s*event\.stopPropagation\(\);\s*onClose\(\);\s*return;/,
    "the nested dialog must consume Escape before closing itself",
  );
  assert.match(
    dialog,
    /restoreFocusRef\.current = document\.activeElement as HTMLElement \| null;\s*closeRef\.current\?\.focus\(\);\s*return \(\) => restoreFocusRef\.current\?\.focus\(\);/,
    "closing the nested dialog must restore focus to its opener",
  );
});

test("calculated-column operation closes only after its unchanged operation succeeds", async () => {
  const overlay = await source("src/components/module-1/ExcelPreviewOverlay.tsx");
  const start = overlay.indexOf("const handleAddCalcColumn");
  const end = overlay.indexOf("// --- Filter ---", start);
  const handler = overlay.slice(start, end);

  assert.match(handler, /await executeOperation\("calculated_column", config as unknown as Record<string, unknown>\);\s*setShowCalcDialog\(false\);/);
});
