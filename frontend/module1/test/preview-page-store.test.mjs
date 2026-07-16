import test from "node:test";
import assert from "node:assert/strict";
import { preserveResolvedRowIds, previewPageOffset, previewQueryKey, readPreviewRow, storePreviewPage } from "../src/utils/previewPageStore.js";

test("maps absolute rows to exact page and local index", () => {
  const pages = new Map([
    [0, { rows: Array.from({ length: 200 }, (_, id) => ({ id })) }],
    [200, { rows: Array.from({ length: 200 }, (_, index) => ({ id: 200 + index })) }],
    [400, { rows: [{ id: 400 }] }],
  ]);
  assert.equal(previewPageOffset(199), 0);
  assert.equal(previewPageOffset(200), 200);
  assert.equal(previewPageOffset(399), 200);
  assert.equal(previewPageOffset(400), 400);
  assert.deepEqual(readPreviewRow(pages, 199), { id: 199 });
  assert.deepEqual(readPreviewRow(pages, 200), { id: 200 });
  assert.deepEqual(readPreviewRow(pages, 201), { id: 201 });
  assert.deepEqual(readPreviewRow(pages, 399), { id: 399 });
  assert.deepEqual(readPreviewRow(pages, 400), { id: 400 });
});

test("bounds the cache around the focused page", () => {
  let pages = new Map();
  for (const offset of [0, 200, 400, 600]) pages = storePreviewPage(pages, offset, { rows: [] }, 400);
  assert.equal(pages.size, 3);
  assert.deepEqual([...pages.keys()].sort((a, b) => a - b), [200, 400, 600]);
});

test("keeps exact previous/current/next keys across forward, back, and mutation windows", () => {
  let pages = new Map([[0, { rows: [] }], [200, { rows: [] }], [400, { rows: [] }]]);
  pages = storePreviewPage(pages, 600, { rows: [] }, 400);
  assert.deepEqual([...pages.keys()].sort((a, b) => a - b), [200, 400, 600]);

  pages = storePreviewPage(pages, 0, { rows: [] }, 200);
  assert.deepEqual([...pages.keys()].sort((a, b) => a - b), [0, 200, 400]);

  pages = storePreviewPage(new Map(), 400, { rows: [] }, 400);
  pages = storePreviewPage(pages, 200, { rows: [] }, 400);
  pages = storePreviewPage(pages, 600, { rows: [] }, 400);
  assert.deepEqual([...pages.keys()].sort((a, b) => a - b), [200, 400, 600]);
});

test("query keys isolate table, search, filter, and sort views", () => {
  const base = { tableKey: "a", search: "", filters: [], sort: [] };
  assert.notEqual(previewQueryKey(base), previewQueryKey({ ...base, tableKey: "b" }));
  assert.notEqual(previewQueryKey(base), previewQueryKey({ ...base, search: "x" }));
  assert.notEqual(
    previewQueryKey(base),
    previewQueryKey({ ...base, filters: [{ column: "Status", op: "eq", value: "Open" }] }),
  );
  assert.notEqual(
    previewQueryKey(base),
    previewQueryKey({ ...base, sort: [{ column: "Amount", dir: "desc" }] }),
  );
});

test("preserves selected row IDs after their pages are evicted", () => {
  const existing = new Map([[5, 1005], [205, 1205]]);
  const cachedIds = new Map([[405, 1405]]);
  const { resolved, unresolved } = preserveResolvedRowIds(
    existing,
    [5, 205, 405, 605],
    (rowIndex) => cachedIds.get(rowIndex),
  );
  assert.deepEqual([...resolved], [[5, 1005], [205, 1205], [405, 1405]]);
  assert.deepEqual(unresolved, [605]);
});
