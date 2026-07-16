import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const source = (relativePath) => readFile(new URL(`../${relativePath}`, import.meta.url), "utf8");

test("all normalization operations render inline result, error, and cancellation feedback", async () => {
  const dashboard = await source("src/components/module-2/NormDashboard.tsx");

  for (const snippet of [
    'const operationResult = opResults[singleOp.id];',
    'const operationResultIsError = operationResult?.startsWith("Error") ?? false;',
    'const operationResultIsCancelled = /cancelled/i.test(operationResult || "");',
    '{operationResult && (',
    'role={operationResultIsError ? "alert" : "status"}',
    '{operationResult}',
    'new Set(["currency_conversion", "supplier_country", "region"])',
    '[agentId]: "Error: Select both a currency column and a spend column before running Currency Conversion."',
    '[agentId]: opLabel + " cancelled."',
  ]) assert.ok(dashboard.includes(snippet), `missing normalization feedback contract: ${snippet}`);

  assert.doesNotMatch(
    dashboard,
    /opResults\[singleOp\.id\][^\n]*singleOp\.id !== "currency_conversion"/,
    "currency-conversion feedback must not be filtered from the shared result renderer",
  );
  assert.doesNotMatch(
    dashboard,
    /opResults\[singleOp\.id\][^\n]*singleOp\.id !== "supplier_country"/,
    "supplier-country feedback must not be filtered from the shared result renderer",
  );
});

test("download feedback is shared across operation views with alert semantics", async () => {
  const dashboard = await source("src/components/module-2/NormDashboard.tsx");

  for (const snippet of [
    'const [downloadStatus, setDownloadStatus] = useState<{ ok: boolean; message: string } | null>(null);',
    'setDownloadStatus({ ok: false, message: "CSV download cancelled." });',
    'setDownloadStatus({ ok: true, message: "Download complete." });',
    'setDownloadStatus({ ok: false, message: "Download failed: " + err.message });',
    'role={downloadStatus.ok ? "status" : "alert"}',
    '{downloadStatus.message}',
  ]) assert.ok(dashboard.includes(snippet), `missing download feedback contract: ${snippet}`);

  const renderCount = [...dashboard.matchAll(/role=\{downloadStatus\.ok \? "status" : "alert"\}/g)].length;
  assert.equal(renderCount, 3, "download feedback must render in every normalization operation-view layout");
});
