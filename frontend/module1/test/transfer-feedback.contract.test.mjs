import test from "node:test";
import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";

const source = (relativePath) => readFile(new URL(`../${relativePath}`, import.meta.url), "utf8");

test("merge transfers preserve navigation while surfacing CSV fallback diagnostics", async () => {
  const panel = await source("src/components/module-1/MergeOutputsPanel.tsx");

  for (const snippet of [
    "interface TransferResponse",
    "transport?: string",
    "warning?: unknown",
    "warnings?: unknown",
    "function isCsvTransport",
    "CSV fallback (lossy)",
    "Transport: {sendResult.transport}",
    "setSendResult(transferSuccessMessage(\"Spend Summarizer\", data))",
    "setSendResult(transferSuccessMessage(\"Data Normalizer\", data))",
  ]) {
    assert.ok(panel.includes(snippet), `missing transfer feedback contract: ${snippet}`);
  }
});
