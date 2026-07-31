import assert from "node:assert/strict";
import { readFile } from "node:fs/promises";
import path from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const root = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "..");

async function source(relativePath) {
  return readFile(path.join(root, relativePath), "utf8");
}

test("complete analysis export requests the XLSX endpoint and exposes the Dashboard action", async () => {
  const [client, dashboard, app] = await Promise.all([
    source("src/api/client.ts"),
    source("src/components/dashboard/Dashboard.tsx"),
    source("src/App.tsx"),
  ]);

  assert.match(client, /export async function exportCompleteAnalysis\(sessionId: string\)/);
  assert.match(client, /\/export\/xlsx\/complete-analysis/);
  assert.match(client, /body: JSON\.stringify\(\{ sessionId \}\)/);
  assert.match(client, /new ApiClientError\(/);
  assert.match(client, /getDownloadFilename\(res\.headers\.get\("Content-Disposition"\)\)/);

  assert.match(dashboard, /onExportCompleteAnalysis\?: \(\) => Promise<void>;/);
  assert.match(dashboard, /disabled=\{exporting \|\| views\.length === 0\}/);
  assert.match(dashboard, /Preparing Excel/);
  assert.match(dashboard, /role="alert"/);
  assert.match(dashboard, /Export complete analysis/);

  assert.match(app, /const handleExportCompleteAnalysis = useCallback\(async \(\) => \{/);
  assert.match(app, /exportCompleteAnalysis\(sessionId\)/);
  assert.match(app, /filename \|\| "spend-quality-and-views\.xlsx"/);
  assert.match(app, /onExportCompleteAnalysis=\{handleExportCompleteAnalysis\}/);
});
