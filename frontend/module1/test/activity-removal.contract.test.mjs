import test from "node:test";
import assert from "node:assert/strict";
import { readdir, readFile } from "node:fs/promises";
import path from "node:path";
import { fileURLToPath } from "node:url";

const repositoryRoot = path.resolve(path.dirname(fileURLToPath(import.meta.url)), "../../..");
const sourceRoots = [
  "frontend/landing/src",
  "frontend/module1/src",
  "frontend/module2/src",
  "frontend/module3/src",
  "backend/module1",
  "backend/module2",
  "backend/module3",
];
const sourceExtensions = new Set([".js", ".jsx", ".mjs", ".ts", ".tsx", ".py"]);
const excludedDirectories = new Set(["__pycache__", ".pytest_cache", "node_modules"]);

const forbiddenActivityTokens = [
  ["StatusLog", /\bStatusLog\b/g],
  ["LogEntry", /\bLogEntry\b/g],
  ["statusLog", /\bstatusLog\b/g],
  ["logIdRef", /\blogIdRef\b/g],
  ["addLog", /\baddLog\b/g],
  ["Live pipeline activity", /Live\s+pipeline\s+activity/gi],
];

async function collectSourceFiles(directory) {
  const entries = await readdir(directory, { withFileTypes: true });
  const files = [];
  for (const entry of entries) {
    if (entry.name.startsWith(".") || excludedDirectories.has(entry.name)) continue;
    const absolute = path.join(directory, entry.name);
    if (entry.isDirectory()) files.push(...await collectSourceFiles(absolute));
    else if (sourceExtensions.has(path.extname(entry.name))) files.push(absolute);
  }
  return files;
}

function findForbiddenActivityTokens(text) {
  return forbiddenActivityTokens
    .filter(([, pattern]) => {
      pattern.lastIndex = 0;
      return pattern.test(text);
    })
    .map(([label]) => label);
}

test("Live pipeline activity feature tokens are absent from every application source root", async () => {
  const failures = [];
  for (const relativeRoot of sourceRoots) {
    const absoluteRoot = path.join(repositoryRoot, relativeRoot);
    for (const file of await collectSourceFiles(absoluteRoot)) {
      const matches = findForbiddenActivityTokens(await readFile(file, "utf8"));
      if (matches.length > 0) {
        failures.push(`${path.relative(repositoryRoot, file)}: ${matches.join(", ")}`);
      }
    }
  }
  assert.deepEqual(failures, []);
});

test("the removal boundary preserves generic diagnostics, audit logs, and history terms", () => {
  const preserved = "logger logging merge_log merge_history audit provenance history undo redo progress";
  assert.deepEqual(findForbiddenActivityTokens(preserved), []);
});
