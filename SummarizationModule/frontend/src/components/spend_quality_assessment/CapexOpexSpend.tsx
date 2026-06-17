import React, { useCallback, useEffect, useState } from "react";
import { motion } from "framer-motion";
import { AlertCircle, Coins, Info, Loader2, Play } from "lucide-react";
import { SurfaceCard, itemVariants } from "../common/ui";
import {
  classifyCapexOpexSpend,
  getCapexOpexColumns,
  type CapexOpexCategory,
  type SearchableColumn,
} from "../../api/client";

function fmtNumber(val: number): string {
  return Math.round(val).toLocaleString();
}

interface CapexOpexSpendProps {
  sessionId: string;
}

export default function CapexOpexSpend({ sessionId }: CapexOpexSpendProps) {
  const [candidates, setCandidates] = useState<SearchableColumn[]>([]);
  const [selectedColumn, setSelectedColumn] = useState<string | undefined>(
    undefined,
  );

  const [feasible, setFeasible] = useState<boolean | null>(null);
  const [message, setMessage] = useState<string | null>(null);
  const [sourceColumn, setSourceColumn] = useState<string | null>(null);
  const [sourceColumnDisplayName, setSourceColumnDisplayName] = useState<
    string | null
  >(null);
  const [categories, setCategories] = useState<CapexOpexCategory[]>([]);
  const [assumptions, setAssumptions] = useState<string[]>([]);
  const [unclassifiedRows, setUnclassifiedRows] = useState<number | null>(
    null,
  );
  const [unclassifiedSpend, setUnclassifiedSpend] = useState<number | null>(
    null,
  );

  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [hasRun, setHasRun] = useState(false);

  const runClassification = useCallback(async () => {
    setLoading(true);
    setError(null);
    try {
      const data = await classifyCapexOpexSpend(sessionId, selectedColumn);
      setHasRun(true);
      setFeasible(data.feasible);
      setMessage(data.message);
      setSourceColumn(data.sourceColumn);
      setSourceColumnDisplayName(data.sourceColumnDisplayName);
      setCategories(data.categories);
      setAssumptions(data.assumptions);
      setUnclassifiedRows(data.unclassifiedRows);
      setUnclassifiedSpend(data.unclassifiedSpend);
    } catch (err: any) {
      setError(err?.message || "Classification failed");
    } finally {
      setLoading(false);
    }
  }, [sessionId, selectedColumn]);

  useEffect(() => {
    let cancelled = false;
    (async () => {
      try {
        const data = await getCapexOpexColumns(sessionId);
        if (!cancelled) {
          setCandidates(data.columns);
        }
      } catch {
        /* candidates optional */
      }
    })();
    return () => {
      cancelled = true;
    };
  }, [sessionId]);

  const totalRows = categories.reduce((sum, c) => sum + c.matchingRows, 0);
  const totalSpend = categories.reduce((sum, c) => sum + c.totalSpend, 0);

  return (
    <motion.div variants={itemVariants} className="space-y-6">
      <SurfaceCard noPadding>
        <div className="rounded-2xl bg-gradient-to-r from-red-600 to-rose-600 p-7 text-white">
          <div className="flex items-center gap-2 mb-2">
            <Coins className="w-6 h-6" />
            <h2 className="text-xl font-semibold tracking-tight">
              CAPEX / OPEX Spend
            </h2>
          </div>
          <p className="text-red-50/90 text-sm max-w-xl">
            Auto-detects a CAPEX/OPEX indicator or GL account column and
            classifies spend — descriptions are not scanned.
          </p>
        </div>
      </SurfaceCard>

      <SurfaceCard>
        <div className="flex items-center justify-between gap-3 mb-3">
          <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
            Indicator Column
          </h3>
          <button
            onClick={runClassification}
            disabled={loading}
            className="inline-flex items-center gap-1.5 px-4 py-2 rounded-xl bg-red-600 text-white text-sm font-semibold hover:bg-red-700 transition-colors disabled:opacity-50"
          >
            {loading ? (
              <Loader2 className="w-4 h-4 animate-spin" />
            ) : (
              <Play className="w-4 h-4" />
            )}
            {hasRun ? "Re-run Classification" : "Auto-detect & Classify"}
          </button>
        </div>
        <div className="flex flex-wrap gap-2 mb-3">
          <button
            type="button"
            onClick={() => setSelectedColumn(undefined)}
            className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition-colors ${
              selectedColumn === undefined
                ? "bg-indigo-100 dark:bg-indigo-950/40 text-indigo-700 dark:text-indigo-300 ring-1 ring-indigo-300 dark:ring-indigo-700"
                : "bg-neutral-100 dark:bg-neutral-800 text-neutral-500 dark:text-neutral-400 hover:bg-neutral-200 dark:hover:bg-neutral-700"
            }`}
          >
            Auto-detect (recommended)
          </button>
          {candidates.map((col) => (
            <button
              key={col.fieldKey}
              type="button"
              onClick={() => setSelectedColumn(col.fieldKey)}
              className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition-colors ${
                selectedColumn === col.fieldKey
                  ? "bg-indigo-100 dark:bg-indigo-950/40 text-indigo-700 dark:text-indigo-300 ring-1 ring-indigo-300 dark:ring-indigo-700"
                  : "bg-neutral-100 dark:bg-neutral-800 text-neutral-500 dark:text-neutral-400 hover:bg-neutral-200 dark:hover:bg-neutral-700"
              }`}
            >
              {col.displayName}
            </button>
          ))}
        </div>
        {candidates.length === 0 && (
          <p className="text-xs text-amber-600 dark:text-amber-400 mb-2">
            No qualifying indicator columns detected yet. Map CAPEX/OPEX
            Indicator or GL Account in Step 3, then run classification.
          </p>
        )}
        <ul className="space-y-1.5 text-xs text-neutral-600 dark:text-neutral-400">
          {(assumptions.length > 0
            ? assumptions
            : [
                "Only an explicit CAPEX/OPEX indicator or GL account column is used.",
                "Explicit CAPEX values (CAPEX, Y, 1) count as CAPEX.",
                "All other non-empty values in that column count as OPEX.",
                "Blank rows are excluded from both totals.",
              ]
          ).map((rule) => (
            <li key={rule} className="flex gap-2">
              <Info className="w-3.5 h-3.5 shrink-0 mt-0.5 text-indigo-500" />
              <span>{rule}</span>
            </li>
          ))}
        </ul>
      </SurfaceCard>

      {sourceColumn && feasible && (
        <SurfaceCard>
          <p className="text-sm text-neutral-700 dark:text-neutral-300">
            Detected column:{" "}
            <span className="font-semibold text-indigo-600 dark:text-indigo-400">
              {sourceColumnDisplayName ?? sourceColumn}
            </span>
          </p>
        </SurfaceCard>
      )}

      {error && (
        <p className="text-xs text-red-500 dark:text-red-400">{error}</p>
      )}

      {feasible === false && message && !loading && (
        <SurfaceCard>
          <div className="flex gap-3 items-start">
            <AlertCircle className="w-5 h-5 text-amber-500 shrink-0 mt-0.5" />
            <div>
              <p className="text-sm font-semibold text-neutral-800 dark:text-neutral-200 mb-1">
                Classification not available
              </p>
              <p className="text-sm text-neutral-600 dark:text-neutral-400">
                {message}
              </p>
            </div>
          </div>
        </SurfaceCard>
      )}

      {!hasRun && !loading && (
        <SurfaceCard>
          <p className="text-sm text-neutral-500 text-center py-4">
            Click <strong>Auto-detect &amp; Classify</strong> to scan your data.
          </p>
        </SurfaceCard>
      )}

      {(categories.length > 0 || loading) && feasible !== false && hasRun && (
        <SurfaceCard noPadding>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500">
                  <th className="px-6 py-3">Category</th>
                  <th className="px-4 py-3 text-right">Matching Rows</th>
                  <th className="px-4 py-3 text-right">Total Spend</th>
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                {loading && categories.length === 0 ? (
                  <tr>
                    <td colSpan={3} className="px-6 py-8 text-center text-neutral-500">
                      <Loader2 className="w-5 h-5 animate-spin inline-block mr-2" />
                      Classifying spend…
                    </td>
                  </tr>
                ) : (
                  categories.map((cat) => (
                    <tr key={cat.label}>
                      <td className="px-6 py-3 font-medium">
                        <span
                          className={`inline-flex items-center px-2 py-0.5 rounded-md text-xs font-bold ${
                            cat.label === "CAPEX"
                              ? "bg-amber-100 dark:bg-amber-950/40 text-amber-800 dark:text-amber-300"
                              : "bg-sky-100 dark:bg-sky-950/40 text-sky-800 dark:text-sky-300"
                          }`}
                        >
                          {cat.label}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right tabular-nums">
                        {fmtNumber(cat.matchingRows)}
                      </td>
                      <td className="px-4 py-3 text-right tabular-nums">
                        {fmtNumber(cat.totalSpend)}
                      </td>
                    </tr>
                  ))
                )}
              </tbody>
              {!loading && categories.length > 0 && (
                <tfoot>
                  <tr className="bg-neutral-50 dark:bg-neutral-800/50 font-semibold">
                    <td className="px-6 py-3">Total classified</td>
                    <td className="px-4 py-3 text-right tabular-nums">
                      {fmtNumber(totalRows)}
                    </td>
                    <td className="px-4 py-3 text-right tabular-nums">
                      {fmtNumber(totalSpend)}
                    </td>
                  </tr>
                  {(unclassifiedRows ?? 0) > 0 && (
                    <tr className="text-neutral-500 text-xs">
                      <td className="px-6 py-2">Excluded (blank indicator)</td>
                      <td className="px-4 py-2 text-right tabular-nums">
                        {fmtNumber(unclassifiedRows ?? 0)}
                      </td>
                      <td className="px-4 py-2 text-right tabular-nums">
                        {fmtNumber(unclassifiedSpend ?? 0)}
                      </td>
                    </tr>
                  )}
                </tfoot>
              )}
            </table>
          </div>
        </SurfaceCard>
      )}
    </motion.div>
  );
}
