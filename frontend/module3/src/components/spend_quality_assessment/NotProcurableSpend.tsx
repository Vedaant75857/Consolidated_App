import React, { useCallback, useEffect, useState } from "react";
import { motion } from "framer-motion";
import {
  AlertCircle,
  Ban,
  Loader2,
  Play,
  Plus,
  Search,
  Trash2,
  X,
} from "lucide-react";
import { SurfaceCard, itemVariants } from "../common/ui";
import {
  detectNotProcurableSpend,
  getNotProcurableColumns,
  searchNotProcurableKeyword,
  type KeywordSearchResult,
  type SearchableColumn,
} from "../../api/client";

function fmtNumber(val: number): string {
  return Math.round(val).toLocaleString();
}

interface NotProcurableSpendProps {
  sessionId: string;
}

export default function NotProcurableSpend({
  sessionId,
}: NotProcurableSpendProps) {
  const [columns, setColumns] = useState<SearchableColumn[]>([]);
  const [columnsLoading, setColumnsLoading] = useState(false);
  const [columnsError, setColumnsError] = useState<string | null>(null);

  const [selectedColumns, setSelectedColumns] = useState<Set<string>>(new Set());
  const [keywords, setKeywords] = useState<KeywordSearchResult[]>([]);
  const [uniqueMatchingRows, setUniqueMatchingRows] = useState<number | null>(
    null,
  );
  const [uniqueTotalSpend, setUniqueTotalSpend] = useState<number | null>(null);
  const [columnsUsed, setColumnsUsed] = useState<string[]>([]);
  const [detectMessage, setDetectMessage] = useState<string | null>(null);

  const [inputValue, setInputValue] = useState("");
  const [searching, setSearching] = useState(false);
  const [detecting, setDetecting] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);

  const fetchColumns = useCallback(async () => {
    setColumnsLoading(true);
    setColumnsError(null);
    try {
      const data = await getNotProcurableColumns(sessionId);
      setColumns(data.columns);
      if (data.columns.length > 0) {
        const preferred = data.columns.find((c) => c.fieldKey === "description");
        const initial = preferred
          ? [preferred.fieldKey]
          : [data.columns[0].fieldKey];
        setSelectedColumns(new Set(initial));
      } else {
        setSelectedColumns(new Set());
      }
    } catch (err: any) {
      setColumnsError(err?.message || "Failed to load columns");
    } finally {
      setColumnsLoading(false);
    }
  }, [sessionId]);

  useEffect(() => {
    fetchColumns();
  }, [fetchColumns]);

  const toggleColumn = (fieldKey: string) => {
    setSelectedColumns((prev) => {
      const next = new Set(prev);
      if (next.has(fieldKey)) {
        if (next.size > 1) next.delete(fieldKey);
      } else {
        next.add(fieldKey);
      }
      return next;
    });
  };

  const selectedList = Array.from(selectedColumns);

  const runDetection = useCallback(async () => {
    setDetecting(true);
    setSearchError(null);
    setDetectMessage(null);
    try {
      const data = await detectNotProcurableSpend(
        sessionId,
        selectedList.length > 0 ? selectedList : undefined,
      );
      if (!data.feasible) {
        setDetectMessage(data.message);
        setKeywords([]);
        setUniqueMatchingRows(null);
        setUniqueTotalSpend(null);
        return;
      }
      setKeywords(data.keywords);
      setColumnsUsed(data.columnsUsed);
      setUniqueMatchingRows(data.uniqueMatchingRows);
      setUniqueTotalSpend(data.uniqueTotalSpend);
      setDetectMessage(data.message);
    } catch (err: any) {
      setSearchError(err?.message || "Detection failed");
    } finally {
      setDetecting(false);
    }
  }, [sessionId, selectedList]);

  const handleSearch = useCallback(async () => {
    const trimmed = inputValue.trim();
    if (!trimmed) return;

    if (keywords.some((k) => k.keyword.toLowerCase() === trimmed.toLowerCase())) {
      setSearchError("Keyword already added.");
      return;
    }
    if (selectedColumns.size === 0) {
      setSearchError("Select at least one column with data.");
      return;
    }

    setSearching(true);
    setSearchError(null);
    try {
      const result = await searchNotProcurableKeyword(
        sessionId,
        selectedList,
        trimmed,
      );
      setKeywords((prev) => [...prev, result]);
      setInputValue("");
      setUniqueMatchingRows(null);
      setUniqueTotalSpend(null);
    } catch (err: any) {
      setSearchError(err?.message || "Search failed");
    } finally {
      setSearching(false);
    }
  }, [inputValue, keywords, selectedList, selectedColumns.size, sessionId]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      e.preventDefault();
      handleSearch();
    }
  };

  const removeKeyword = (keyword: string) => {
    setKeywords((prev) => prev.filter((k) => k.keyword !== keyword));
    setUniqueMatchingRows(null);
    setUniqueTotalSpend(null);
  };

  const totalRows = keywords.reduce((sum, k) => sum + k.matchingRows, 0);
  const totalSpend = keywords.reduce((sum, k) => sum + k.totalSpend, 0);

  if (columnsLoading) {
    return (
      <motion.div
        variants={itemVariants}
        className="flex flex-col items-center justify-center py-24 gap-4"
      >
        <Loader2 className="w-8 h-8 text-red-500 animate-spin" />
        <p className="text-lg font-semibold text-neutral-800 dark:text-neutral-200">
          Loading columns…
        </p>
      </motion.div>
    );
  }

  if (columnsError) {
    return (
      <motion.div variants={itemVariants} className="space-y-6">
        <SurfaceCard>
          <div className="text-center py-8">
            <p className="text-red-600 dark:text-red-400 font-semibold mb-2">
              Failed to load columns
            </p>
            <p className="text-sm text-neutral-500 mb-4">{columnsError}</p>
            <button
              onClick={fetchColumns}
              className="px-4 py-2 rounded-xl bg-red-600 text-white text-sm font-semibold"
            >
              Retry
            </button>
          </div>
        </SurfaceCard>
      </motion.div>
    );
  }

  return (
    <motion.div variants={itemVariants} className="space-y-6">
      <SurfaceCard noPadding>
        <div className="rounded-2xl bg-gradient-to-r from-red-600 to-rose-600 p-7 text-white">
          <div className="flex items-center gap-2 mb-2">
            <Ban className="w-6 h-6" />
            <h2 className="text-xl font-semibold tracking-tight">
              Not Procurable Spend
            </h2>
          </div>
          <p className="text-red-50/90 text-sm max-w-xl">
            Scan description and taxonomy columns for built-in non-procurable
            keywords, or add your own keyword searches.
          </p>
        </div>
      </SurfaceCard>

      {columns.length === 0 ? (
        <SurfaceCard>
          <div className="flex gap-3 items-start">
            <AlertCircle className="w-5 h-5 text-amber-500 shrink-0 mt-0.5" />
            <div>
              <p className="text-sm font-semibold text-neutral-800 dark:text-neutral-200 mb-1">
                No searchable columns found
              </p>
              <p className="text-sm text-neutral-600 dark:text-neutral-400">
                Map <strong>Description</strong>, PO Material Description, or
                L1–L4 in Step 3 and ensure those columns contain data.
              </p>
            </div>
          </div>
        </SurfaceCard>
      ) : (
        <>
          <SurfaceCard>
            <div className="flex items-center justify-between gap-3 mb-3">
              <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
                Columns to Scan
              </h3>
              <button
                onClick={runDetection}
                disabled={detecting || selectedColumns.size === 0}
                className="inline-flex items-center gap-1.5 px-4 py-2 rounded-xl bg-red-600 text-white text-sm font-semibold hover:bg-red-700 transition-colors disabled:opacity-50"
              >
                {detecting ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Play className="w-4 h-4" />
                )}
                Run Detection
              </button>
            </div>
            <div className="flex flex-wrap gap-2">
              {columns.map((col) => {
                const isSelected = selectedColumns.has(col.fieldKey);
                return (
                  <button
                    key={col.fieldKey}
                    type="button"
                    onClick={() => toggleColumn(col.fieldKey)}
                    className={`px-3 py-1.5 rounded-lg text-xs font-semibold transition-colors ${
                      isSelected
                        ? "bg-indigo-100 dark:bg-indigo-950/40 text-indigo-700 dark:text-indigo-300 ring-1 ring-indigo-300 dark:ring-indigo-700"
                        : "bg-neutral-100 dark:bg-neutral-800 text-neutral-500 dark:text-neutral-400 hover:bg-neutral-200 dark:hover:bg-neutral-700"
                    }`}
                  >
                    {col.displayName}
                  </button>
                );
              })}
            </div>
            <p className="text-xs text-neutral-400 mt-2">
              Select one or more columns, then click Run Detection to auto-scan
              for tax, freight, payroll, utilities, and other non-procurable
              keywords.
            </p>
          </SurfaceCard>

          <SurfaceCard>
            <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200 mb-3">
              Add Custom Keyword
            </h3>
            <div className="flex gap-2">
              <div className="relative flex-1">
                <Search className="absolute left-3 top-1/2 -translate-y-1/2 w-4 h-4 text-neutral-400" />
                <input
                  type="text"
                  value={inputValue}
                  onChange={(e) => {
                    setInputValue(e.target.value);
                    setSearchError(null);
                  }}
                  onKeyDown={handleKeyDown}
                  placeholder="Type a keyword and press Enter…"
                  disabled={searching}
                  className="w-full pl-10 pr-4 py-2.5 rounded-xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 text-sm focus:outline-none focus:ring-2 focus:ring-red-500/40 disabled:opacity-50"
                />
              </div>
              <button
                onClick={handleSearch}
                disabled={searching || !inputValue.trim()}
                className="inline-flex items-center gap-1.5 px-4 py-2.5 rounded-xl bg-neutral-800 dark:bg-neutral-700 text-white text-sm font-semibold disabled:opacity-50 shrink-0"
              >
                {searching ? (
                  <Loader2 className="w-4 h-4 animate-spin" />
                ) : (
                  <Plus className="w-4 h-4" />
                )}
                Add
              </button>
            </div>
            {searchError && (
              <p className="text-xs text-red-500 mt-2">{searchError}</p>
            )}
          </SurfaceCard>
        </>
      )}

      {detectMessage && (
        <SurfaceCard>
          <div className="flex gap-3 items-start">
            <AlertCircle className="w-5 h-5 text-amber-500 shrink-0 mt-0.5" />
            <p className="text-sm text-neutral-600 dark:text-neutral-400">
              {detectMessage}
            </p>
          </div>
        </SurfaceCard>
      )}

      {columnsUsed.length > 0 && keywords.length > 0 && (
        <SurfaceCard>
          <p className="text-sm text-neutral-700 dark:text-neutral-300">
            Scanned columns:{" "}
            <span className="font-semibold text-indigo-600 dark:text-indigo-400">
              {columnsUsed.join(", ")}
            </span>
          </p>
        </SurfaceCard>
      )}

      {keywords.length > 0 && (
        <SurfaceCard noPadding>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500">
                  <th className="px-6 py-3">Keyword</th>
                  <th className="px-4 py-3 text-right">Matching Rows</th>
                  <th className="px-4 py-3 text-right">Total Spend</th>
                  <th className="px-4 py-3 w-12" />
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                {keywords.map((k) => (
                  <tr key={k.keyword}>
                    <td className="px-6 py-3 font-medium">{k.keyword}</td>
                    <td className="px-4 py-3 text-right tabular-nums">
                      {fmtNumber(k.matchingRows)}
                    </td>
                    <td className="px-4 py-3 text-right tabular-nums">
                      {fmtNumber(k.totalSpend)}
                    </td>
                    <td className="px-4 py-3 text-center">
                      <button
                        onClick={() => removeKeyword(k.keyword)}
                        className="p-1 rounded-lg text-neutral-400 hover:text-red-500"
                      >
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                {uniqueMatchingRows != null ? (
                  <tr className="bg-neutral-50 dark:bg-neutral-800/50 font-semibold">
                    <td className="px-6 py-3">Unique rows (de-duplicated)</td>
                    <td className="px-4 py-3 text-right tabular-nums">
                      {fmtNumber(uniqueMatchingRows)}
                    </td>
                    <td className="px-4 py-3 text-right tabular-nums">
                      {fmtNumber(uniqueTotalSpend ?? 0)}
                    </td>
                    <td />
                  </tr>
                ) : (
                  <tr className="bg-neutral-50 dark:bg-neutral-800/50 font-semibold">
                    <td className="px-6 py-3">Total (may overlap)</td>
                    <td className="px-4 py-3 text-right tabular-nums">
                      {fmtNumber(totalRows)}
                    </td>
                    <td className="px-4 py-3 text-right tabular-nums">
                      {fmtNumber(totalSpend)}
                    </td>
                    <td />
                  </tr>
                )}
              </tfoot>
            </table>
          </div>
        </SurfaceCard>
      )}
    </motion.div>
  );
}
