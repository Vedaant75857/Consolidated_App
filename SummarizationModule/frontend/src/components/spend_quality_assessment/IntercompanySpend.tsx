import React, { useCallback, useEffect, useState } from "react";
import { motion } from "framer-motion";
import {
  Building2,
  Loader2,
  Plus,
  Search,
  Trash2,
  X,
} from "lucide-react";
import { SurfaceCard, itemVariants } from "../common/ui";
import {
  getIntercompanyColumns,
  searchIntercompanyKeyword,
  type SearchableColumn,
  type KeywordSearchResult,
} from "../../api/client";

/* ── Helpers ───────────────────────────────────────────────────────────── */

function fmtNumber(val: number): string {
  return Math.round(val).toLocaleString();
}

/* ── Props ─────────────────────────────────────────────────────────────── */

interface IntercompanySpendProps {
  sessionId: string;
}

/* ── Component ─────────────────────────────────────────────────────────── */

export default function IntercompanySpend({
  sessionId,
}: IntercompanySpendProps) {
  const [columns, setColumns] = useState<SearchableColumn[]>([]);
  const [columnsLoading, setColumnsLoading] = useState(false);
  const [columnsError, setColumnsError] = useState<string | null>(null);

  const [selectedColumns, setSelectedColumns] = useState<Set<string>>(new Set());
  const [keywords, setKeywords] = useState<KeywordSearchResult[]>([]);
  const [inputValue, setInputValue] = useState("");
  const [searching, setSearching] = useState(false);
  const [searchError, setSearchError] = useState<string | null>(null);

  /* ── Fetch available columns on mount ──────────────────────────────── */

  const fetchColumns = useCallback(async () => {
    setColumnsLoading(true);
    setColumnsError(null);
    try {
      const data = await getIntercompanyColumns(sessionId);
      setColumns(data.columns);
      if (data.columns.length > 0) {
        setSelectedColumns(new Set([data.columns[0].fieldKey]));
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

  /* ── Column toggle ─────────────────────────────────────────────────── */

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

  /* ── Keyword search ────────────────────────────────────────────────── */

  const handleSearch = useCallback(async () => {
    const trimmed = inputValue.trim();
    if (!trimmed) return;

    if (keywords.some((k) => k.keyword.toLowerCase() === trimmed.toLowerCase())) {
      setSearchError("Keyword already added.");
      return;
    }
    if (selectedColumns.size === 0) {
      setSearchError("Select at least one column.");
      return;
    }

    setSearching(true);
    setSearchError(null);
    try {
      const result = await searchIntercompanyKeyword(
        sessionId,
        Array.from(selectedColumns),
        trimmed,
      );
      setKeywords((prev) => [...prev, result]);
      setInputValue("");
    } catch (err: any) {
      setSearchError(err?.message || "Search failed");
    } finally {
      setSearching(false);
    }
  }, [inputValue, keywords, selectedColumns, sessionId]);

  const handleKeyDown = (e: React.KeyboardEvent) => {
    if (e.key === "Enter") {
      e.preventDefault();
      handleSearch();
    }
  };

  const removeKeyword = (keyword: string) => {
    setKeywords((prev) => prev.filter((k) => k.keyword !== keyword));
  };

  /* ── Totals ────────────────────────────────────────────────────────── */

  const totalRows = keywords.reduce((sum, k) => sum + k.matchingRows, 0);
  const totalSpend = keywords.reduce((sum, k) => sum + k.totalSpend, 0);

  /* ── Loading columns ───────────────────────────────────────────────── */

  if (columnsLoading) {
    return (
      <motion.div
        variants={itemVariants}
        className="flex flex-col items-center justify-center py-24 gap-4"
      >
        <div className="relative">
          <div className="absolute inset-0 rounded-full bg-red-500/20 blur-xl animate-pulse" />
          <div className="relative w-16 h-16 rounded-full bg-gradient-to-br from-red-500 to-rose-600 flex items-center justify-center shadow-lg">
            <Loader2 className="w-8 h-8 text-white animate-spin" />
          </div>
        </div>
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
            <p className="text-sm text-neutral-500 dark:text-neutral-400 mb-4">
              {columnsError}
            </p>
            <button
              onClick={fetchColumns}
              className="inline-flex items-center gap-2 px-4 py-2 rounded-xl bg-red-600 text-white text-sm font-semibold hover:bg-red-700 transition-colors"
            >
              Retry
            </button>
          </div>
        </SurfaceCard>
      </motion.div>
    );
  }

  /* ── Main render ───────────────────────────────────────────────────── */

  return (
    <motion.div variants={itemVariants} className="space-y-6">
      {/* Header */}
      <SurfaceCard noPadding>
        <div className="rounded-2xl bg-gradient-to-r from-red-600 to-rose-600 p-7 text-white">
          <div className="flex items-center gap-2 mb-2">
            <Building2 className="w-6 h-6" />
            <h2 className="text-xl font-semibold tracking-tight">
              Intercompany Spend
            </h2>
          </div>
          <p className="text-red-50/90 text-sm max-w-xl">
            Search keywords across vendor columns to identify and tally
            intercompany spend.
          </p>
        </div>
      </SurfaceCard>

      {/* Column selector */}
      <SurfaceCard>
        <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200 mb-3">
          Select Vendor Columns
        </h3>
        <div className="flex flex-wrap gap-2">
          {columns.map((col) => {
            const isSelected = selectedColumns.has(col.fieldKey);
            return (
              <button
                key={col.fieldKey}
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
          Select the vendor columns you want to search across
        </p>
      </SurfaceCard>

      {/* Search bar */}
      <SurfaceCard>
        <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200 mb-3">
          Keyword Search
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
              className="w-full pl-10 pr-4 py-2.5 rounded-xl border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 text-sm text-neutral-800 dark:text-neutral-200 placeholder:text-neutral-400 focus:outline-none focus:ring-2 focus:ring-red-500/40 disabled:opacity-50 transition-colors"
            />
          </div>
          <button
            onClick={handleSearch}
            disabled={searching || !inputValue.trim()}
            className="inline-flex items-center gap-1.5 px-4 py-2.5 rounded-xl bg-red-600 text-white text-sm font-semibold hover:bg-red-700 transition-colors disabled:opacity-50 disabled:cursor-not-allowed shrink-0"
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
          <p className="text-xs text-red-500 dark:text-red-400 mt-2">
            {searchError}
          </p>
        )}

        {/* Active keyword chips */}
        {keywords.length > 0 && (
          <div className="flex flex-wrap gap-1.5 mt-3">
            {keywords.map((k) => (
              <span
                key={k.keyword}
                className="inline-flex items-center gap-1 px-2.5 py-1 rounded-lg bg-neutral-100 dark:bg-neutral-800 text-xs font-medium text-neutral-700 dark:text-neutral-300"
              >
                {k.keyword}
                <button
                  onClick={() => removeKeyword(k.keyword)}
                  className="ml-0.5 p-0.5 rounded hover:bg-neutral-200 dark:hover:bg-neutral-700 transition-colors"
                >
                  <X className="w-3 h-3" />
                </button>
              </span>
            ))}
          </div>
        )}
      </SurfaceCard>

      {/* Results table */}
      {keywords.length > 0 && (
        <SurfaceCard noPadding>
          <div className="overflow-x-auto">
            <table className="w-full text-sm">
              <thead>
                <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                  <th className="px-6 py-3">Keyword</th>
                  <th className="px-4 py-3 text-right">Matching Rows</th>
                  <th className="px-4 py-3 text-right">Total Spend</th>
                  <th className="px-4 py-3 w-12" />
                </tr>
              </thead>
              <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                {keywords.map((k) => (
                  <tr
                    key={k.keyword}
                    className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
                  >
                    <td className="px-6 py-3 font-medium text-neutral-800 dark:text-neutral-200">
                      {k.keyword}
                    </td>
                    <td className="px-4 py-3 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                      {fmtNumber(k.matchingRows)}
                    </td>
                    <td className="px-4 py-3 text-right tabular-nums text-neutral-700 dark:text-neutral-300">
                      {fmtNumber(k.totalSpend)}
                    </td>
                    <td className="px-4 py-3 text-center">
                      <button
                        onClick={() => removeKeyword(k.keyword)}
                        className="p-1 rounded-lg text-neutral-400 hover:text-red-500 hover:bg-red-50 dark:hover:bg-red-950/30 transition-colors"
                        title="Remove keyword"
                      >
                        <Trash2 className="w-4 h-4" />
                      </button>
                    </td>
                  </tr>
                ))}
              </tbody>
              <tfoot>
                <tr className="bg-neutral-50 dark:bg-neutral-800/50 font-semibold">
                  <td className="px-6 py-3 text-neutral-800 dark:text-neutral-200">
                    Total
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums text-neutral-800 dark:text-neutral-200">
                    {fmtNumber(totalRows)}
                  </td>
                  <td className="px-4 py-3 text-right tabular-nums text-neutral-800 dark:text-neutral-200">
                    {fmtNumber(totalSpend)}
                  </td>
                  <td />
                </tr>
              </tfoot>
            </table>
          </div>
        </SurfaceCard>
      )}
    </motion.div>
  );
}
