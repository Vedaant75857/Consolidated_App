import React, { useCallback, useEffect, useRef, useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import {
  BarChart3,
  CalendarDays,
  ChevronDown,
  Columns3,
  Loader2,
  RefreshCw,
  SplitSquareHorizontal,
  TrendingUp,
} from "lucide-react";
import { SurfaceCard, itemVariants } from "../common/ui";
import {
  getExecutiveSummary,
  type ExecutiveSummaryResult,
  type DatePivotResult,
  type ParetoAnalysisResult,
  type ParetoThresholdMetrics,
  type SpendBifurcationResult,
  type ColumnFillRateResult,
} from "../../api/client";

function fmtSpend(val: number | null | undefined): string {
  if (val == null) return "N/A";
  return Math.round(val).toLocaleString();
}

function fmtPercent(val: number | null | undefined): string {
  if (val == null || Number.isNaN(val)) return "N/A";
  return `${val.toFixed(1)}%`;
}

function renderBold(text: string): React.ReactNode {
  const parts = text.split(/(\*\*[^*]+\*\*)/g);
  return parts.map((part, i) =>
    part.startsWith("**") && part.endsWith("**") ? (
      <strong key={i} className="font-semibold text-neutral-950 dark:text-white">
        {part.slice(2, -2)}
      </strong>
    ) : (
      <React.Fragment key={i}>{part}</React.Fragment>
    )
  );
}

interface ExecutiveSummaryProps {
  sessionId: string;
  apiKey: string;
  onLoaded?: () => void;
}

export default function ExecutiveSummary({
  sessionId,
  apiKey,
  onLoaded,
}: ExecutiveSummaryProps) {
  const [result, setResult] = useState<ExecutiveSummaryResult | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const [fillRateOpen, setFillRateOpen] = useState(true);
  const [datePivotOpen, setDatePivotOpen] = useState(true);
  const [bifurcationOpen, setBifurcationOpen] = useState(true);
  const [paretoOpen, setParetoOpen] = useState(true);

  const MAX_RETRIES = 3;
  const RETRY_BASE_MS = 2000;
  const [retryAttempt, setRetryAttempt] = useState(0);
  const fetchingRef = useRef(false);

  const runAssessment = useCallback(
    async (force = false) => {
      if (fetchingRef.current) return;
      fetchingRef.current = true;
      setLoading(true);
      setError(null);

      for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        setRetryAttempt(attempt);
        try {
          const data = await getExecutiveSummary(sessionId, apiKey, force);
          setResult(data);
          setRetryAttempt(0);
          onLoaded?.();
          setLoading(false);
          fetchingRef.current = false;
          return;
        } catch (err: any) {
          if (attempt < MAX_RETRIES - 1) {
            await new Promise((r) => setTimeout(r, RETRY_BASE_MS * 2 ** attempt));
          } else {
            setError(err?.message || "Spend Quality Assessment failed");
          }
        }
      }

      setRetryAttempt(0);
      setLoading(false);
      fetchingRef.current = false;
    },
    [sessionId, apiKey, onLoaded]
  );

  useEffect(() => {
    if (!result && !loading && !error && sessionId && apiKey) {
      runAssessment(false);
    } else if (!apiKey) {
      setError("API key is required to run the Spend Quality Assessment.");
    }
  }, [sessionId, apiKey]); // eslint-disable-line react-hooks/exhaustive-deps

  if (loading) {
    return (
      <motion.div
        variants={itemVariants}
        className="flex flex-col items-center justify-center py-24 gap-4"
      >
        <div className="relative">
          <div className="absolute inset-0 rounded-full bg-red-500/20 blur-xl animate-pulse" />
          <div className="relative w-16 h-16 rounded-full bg-red-600 flex items-center justify-center shadow-lg">
            <Loader2 className="w-8 h-8 text-white animate-spin" />
          </div>
        </div>
        <div className="text-center">
          <p className="text-lg font-semibold text-neutral-800 dark:text-neutral-200">
            Generating Spend Quality Assessment
          </p>
          <p className="text-sm text-neutral-500 dark:text-neutral-400 mt-1 max-w-md">
            {retryAttempt > 0
              ? `Attempt ${retryAttempt + 1}/${MAX_RETRIES}... Retrying...`
              : "Computing metrics and generating AI insights. This may take a moment."}
          </p>
        </div>
      </motion.div>
    );
  }

  if (error && !result) {
    return (
      <motion.div variants={itemVariants} className="space-y-6">
        <SurfaceCard>
          <div className="text-center py-8">
            <p className="text-red-600 dark:text-red-400 font-semibold mb-2">
              Spend Quality Assessment Failed
            </p>
            <p className="text-sm text-neutral-500 dark:text-neutral-400 mb-4">
              {error}
            </p>
            <button
              onClick={() => runAssessment(true)}
              className="inline-flex items-center gap-2 px-4 py-2 rounded-xl bg-red-600 text-white text-sm font-semibold hover:bg-red-700 transition-colors"
            >
              <RefreshCw className="w-4 h-4" /> Retry
            </button>
          </div>
        </SurfaceCard>
      </motion.div>
    );
  }

  return (
    <motion.div variants={itemVariants} className="space-y-6">
      {result && (
        <ExecutiveSummaryPanel
          result={result}
          onRefresh={() => runAssessment(true)}
          loading={loading}
        />
      )}

      {result && (
        <>
          <ColumnFillRatePanel
            data={result.columnFillRate}
            expanded={fillRateOpen}
            onToggle={() => setFillRateOpen((p) => !p)}
          />

          <SpendBifurcationPanel
            data={result.spendBifurcation}
            expanded={bifurcationOpen}
            onToggle={() => setBifurcationOpen((p) => !p)}
          />

          <DatePivotPanel
            data={result.datePivot}
            expanded={datePivotOpen}
            onToggle={() => setDatePivotOpen((p) => !p)}
          />

          <ParetoPanel
            data={result.paretoAnalysis}
            expanded={paretoOpen}
            onToggle={() => setParetoOpen((p) => !p)}
          />
        </>
      )}
    </motion.div>
  );
}

function ExecutiveSummaryPanel({
  result,
  onRefresh,
  loading,
}: {
  result: ExecutiveSummaryResult;
  onRefresh: () => void;
  loading: boolean;
}) {
  const rows = result.executiveSummary?.rows ?? [];

  return (
    <SurfaceCard noPadding>
      <div className="px-6 py-5 border-b border-neutral-100 dark:border-neutral-800 flex items-start justify-between gap-4">
        <div className="flex items-center gap-3 min-w-0">
          <span className="w-8 h-8 rounded-lg bg-red-50 dark:bg-red-950/30 flex items-center justify-center">
            <BarChart3 className="w-4 h-4 text-red-600 dark:text-red-400" />
          </span>
          <div>
            <h2 className="text-base font-semibold tracking-tight text-neutral-900 dark:text-white">
              Executive Summary
            </h2>
            <p className="text-xs text-neutral-500 dark:text-neutral-400">
              Key procurement data readiness points
            </p>
          </div>
        </div>

        <div className="flex items-center gap-3 shrink-0">
          <div className="text-right">
            <p className="text-[10px] uppercase tracking-wider text-neutral-400">
              Total Rows
            </p>
            <p className="text-sm font-semibold tabular-nums text-neutral-900 dark:text-white">
              {result.totalRows.toLocaleString()}
            </p>
          </div>
          <button
            onClick={onRefresh}
            disabled={loading}
            className="inline-flex h-9 w-9 items-center justify-center rounded-lg border border-neutral-200 text-neutral-500 hover:bg-neutral-50 hover:text-neutral-800 dark:border-neutral-700 dark:text-neutral-400 dark:hover:bg-neutral-800 dark:hover:text-neutral-100 disabled:opacity-50"
            title="Re-run Assessment"
          >
            <RefreshCw className="w-4 h-4" />
          </button>
        </div>
      </div>

      <div className="overflow-x-auto">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
              <th className="px-6 py-3 w-[220px]">Key Point</th>
              <th className="px-6 py-3">Summary</th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
            {rows.map((row) => (
              <tr
                key={row.key}
                className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
              >
                <td className="px-6 py-3 font-medium text-neutral-800 dark:text-neutral-200">
                  {row.label}
                </td>
                <td className="px-6 py-3 text-neutral-600 dark:text-neutral-300">
                  {renderBold(row.text)}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </SurfaceCard>
  );
}

function ColumnFillRatePanel({
  data,
  expanded,
  onToggle,
}: {
  data: ColumnFillRateResult;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <SurfaceCard noPadding>
      <PanelHeader
        icon={<Columns3 className="w-4 h-4 text-violet-600 dark:text-violet-400" />}
        iconClassName="bg-violet-100 dark:bg-violet-950/40"
        title="Column Fill Rate with Spend Coverage"
        subtitle="Fill rate and spend coverage for original dataset columns"
        expanded={expanded}
        onToggle={onToggle}
      />

      <AnimatePresence initial={false}>
        {expanded && (
          <PanelBody>
            {!data?.feasible || data.columns.length === 0 ? (
              <EmptyPanel>Column fill rate data not available.</EmptyPanel>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                      <th className="px-6 py-3">Original Column</th>
                      <th className="px-4 py-3 text-right">Fill Rate (%)</th>
                      <th className="px-4 py-3 text-right">Spend Coverage (%)</th>
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                    {data.columns.map((col) => (
                      <tr
                        key={`${col.sourceColumn}-${col.order}`}
                        className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
                      >
                        <td className="px-6 py-2.5 font-medium text-neutral-800 dark:text-neutral-200">
                          {col.columnName}
                        </td>
                        <td className="px-4 py-2.5 text-right tabular-nums">
                          <FillRateBadge value={col.fillRate} />
                        </td>
                        <td className="px-4 py-2.5 text-right tabular-nums">
                          {col.spendCoverage != null ? (
                            <FillRateBadge value={col.spendCoverage} />
                          ) : (
                            <span className="text-neutral-400">N/A</span>
                          )}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </PanelBody>
        )}
      </AnimatePresence>
    </SurfaceCard>
  );
}

function FillRateBadge({ value }: { value: number }) {
  const color =
    value >= 90
      ? "text-emerald-700 dark:text-emerald-400"
      : value >= 70
        ? "text-amber-700 dark:text-amber-400"
        : "text-red-600 dark:text-red-400";
  return <span className={color}>{value.toFixed(1)}%</span>;
}

function SpendBifurcationPanel({
  data,
  expanded,
  onToggle,
}: {
  data: SpendBifurcationResult;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <SurfaceCard noPadding>
      <PanelHeader
        icon={<SplitSquareHorizontal className="w-4 h-4 text-indigo-600 dark:text-indigo-400" />}
        iconClassName="bg-indigo-100 dark:bg-indigo-950/40"
        title="Negative vs Positive Spend"
        subtitle={`Positive vs negative reporting-spend breakdown${
          data?.feasible && data.netSpend != null ? ` - Net: ${fmtSpend(data.netSpend)}` : ""
        }`}
        expanded={expanded}
        onToggle={onToggle}
      />

      <AnimatePresence initial={false}>
        {expanded && (
          <PanelBody>
            {!data?.feasible ? (
              <EmptyPanel>{data?.message || "total_spend is not mapped."}</EmptyPanel>
            ) : (
              <div className="grid grid-cols-1 md:grid-cols-5 divide-y md:divide-y-0 md:divide-x divide-neutral-100 dark:divide-neutral-800">
                <SpendMetric
                  label="Positive Spend"
                  value={fmtSpend(data.positiveSpend)}
                  className="text-emerald-700 dark:text-emerald-400"
                />
                <SpendMetric
                  label="Positive % of Net Spend"
                  value={fmtPercent(data.positivePctOfNet)}
                  className="text-emerald-700 dark:text-emerald-400"
                />
                <SpendMetric
                  label="Negative Spend"
                  value={fmtSpend(data.negativeSpend)}
                  className="text-red-600 dark:text-red-400"
                />
                <SpendMetric
                  label="Negative % of Net Spend"
                  value={fmtPercent(data.negativePctOfNet)}
                  className="text-red-600 dark:text-red-400"
                />
                <SpendMetric
                  label="Net Spend"
                  value={fmtSpend(data.netSpend)}
                  className="text-neutral-900 dark:text-white"
                />
              </div>
            )}
          </PanelBody>
        )}
      </AnimatePresence>
    </SurfaceCard>
  );
}

function SpendMetric({
  label,
  value,
  className,
}: {
  label: string;
  value: string;
  className: string;
}) {
  return (
    <div className="px-5 py-5">
      <p className="text-[11px] uppercase tracking-wider text-neutral-400">
        {label}
      </p>
      <p className={`mt-1 text-lg font-semibold tabular-nums ${className}`}>
        {value}
      </p>
    </div>
  );
}

function DatePivotPanel({
  data,
  expanded,
  onToggle,
}: {
  data: DatePivotResult;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <SurfaceCard noPadding>
      <PanelHeader
        icon={<CalendarDays className="w-4 h-4 text-blue-600 dark:text-blue-400" />}
        iconClassName="bg-blue-100 dark:bg-blue-950/40"
        title="Year / Monthly Spend Split"
        subtitle="Spend by year and month"
        expanded={expanded}
        onToggle={onToggle}
      />

      <AnimatePresence initial={false}>
        {expanded && (
          <PanelBody>
            {!data.feasible ? (
              <EmptyPanel>{data.message || "Date pivot not available."}</EmptyPanel>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                      <th className="px-4 py-3 sticky left-0 bg-neutral-50 dark:bg-neutral-800/50 z-10">
                        Month
                      </th>
                      {data.years.map((yr) => (
                        <th key={yr} className="px-4 py-3 text-right min-w-[120px]">
                          {yr}
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                    {data.months.map((monthName, idx) => {
                      const monthNum = String(idx + 1);
                      return (
                        <tr
                          key={monthNum}
                          className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
                        >
                          <td className="px-4 py-2.5 font-medium text-neutral-800 dark:text-neutral-200 sticky left-0 bg-white dark:bg-neutral-900 z-10">
                            {monthName}
                          </td>
                          {data.years.map((yr) => {
                            const val = data.cells[String(yr)]?.[monthNum] ?? 0;
                            return (
                              <td
                                key={yr}
                                className="px-4 py-2.5 text-right tabular-nums text-neutral-700 dark:text-neutral-300"
                              >
                                {val === 0 ? (
                                  <span className="text-neutral-300 dark:text-neutral-600">0</span>
                                ) : (
                                  fmtSpend(val)
                                )}
                              </td>
                            );
                          })}
                        </tr>
                      );
                    })}
                  </tbody>
                  <tfoot>
                    <tr className="border-t-2 border-neutral-200 dark:border-neutral-700 bg-neutral-50/80 dark:bg-neutral-800/40">
                      <td className="px-4 py-2.5 font-semibold text-neutral-900 dark:text-white sticky left-0 bg-neutral-50/80 dark:bg-neutral-800/40 z-10">
                        Total
                      </td>
                      {data.years.map((yr) => {
                        let total = 0;
                        for (let m = 1; m <= 12; m++) {
                          total += data.cells[String(yr)]?.[String(m)] ?? 0;
                        }
                        return (
                          <td
                            key={yr}
                            className="px-4 py-2.5 text-right tabular-nums font-semibold text-neutral-900 dark:text-white"
                          >
                            {total === 0 ? (
                              <span className="text-neutral-300 dark:text-neutral-600">0</span>
                            ) : (
                              fmtSpend(total)
                            )}
                          </td>
                        );
                      })}
                    </tr>
                  </tfoot>
                </table>
              </div>
            )}
          </PanelBody>
        )}
      </AnimatePresence>
    </SurfaceCard>
  );
}

const PARETO_ROW_LABELS: { key: keyof ParetoThresholdMetrics; label: string }[] = [
  { key: "totalSpend", label: "Total Spend" },
  { key: "transactionCount", label: "Number of Transactions" },
  { key: "uniqueTransactions", label: "Unique Transactions" },
  { key: "supplierCount", label: "Number of Suppliers" },
];

function ParetoPanel({
  data,
  expanded,
  onToggle,
}: {
  data: ParetoAnalysisResult;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <SurfaceCard noPadding>
      <PanelHeader
        icon={<TrendingUp className="w-4 h-4 text-emerald-600 dark:text-emerald-400" />}
        iconClassName="bg-emerald-100 dark:bg-emerald-950/40"
        title="Pareto Cuts (80 / 85 / 90 / 95)"
        subtitle={`Spend and supplier analysis at key thresholds${
          data.feasible && data.totalDatasetSpend > 0 ? ` - Total: ${fmtSpend(data.totalDatasetSpend)}` : ""
        }`}
        expanded={expanded}
        onToggle={onToggle}
      />

      <AnimatePresence initial={false}>
        {expanded && (
          <PanelBody>
            {!data.feasible ? (
              <EmptyPanel>{data.message || "Pareto analysis not available."}</EmptyPanel>
            ) : (
              <div className="overflow-x-auto">
                <table className="w-full text-sm">
                  <thead>
                    <tr className="bg-neutral-50 dark:bg-neutral-800/50 text-left text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400">
                      <th className="px-4 py-3 sticky left-0 bg-neutral-50 dark:bg-neutral-800/50 z-10 min-w-[180px]">
                        Metric
                      </th>
                      {data.thresholds.map((t) => (
                        <th key={t} className="px-4 py-3 text-right min-w-[120px]">
                          Top {t}%
                        </th>
                      ))}
                    </tr>
                  </thead>
                  <tbody className="divide-y divide-neutral-100 dark:divide-neutral-800">
                    {PARETO_ROW_LABELS.map(({ key, label }) => (
                      <tr
                        key={key}
                        className="hover:bg-neutral-50/50 dark:hover:bg-neutral-800/30 transition-colors"
                      >
                        <td className="px-4 py-2.5 font-medium text-neutral-800 dark:text-neutral-200 sticky left-0 bg-white dark:bg-neutral-900 z-10">
                          {label}
                        </td>
                        {data.thresholds.map((t) => {
                          const m = data.metrics[String(t)];
                          const val = m ? m[key] : 0;
                          return (
                            <td
                              key={t}
                              className="px-4 py-2.5 text-right tabular-nums text-neutral-700 dark:text-neutral-300"
                            >
                              {fmtSpend(val)}
                            </td>
                          );
                        })}
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}
          </PanelBody>
        )}
      </AnimatePresence>
    </SurfaceCard>
  );
}

function PanelHeader({
  icon,
  iconClassName,
  title,
  subtitle,
  expanded,
  onToggle,
}: {
  icon: React.ReactNode;
  iconClassName: string;
  title: string;
  subtitle: string;
  expanded: boolean;
  onToggle: () => void;
}) {
  return (
    <button
      onClick={onToggle}
      className="flex items-center justify-between w-full px-6 py-4 text-left hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors rounded-t-2xl"
    >
      <div className="flex items-center gap-3 min-w-0">
        <span className={`w-8 h-8 rounded-lg flex items-center justify-center shrink-0 ${iconClassName}`}>
          {icon}
        </span>
        <div className="min-w-0">
          <h3 className="text-sm font-semibold text-neutral-800 dark:text-neutral-200">
            {title}
          </h3>
          <p className="text-xs text-neutral-400 truncate">
            {subtitle}
          </p>
        </div>
      </div>
      <ChevronDown
        className={`w-4 h-4 text-neutral-400 transition-transform ${
          expanded ? "" : "-rotate-90"
        }`}
      />
    </button>
  );
}

function PanelBody({ children }: { children: React.ReactNode }) {
  return (
    <motion.div
      initial={{ height: 0, opacity: 0 }}
      animate={{ height: "auto", opacity: 1 }}
      exit={{ height: 0, opacity: 0 }}
      transition={{ duration: 0.25, ease: [0.22, 1, 0.36, 1] }}
      className="overflow-hidden"
    >
      <div className="border-t border-neutral-100 dark:border-neutral-800">
        {children}
      </div>
    </motion.div>
  );
}

function EmptyPanel({ children }: { children: React.ReactNode }) {
  return (
    <div className="px-6 py-8 text-center text-sm text-neutral-500 dark:text-neutral-400">
      {children}
    </div>
  );
}
