import { useRef, useState, useEffect } from "react";
import {
  Download,
  ChevronLeft,
  ChevronRight,
  ChevronDown,
  ChevronUp,
  Loader2,
  Table2,
} from "lucide-react";
import type { ViewResult, ViewConfig, MekkoData } from "../../types";
import AISummary from "./AISummary";
import DataTable from "./DataTable";
import TreePivotTable from "./TreePivotTable";
import SpendBarChart from "./charts/SpendBarChart";
import HBarChart from "./charts/HBarChart";
import ParetoChart from "./charts/ParetoChart";
import MekkoChart from "./charts/MekkoChart";

interface Props {
  view: ViewResult;
  onExportCsv: () => void;
  onRecomputeView?: (viewId: string, config: ViewConfig) => Promise<ViewResult>;
}

function getTableRows(view: ViewResult): Record<string, any>[] {
  const td = view.tableData;
  if (Array.isArray(td)) return td;
  if (td && typeof td === "object") {
    for (const key of ["last12", "monthly", "yearly"]) {
      if (Array.isArray(td[key])) return td[key];
    }
    const first = Object.values(td)[0];
    if (Array.isArray(first)) return first as Record<string, any>[];
  }
  return [];
}

function useDebounce<T>(value: T, delay: number): T {
  const [debounced, setDebounced] = useState(value);
  useEffect(() => {
    const timer = setTimeout(() => setDebounced(value), delay);
    return () => clearTimeout(timer);
  }, [value, delay]);
  return debounced;
}

export default function ViewPanel({ view, onExportCsv, onRecomputeView }: Props) {
  const chartDivRef = useRef<HTMLDivElement>(null);
  const [summaryExpanded, setSummaryExpanded] = useState(true);
  const [tableExpanded, setTableExpanded] = useState(false);
  const [recomputing, setRecomputing] = useState(false);

  const [paretoThreshold, setParetoThreshold] = useState(view.threshold ?? 80);
  const [topN, setTopN] = useState(20);
  const [selectedL1, setSelectedL1] = useState<string>("");

  const debouncedPareto = useDebounce(paretoThreshold, 500);
  const debouncedTopN = useDebounce(topN, 500);

  useEffect(() => {
    if (view.viewId === "l2_vs_l3_mekko" && view.chartData?.perL1) {
      const keys = Object.keys(view.chartData.perL1);
      if (keys.length > 0 && !selectedL1) {
        setSelectedL1(keys[0]);
      }
    }
  }, [view, selectedL1]);

  useEffect(() => {
    if (view.viewId !== "pareto_analysis" || !onRecomputeView) return;
    if (debouncedPareto === (view.threshold ?? 80)) return;
    setRecomputing(true);
    onRecomputeView(view.viewId, { paretoThreshold: debouncedPareto })
      .finally(() => setRecomputing(false));
  }, [debouncedPareto]);

  useEffect(() => {
    if (view.viewId !== "supplier_ranking" || !onRecomputeView) return;
    setRecomputing(true);
    onRecomputeView(view.viewId, { topN: debouncedTopN })
      .finally(() => setRecomputing(false));
  }, [debouncedTopN]);

  useEffect(() => {
    if (view.threshold != null) setParetoThreshold(view.threshold);
  }, [view.threshold]);

  const tableRows = getTableRows(view);

  const renderChart = () => {
    const cd = view.chartData;
    if (!cd) return null;

    switch (view.chartType) {
      case "bar":
        return <SpendBarChart labels={cd.labels || []} values={cd.values || []} title={view.title} />;
      case "hbar":
        return <HBarChart labels={cd.labels || []} values={cd.values || []} title={view.title} />;
      case "pareto":
        return (
          <ParetoChart
            labels={cd.labels || []}
            spendValues={cd.spendValues || []}
            cumulativePercent={cd.cumulativePercent || []}
            title={view.title}
          />
        );
      case "mekko":
        if (cd.columns) {
          return <MekkoChart data={cd} title={view.title} />;
        }
        if (cd.perL1 && selectedL1 && cd.perL1[selectedL1]) {
          return (
            <MekkoChart
              data={cd.perL1[selectedL1] as MekkoData}
              title={`${view.title} — ${selectedL1}`}
            />
          );
        }
        if (cd.perL1) {
          const firstKey = Object.keys(cd.perL1)[0];
          return firstKey ? (
            <MekkoChart data={cd.perL1[firstKey]} title={`${view.title} — ${firstKey}`} />
          ) : null;
        }
        return null;
      default:
        return null;
    }
  };

  if (view.error) {
    return (
      <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 p-6">
        <h3 className="font-semibold text-neutral-900 dark:text-neutral-100">{view.title}</h3>
        <p className="text-sm text-primary mt-2">Error: {view.error}</p>
      </div>
    );
  }

  const hasAiSummary = view.aiSummary && view.aiSummary.trim();
  const showSummaryLoading = !hasAiSummary && view.viewId !== "category_drilldown";
  const l1Keys = view.viewId === "l2_vs_l3_mekko" && view.chartData?.perL1
    ? Object.keys(view.chartData.perL1)
    : [];

  return (
    <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 shadow-sm overflow-hidden">
      {/* Header */}
      <div className="flex items-center justify-between px-6 py-4 border-b border-neutral-100 dark:border-neutral-800">
        <h3 className="font-semibold text-neutral-900 dark:text-neutral-100">{view.title}</h3>
        <button
          onClick={onExportCsv}
          className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border border-neutral-200 dark:border-neutral-700 text-neutral-600 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
        >
          <Download className="w-3 h-3" /> CSV
        </button>
      </div>

      {/* Controls bar for interactive views */}
      {(view.viewId === "pareto_analysis" || view.viewId === "supplier_ranking" || l1Keys.length > 0) && (
        <div className="flex flex-wrap items-center gap-4 px-6 py-3 border-b border-neutral-100 dark:border-neutral-800 bg-neutral-50/50 dark:bg-neutral-800/30">
          {view.viewId === "pareto_analysis" && (
            <div className="flex items-center gap-3 flex-1 min-w-[200px]">
              <span className="text-xs font-medium text-neutral-500 dark:text-neutral-400 whitespace-nowrap">
                Threshold
              </span>
              <input
                type="range"
                min={50}
                max={95}
                value={paretoThreshold}
                onChange={(e) => setParetoThreshold(Number(e.target.value))}
                className="flex-1 accent-primary h-1.5"
              />
              <span className="text-xs font-semibold text-neutral-900 dark:text-neutral-100 tabular-nums w-10 text-right">
                {paretoThreshold}%
              </span>
              <span className="inline-flex items-center gap-1 px-2.5 py-1 rounded-full text-xs font-medium bg-primary-50 dark:bg-primary-900/20 text-primary border border-primary-100 dark:border-primary-900/30">
                {recomputing ? (
                  <Loader2 className="w-3 h-3 animate-spin" />
                ) : (
                  <>
                    {view.suppliersInGroup ?? "—"}
                    <span className="text-neutral-400 font-normal">of</span>
                    {view.totalSuppliers ?? "—"}
                  </>
                )}
              </span>
            </div>
          )}

          {view.viewId === "supplier_ranking" && (
            <div className="flex items-center gap-3 flex-1 min-w-[200px]">
              <span className="text-xs font-medium text-neutral-500 dark:text-neutral-400 whitespace-nowrap">
                Top N
              </span>
              <input
                type="range"
                min={5}
                max={50}
                value={topN}
                onChange={(e) => setTopN(Number(e.target.value))}
                className="flex-1 accent-primary h-1.5"
              />
              <span className="text-xs font-semibold text-neutral-900 dark:text-neutral-100 tabular-nums w-8 text-right">
                {topN}
              </span>
              {recomputing && <Loader2 className="w-3.5 h-3.5 text-primary animate-spin" />}
            </div>
          )}

          {l1Keys.length > 0 && (
            <div className="flex items-center gap-2">
              <span className="text-xs font-medium text-neutral-500 dark:text-neutral-400">L1 Filter</span>
              <select
                value={selectedL1}
                onChange={(e) => setSelectedL1(e.target.value)}
                className="text-xs px-3 py-1.5 rounded-lg border border-neutral-200 dark:border-neutral-700 bg-white dark:bg-neutral-800 text-neutral-900 dark:text-neutral-100 focus:outline-none focus:ring-1 focus:ring-primary"
              >
                {l1Keys.map((key) => (
                  <option key={key} value={key}>{key}</option>
                ))}
              </select>
            </div>
          )}
        </div>
      )}

      {/* Main content area */}
      <div className="flex flex-col lg:flex-row">
        {/* Collapsible AI summary panel */}
        {(hasAiSummary || showSummaryLoading) && (
          <div
            className={`shrink-0 border-b lg:border-b-0 lg:border-r border-neutral-100 dark:border-neutral-800 transition-all duration-300 ease-in-out relative ${
              summaryExpanded ? "lg:w-80" : "lg:w-10"
            }`}
          >
            <button
              onClick={() => setSummaryExpanded(!summaryExpanded)}
              className="absolute top-3 right-0 translate-x-1/2 z-10 w-6 h-6 rounded-full bg-white dark:bg-neutral-800 border border-neutral-200 dark:border-neutral-700 flex items-center justify-center text-neutral-400 hover:text-primary transition-colors shadow-sm hidden lg:flex"
            >
              {summaryExpanded ? <ChevronLeft className="w-3.5 h-3.5" /> : <ChevronRight className="w-3.5 h-3.5" />}
            </button>
            {summaryExpanded && (
              <div className="p-4">
                {hasAiSummary ? (
                  <AISummary summary={view.aiSummary!} />
                ) : (
                  <AISummary loading />
                )}
              </div>
            )}
          </div>
        )}

        {/* Chart + table area */}
        <div className="flex-1 min-w-0 p-4 space-y-4">
          <div ref={chartDivRef} className="bg-white dark:bg-neutral-900 rounded-lg relative">
            {recomputing && (
              <div className="absolute inset-0 bg-white/60 dark:bg-neutral-900/60 flex items-center justify-center z-10 rounded-lg">
                <Loader2 className="w-6 h-6 text-primary animate-spin" />
              </div>
            )}
            {renderChart()}
          </div>

          {view.excludedRows != null && view.excludedRows > 0 && (
            <p className="text-xs text-neutral-400">
              {view.excludedRows.toLocaleString()} rows excluded due to null values in required columns
            </p>
          )}
        </div>
      </div>

      {/* Collapsible data table */}
      {(tableRows.length > 0 || (view.chartType === "tree_pivot" && view.treeData)) && (
        <div className="border-t border-neutral-100 dark:border-neutral-800">
          <button
            onClick={() => setTableExpanded(!tableExpanded)}
            className="w-full flex items-center gap-2 px-6 py-2.5 text-xs font-medium text-neutral-500 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-800/50 transition-colors"
          >
            <Table2 className="w-3.5 h-3.5" />
            {tableExpanded ? "Hide Data Table" : "Show Data Table"}
            {tableExpanded ? (
              <ChevronUp className="w-3.5 h-3.5 ml-auto" />
            ) : (
              <ChevronDown className="w-3.5 h-3.5 ml-auto" />
            )}
          </button>
          {tableExpanded && (
            <div className="px-4 pb-4">
              {view.chartType === "tree_pivot" && view.treeData ? (
                <TreePivotTable data={view.treeData} />
              ) : (
                <DataTable data={tableRows} />
              )}
            </div>
          )}
        </div>
      )}
    </div>
  );
}
