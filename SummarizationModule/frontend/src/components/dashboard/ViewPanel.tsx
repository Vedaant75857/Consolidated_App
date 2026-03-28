import { useRef } from "react";
import { Download, Image as ImageIcon } from "lucide-react";
import html2canvas from "html2canvas";
import type { ViewResult } from "../../types";
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
  chartRef?: (viewId: string, ref: HTMLDivElement | null) => void;
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

function renderChart(view: ViewResult) {
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
      if (cd.perL1) {
        const firstKey = Object.keys(cd.perL1)[0];
        return firstKey ? <MekkoChart data={cd.perL1[firstKey]} title={`${view.title} — ${firstKey}`} /> : null;
      }
      if (cd.perL2) {
        const firstKey = Object.keys(cd.perL2)[0];
        return firstKey ? <MekkoChart data={cd.perL2[firstKey]} title={`${view.title} — ${firstKey}`} /> : null;
      }
      return null;
    default:
      return null;
  }
}

export default function ViewPanel({ view, onExportCsv, chartRef }: Props) {
  const chartDivRef = useRef<HTMLDivElement>(null);

  const handlePng = async () => {
    if (!chartDivRef.current) return;
    const canvas = await html2canvas(chartDivRef.current, { backgroundColor: "#ffffff" });
    const url = canvas.toDataURL("image/png");
    const a = document.createElement("a");
    a.href = url;
    a.download = `${view.viewId}.png`;
    a.click();
  };

  const setRef = (el: HTMLDivElement | null) => {
    (chartDivRef as any).current = el;
    chartRef?.(view.viewId, el);
  };

  const tableRows = getTableRows(view);

  if (view.error) {
    return (
      <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 p-6">
        <h3 className="font-semibold text-neutral-900 dark:text-neutral-100">{view.title}</h3>
        <p className="text-sm text-primary mt-2">Error: {view.error}</p>
      </div>
    );
  }

  return (
    <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 shadow-sm overflow-hidden">
      <div className="flex items-center justify-between px-6 py-4 border-b border-neutral-100 dark:border-neutral-800">
        <h3 className="font-semibold text-neutral-900 dark:text-neutral-100">{view.title}</h3>
        <div className="flex items-center gap-1.5">
          <button
            onClick={onExportCsv}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border border-neutral-200 dark:border-neutral-700 text-neutral-600 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
          >
            <Download className="w-3 h-3" /> CSV
          </button>
          <button
            onClick={handlePng}
            className="flex items-center gap-1.5 px-3 py-1.5 rounded-lg text-xs font-medium border border-neutral-200 dark:border-neutral-700 text-neutral-600 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
          >
            <ImageIcon className="w-3 h-3" /> PNG
          </button>
        </div>
      </div>

      <div className="flex flex-col lg:flex-row">
        {view.aiSummary && (
          <div className="lg:w-80 shrink-0 p-4 border-b lg:border-b-0 lg:border-r border-neutral-100 dark:border-neutral-800">
            <AISummary summary={view.aiSummary} />
          </div>
        )}

        <div className="flex-1 min-w-0 p-4 space-y-4">
          <div ref={setRef} className="bg-white dark:bg-neutral-900 rounded-lg">
            {renderChart(view)}
          </div>

          {view.chartType === "tree_pivot" && view.treeData ? (
            <TreePivotTable data={view.treeData} />
          ) : (
            tableRows.length > 0 && <DataTable data={tableRows} />
          )}

          {view.excludedRows != null && view.excludedRows > 0 && (
            <p className="text-xs text-neutral-400">
              {view.excludedRows.toLocaleString()} rows excluded due to null values in required columns
            </p>
          )}
        </div>
      </div>
    </div>
  );
}
