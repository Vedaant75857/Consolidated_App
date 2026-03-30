import { useRef } from "react";
import type { ViewResult, ViewConfig } from "../../types";
import ViewPanel from "./ViewPanel";
import ExportBar from "../export/ExportBar";

interface Props {
  views: ViewResult[];
  sessionId: string;
  onExportCsv: (viewId: string) => void;
  onExportPdf: (chartImages: Record<string, string>) => void;
  onRecomputeView?: (viewId: string, config: ViewConfig) => Promise<ViewResult>;
}

export default function Dashboard({
  views,
  sessionId,
  onExportCsv,
  onExportPdf,
  onRecomputeView,
}: Props) {
  const chartRefs = useRef<Record<string, HTMLDivElement | null>>({});

  const setChartRef = (viewId: string, el: HTMLDivElement | null) => {
    chartRefs.current[viewId] = el;
  };

  return (
    <div className="space-y-6">
      <ExportBar
        onExportPdf={onExportPdf}
        chartRefs={chartRefs}
        viewCount={views.length}
      />

      {views.map((view) => (
        <ViewPanel
          key={view.viewId}
          view={view}
          onExportCsv={() => onExportCsv(view.viewId)}
          chartRef={setChartRef}
          onRecomputeView={onRecomputeView}
        />
      ))}
    </div>
  );
}
