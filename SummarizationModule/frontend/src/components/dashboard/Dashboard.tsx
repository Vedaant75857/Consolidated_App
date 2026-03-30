import { useRef } from "react";
import { Mail } from "lucide-react";
import type { ViewResult, ViewConfig } from "../../types";
import ViewPanel from "./ViewPanel";
import ExportBar from "../export/ExportBar";

interface Props {
  views: ViewResult[];
  sessionId: string;
  onExportCsv: (viewId: string) => void;
  onExportPdf: (chartImages: Record<string, string>) => void;
  onRecomputeView?: (viewId: string, config: ViewConfig) => Promise<ViewResult>;
  onGenerateEmail?: () => void;
}

export default function Dashboard({
  views,
  sessionId,
  onExportCsv,
  onExportPdf,
  onRecomputeView,
  onGenerateEmail,
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

      {onGenerateEmail && (
        <div className="flex justify-center pt-2 pb-4">
          <button
            onClick={onGenerateEmail}
            className="flex items-center gap-2 px-6 py-3 rounded-xl bg-primary text-white text-sm font-semibold hover:bg-primary-hover transition-colors shadow-sm"
          >
            <Mail className="w-4 h-4" />
            Generate Client Email
          </button>
        </div>
      )}
    </div>
  );
}
