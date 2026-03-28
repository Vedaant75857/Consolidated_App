import { useState } from "react";
import { FileDown, Loader2 } from "lucide-react";
import html2canvas from "html2canvas";

interface Props {
  onExportPdf: (chartImages: Record<string, string>) => void;
  chartRefs: React.MutableRefObject<Record<string, HTMLDivElement | null>>;
  viewCount: number;
}

export default function ExportBar({ onExportPdf, chartRefs, viewCount }: Props) {
  const [exporting, setExporting] = useState(false);

  const handlePdfExport = async () => {
    setExporting(true);
    try {
      const chartImages: Record<string, string> = {};
      for (const [viewId, el] of Object.entries(chartRefs.current)) {
        if (!el) continue;
        try {
          const canvas = await html2canvas(el, { backgroundColor: "#ffffff", scale: 2 });
          const dataUrl = canvas.toDataURL("image/png");
          chartImages[viewId] = dataUrl.split(",")[1];
        } catch {
          // skip if chart capture fails
        }
      }
      onExportPdf(chartImages);
    } finally {
      setExporting(false);
    }
  };

  return (
    <div className="flex items-center justify-between bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 shadow-sm px-6 py-4">
      <div>
        <h2 className="text-lg font-semibold text-neutral-900 dark:text-neutral-100">Analysis Dashboard</h2>
        <p className="text-xs text-neutral-500">
          {viewCount} view{viewCount !== 1 ? "s" : ""} generated
        </p>
      </div>
      <button
        onClick={handlePdfExport}
        disabled={exporting}
        className="flex items-center gap-2 px-5 py-2.5 rounded-lg bg-primary text-white text-sm font-semibold hover:bg-primary-hover disabled:opacity-50 transition-colors"
      >
        {exporting ? <Loader2 className="w-4 h-4 animate-spin" /> : <FileDown className="w-4 h-4" />}
        {exporting ? "Generating PDF..." : "Download Full PDF Report"}
      </button>
    </div>
  );
}
