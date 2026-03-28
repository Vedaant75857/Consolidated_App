import { useState } from "react";
import { Loader2, ArrowRight, Lock, BarChart3, TrendingUp, PieChart, Globe2, Layers } from "lucide-react";
import type { ViewDefinition, ViewConfig } from "../../types";

interface Props {
  views: ViewDefinition[];
  onCompute: (selectedViews: string[], config: ViewConfig) => Promise<void>;
  loading: boolean;
}

const VIEW_ICONS: Record<string, React.ReactNode> = {
  spend_over_time: <TrendingUp className="w-4 h-4" />,
  supplier_ranking: <BarChart3 className="w-4 h-4" />,
  pareto_analysis: <PieChart className="w-4 h-4" />,
  currency_spend: <span className="text-xs font-bold">$</span>,
  country_spend: <Globe2 className="w-4 h-4" />,
  l1_spend: <Layers className="w-4 h-4" />,
  l1_vs_l2_mekko: <Layers className="w-4 h-4" />,
  l2_vs_l3_mekko: <Layers className="w-4 h-4" />,
  l3_vs_l4: <Layers className="w-4 h-4" />,
  category_drilldown: <Layers className="w-4 h-4" />,
};

export default function ViewSelectionStep({ views, onCompute, loading }: Props) {
  const [selected, setSelected] = useState<Set<string>>(() => {
    return new Set(views.filter((v) => v.available).map((v) => v.viewId));
  });
  const [topN, setTopN] = useState(20);
  const [paretoThreshold, setParetoThreshold] = useState(80);
  const [error, setError] = useState("");

  const toggleView = (viewId: string) => {
    setSelected((prev) => {
      const next = new Set(prev);
      if (next.has(viewId)) next.delete(viewId);
      else next.add(viewId);
      return next;
    });
  };

  const handleSubmit = async () => {
    const sel = Array.from(selected);
    if (!sel.length) {
      setError("Select at least one view");
      return;
    }
    setError("");
    try {
      await onCompute(sel, { topN, paretoThreshold });
    } catch (err: any) {
      setError(err.message || "Computation failed");
    }
  };

  return (
    <div className="max-w-3xl mx-auto">
      <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 shadow-sm overflow-hidden">
        <div className="px-8 pt-8 pb-4">
          <h2 className="text-lg font-semibold text-neutral-900 dark:text-neutral-100">Select Views</h2>
          <p className="text-xs text-neutral-500 mt-1">
            Choose which analyses to generate. Unavailable views are missing required columns.
          </p>
        </div>

        <div className="px-8 pb-6 space-y-2">
          {views.map((v) => (
            <label
              key={v.viewId}
              className={`flex items-center gap-3 px-4 py-3 rounded-xl border transition-all cursor-pointer
                ${!v.available
                  ? "opacity-40 cursor-not-allowed border-neutral-200 dark:border-neutral-800"
                  : selected.has(v.viewId)
                    ? "border-primary bg-primary-50/50 dark:bg-primary-900/10 dark:border-primary/30"
                    : "border-neutral-200 dark:border-neutral-700 hover:border-primary/30"
                }`}
            >
              <input
                type="checkbox"
                checked={selected.has(v.viewId)}
                onChange={() => v.available && toggleView(v.viewId)}
                disabled={!v.available}
                className="sr-only"
              />
              <div
                className={`w-5 h-5 rounded border-2 flex items-center justify-center shrink-0 transition-all
                  ${selected.has(v.viewId) && v.available
                    ? "bg-primary border-primary text-white"
                    : "border-neutral-300 dark:border-neutral-600"
                  }`}
              >
                {selected.has(v.viewId) && v.available && (
                  <svg className="w-3 h-3" fill="none" viewBox="0 0 24 24" stroke="currentColor" strokeWidth={3}>
                    <path strokeLinecap="round" strokeLinejoin="round" d="M5 13l4 4L19 7" />
                  </svg>
                )}
              </div>
              <div className="w-8 h-8 rounded-lg bg-neutral-100 dark:bg-neutral-800 flex items-center justify-center text-neutral-500 shrink-0">
                {VIEW_ICONS[v.viewId] || <BarChart3 className="w-4 h-4" />}
              </div>
              <div className="flex-1 min-w-0">
                <div className="text-sm font-medium text-neutral-900 dark:text-neutral-100">{v.title}</div>
                <div className="text-xs text-neutral-500 truncate">{v.description}</div>
              </div>
              {!v.available && (
                <div className="flex items-center gap-1 text-xs text-neutral-400">
                  <Lock className="w-3 h-3" />
                  Missing columns
                </div>
              )}
            </label>
          ))}
        </div>

        <div className="px-8 pb-6 space-y-4 border-t border-neutral-100 dark:border-neutral-800 pt-6">
          <h3 className="text-sm font-semibold text-neutral-900 dark:text-neutral-100">Configuration</h3>

          {selected.has("supplier_ranking") && (
            <div className="space-y-2">
              <div className="flex items-center justify-between text-sm">
                <span className="text-neutral-600 dark:text-neutral-400">Top N Suppliers</span>
                <span className="font-semibold text-neutral-900 dark:text-neutral-100 tabular-nums">{topN}</span>
              </div>
              <input
                type="range"
                min={5}
                max={50}
                value={topN}
                onChange={(e) => setTopN(Number(e.target.value))}
                className="w-full accent-primary"
              />
              <div className="flex justify-between text-xs text-neutral-400">
                <span>5</span>
                <span>50</span>
              </div>
            </div>
          )}

          {selected.has("pareto_analysis") && (
            <div className="space-y-2">
              <div className="flex items-center justify-between text-sm">
                <span className="text-neutral-600 dark:text-neutral-400">Pareto Threshold</span>
                <span className="font-semibold text-neutral-900 dark:text-neutral-100 tabular-nums">{paretoThreshold}%</span>
              </div>
              <input
                type="range"
                min={50}
                max={95}
                value={paretoThreshold}
                onChange={(e) => setParetoThreshold(Number(e.target.value))}
                className="w-full accent-primary"
              />
              <div className="flex justify-between text-xs text-neutral-400">
                <span>50%</span>
                <span>95%</span>
              </div>
            </div>
          )}
        </div>

        {error && (
          <div className="px-8 pb-4">
            <p className="text-sm text-primary bg-primary-50 dark:bg-primary-900/20 px-4 py-2 rounded-lg">{error}</p>
          </div>
        )}

        <div className="px-8 pb-8 flex justify-end border-t border-neutral-100 dark:border-neutral-800 pt-4">
          <button
            onClick={handleSubmit}
            disabled={loading || !selected.size}
            className="flex items-center gap-2 px-6 py-2.5 rounded-lg bg-primary text-white text-sm font-semibold hover:bg-primary-hover disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
          >
            {loading ? <Loader2 className="w-4 h-4 animate-spin" /> : null}
            {loading ? "Computing..." : "Generate Views"}
            {!loading && <ArrowRight className="w-4 h-4" />}
          </button>
        </div>
      </div>
    </div>
  );
}
