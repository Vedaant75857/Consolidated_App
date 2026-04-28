import { useEffect, useRef, useState } from "react";
import { CheckCircle2, XCircle, Loader2, ArrowRight } from "lucide-react";
import type { ProcurementViewAvailability, AnalysisFeasibilityResult } from "../../types";

interface Props {
  sessionId: string;
  onFetchViews: () => Promise<AnalysisFeasibilityResult>;
  cachedViews?: AnalysisFeasibilityResult | null;
  onGenerateEmail?: () => void;
}

type TabKey = "spendXray" | "categoryNavigator";

const TAB_LABELS: Record<TabKey, string> = {
  spendXray: "Spend X-ray Analysis",
  categoryNavigator: "Category Navigator Levers",
};

function sortViews(views: ProcurementViewAvailability[]) {
  return [...views].sort((a, b) => {
    if (a.available && !b.available) return -1;
    if (!a.available && b.available) return 1;
    return 0;
  });
}

export default function ProcurementViewsStep({
  sessionId,
  onFetchViews,
  cachedViews,
  onGenerateEmail,
}: Props) {
  const [data, setData] = useState<AnalysisFeasibilityResult | null>(cachedViews ?? null);
  const [loading, setLoading] = useState(!cachedViews);
  const [error, setError] = useState("");
  const [activeTab, setActiveTab] = useState<TabKey>("spendXray");
  const fetchedRef = useRef(!!cachedViews);

  useEffect(() => {
    if (cachedViews) {
      setData(cachedViews);
      setLoading(false);
      fetchedRef.current = true;
      return;
    }

    if (fetchedRef.current) return;
    fetchedRef.current = true;
    let cancelled = false;

    setLoading(true);
    setError("");
    onFetchViews()
      .then((result) => {
        if (!cancelled) {
          setData(result);
          setLoading(false);
        }
      })
      .catch((err) => {
        if (!cancelled) {
          setError(err.message || "Failed to load analysis feasibility");
          setLoading(false);
          fetchedRef.current = false;
        }
      });
    return () => { cancelled = true; };
  }, [sessionId, onFetchViews, cachedViews]);

  if (loading) {
    return (
      <div className="max-w-2xl mx-auto">
        <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-neutral-200 dark:border-neutral-800 shadow-sm p-12 text-center">
          <Loader2 className="w-10 h-10 text-primary animate-spin mx-auto mb-4" />
          <p className="text-sm text-neutral-600 dark:text-neutral-400">
            Checking analysis feasibility...
          </p>
        </div>
      </div>
    );
  }

  if (error) {
    return (
      <div className="max-w-2xl mx-auto">
        <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-red-200 dark:border-red-900/50 shadow-sm p-8 text-center">
          <p className="text-sm text-red-600 dark:text-red-400">{error}</p>
        </div>
      </div>
    );
  }

  const views = data ? sortViews(data[activeTab]) : [];
  const feasibleCount = views.filter((v) => v.available).length;
  const totalCount = views.length;

  return (
    <div className="space-y-4">
      {/* Tab bar */}
      <div className="flex gap-2 border-b border-neutral-200 dark:border-neutral-700 pb-0">
        {(Object.keys(TAB_LABELS) as TabKey[]).map((key) => {
          const isActive = activeTab === key;
          const tabViews = data ? data[key] : [];
          const tabFeasible = tabViews.filter((v) => v.available).length;
          return (
            <button
              key={key}
              onClick={() => setActiveTab(key)}
              className={`relative px-5 py-3 text-sm font-semibold transition-colors rounded-t-xl ${
                isActive
                  ? "text-red-700 dark:text-red-400 bg-white dark:bg-neutral-900 border border-b-0 border-neutral-200 dark:border-neutral-700"
                  : "text-neutral-500 dark:text-neutral-400 hover:text-neutral-700 dark:hover:text-neutral-300"
              }`}
            >
              {TAB_LABELS[key]}
              <span className={`ml-2 text-xs font-bold px-2 py-0.5 rounded-full ${
                isActive
                  ? "bg-red-100 dark:bg-red-950/40 text-red-700 dark:text-red-400"
                  : "bg-neutral-100 dark:bg-neutral-800 text-neutral-500 dark:text-neutral-400"
              }`}>
                {tabFeasible}/{tabViews.length}
              </span>
            </button>
          );
        })}
      </div>

      {/* Summary bar */}
      <div className="flex items-center gap-3 text-sm text-neutral-600 dark:text-neutral-400">
        <span className="font-semibold text-emerald-600 dark:text-emerald-400">{feasibleCount}</span>
        <span>feasible</span>
        <span className="text-neutral-300 dark:text-neutral-600">/</span>
        <span className="font-semibold">{totalCount}</span>
        <span>total</span>
      </div>

      {/* View list */}
      <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
        {views.map((view) => (
          <div
            key={view.viewId}
            className={`rounded-2xl border p-5 transition-colors ${
              view.available
                ? "bg-emerald-50/60 dark:bg-emerald-950/20 border-emerald-300 dark:border-emerald-800"
                : "bg-neutral-50 dark:bg-neutral-800/40 border-neutral-200 dark:border-neutral-700 opacity-70"
            }`}
          >
            <div className="flex items-start gap-3">
              {view.available ? (
                <CheckCircle2 className="w-5 h-5 text-emerald-600 dark:text-emerald-400 mt-0.5 shrink-0" />
              ) : (
                <XCircle className="w-5 h-5 text-neutral-400 dark:text-neutral-500 mt-0.5 shrink-0" />
              )}
              <div className="min-w-0 flex-1">
                <h3
                  className={`text-sm font-semibold ${
                    view.available
                      ? "text-emerald-800 dark:text-emerald-300"
                      : "text-neutral-500 dark:text-neutral-400"
                  }`}
                >
                  {view.title}
                </h3>
                {!view.available && view.missingFields.length > 0 && (
                  <div className="mt-2">
                    <p className="text-[10px] uppercase tracking-wider font-bold text-neutral-400 dark:text-neutral-500 mb-1">
                      Missing fields
                    </p>
                    <ul className="space-y-0.5">
                      {view.missingFields.map((field) => (
                        <li
                          key={field}
                          className="text-xs text-neutral-500 dark:text-neutral-400 flex items-center gap-1.5"
                        >
                          <span className="w-1 h-1 rounded-full bg-neutral-400 dark:bg-neutral-500 shrink-0" />
                          {field}
                        </li>
                      ))}
                    </ul>
                  </div>
                )}
              </div>
            </div>
          </div>
        ))}
      </div>

      {onGenerateEmail && (
        <div className="flex justify-end pt-2">
          <button
            onClick={onGenerateEmail}
            className="flex items-center gap-2 px-6 py-2.5 rounded-xl bg-primary text-white text-sm font-semibold hover:bg-primary-hover transition-colors shadow-md shadow-red-200/30 dark:shadow-red-900/20"
          >
            Continue to Email
            <ArrowRight className="w-4 h-4" />
          </button>
        </div>
      )}
    </div>
  );
}
