import { useState, useCallback } from "react";
import { motion } from "framer-motion";
import { ArrowRight, BarChart3, Ban, Building2, KeyRound } from "lucide-react";
import ExecutiveSummary from "./ExecutiveSummary";
import NotProcurableSpend from "./NotProcurableSpend";
import IntercompanySpend from "./IntercompanySpend";

type Tab = "executive" | "not-procurable" | "intercompany";

interface DataQualityStepProps {
  sessionId: string;
  apiKey: string;
  setApiKey: (key: string) => void;
  onProceed: () => void;
}

export default function DataQualityStep({
  sessionId,
  apiKey,
  setApiKey,
  onProceed,
}: DataQualityStepProps) {
  const [loaded, setLoaded] = useState(false);
  const [activeTab, setActiveTab] = useState<Tab>("executive");

  const handleLoaded = useCallback(() => {
    setLoaded(true);
  }, []);

  return (
    <div className="space-y-6">
      {/* API key field — shown only when missing so the user can fix it inline */}
      {!apiKey && (
        <div className="rounded-2xl bg-neutral-50/80 dark:bg-neutral-800/40 p-4 space-y-2">
          <label className="flex items-center gap-2 text-xs uppercase tracking-[0.16em] text-neutral-400 dark:text-neutral-500 font-semibold">
            <KeyRound className="w-4 h-4" />
            Portkey API Key
          </label>
          <input
            type="password"
            value={apiKey}
            onChange={(e) => setApiKey(e.target.value)}
            placeholder="Paste your Portkey API key here"
            className="w-full px-4 py-2.5 text-sm border border-neutral-200 dark:border-neutral-700 rounded-xl bg-white dark:bg-neutral-900 focus:outline-none focus:ring-2 focus:ring-red-500 focus:border-transparent placeholder:text-neutral-400 dark:placeholder:text-neutral-500 transition-shadow"
          />
          <p className="text-xs text-neutral-400 dark:text-neutral-500">
            Required for AI-powered quality assessment and summaries.
          </p>
        </div>
      )}

      {/* Tab bar */}
      <div className="flex gap-1 rounded-xl bg-neutral-100 dark:bg-neutral-800 p-1 w-fit">
        <button
          onClick={() => setActiveTab("executive")}
          className={`inline-flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-semibold transition-colors ${
            activeTab === "executive"
              ? "bg-white dark:bg-neutral-700 text-red-700 dark:text-red-400 shadow-sm"
              : "text-neutral-500 dark:text-neutral-400 hover:text-neutral-700 dark:hover:text-neutral-300"
          }`}
        >
          <BarChart3 className="w-4 h-4" />
          Executive Summary
        </button>
        <button
          onClick={() => setActiveTab("not-procurable")}
          className={`inline-flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-semibold transition-colors ${
            activeTab === "not-procurable"
              ? "bg-white dark:bg-neutral-700 text-red-700 dark:text-red-400 shadow-sm"
              : "text-neutral-500 dark:text-neutral-400 hover:text-neutral-700 dark:hover:text-neutral-300"
          }`}
        >
          <Ban className="w-4 h-4" />
          Not Procurable Spend
        </button>
        <button
          onClick={() => setActiveTab("intercompany")}
          className={`inline-flex items-center gap-1.5 px-4 py-2 rounded-lg text-sm font-semibold transition-colors ${
            activeTab === "intercompany"
              ? "bg-white dark:bg-neutral-700 text-red-700 dark:text-red-400 shadow-sm"
              : "text-neutral-500 dark:text-neutral-400 hover:text-neutral-700 dark:hover:text-neutral-300"
          }`}
        >
          <Building2 className="w-4 h-4" />
          Intercompany Spend
        </button>
      </div>

      {/* Tab content — ExecutiveSummary stays mounted (hidden) to preserve fetch state */}
      <div className={activeTab === "executive" ? "" : "hidden"}>
        <ExecutiveSummary
          sessionId={sessionId}
          apiKey={apiKey}
          onLoaded={handleLoaded}
        />
      </div>

      {activeTab === "not-procurable" && (
        <NotProcurableSpend sessionId={sessionId} />
      )}

      {activeTab === "intercompany" && (
        <IntercompanySpend sessionId={sessionId} />
      )}

      {/* Proceed button — visible once Executive Summary has loaded */}
      {loaded && (
        <motion.div
          initial={{ opacity: 0, y: 12 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ duration: 0.3, ease: [0.22, 1, 0.36, 1] }}
          className="flex justify-center pt-2 pb-4"
        >
          <button
            onClick={onProceed}
            className="flex items-center gap-2 px-6 py-3 rounded-xl bg-primary text-white text-sm font-semibold hover:bg-primary-hover transition-colors shadow-sm"
          >
            Proceed to Select Views
            <ArrowRight className="w-4 h-4" />
          </button>
        </motion.div>
      )}
    </div>
  );
}