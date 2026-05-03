import { useState, useCallback } from "react";
import { motion } from "framer-motion";
import { ArrowRight, BarChart3, Ban, Building2 } from "lucide-react";
import ExecutiveSummary from "./ExecutiveSummary";
import NotProcurableSpend from "./NotProcurableSpend";
import IntercompanySpend from "./IntercompanySpend";

type Tab = "executive" | "not-procurable" | "intercompany";

interface DataQualityStepProps {
  sessionId: string;
  apiKey: string;
  onProceed: () => void;
}

export default function DataQualityStep({
  sessionId,
  apiKey,
  onProceed,
}: DataQualityStepProps) {
  const [loaded, setLoaded] = useState(false);
  const [activeTab, setActiveTab] = useState<Tab>("executive");

  const handleLoaded = useCallback(() => {
    setLoaded(true);
  }, []);

  return (
    <div className="space-y-6">
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

      {/* Tab content */}
      {activeTab === "executive" && (
        <ExecutiveSummary
          sessionId={sessionId}
          apiKey={apiKey}
          onLoaded={handleLoaded}
        />
      )}

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