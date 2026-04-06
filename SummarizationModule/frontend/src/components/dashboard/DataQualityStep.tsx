import { useState, useCallback } from "react";
import { motion } from "framer-motion";
import { ArrowRight } from "lucide-react";
import ExecutiveSummary from "./ExecutiveSummary";

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

  const handleLoaded = useCallback(() => {
    setLoaded(true);
  }, []);

  return (
    <div className="space-y-6">
      <ExecutiveSummary
        sessionId={sessionId}
        apiKey={apiKey}
        onLoaded={handleLoaded}
      />

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