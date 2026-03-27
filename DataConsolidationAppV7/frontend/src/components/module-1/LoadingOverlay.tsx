import React, { useEffect, useState, useRef } from "react";
import { motion, AnimatePresence } from "motion/react";
import { BrainCircuit, X } from "lucide-react";

const AI_MESSAGES = [
  "Comparing schemas across extracted tables...",
  "Finding likely join keys and resolving ambiguity...",
  "Building a normalized procurement field map...",
  "Validating column compatibility...",
  "Analyzing value distributions for best matches...",
];

interface LoadingOverlayProps {
  isLoading: boolean;
  message?: string;
  onCancel?: () => void;
  onForceDismiss?: () => void;
}

function useElapsedSeconds(running: boolean) {
  const [elapsed, setElapsed] = useState(0);
  const intervalRef = useRef<ReturnType<typeof setInterval> | null>(null);

  useEffect(() => {
    if (running) {
      setElapsed(0);
      intervalRef.current = setInterval(() => setElapsed((s) => s + 1), 1000);
    } else {
      setElapsed(0);
      if (intervalRef.current) clearInterval(intervalRef.current);
    }
    return () => {
      if (intervalRef.current) clearInterval(intervalRef.current);
    };
  }, [running]);

  return elapsed;
}

function formatElapsed(seconds: number) {
  if (seconds < 60) return `${seconds}s`;
  const m = Math.floor(seconds / 60);
  const s = seconds % 60;
  return `${m}m ${s}s`;
}

export default function LoadingOverlay({ isLoading, message, onCancel }: LoadingOverlayProps) {
  const elapsed = useElapsedSeconds(isLoading);
  const msgIndex = Math.floor(elapsed / 4) % AI_MESSAGES.length;

  return (
    <AnimatePresence>
      {isLoading && (
        <>
          {/* Non-blocking top progress bar */}
          <motion.div
            key="ai-progress-bar"
            initial={{ scaleX: 0, originX: 0 }}
            animate={{ scaleX: 1 }}
            exit={{ opacity: 0 }}
            transition={{ duration: 2, repeat: Infinity, ease: "easeInOut" }}
            className="fixed top-0 left-0 right-0 h-1 bg-gradient-to-r from-red-500 to-rose-500 z-[100] pointer-events-none"
          />

          {/* Non-blocking floating toast — bottom-right */}
          <motion.div
            key="ai-toast"
            initial={{ opacity: 0, y: 20, scale: 0.95 }}
            animate={{ opacity: 1, y: 0, scale: 1 }}
            exit={{ opacity: 0, y: 10, scale: 0.95 }}
            transition={{ type: "spring", stiffness: 400, damping: 30 }}
            className="fixed bottom-20 right-6 z-[100] w-80 rounded-2xl border border-neutral-200/80 dark:border-neutral-700/80 bg-white/95 dark:bg-neutral-900/95 shadow-xl backdrop-blur-sm pointer-events-auto"
          >
            <div className="px-4 py-3 flex items-start gap-3">
              {/* Pulsing icon */}
              <div className="relative w-9 h-9 shrink-0 mt-0.5">
                <div className="absolute inset-0 rounded-xl bg-red-50 dark:bg-red-950/30" />
                <motion.div
                  animate={{ scale: [1, 1.1, 1], opacity: [0.8, 1, 0.8] }}
                  transition={{ duration: 1.5, repeat: Infinity, ease: "easeInOut" }}
                  className="absolute inset-1.5 rounded-lg bg-gradient-to-br from-red-500 to-rose-500 flex items-center justify-center"
                >
                  <BrainCircuit className="w-3.5 h-3.5 text-white" />
                </motion.div>
              </div>

              <div className="flex-1 min-w-0">
                <div className="flex items-center justify-between gap-2">
                  <p className="text-xs font-semibold text-neutral-900 dark:text-white truncate">
                    AI is working
                  </p>
                  <span className="text-[10px] tabular-nums font-medium text-neutral-400 dark:text-neutral-500 shrink-0">
                    {formatElapsed(elapsed)}
                  </span>
                </div>

                <p className="text-[11px] text-neutral-500 dark:text-neutral-400 mt-0.5 leading-snug truncate">
                  {message || "Analyzing your data..."}
                </p>

                {/* Rotating AI sub-message */}
                <AnimatePresence mode="wait">
                  <motion.p
                    key={msgIndex}
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    exit={{ opacity: 0 }}
                    transition={{ duration: 0.2 }}
                    className="text-[10px] text-red-500 dark:text-red-400 mt-1 italic truncate"
                  >
                    {AI_MESSAGES[msgIndex]}
                  </motion.p>
                </AnimatePresence>

                {/* Indeterminate progress bar */}
                <div className="h-1 w-full bg-neutral-100 dark:bg-neutral-800 rounded-full overflow-hidden mt-2">
                  <motion.div
                    animate={{ x: ["-100%", "100%"] }}
                    transition={{ duration: 1.5, repeat: Infinity, ease: "easeInOut" }}
                    className="h-full w-1/2 bg-gradient-to-r from-red-500 to-rose-500 rounded-full"
                  />
                </div>
              </div>

              {onCancel && (
                <button
                  type="button"
                  onClick={onCancel}
                  className="p-1 rounded-lg text-neutral-400 hover:text-neutral-600 dark:hover:text-neutral-300 hover:bg-neutral-100 dark:hover:bg-neutral-800 transition-colors shrink-0"
                  title="Cancel AI request"
                >
                  <X className="w-3.5 h-3.5" />
                </button>
              )}
            </div>
          </motion.div>
        </>
      )}
    </AnimatePresence>
  );
}
