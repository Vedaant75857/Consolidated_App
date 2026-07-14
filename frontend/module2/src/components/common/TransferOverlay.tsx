import { createPortal } from "react-dom";
import { motion, AnimatePresence } from "motion/react";

interface TransferOverlayProps {
  visible: boolean;
  destinationName: string;
  sourceName: string;
}

/* ── Connection Beam with particles ── */
function ConnectionBeam({ sourceName }: { sourceName: string }) {
  const beamHeight = 160;
  const particles = [0, 1, 2, 3];

  return (
    <motion.div
      className="flex flex-col items-center gap-2"
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ delay: 0.3, duration: 0.6 }}
    >
      <svg
        width="40"
        height={beamHeight}
        viewBox={`0 0 40 ${beamHeight}`}
        className="overflow-visible"
      >
        {/* Glow filter */}
        <defs>
          <filter id="beam-glow" x="-50%" y="-50%" width="200%" height="200%">
            <feGaussianBlur stdDeviation="3" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          <filter id="particle-glow" x="-100%" y="-100%" width="300%" height="300%">
            <feGaussianBlur stdDeviation="2" result="blur" />
            <feMerge>
              <feMergeNode in="blur" />
              <feMergeNode in="SourceGraphic" />
            </feMerge>
          </filter>
          {/* Gradient for the beam line */}
          <linearGradient id="beam-gradient" x1="0" y1="1" x2="0" y2="0">
            <stop offset="0%" stopColor="#ef4444" stopOpacity="0.05" />
            <stop offset="30%" stopColor="#ef4444" stopOpacity="0.25" />
            <stop offset="70%" stopColor="#ef4444" stopOpacity="0.25" />
            <stop offset="100%" stopColor="#ef4444" stopOpacity="0.05" />
          </linearGradient>
        </defs>

        {/* Main beam line — faint vertical */}
        <line
          x1="20" y1="0" x2="20" y2={beamHeight}
          stroke="url(#beam-gradient)"
          strokeWidth="2"
          filter="url(#beam-glow)"
        />

        {/* Wider glow halo behind the beam */}
        <line
          x1="20" y1="0" x2="20" y2={beamHeight}
          stroke="#ef4444"
          strokeWidth="8"
          opacity="0.04"
          className="dark:opacity-[0.08]"
        />

        {/* Animated particles flowing upward */}
        {particles.map((i) => (
          <motion.circle
            key={i}
            cx="20"
            r="3"
            fill="#ef4444"
            filter="url(#particle-glow)"
            initial={{ cy: beamHeight + 10, opacity: 0 }}
            animate={{ cy: -10, opacity: [0, 1, 1, 0] }}
            transition={{
              duration: 1.8,
              delay: i * 0.45,
              repeat: Infinity,
              ease: "easeInOut",
            }}
          />
        ))}

        {/* Source node dot (bottom) */}
        <circle cx="20" cy={beamHeight} r="4" fill="#ef4444" opacity="0.3" />
        <circle cx="20" cy={beamHeight} r="2" fill="#ef4444" opacity="0.7" />

        {/* Destination node dot (top) */}
        <circle cx="20" cy="0" r="4" fill="#ef4444" opacity="0.3" />
        <circle cx="20" cy="0" r="2" fill="#ef4444" opacity="0.7" />
      </svg>

      {/* Source label at the bottom of the beam */}
      <p className="text-[10px] font-semibold uppercase tracking-[0.15em] text-neutral-400 dark:text-neutral-500">
        {sourceName}
      </p>
    </motion.div>
  );
}

export default function TransferOverlay({
  visible,
  destinationName,
  sourceName,
}: TransferOverlayProps) {
  return createPortal(
    <AnimatePresence>
      {visible && (
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          exit={{ opacity: 0 }}
          transition={{ duration: 0.4 }}
          className="fixed inset-0 z-[9999] flex flex-col items-center justify-center bg-white dark:bg-neutral-950 overflow-hidden"
        >
          {/* ── Pulsing dot-grid background ── */}
          <div className="absolute inset-0 pointer-events-none overflow-hidden">
            <div className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 w-[700px] h-[700px] rounded-full bg-red-500/5 dark:bg-red-500/10 blur-3xl" />
            {[0, 1, 2, 3].map((i) => (
              <motion.div
                key={i}
                className="absolute top-1/2 left-1/2 -translate-x-1/2 -translate-y-1/2 rounded-full border border-red-500/10 dark:border-red-500/15"
                initial={{ width: 80, height: 80, opacity: 0.6 }}
                animate={{ width: 900, height: 900, opacity: 0 }}
                transition={{
                  duration: 4,
                  delay: i * 1,
                  repeat: Infinity,
                  ease: "easeOut",
                }}
              />
            ))}
            <div
              className="absolute inset-0 opacity-[0.03] dark:opacity-[0.05]"
              style={{
                backgroundImage: "radial-gradient(circle, currentColor 1px, transparent 1px)",
                backgroundSize: "32px 32px",
                color: "#ef4444",
              }}
            />
          </div>

          {/* ── Content ── */}
          <div className="relative flex flex-col items-center gap-4">
            {/* Destination name */}
            <motion.div
              className="flex flex-col items-center gap-2"
              initial={{ y: 20, opacity: 0 }}
              animate={{ y: 0, opacity: 1 }}
              transition={{ delay: 0.15, duration: 0.5 }}
            >
              <h1 className="text-2xl sm:text-3xl font-black tracking-[0.25em] uppercase text-red-600 dark:text-red-500">
                {destinationName}
              </h1>
              <div className="w-24 h-0.5 bg-red-500/30 dark:bg-red-500/20 rounded-full" />
            </motion.div>

            {/* ── Connection Beam (destination ↑ source) ── */}
            <ConnectionBeam sourceName={sourceName} />

            {/* ── Orbital ring spinner ── */}
            <motion.div
              className="relative w-20 h-20"
              initial={{ scale: 0.5, opacity: 0 }}
              animate={{ scale: 1, opacity: 1 }}
              transition={{ delay: 0.25, duration: 0.5, ease: "easeOut" }}
            >
              <div
                className="absolute inset-0 rounded-full border-2 border-transparent"
                style={{
                  borderTopColor: "#ef4444",
                  borderRightColor: "rgba(239,68,68,0.2)",
                  animation: "spin 3s linear infinite",
                  filter: "drop-shadow(0 0 6px rgba(239,68,68,0.4))",
                }}
              />
              <div
                className="absolute inset-2 rounded-full border-2 border-transparent"
                style={{
                  borderTopColor: "rgba(239,68,68,0.7)",
                  borderLeftColor: "rgba(239,68,68,0.15)",
                  animation: "spin 2s linear infinite reverse",
                  filter: "drop-shadow(0 0 4px rgba(239,68,68,0.3))",
                }}
              />
              <div
                className="absolute inset-4 rounded-full border-2 border-transparent"
                style={{
                  borderBottomColor: "#ef4444",
                  borderRightColor: "rgba(239,68,68,0.25)",
                  animation: "spin 1.2s linear infinite",
                  filter: "drop-shadow(0 0 3px rgba(239,68,68,0.5))",
                }}
              />
              <div className="absolute inset-0 flex items-center justify-center">
                <div className="w-2 h-2 rounded-full bg-red-500 shadow-[0_0_8px_rgba(239,68,68,0.6)]" />
              </div>
            </motion.div>

            {/* Status text */}
            <motion.p
              className="text-sm font-medium text-neutral-500 dark:text-neutral-400"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.5, duration: 0.4 }}
            >
              Transferring data...
            </motion.p>

            {/* Progress bar with glow */}
            <motion.div
              className="w-64"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.45, duration: 0.4 }}
            >
              <div className="h-1 bg-neutral-200 dark:bg-neutral-800 rounded-full overflow-hidden">
                <motion.div
                  className="h-full bg-red-600 dark:bg-red-500 rounded-full"
                  style={{ boxShadow: "0 0 12px rgba(239,68,68,0.5)" }}
                  initial={{ width: "0%" }}
                  animate={{ width: "85%" }}
                  transition={{ duration: 8, ease: "easeOut" }}
                />
              </div>
            </motion.div>

            <motion.p
              className="text-xs text-neutral-400 dark:text-neutral-500"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1 }}
              transition={{ delay: 0.6, duration: 0.4 }}
            >
              This may take a moment for large datasets
            </motion.p>
          </div>

          <style>{`
            @keyframes spin {
              from { transform: rotate(0deg); }
              to { transform: rotate(360deg); }
            }
          `}</style>
        </motion.div>
      )}
    </AnimatePresence>,
    document.body,
  );
}
