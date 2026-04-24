import { useRef, useState } from "react";
import { motion } from "motion/react";

/* ── Pipeline data ───────────────────────────────────────────────── */

const PIPELINES = [
  {
    module: "Data Stitcher",
    tag: "Module 1",
    tagColor: "text-red-500 dark:text-red-400",
    gradient: "from-red-600 to-rose-600",
    trackBg: "bg-red-50/60 dark:bg-red-950/20",
    borderColor: "border-red-200/40 dark:border-red-800/30",
    connectorColor: "#ef4444",
    steps: [
      { num: 1, label: "Upload", desc: "Upload files & configure API key" },
      { num: 2, label: "Preview", desc: "Review tables & adjust headers" },
      { num: 3, label: "Append", desc: "AI groups & aligns related tables" },
      { num: 4, label: "Normalise", desc: "AI maps columns to standard schema" },
      { num: 5, label: "Clean", desc: "Trim, deduplicate & format data" },
      { num: 6, label: "Merge", desc: "AI-guided join on key columns" },
      { num: 7, label: "Quality", desc: "AI data quality assessment" },
    ],
  },
  {
    module: "Data Normalizer",
    tag: "Module 2",
    tagColor: "text-rose-500 dark:text-rose-400",
    gradient: "from-rose-600 to-red-700",
    trackBg: "bg-rose-50/60 dark:bg-rose-950/20",
    borderColor: "border-rose-200/40 dark:border-rose-800/30",
    connectorColor: "#e11d48",
    steps: [
      { num: 1, label: "Upload", desc: "Import raw procurement data" },
      { num: 2, label: "Preview", desc: "Review fields & column types" },
      { num: 3, label: "Suppliers", desc: "AI resolves supplier name variants" },
      { num: 4, label: "Country", desc: "Standardize via ISO lookup & AI" },
      { num: 5, label: "Dates", desc: "Normalize to target date format" },
      { num: 6, label: "Currency", desc: "Convert to target currency" },
      { num: 7, label: "Terms", desc: "Extract numeric payment terms" },
      { num: 8, label: "Regions", desc: "Map to NA / EMEA / APAC / LATAM" },
      { num: 9, label: "Plant", desc: "Standardize plant & site codes" },
    ],
  },
  {
    module: "Spend Summarizer",
    tag: "Module 3",
    tagColor: "text-amber-600 dark:text-amber-400",
    gradient: "from-amber-600 to-orange-600",
    trackBg: "bg-amber-50/60 dark:bg-amber-950/20",
    borderColor: "border-amber-200/40 dark:border-amber-800/30",
    connectorColor: "#d97706",
    steps: [
      { num: 1, label: "Upload", desc: "Import CSV, Excel or ZIP files" },
      { num: 2, label: "Preview", desc: "Review tables & adjust headers" },
      { num: 3, label: "Map Columns", desc: "AI maps to procurement fields" },
      { num: 4, label: "Quality", desc: "AI assesses data readiness" },
      { num: 5, label: "Select Views", desc: "Choose analyses to generate" },
      { num: 6, label: "Dashboard", desc: "Interactive charts & summaries" },
      { num: 7, label: "X-ray", desc: "Check procurement view feasibility" },
      { num: 8, label: "Email", desc: "AI-drafted client summary email" },
    ],
  },
];

/* ── Single pipeline row ─────────────────────────────────────────── */

function PipelineRow({ pipeline, delay = 0 }: { pipeline: typeof PIPELINES[number]; delay?: number }) {
  const trackRef = useRef<HTMLDivElement>(null);
  const [paused, setPaused] = useState(false);
  const [hoveredStep, setHoveredStep] = useState<number | null>(null);

  /* Duplicate steps for seamless loop */
  const allSteps = [...pipeline.steps, ...pipeline.steps];

  return (
    <motion.div
      initial={{ opacity: 0, y: 24 }}
      whileInView={{ opacity: 1, y: 0 }}
      viewport={{ once: true, margin: "-40px" }}
      transition={{ duration: 0.6, delay }}
      className={`rounded-2xl border ${pipeline.borderColor} ${pipeline.trackBg} backdrop-blur-sm p-6 overflow-hidden`}
    >
      {/* Row header */}
      <div className="flex items-center gap-3 mb-5">
        <span className={`text-xs font-semibold uppercase tracking-widest px-2.5 py-1 rounded-full bg-white/60 dark:bg-neutral-900/40 ${pipeline.tagColor}`}>
          {pipeline.tag}
        </span>
        <h3 className="text-sm font-bold text-neutral-900 dark:text-white">
          {pipeline.module}
        </h3>
      </div>

      {/* Scrolling track container */}
      <div
        className="relative overflow-hidden"
        onMouseEnter={() => setPaused(true)}
        onMouseLeave={() => { setPaused(false); setHoveredStep(null); }}
      >
        {/* Fade masks */}
        <div className="absolute left-0 top-0 bottom-0 w-10 z-10 pointer-events-none bg-gradient-to-r from-white/80 dark:from-neutral-900/60 to-transparent" />
        <div className="absolute right-0 top-0 bottom-0 w-10 z-10 pointer-events-none bg-gradient-to-l from-white/80 dark:from-neutral-900/60 to-transparent" />

        {/* Track */}
        <div
          ref={trackRef}
          className={`flex gap-0 pipeline-track ${paused ? "paused" : ""}`}
          style={{
            animationDuration: "14s",
            animationTimingFunction: "linear",
          }}
        >
          {allSteps.map((step, idx) => {
            const isLast = idx === allSteps.length - 1;
            const stepKey = `${pipeline.module}-${idx}`;
            const isHovered = hoveredStep === idx;

            return (
              <div key={stepKey} className="flex items-center shrink-0">
                {/* Step card */}
                <motion.div
                  onMouseEnter={() => setHoveredStep(idx)}
                  onMouseLeave={() => setHoveredStep(null)}
                  animate={isHovered ? { scale: 1.05, y: -2 } : { scale: 1, y: 0 }}
                  transition={{ type: "spring", stiffness: 300, damping: 20 }}
                  className={`relative flex flex-col items-center w-[175px] px-5 py-4 rounded-xl cursor-pointer ${
                    isHovered
                      ? "bg-white dark:bg-neutral-800/80 shadow-lg"
                      : "bg-white/50 dark:bg-neutral-900/30"
                  }`}
                >
                  {/* Step number badge */}
                  <div
                    className={`w-9 h-9 rounded-full flex items-center justify-center text-xs font-bold text-white mb-2 bg-gradient-to-br ${pipeline.gradient}`}
                  >
                    {step.num}
                  </div>
                  <span className="text-sm font-semibold text-neutral-900 dark:text-white">
                    {step.label}
                  </span>
                  <span className="text-[10px] text-neutral-500 dark:text-neutral-400 text-center leading-tight mt-0.5">
                    {step.desc}
                  </span>

                  {/* Hover glow */}
                  <div
                    className={`absolute inset-0 rounded-xl bg-gradient-to-br ${pipeline.gradient} pointer-events-none transition-opacity duration-200`}
                    style={{ opacity: isHovered ? 0.1 : 0 }}
                  />
                </motion.div>

                {/* Connector arrow */}
                {!isLast && (
                  <div className="flex items-center mx-1">
                    <div className="w-5 h-[2px] rounded-full opacity-30" style={{ backgroundColor: pipeline.connectorColor }} />
                    <svg width="7" height="10" viewBox="0 0 7 10" fill="none" className="opacity-40">
                      <path d="M1 1 L6 5 L1 9" stroke={pipeline.connectorColor} strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" />
                    </svg>
                  </div>
                )}
              </div>
            );
          })}
        </div>
      </div>
    </motion.div>
  );
}

/* ── Section export ──────────────────────────────────────────────── */

export default function PipelineSection() {
  return (
    <section className="py-24 px-6">
      <div className="max-w-5xl mx-auto">
        {/* Header */}
        <div className="text-center mb-16">
          <motion.p
            initial={{ opacity: 0, y: 10 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5 }}
            className="text-xs font-semibold uppercase tracking-widest text-red-500 dark:text-red-400 mb-3"
          >
            End-to-End Flow
          </motion.p>
          <motion.h2
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.1 }}
            className="text-4xl font-bold tracking-tight text-neutral-900 dark:text-white text-balance"
          >
            How It Works
          </motion.h2>
          <motion.p
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.2 }}
            className="mt-3 text-neutral-500 dark:text-neutral-400 max-w-lg mx-auto text-balance"
          >
            Each module follows a clear, numbered pipeline. Hover over any step to focus — the flow pauses automatically.
          </motion.p>
        </div>

        {/* Pipeline rows */}
        <div className="flex flex-col gap-6">
          {PIPELINES.map((pipeline, idx) => (
            <PipelineRow key={pipeline.module} pipeline={pipeline} delay={idx * 0.15} />
          ))}
        </div>
      </div>
    </section>
  );
}
