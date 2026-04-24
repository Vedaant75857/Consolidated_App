"use client";

import { useRef, useState } from "react";
import { motion } from "framer-motion";

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
      { num: 1, label: "Upload", desc: "Ingest CSV files from multiple sources", icon: "⬆" },
      { num: 2, label: "Merge", desc: "Append and combine all uploads", icon: "⬡" },
      { num: 3, label: "Normalize", desc: "Standardize headers & column names", icon: "≈" },
      { num: 4, label: "Clean", desc: "Remove duplicates & fix errors", icon: "✦" },
      { num: 5, label: "Analyze", desc: "Run quality checks & validation", icon: "⬡" },
      { num: 6, label: "Deliver", desc: "Export unified dataset", icon: "↓" },
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
      { num: 1, label: "Upload", desc: "Load raw procurement data", icon: "⬆" },
      { num: 2, label: "Parse", desc: "Detect fields & column types", icon: "⬡" },
      { num: 3, label: "Map", desc: "AI maps suppliers & regions", icon: "≈" },
      { num: 4, label: "Transform", desc: "Apply normalization rules", icon: "✦" },
      { num: 5, label: "Review", desc: "Confidence-scored output", icon: "⬡" },
      { num: 6, label: "Deliver", desc: "Clean, normalized dataset", icon: "↓" },
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
      { num: 1, label: "Upload", desc: "Import spend data files", icon: "⬆" },
      { num: 2, label: "Map", desc: "AI column mapping wizard", icon: "≈" },
      { num: 3, label: "Enrich", desc: "Categorize & tag spend lines", icon: "✦" },
      { num: 4, label: "Analyze", desc: "Pareto & supplier breakdown", icon: "⬡" },
      { num: 5, label: "Visualize", desc: "Generate charts & dashboards", icon: "⬡" },
      { num: 6, label: "Deliver", desc: "Export PDF reports", icon: "↓" },
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
            animationDuration: "22s",
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
                  className={`relative flex flex-col items-center w-[140px] px-4 py-3 rounded-xl cursor-pointer transition-colors duration-200 ${
                    isHovered
                      ? "bg-white dark:bg-neutral-800/80 shadow-lg"
                      : "bg-white/50 dark:bg-neutral-900/30"
                  }`}
                >
                  {/* Step number badge */}
                  <div
                    className={`w-8 h-8 rounded-full flex items-center justify-center text-xs font-bold text-white mb-2 bg-gradient-to-br ${pipeline.gradient}`}
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
                  {isHovered && (
                    <motion.div
                      layoutId={`glow-${pipeline.module}`}
                      className={`absolute inset-0 rounded-xl opacity-10 bg-gradient-to-br ${pipeline.gradient} pointer-events-none`}
                    />
                  )}
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
