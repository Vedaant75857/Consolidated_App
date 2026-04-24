"use client";

import { useState } from "react";
import { motion, AnimatePresence } from "framer-motion";
import { ArrowLeft, ArrowRight } from "lucide-react";

const MODULES = [
  {
    title: "Data Stitcher",
    tag: "Module 1",
    tagColor: "text-red-500 dark:text-red-400",
    tagBg: "bg-red-50 dark:bg-red-950/40",
    gradient: "from-red-600 to-rose-600",
    borderColor: "border-red-500/20 dark:border-red-500/15",
    glowColor: "rgba(239,68,68,0.12)",
    description:
      "Consolidate and merge multiple data sources into a single, unified dataset. Upload CSVs from different systems, append rows, normalize mismatched headers, remove duplicates, and merge — all in one guided, step-by-step pipeline.",
    bullets: [
      "Multi-file upload & append",
      "Intelligent header normalization",
      "Duplicate detection & removal",
      "Guided merge wizard",
    ],
    icon: (
      <svg viewBox="0 0 56 56" fill="none" className="w-14 h-14">
        <rect width="56" height="56" rx="16" fill="url(#g1)" />
        <defs>
          <linearGradient id="g1" x1="0" y1="0" x2="56" y2="56">
            <stop stopColor="#ef4444" />
            <stop offset="1" stopColor="#e11d48" />
          </linearGradient>
        </defs>
        <rect x="10" y="12" width="14" height="18" rx="2" fill="white" opacity="0.8" />
        <rect x="13" y="16" width="8" height="1.5" rx="1" fill="white" opacity="0.5" />
        <rect x="13" y="20" width="6" height="1.5" rx="1" fill="white" opacity="0.4" />
        <rect x="32" y="12" width="14" height="18" rx="2" fill="white" opacity="0.6" />
        <rect x="35" y="16" width="8" height="1.5" rx="1" fill="white" opacity="0.5" />
        <rect x="35" y="20" width="6" height="1.5" rx="1" fill="white" opacity="0.4" />
        <path d="M24 21 L32 21" stroke="white" strokeWidth="1.5" strokeLinecap="round" strokeDasharray="2 2" opacity="0.7" />
        <rect x="16" y="36" width="24" height="8" rx="3" fill="white" opacity="0.9" />
        <rect x="19" y="38.5" width="18" height="1.5" rx="1" fill="#ef4444" opacity="0.7" />
        <rect x="19" y="41" width="12" height="1.5" rx="1" fill="#ef4444" opacity="0.4" />
      </svg>
    ),
  },
  {
    title: "Data Normalizer",
    tag: "Module 2",
    tagColor: "text-rose-500 dark:text-rose-400",
    tagBg: "bg-rose-50 dark:bg-rose-950/40",
    gradient: "from-rose-600 to-red-700",
    borderColor: "border-rose-500/20 dark:border-rose-500/15",
    glowColor: "rgba(225,29,72,0.12)",
    description:
      "Leverage AI-powered transformations to normalize supplier names, countries, dates, payment terms, regions, plants, and currencies across your entire procurement dataset with confidence scoring.",
    bullets: [
      "AI supplier name matching",
      "Country & currency standardization",
      "Date & term normalization",
      "Confidence score per field",
    ],
    icon: (
      <svg viewBox="0 0 56 56" fill="none" className="w-14 h-14">
        <rect width="56" height="56" rx="16" fill="url(#g2)" />
        <defs>
          <linearGradient id="g2" x1="0" y1="0" x2="56" y2="56">
            <stop stopColor="#e11d48" />
            <stop offset="1" stopColor="#dc2626" />
          </linearGradient>
        </defs>
        <polygon points="28,10 44,44 12,44" fill="white" opacity="0.1" stroke="white" strokeWidth="1.5" />
        <line x1="28" y1="16" x2="28" y2="36" stroke="white" strokeWidth="1.5" strokeLinecap="round" />
        <line x1="18" y1="37" x2="38" y2="37" stroke="white" strokeWidth="1.5" strokeLinecap="round" />
        <circle cx="28" cy="27" r="4" fill="white" opacity="0.9" />
        <circle cx="28" cy="27" r="2" fill="#e11d48" opacity="0.9" />
      </svg>
    ),
  },
  {
    title: "Spend Summarizer",
    tag: "Module 3",
    tagColor: "text-amber-600 dark:text-amber-400",
    tagBg: "bg-amber-50 dark:bg-amber-950/40",
    gradient: "from-amber-600 to-orange-600",
    borderColor: "border-amber-500/20 dark:border-amber-500/15",
    glowColor: "rgba(217,119,6,0.12)",
    description:
      "Upload procurement data, map columns with AI, and generate interactive spend dashboards featuring charts, Pareto analysis, supplier breakdowns, and exportable PDF reports for executive review.",
    bullets: [
      "AI-powered column mapping",
      "Interactive spend dashboards",
      "Pareto & supplier analytics",
      "Exportable PDF reports",
    ],
    icon: (
      <svg viewBox="0 0 56 56" fill="none" className="w-14 h-14">
        <rect width="56" height="56" rx="16" fill="url(#g3)" />
        <defs>
          <linearGradient id="g3" x1="0" y1="0" x2="56" y2="56">
            <stop stopColor="#d97706" />
            <stop offset="1" stopColor="#ea580c" />
          </linearGradient>
        </defs>
        <rect x="10" y="34" width="8" height="12" rx="2" fill="white" opacity="0.8" />
        <rect x="24" y="26" width="8" height="20" rx="2" fill="white" opacity="0.9" />
        <rect x="38" y="18" width="8" height="28" rx="2" fill="white" opacity="0.7" />
        <path d="M14 33 L28 25 L42 17" stroke="white" strokeWidth="1.8" strokeLinecap="round" strokeLinejoin="round" opacity="0.95" />
        <circle cx="14" cy="33" r="2" fill="white" />
        <circle cx="28" cy="25" r="2" fill="white" />
        <circle cx="42" cy="17" r="2" fill="white" />
      </svg>
    ),
  },
];

const variants = {
  enter: (dir: number) => ({ x: dir > 0 ? 120 : -120, opacity: 0, scale: 0.92 }),
  center: { x: 0, opacity: 1, scale: 1 },
  exit: (dir: number) => ({ x: dir < 0 ? 120 : -120, opacity: 0, scale: 0.92 }),
};

export default function ModulesCarousel() {
  const [active, setActive] = useState(0);
  const [direction, setDirection] = useState(0);

  function navigate(delta: number) {
    setDirection(delta);
    setActive((prev) => (prev + delta + MODULES.length) % MODULES.length);
  }

  const mod = MODULES[active];

  /* Card offset logic: cards 0 and 2 lean left, card 1 leans right */
  function getCardTransform(idx: number) {
    const offsets: Record<number, string> = {
      0: "-translate-x-3",
      1: "translate-x-3",
      2: "-translate-x-3",
    };
    return offsets[idx] ?? "";
  }

  return (
    <section className="relative py-24 px-6">
      {/* Section header */}
      <div className="text-center mb-16">
        <motion.p
          initial={{ opacity: 0, y: 10 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
          className="text-xs font-semibold uppercase tracking-widest text-red-500 dark:text-red-400 mb-3"
        >
          Our Modules
        </motion.p>
        <motion.h2
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5, delay: 0.1 }}
          className="text-4xl font-bold tracking-tight text-neutral-900 dark:text-white text-balance"
        >
          Explore Our Modules
        </motion.h2>
        <motion.p
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5, delay: 0.2 }}
          className="mt-3 text-neutral-500 dark:text-neutral-400 max-w-lg mx-auto text-balance"
        >
          Three purpose-built tools that work together as a single, seamless procurement data pipeline.
        </motion.p>
      </div>

      {/* Cards row */}
      <div className="max-w-5xl mx-auto flex items-center justify-center gap-4 relative">
        {/* Thumbnail cards */}
        {MODULES.map((m, idx) => {
          const isActive = idx === active;
          return (
            <motion.button
              key={m.title}
              onClick={() => {
                setDirection(idx > active ? 1 : -1);
                setActive(idx);
              }}
              whileHover={{ scale: isActive ? 1 : 1.03 }}
              whileTap={{ scale: 0.97 }}
              className={`relative flex-1 max-w-[220px] rounded-2xl p-5 text-left transition-all duration-300 ${getCardTransform(idx)} ${
                isActive
                  ? "glass-card shadow-xl"
                  : "bg-white/30 dark:bg-neutral-900/30 backdrop-blur-md border border-white/20 dark:border-neutral-700/30 shadow-sm opacity-60"
              }`}
              style={isActive ? { boxShadow: `0 0 40px ${m.glowColor}, 0 8px 32px rgba(0,0,0,0.08)` } : {}}
              aria-label={`View ${m.title}`}
            >
              <div className="mb-4">{m.icon}</div>
              <span className={`inline-flex items-center gap-1 text-[10px] font-semibold uppercase tracking-widest px-2 py-0.5 rounded-full mb-2 ${m.tagBg} ${m.tagColor}`}>
                <span className="w-1 h-1 rounded-full bg-current" />
                {m.tag}
              </span>
              <h3 className="text-sm font-bold text-neutral-900 dark:text-white leading-tight mt-1">
                {m.title}
              </h3>
              {isActive && (
                <motion.div
                  layoutId="activeIndicator"
                  className={`absolute bottom-0 left-1/2 -translate-x-1/2 translate-y-1/2 w-2 h-2 rounded-full bg-gradient-to-br ${m.gradient}`}
                />
              )}
            </motion.button>
          );
        })}
      </div>

      {/* Main detail card */}
      <div className="max-w-2xl mx-auto mt-10 relative overflow-hidden">
        <AnimatePresence mode="wait" custom={direction}>
          <motion.div
            key={active}
            custom={direction}
            variants={variants}
            initial="enter"
            animate="center"
            exit="exit"
            transition={{ type: "spring", stiffness: 300, damping: 30 }}
            className="glass-card rounded-3xl p-8 relative overflow-hidden"
            style={{ boxShadow: `0 0 60px ${mod.glowColor}, 0 16px 48px rgba(0,0,0,0.06)` }}
          >
            {/* Background glow */}
            <div className={`absolute top-0 right-0 w-48 h-48 rounded-full bg-gradient-to-br ${mod.gradient} opacity-10 blur-3xl pointer-events-none`} />

            <div className="relative z-10">
              <div className="flex items-start gap-5 mb-6">
                <div>{mod.icon}</div>
                <div>
                  <span className={`inline-flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-widest px-2.5 py-1 rounded-full ${mod.tagBg} ${mod.tagColor} mb-2`}>
                    <motion.span className="w-1.5 h-1.5 rounded-full bg-current" animate={{ opacity: [1, 0.3, 1] }} transition={{ duration: 2, repeat: Infinity }} />
                    {mod.tag}
                  </span>
                  <h3 className="text-2xl font-bold text-neutral-900 dark:text-white">
                    {mod.title}
                  </h3>
                </div>
              </div>

              <p className="text-neutral-600 dark:text-neutral-400 leading-relaxed mb-6">
                {mod.description}
              </p>

              <ul className="grid grid-cols-2 gap-2 mb-7">
                {mod.bullets.map((b) => (
                  <li key={b} className="flex items-center gap-2 text-sm text-neutral-700 dark:text-neutral-300">
                    <span className={`w-1.5 h-1.5 rounded-full bg-gradient-to-br ${mod.gradient} shrink-0`} />
                    {b}
                  </li>
                ))}
              </ul>

              <div className="flex items-center justify-between">
                <span className={`text-sm font-medium ${mod.tagColor}`}>Explore module</span>
                <motion.div
                  whileHover={{ x: 4 }}
                  className={`w-9 h-9 rounded-xl flex items-center justify-center bg-gradient-to-br ${mod.gradient} shadow-md cursor-pointer`}
                >
                  <ArrowRight className="w-4 h-4 text-white" />
                </motion.div>
              </div>
            </div>
          </motion.div>
        </AnimatePresence>
      </div>

      {/* Navigation */}
      <div className="flex items-center justify-center gap-5 mt-8">
        <motion.button
          onClick={() => navigate(-1)}
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.9 }}
          aria-label="Previous module"
          className="w-10 h-10 rounded-full flex items-center justify-center bg-white/70 dark:bg-neutral-800/70 border border-neutral-200/60 dark:border-neutral-700/60 text-neutral-600 dark:text-neutral-400 backdrop-blur-sm hover:border-red-400/40 hover:text-red-500 dark:hover:text-red-400 transition-all"
        >
          <ArrowLeft className="w-4 h-4" />
        </motion.button>

        <div className="flex items-center gap-2">
          {MODULES.map((_, idx) => (
            <button
              key={idx}
              onClick={() => {
                setDirection(idx > active ? 1 : -1);
                setActive(idx);
              }}
              aria-label={`Go to module ${idx + 1}`}
              className={`rounded-full transition-all duration-300 ${
                idx === active
                  ? "w-6 h-2 bg-gradient-to-r from-red-500 to-rose-500"
                  : "w-2 h-2 bg-neutral-300 dark:bg-neutral-700 hover:bg-neutral-400 dark:hover:bg-neutral-600"
              }`}
            />
          ))}
        </div>

        <motion.button
          onClick={() => navigate(1)}
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.9 }}
          aria-label="Next module"
          className="w-10 h-10 rounded-full flex items-center justify-center bg-white/70 dark:bg-neutral-800/70 border border-neutral-200/60 dark:border-neutral-700/60 text-neutral-600 dark:text-neutral-400 backdrop-blur-sm hover:border-red-400/40 hover:text-red-500 dark:hover:text-red-400 transition-all"
        >
          <ArrowRight className="w-4 h-4" />
        </motion.button>
      </div>
    </section>
  );
}
