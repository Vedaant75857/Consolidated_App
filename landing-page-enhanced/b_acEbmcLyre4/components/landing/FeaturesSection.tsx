"use client";

import { motion } from "framer-motion";

const FEATURES = [
  {
    title: "AI-Powered Mapping",
    description:
      "Automatically map and classify procurement fields using large language models. Reduce manual effort by over 80% with intelligent column detection.",
    gradient: "from-red-500 to-rose-500",
    glow: "rgba(239,68,68,0.15)",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" className="w-6 h-6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M12 2L2 7l10 5 10-5-10-5z" />
        <path d="M2 17l10 5 10-5" />
        <path d="M2 12l10 5 10-5" />
      </svg>
    ),
  },
  {
    title: "Multi-Source Ingestion",
    description:
      "Pull data from ERPs, spreadsheets, databases, and APIs. Unify disparate formats into a single coherent schema without custom engineering.",
    gradient: "from-rose-500 to-red-600",
    glow: "rgba(225,29,72,0.15)",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" className="w-6 h-6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <circle cx="18" cy="5" r="3" />
        <circle cx="6" cy="12" r="3" />
        <circle cx="18" cy="19" r="3" />
        <path d="M8.59 13.51l6.83 3.98M15.41 6.51L8.59 10.49" />
      </svg>
    ),
  },
  {
    title: "Data Cleaning",
    description:
      "Detect and resolve duplicates, null values, mismatched data types, and outliers automatically. Keep your dataset audit-ready at all times.",
    gradient: "from-red-600 to-amber-500",
    glow: "rgba(239,68,68,0.12)",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" className="w-6 h-6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <polyline points="20 6 9 17 4 12" />
        <circle cx="12" cy="12" r="10" />
      </svg>
    ),
  },
  {
    title: "Smart Normalization",
    description:
      "Harmonize supplier names, country codes, date formats, currencies, and payment terms across thousands of rows with AI confidence scoring.",
    gradient: "from-amber-500 to-orange-500",
    glow: "rgba(245,158,11,0.12)",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" className="w-6 h-6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 6h16M4 12h16M4 18h7" />
        <path d="M15 15l5 5-5 5" />
      </svg>
    ),
  },
  {
    title: "Analytics Dashboards",
    description:
      "Visualize spend categories, supplier performance, and Pareto distribution through interactive charts. Filter, drill down, and export in one click.",
    gradient: "from-orange-500 to-red-500",
    glow: "rgba(249,115,22,0.12)",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" className="w-6 h-6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <rect x="3" y="3" width="18" height="18" rx="2" />
        <path d="M3 9h18M9 21V9" />
      </svg>
    ),
  },
  {
    title: "Generate Emails",
    description:
      "Auto-draft supplier outreach, RFQ responses, and data-request emails from your normalized dataset. Save hours on repetitive procurement communication.",
    gradient: "from-rose-600 to-rose-400",
    glow: "rgba(225,29,72,0.12)",
    icon: (
      <svg viewBox="0 0 24 24" fill="none" className="w-6 h-6" stroke="currentColor" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round">
        <path d="M4 4h16c1.1 0 2 .9 2 2v12c0 1.1-.9 2-2 2H4c-1.1 0-2-.9-2-2V6c0-1.1.9-2 2-2z" />
        <polyline points="22,6 12,13 2,6" />
      </svg>
    ),
  },
];

export default function FeaturesSection() {
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
            Capabilities
          </motion.p>
          <motion.h2
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.1 }}
            className="text-4xl font-bold tracking-tight text-neutral-900 dark:text-white text-balance"
          >
            Powerful Features
          </motion.h2>
          <motion.p
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.2 }}
            className="mt-3 text-neutral-500 dark:text-neutral-400 max-w-lg mx-auto text-balance"
          >
            Everything you need to turn raw procurement data into trusted, actionable intelligence.
          </motion.p>
        </div>

        {/* Grid */}
        <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-3 gap-5">
          {FEATURES.map((feat, idx) => (
            <motion.div
              key={feat.title}
              initial={{ opacity: 0, y: 28 }}
              whileInView={{ opacity: 1, y: 0 }}
              viewport={{ once: true, margin: "-30px" }}
              transition={{ duration: 0.55, delay: idx * 0.08 }}
              whileHover={{ y: -4, scale: 1.01 }}
              className="group relative rounded-2xl p-6 bg-white/60 dark:bg-neutral-900/40 backdrop-blur-sm border border-neutral-200/50 dark:border-neutral-700/30 overflow-hidden cursor-default transition-shadow duration-300"
              style={{ boxShadow: "0 1px 12px rgba(0,0,0,0.04)" }}
            >
              {/* Hover glow */}
              <motion.div
                className="absolute inset-0 rounded-2xl opacity-0 group-hover:opacity-100 transition-opacity duration-400 pointer-events-none"
                style={{ background: `radial-gradient(circle at 30% 30%, ${feat.glow}, transparent 70%)` }}
              />

              {/* Top gradient bar */}
              <div className={`absolute top-0 left-0 right-0 h-[2px] bg-gradient-to-r ${feat.gradient} opacity-0 group-hover:opacity-100 transition-opacity duration-300`} />

              {/* Icon */}
              <div className={`w-11 h-11 rounded-xl flex items-center justify-center mb-4 bg-gradient-to-br ${feat.gradient} text-white shadow-md relative z-10`}>
                {feat.icon}
              </div>

              {/* Text */}
              <h3 className="text-base font-bold text-neutral-900 dark:text-white mb-2 relative z-10">
                {feat.title}
              </h3>
              <p className="text-sm text-neutral-500 dark:text-neutral-400 leading-relaxed relative z-10">
                {feat.description}
              </p>
            </motion.div>
          ))}
        </div>
      </div>
    </section>
  );
}
