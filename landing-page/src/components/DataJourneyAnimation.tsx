import { useState, useEffect, useRef } from "react";
import { motion, AnimatePresence } from "motion/react";
import { ChevronLeft, ChevronRight } from "lucide-react";

type Stage = "stitching" | "normalizing" | "summarizing" | "complete";

export default function DataJourneyAnimation() {
  const [stage, setStage] = useState<Stage>("stitching");
  const [isDark, setIsDark] = useState(false);
  const [isAutoPlay, setIsAutoPlay] = useState(false);
  const [isInView, setIsInView] = useState(false);
  const sectionRef = useRef<HTMLDivElement>(null);

  const stages: Stage[] = ["stitching", "normalizing", "summarizing", "complete"];
  const currentIndex = stages.indexOf(stage);

  // Detect when section is in view
  useEffect(() => {
    const observer = new IntersectionObserver(
      ([entry]) => {
        if (entry.isIntersecting) {
          setIsInView(true);
          setStage("stitching"); // Reset to first stage
          setIsAutoPlay(true); // Start auto-play
        } else {
          setIsAutoPlay(false); // Pause when out of view
        }
      },
      { threshold: 0.3 } // Trigger when 30% of section is visible
    );

    if (sectionRef.current) {
      observer.observe(sectionRef.current);
    }

    return () => {
      if (sectionRef.current) {
        observer.unobserve(sectionRef.current);
      }
    };
  }, []);

  // Detect theme
  useEffect(() => {
    setIsDark(document.documentElement.classList.contains("dark"));
    const observer = new MutationObserver(() => {
      setIsDark(document.documentElement.classList.contains("dark"));
    });
    observer.observe(document.documentElement, {
      attributes: true,
      attributeFilter: ["class"],
    });
    return () => observer.disconnect();
  }, []);

  // Auto-play progression
  useEffect(() => {
    if (!isAutoPlay) return;

    const timings: Record<Stage, number> = {
      stitching: 4000,
      normalizing: 4000,
      summarizing: 3500,
      complete: 2000,
    };

    const timer = setTimeout(() => {
      const nextIndex = (currentIndex + 1) % stages.length;
      setStage(stages[nextIndex]);
    }, timings[stage]);

    return () => clearTimeout(timer);
  }, [stage, isAutoPlay, currentIndex, stages]);

  // Navigation handlers
  const handlePrevious = () => {
    const prevIndex = (currentIndex - 1 + stages.length) % stages.length;
    setStage(stages[prevIndex]);
  };

  const handleNext = () => {
    const nextIndex = (currentIndex + 1) % stages.length;
    setStage(stages[nextIndex]);
  };

  const handleStageClick = (clickedStage: Stage) => {
    setStage(clickedStage);
  };

  const bgColor = isDark ? "bg-neutral-900/40" : "bg-neutral-100/40";
  const borderColor = isDark ? "border-white/10" : "border-neutral-300";
  const headerBg = isDark ? "bg-neutral-800/60" : "bg-neutral-300/40";
  const textColor = isDark ? "text-neutral-200" : "text-neutral-800";
  const mutedText = isDark ? "text-neutral-500" : "text-neutral-600";

  return (
    <section ref={sectionRef} className={`relative py-24 px-6 ${bgColor}`}>
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
            Live Data Processing
          </motion.p>
          <motion.h2
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.1 }}
            className="text-4xl font-bold tracking-tight text-neutral-900 dark:text-white text-balance mb-3"
          >
            Watch Your Data Transform
          </motion.h2>
          <motion.p
            initial={{ opacity: 0, y: 16 }}
            whileInView={{ opacity: 1, y: 0 }}
            viewport={{ once: true }}
            transition={{ duration: 0.5, delay: 0.2 }}
            className={`${mutedText} max-w-lg mx-auto text-balance`}
          >
            See how your procurement data flows through our three-stage pipeline
            in real-time.
          </motion.p>
        </div>

        {/* Stage Indicator */}
        <div className="flex items-center justify-center mb-12">
          {/* Stage Badges */}
          <div className="flex items-center gap-4">
            {[
              { label: "Stitch", stage: "stitching" as const },
              { label: "Normalize", stage: "normalizing" as const },
              { label: "Summarize", stage: "summarizing" as const },
            ].map((item, i) => {
              const isActive = stage === item.stage;
              const isComplete =
                stages.indexOf(stage) > stages.indexOf(item.stage) ||
                stage === "complete";
              return (
                <div key={item.stage} className="flex items-center">
                  <motion.button
                    onClick={() => handleStageClick(item.stage)}
                    className={`w-9 h-9 rounded-full flex items-center justify-center text-xs font-bold transition-all cursor-pointer ${
                      isActive
                        ? "bg-gradient-to-br from-red-500 to-rose-500 text-white shadow-lg"
                        : isComplete
                        ? "bg-emerald-500 text-white"
                        : isDark
                        ? "bg-neutral-700 text-neutral-400 hover:bg-neutral-600"
                        : "bg-neutral-300 text-neutral-600 hover:bg-neutral-400"
                    }`}
                    animate={
                      isActive
                        ? { scale: [1, 1.1, 1] }
                        : { scale: 1 }
                    }
                    transition={{ duration: 1, repeat: Infinity }}
                    whileHover={{ scale: isActive ? undefined : 1.05 }}
                  >
                    {isComplete ? "✓" : i + 1}
                  </motion.button>
                  <p className={`text-sm font-medium ml-2 ${textColor}`}>
                    {item.label}
                  </p>
                  {i < 2 && (
                    <div
                      className={`w-12 h-0.5 mx-4 transition-all ${
                        isComplete
                          ? "bg-emerald-500"
                          : isDark
                          ? "bg-neutral-700"
                          : "bg-neutral-300"
                      }`}
                    />
                  )}
                </div>
              );
            })}
          </div>
        </div>

        {/* Animation Container with Side Navigation */}
        <div className="flex items-center gap-4 justify-center">
          {/* Left Arrow */}
          <motion.button
            onClick={handlePrevious}
            whileHover={{ scale: 1.1 }}
            whileTap={{ scale: 0.9 }}
            className={`p-3 rounded-lg transition-all flex-shrink-0 ${
              isDark
                ? "bg-neutral-800 hover:bg-neutral-700 text-neutral-400 hover:text-neutral-200"
                : "bg-neutral-200 hover:bg-neutral-300 text-neutral-600 hover:text-neutral-800"
            }`}
            aria-label="Previous stage"
          >
            <ChevronLeft className="w-6 h-6" />
          </motion.button>

          {/* Card */}
          <div className={`rounded-2xl border ${borderColor} overflow-hidden flex-1 max-w-5xl`}>
          <AnimatePresence mode="wait">
            {/* Stage 1: Stitching */}
            {stage === "stitching" && (
              <StitchingStage
                isDark={isDark}
                headerBg={headerBg}
                borderColor={borderColor}
                textColor={textColor}
                mutedText={mutedText}
              />
            )}

            {/* Stage 2: Normalizing */}
            {stage === "normalizing" && (
              <NormalizingStage
                isDark={isDark}
                headerBg={headerBg}
                borderColor={borderColor}
                textColor={textColor}
                mutedText={mutedText}
              />
            )}

            {/* Stage 3: Summarizing */}
            {stage === "summarizing" && (
              <SummarizingStage
                isDark={isDark}
                headerBg={headerBg}
                borderColor={borderColor}
                textColor={textColor}
                mutedText={mutedText}
              />
            )}

            {/* Complete State */}
            {stage === "complete" && (
              <CompleteStage
                isDark={isDark}
                headerBg={headerBg}
                borderColor={borderColor}
                textColor={textColor}
                mutedText={mutedText}
              />
            )}
          </AnimatePresence>
          </div>

          {/* Right Arrow */}
          <motion.button
            onClick={handleNext}
            whileHover={{ scale: 1.1 }}
            whileTap={{ scale: 0.9 }}
            className={`p-3 rounded-lg transition-all flex-shrink-0 ${
              isDark
                ? "bg-neutral-800 hover:bg-neutral-700 text-neutral-400 hover:text-neutral-200"
                : "bg-neutral-200 hover:bg-neutral-300 text-neutral-600 hover:text-neutral-800"
            }`}
            aria-label="Next stage"
          >
            <ChevronRight className="w-6 h-6" />
          </motion.button>
        </div>

        {/* Footer Text */}
        <motion.p
          className={`text-center text-xs ${mutedText} mt-6`}
        >
          Processing 47 supplier records • 3 data sources • ~2.3 seconds total
        </motion.p>
      </div>
    </section>
  );
}

/* ─── Stage 1: Stitching ─────────────────────────────────────────── */

function StitchingStage({ isDark, headerBg, borderColor, textColor, mutedText }: any) {
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.3 }}
      className={`p-6 ${isDark ? "bg-neutral-900/50" : "bg-white/50"}`}
    >
      <div className="mb-4">
        <p className={`text-sm font-semibold ${textColor} mb-2`}>
          Stage 1: Merging Data Sources
        </p>
        <div className="flex gap-4 mb-4">
          {/* Source 1 */}
          <div className="flex-1">
            <p className={`text-xs font-medium ${mutedText} mb-2`}>
              orders_Q1.csv
            </p>
            <div className={`border ${borderColor} rounded-lg overflow-hidden`}>
              <div className={`${headerBg} border-b ${borderColor}`}>
                <div className="grid grid-cols-3 text-[10px] font-bold">
                  <span className={`px-3 py-2 border-r ${borderColor}`}>SUPPLIER_NAME</span>
                  <span className={`px-3 py-2 border-r ${borderColor}`}>DATE</span>
                  <span className={`px-3 py-2`}>AMOUNT_USD</span>
                </div>
              </div>
              {[
                ["ACME Corp.", "03/15/2024", "4500"],
                ["TechSupply Inc", "01/20/2024", "2300"],
              ].map((row, i) => (
                <motion.div
                  key={i}
                  initial={{ opacity: 0, x: -10 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: i * 0.2 }}
                  className={`grid grid-cols-3 border-b ${borderColor} text-[10px]`}
                >
                  {row.map((cell, j) => (
                    <span key={j} className={`px-3 py-2 ${j < 2 ? `border-r ${borderColor}` : ""} ${textColor}`}>
                      {cell}
                    </span>
                  ))}
                </motion.div>
              ))}
            </div>
          </div>

          {/* Arrow */}
          <div className="flex items-center">
            <motion.div
              animate={{ x: [0, 4, 0] }}
              transition={{ duration: 1.5, repeat: Infinity }}
              className="text-red-500"
            >
              →
            </motion.div>
          </div>

          {/* Source 2 */}
          <div className="flex-1">
            <p className={`text-xs font-medium ${mutedText} mb-2`}>
              orders_Q2.csv
            </p>
            <div className={`border ${borderColor} rounded-lg overflow-hidden`}>
              <div className={`${headerBg} border-b ${borderColor}`}>
                <div className="grid grid-cols-3 text-[10px] font-bold">
                  <span className={`px-3 py-2 border-r ${borderColor}`}>VENDOR</span>
                  <span className={`px-3 py-2 border-r ${borderColor}`}>INVOICE_DATE</span>
                  <span className={`px-3 py-2`}>TOTAL</span>
                </div>
              </div>
              {[
                ["Apple Inc", "02/10/2024", "1850"],
                ["GlobalMfg", "02/28/2024", "9100"],
              ].map((row, i) => (
                <motion.div
                  key={i}
                  initial={{ opacity: 0, x: 10 }}
                  animate={{ opacity: 1, x: 0 }}
                  transition={{ delay: i * 0.2 }}
                  className={`grid grid-cols-3 border-b ${borderColor} text-[10px]`}
                >
                  {row.map((cell, j) => (
                    <span key={j} className={`px-3 py-2 ${j < 2 ? `border-r ${borderColor}` : ""} ${textColor}`}>
                      {cell}
                    </span>
                  ))}
                </motion.div>
              ))}
            </div>
          </div>
        </div>

        {/* Arrow to result */}
        <div className="flex justify-center mb-4">
          <motion.div
            animate={{ y: [0, 4, 0] }}
            transition={{ duration: 1.5, repeat: Infinity }}
            className="text-red-500"
          >
            ↓
          </motion.div>
        </div>

        {/* Result */}
        <p className={`text-xs font-medium ${mutedText} mb-2`}>
          Unified Dataset
        </p>
        <div className={`border ${borderColor} rounded-lg overflow-hidden`}>
          <div className={`${headerBg} border-b ${borderColor}`}>
            <div className="grid grid-cols-4 text-[10px] font-bold">
              <span className={`px-3 py-2 border-r ${borderColor}`}>Supplier</span>
              <span className={`px-3 py-2 border-r ${borderColor}`}>Date</span>
              <span className={`px-3 py-2 border-r ${borderColor}`}>Amount</span>
              <span className={`px-3 py-2`}>Source</span>
            </div>
          </div>
          {[
            ["ACME Corp.", "15-03-2024", "$4,500", "Q1"],
            ["TechSupply Inc", "20-01-2024", "$2,300", "Q1"],
            ["Apple Inc", "10-02-2024", "$1,850", "Q2"],
            ["GlobalMfg", "28-02-2024", "$9,100", "Q2"],
          ].map((row, i) => (
            <motion.div
              key={i}
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 1.5 + i * 0.15 }}
              className={`grid grid-cols-4 border-b ${borderColor} text-[10px]`}
            >
              {row.map((cell, j) => (
                <span key={j} className={`px-3 py-2 ${j < 3 ? `border-r ${borderColor}` : ""} ${textColor}`}>
                  {cell}
                </span>
              ))}
            </motion.div>
          ))}
        </div>
      </div>
    </motion.div>
  );
}

/* ─── Stage 2: Normalizing ──────────────────────────────────────── */

function NormalizingStage({ isDark, headerBg, borderColor, textColor, mutedText }: any) {
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.3 }}
      className={`p-6 ${isDark ? "bg-neutral-900/50" : "bg-white/50"}`}
    >
      <div className="mb-4">
        <p className={`text-sm font-semibold ${textColor} mb-4`}>
          Stage 2: Standardizing & Cleaning Data
        </p>

        {/* Before & After */}
        <div className="grid grid-cols-2 gap-4">
          {/* Before */}
          <div>
            <p className={`text-xs font-medium ${mutedText} mb-2`}>
              Before Normalization
            </p>
            <div className={`border ${borderColor} rounded-lg overflow-hidden`}>
              <div className={`${headerBg} border-b ${borderColor}`}>
                <div className="grid grid-cols-3 text-[10px] font-bold">
                  <span className={`px-3 py-2 border-r ${borderColor}`}>Supplier</span>
                  <span className={`px-3 py-2 border-r ${borderColor}`}>Date</span>
                  <span className={`px-3 py-2`}>Currency</span>
                </div>
              </div>
              {[
                ["ACME Corp.", "03/15/2024", "USD 4500"],
                ["apple inc", "2024-01-20", "EUR 1200"],
                ["Siemens AG", "Jan 5, 2024", "8900 CHF"],
              ].map((row, i) => (
                <motion.div
                  key={i}
                  initial={{ opacity: 0, y: -10 }}
                  animate={{ opacity: 1, y: 0 }}
                  transition={{ delay: i * 0.15 }}
                  className={`grid grid-cols-3 border-b ${borderColor} text-[10px]`}
                >
                  {row.map((cell, j) => (
                    <span key={j} className={`px-3 py-2 ${j < 2 ? `border-r ${borderColor}` : ""} text-amber-500/80`}>
                      {cell}
                    </span>
                  ))}
                </motion.div>
              ))}
            </div>
          </div>

        </div>

        {/* After */}
        <div className="mt-4">
          <p className={`text-xs font-medium ${mutedText} mb-2`}>
            After Normalization
          </p>
          <div className={`border ${borderColor} rounded-lg overflow-hidden`}>
            <div className={`${headerBg} border-b ${borderColor}`}>
              <div className="grid grid-cols-4 text-[10px] font-bold">
                <span className={`px-3 py-2 border-r ${borderColor}`}>Supplier</span>
                <span className={`px-3 py-2 border-r ${borderColor}`}>Date (STD)</span>
                <span className={`px-3 py-2 border-r ${borderColor}`}>Amount USD</span>
                <span className={`px-3 py-2`}>Confidence</span>
              </div>
            </div>
            {[
              ["Acme Corporation", "15-03-2024", "$4,500", "98%"],
              ["Apple Inc.", "20-01-2024", "$1,345", "99%"],
              ["Siemens AG", "05-01-2024", "$9,850", "97%"],
            ].map((row, i) => (
              <motion.div
                key={i}
                initial={{ opacity: 0, y: 10 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 1.2 + i * 0.15 }}
                className={`grid grid-cols-4 border-b ${borderColor} text-[10px]`}
              >
                {row.map((cell, j) => (
                  <span
                    key={j}
                    className={`px-3 py-2 ${j < 3 ? `border-r ${borderColor}` : ""} ${
                      j === 3
                        ? "text-emerald-500 font-medium"
                        : textColor
                    }`}
                  >
                    {cell}
                  </span>
                ))}
              </motion.div>
            ))}
          </div>
        </div>
      </div>
    </motion.div>
  );
}

/* ─── Stage 3: Summarizing ──────────────────────────────────────── */

function SummarizingStage({ isDark, headerBg, borderColor, textColor, mutedText }: any) {
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.3 }}
      className={`p-6 ${isDark ? "bg-neutral-900/50" : "bg-white/50"}`}
    >
      <div className="mb-4">
        <p className={`text-sm font-semibold ${textColor} mb-4`}>
          Stage 3: Generating Insights & Summaries
        </p>

        {/* KPIs */}
        <div className="grid grid-cols-4 gap-3 mb-4">
          {[
            { label: "Total Spend", value: "$28,450", color: "from-red-500 to-rose-500" },
            { label: "Suppliers", value: "12", color: "from-rose-500 to-red-700" },
            { label: "Avg per Supplier", value: "$2,371", color: "from-amber-500 to-orange-600" },
            { label: "Data Quality", value: "99.2%", color: "from-emerald-500 to-green-600" },
          ].map((kpi, i) => (
            <motion.div
              key={kpi.label}
              initial={{ opacity: 0, scale: 0.8 }}
              animate={{ opacity: 1, scale: 1 }}
              transition={{ delay: i * 0.15 }}
              className={`rounded-lg bg-gradient-to-br ${kpi.color} p-3 text-center`}
            >
              <p className="text-[10px] text-white/80">{kpi.label}</p>
              <p className="text-lg font-bold text-white">{kpi.value}</p>
            </motion.div>
          ))}
        </div>

        {/* Summary Table */}
        <p className={`text-xs font-medium ${mutedText} mb-2`}>
          Top Suppliers by Spend
        </p>
        <div className={`border ${borderColor} rounded-lg overflow-hidden`}>
          <div className={`${headerBg} border-b ${borderColor}`}>
            <div className="grid grid-cols-4 text-[10px] font-bold">
              <span className={`px-3 py-2 border-r ${borderColor}`}>Rank</span>
              <span className={`px-3 py-2 border-r ${borderColor}`}>Supplier</span>
              <span className={`px-3 py-2 border-r ${borderColor}`}>Spend</span>
              <span className={`px-3 py-2`}>% of Total</span>
            </div>
          </div>
          {[
            ["1", "Acme Corporation", "$9,500", "33.4%"],
            ["2", "GlobalMfg Ltd", "$7,850", "27.6%"],
            ["3", "TechSupply Inc", "$5,200", "18.3%"],
            ["4", "Apple Inc.", "$3,900", "13.7%"],
          ].map((row, i) => (
            <motion.div
              key={i}
              initial={{ opacity: 0, x: -20 }}
              animate={{ opacity: 1, x: 0 }}
              transition={{ delay: 0.8 + i * 0.15 }}
              className={`grid grid-cols-4 border-b ${borderColor} text-[10px] ${
                i === 0 ? (isDark ? "bg-red-500/10" : "bg-red-50") : ""
              }`}
            >
              {row.map((cell, j) => (
                <span
                  key={j}
                  className={`px-3 py-2 ${j < 3 ? `border-r ${borderColor}` : ""} ${
                    j === 3
                      ? "font-medium text-amber-500"
                      : j === 0
                      ? "font-bold text-red-500"
                      : textColor
                  }`}
                >
                  {cell}
                </span>
              ))}
            </motion.div>
          ))}
        </div>
      </div>
    </motion.div>
  );
}

/* ─── Complete State ────────────────────────────────────────────── */

function CompleteStage({ isDark, headerBg, borderColor, textColor, mutedText }: any) {
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      exit={{ opacity: 0 }}
      transition={{ duration: 0.3 }}
      className={`p-6 ${isDark ? "bg-neutral-900/50" : "bg-white/50"}`}
    >
      <div className="text-center py-8">
        <motion.div
          initial={{ scale: 0 }}
          animate={{ scale: 1 }}
          transition={{ duration: 0.4, delay: 0.2 }}
          className="w-16 h-16 rounded-full bg-gradient-to-br from-emerald-500 to-green-600 flex items-center justify-center mx-auto mb-4"
        >
          <span className="text-3xl">✓</span>
        </motion.div>
        <motion.h3
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.3 }}
          className={`text-xl font-bold ${textColor} mb-2`}
        >
          Processing Complete!
        </motion.h3>
        <motion.p
          initial={{ opacity: 0, y: 10 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ delay: 0.4 }}
          className={mutedText}
        >
          47 records processed • 3 sources unified • 99.2% data quality
        </motion.p>
        <motion.div
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.6 }}
          className="mt-6 space-y-2"
        >
          <p className={`text-sm font-medium ${textColor}`}>Ready to:</p>
          <ul className={`text-sm ${mutedText} space-y-1`}>
            <li>✓ Export clean datasets</li>
            <li>✓ Generate spend reports</li>
            <li>✓ Perform advanced analysis</li>
          </ul>
        </motion.div>
      </div>
    </motion.div>
  );
}
