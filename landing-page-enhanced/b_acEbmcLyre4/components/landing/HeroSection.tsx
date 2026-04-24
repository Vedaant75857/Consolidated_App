"use client";

import { useState, useEffect, useRef } from "react";
import { ArrowRight, Sun, Moon } from "lucide-react";
import { motion, AnimatePresence, useMotionValue, useMotionTemplate } from "framer-motion";

/* ── Module data ─────────────────────────────────────────────────── */

const APPS = [
  {
    title: "Data Stitcher",
    description:
      "Consolidate and merge multiple data sources into a unified dataset. Upload, append, normalize headers, clean, and merge — all in one guided pipeline.",
    gradient: "from-red-600 to-rose-600",
    shadowColor: "shadow-red-200/40 dark:shadow-red-900/30",
    accentText: "text-red-600 dark:text-red-400",
    accentBg: "bg-red-50 dark:bg-red-950/30",
    accentRgb: "239, 68, 68",
    tag: "Module 1",
    icon: (
      <svg viewBox="0 0 32 32" fill="none" className="w-8 h-8">
        <rect x="4" y="6" width="10" height="12" rx="2" fill="white" opacity="0.8" />
        <rect x="4" y="21" width="10" height="5" rx="2" fill="white" opacity="0.5" />
        <rect x="18" y="6" width="10" height="12" rx="2" fill="white" opacity="0.6" />
        <path d="M14 12 L18 12" stroke="white" strokeWidth="1.5" strokeLinecap="round" />
        <rect x="9" y="24" width="14" height="4" rx="2" fill="white" opacity="0.9" />
      </svg>
    ),
  },
  {
    title: "Data Normalizer",
    description:
      "Normalize supplier names, countries, dates, payment terms, regions, plants, and currencies with AI-powered transformations.",
    gradient: "from-rose-600 to-red-700",
    shadowColor: "shadow-rose-200/40 dark:shadow-rose-900/30",
    accentText: "text-rose-600 dark:text-rose-400",
    accentBg: "bg-rose-50 dark:bg-rose-950/30",
    accentRgb: "225, 29, 72",
    tag: "Module 2",
    icon: (
      <svg viewBox="0 0 32 32" fill="none" className="w-8 h-8">
        <polygon points="16,4 28,28 4,28" fill="white" opacity="0.15" stroke="white" strokeWidth="1.5" />
        <line x1="16" y1="10" x2="16" y2="22" stroke="white" strokeWidth="1.5" strokeLinecap="round" />
        <line x1="10" y1="22" x2="22" y2="22" stroke="white" strokeWidth="1.5" strokeLinecap="round" />
      </svg>
    ),
  },
  {
    title: "Spend Summarizer",
    description:
      "Upload procurement data, map columns with AI, and generate interactive spend dashboards with charts, Pareto analysis, and exportable PDF reports.",
    gradient: "from-amber-600 to-orange-600",
    shadowColor: "shadow-amber-200/40 dark:shadow-amber-900/30",
    accentText: "text-amber-600 dark:text-amber-400",
    accentBg: "bg-amber-50 dark:bg-amber-950/30",
    accentRgb: "217, 119, 6",
    tag: "Module 3",
    icon: (
      <svg viewBox="0 0 32 32" fill="none" className="w-8 h-8">
        <rect x="4" y="18" width="6" height="10" rx="1" fill="white" opacity="0.8" />
        <rect x="13" y="12" width="6" height="16" rx="1" fill="white" opacity="0.9" />
        <rect x="22" y="7" width="6" height="21" rx="1" fill="white" opacity="0.7" />
        <path d="M7 17 L16 11 L25 6" stroke="white" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" opacity="0.9" />
      </svg>
    ),
  },
];

/* ── Floating orbs ─────────────────────────────────────────────── */

const ORBS = [
  { size: 700, x: [0, 100, -50, 0], y: [0, -80, 60, 0], className: "bg-red-300/30 dark:bg-red-800/15", duration: 22, style: { left: "-10%", top: "-15%" } },
  { size: 500, x: [0, -120, 80, 0], y: [0, 60, -100, 0], className: "bg-rose-400/25 dark:bg-rose-800/10", duration: 18, style: { right: "-5%", top: "20%" } },
  { size: 400, x: [0, 60, -80, 0], y: [0, 100, -40, 0], className: "bg-amber-300/15 dark:bg-amber-800/8", duration: 25, style: { left: "15%", bottom: "-10%" } },
  { size: 300, x: [0, -50, 100, 0], y: [0, -60, 80, 0], className: "bg-red-500/20 dark:bg-red-900/10", duration: 20, style: { right: "20%", bottom: "5%" } },
];

const PARTICLES = Array.from({ length: 14 }, (_, i) => {
  const seed = i * 137.508;
  return {
    id: i,
    size: 2 + (i % 3) * 1.5,
    startX: `${(seed % 100).toFixed(0)}%`,
    startY: `${((i * 23.7) % 100).toFixed(0)}%`,
    driftX: [0, 30 - (i % 5) * 12, -(20 - (i % 4) * 10), 0],
    driftY: [0, -(25 + (i % 3) * 10), 20 - (i % 4) * 8, 0],
    duration: 16 + (i % 5) * 4,
    delay: i * 0.8,
    opacity: 0.15 + (i % 4) * 0.08,
  };
});

function FloatingParticles() {
  return (
    <>
      {PARTICLES.map((p) => (
        <motion.div
          key={p.id}
          className="absolute rounded-full bg-red-400 dark:bg-red-500"
          style={{ width: p.size, height: p.size, left: p.startX, top: p.startY }}
          animate={{ x: p.driftX, y: p.driftY, opacity: [0, p.opacity, p.opacity, 0] }}
          transition={{ duration: p.duration, repeat: Infinity, ease: "easeInOut", delay: p.delay }}
        />
      ))}
    </>
  );
}

/* ── Data flow SVG animation ───────────────────────────────────── */

function DataFlowAnimation() {
  const cycle = 8;
  const c = { f1: "#ef4444", f2: "#e11d48", f3: "#f59e0b", tbl: "#dc2626", chart: "#e11d48", acc: "#f97316" };
  return (
    <motion.div
      initial={{ opacity: 0 }}
      animate={{ opacity: 1 }}
      transition={{ delay: 0.6, duration: 0.8 }}
      className="flex justify-center mt-6 mb-2"
    >
      <svg viewBox="0 0 400 80" className="w-[400px] h-[80px]" fill="none">
        {/* Files */}
        <motion.g animate={{ opacity: [0, 1, 1, 0, 0, 0, 0] }} transition={{ duration: cycle, repeat: Infinity, times: [0, 0.08, 0.28, 0.35, 0.5, 0.92, 1], ease: "easeInOut" }}>
          <rect x="12" y="10" width="22" height="28" rx="3" fill={c.f1} opacity="0.8" />
          <rect x="16" y="18" width="14" height="2" rx="1" fill="white" opacity="0.6" />
          <rect x="16" y="23" width="10" height="2" rx="1" fill="white" opacity="0.4" />
          <rect x="16" y="28" width="12" height="2" rx="1" fill="white" opacity="0.3" />
        </motion.g>
        <motion.g animate={{ opacity: [0, 1, 1, 0, 0, 0, 0] }} transition={{ duration: cycle, repeat: Infinity, times: [0, 0.1, 0.28, 0.35, 0.5, 0.92, 1], ease: "easeInOut" }}>
          <rect x="40" y="26" width="22" height="28" rx="3" fill={c.f2} opacity="0.8" />
          <rect x="44" y="34" width="14" height="2" rx="1" fill="white" opacity="0.6" />
          <rect x="44" y="39" width="10" height="2" rx="1" fill="white" opacity="0.4" />
          <rect x="44" y="44" width="12" height="2" rx="1" fill="white" opacity="0.3" />
        </motion.g>
        <motion.g animate={{ opacity: [0, 1, 1, 0, 0, 0, 0] }} transition={{ duration: cycle, repeat: Infinity, times: [0, 0.12, 0.28, 0.35, 0.5, 0.92, 1], ease: "easeInOut" }}>
          <rect x="68" y="14" width="22" height="28" rx="3" fill={c.f3} opacity="0.8" />
          <rect x="72" y="22" width="14" height="2" rx="1" fill="white" opacity="0.6" />
          <rect x="72" y="27" width="10" height="2" rx="1" fill="white" opacity="0.4" />
          <rect x="72" y="32" width="12" height="2" rx="1" fill="white" opacity="0.3" />
        </motion.g>
        {/* Arrow 1 */}
        <motion.g animate={{ opacity: [0, 0, 0, 1, 1, 0, 0] }} transition={{ duration: cycle, repeat: Infinity, times: [0, 0.25, 0.3, 0.35, 0.48, 0.52, 1], ease: "easeInOut" }}>
          <path d="M100 40 L122 40" stroke={c.tbl} strokeWidth="2" strokeLinecap="round" opacity="0.6" />
          <path d="M118 35 L126 40 L118 45" stroke={c.tbl} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" fill="none" opacity="0.6" />
        </motion.g>
        {/* Table */}
        <motion.g animate={{ opacity: [0, 0, 0, 0, 1, 1, 0], scale: [0.85, 0.85, 0.85, 0.85, 1, 1, 0.85] }} transition={{ duration: cycle, repeat: Infinity, times: [0, 0.32, 0.38, 0.42, 0.5, 0.68, 0.75], ease: "easeInOut" }}>
          <rect x="140" y="15" width="55" height="50" rx="4" fill={c.tbl} opacity="0.12" />
          <rect x="140" y="15" width="55" height="50" rx="4" stroke={c.tbl} strokeWidth="1.5" opacity="0.45" fill="none" />
          <rect x="140" y="15" width="55" height="11" rx="4" fill={c.tbl} opacity="0.25" />
          <line x1="143" y1="34" x2="192" y2="34" stroke={c.tbl} strokeWidth="0.8" opacity="0.3" />
          <line x1="143" y1="43" x2="192" y2="43" stroke={c.tbl} strokeWidth="0.8" opacity="0.3" />
          <line x1="143" y1="52" x2="192" y2="52" stroke={c.tbl} strokeWidth="0.8" opacity="0.3" />
          <line x1="167" y1="17" x2="167" y2="63" stroke={c.tbl} strokeWidth="0.8" opacity="0.2" />
        </motion.g>
        {/* Arrow 2 */}
        <motion.g animate={{ opacity: [0, 0, 0, 0, 0, 1, 1, 0] }} transition={{ duration: cycle, repeat: Infinity, times: [0, 0.5, 0.6, 0.64, 0.66, 0.7, 0.78, 0.82], ease: "easeInOut" }}>
          <path d="M210 40 L232 40" stroke={c.chart} strokeWidth="2" strokeLinecap="round" opacity="0.6" />
          <path d="M228 35 L236 40 L228 45" stroke={c.chart} strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" fill="none" opacity="0.6" />
        </motion.g>
        {/* Bar chart */}
        <motion.g animate={{ opacity: [0, 0, 0, 0, 0, 0, 1, 1, 0] }} transition={{ duration: cycle, repeat: Infinity, times: [0, 0.5, 0.6, 0.65, 0.68, 0.72, 0.78, 0.92, 1], ease: "easeInOut" }}>
          <line x1="255" y1="15" x2="255" y2="65" stroke={c.chart} strokeWidth="1.5" opacity="0.4" />
          <line x1="253" y1="65" x2="375" y2="65" stroke={c.chart} strokeWidth="1.5" opacity="0.4" />
          <motion.rect x="264" width="20" rx="2" fill={c.f1} opacity="0.8" animate={{ y: [65,65,65,65,65,65,38,38,65], height: [0,0,0,0,0,0,27,27,0] }} transition={{ duration: cycle, repeat: Infinity, times: [0,0.5,0.6,0.65,0.7,0.74,0.82,0.92,1], ease: "easeOut" }} />
          <motion.rect x="294" width="20" rx="2" fill={c.chart} opacity="0.8" animate={{ y: [65,65,65,65,65,65,24,24,65], height: [0,0,0,0,0,0,41,41,0] }} transition={{ duration: cycle, repeat: Infinity, times: [0,0.5,0.6,0.65,0.7,0.76,0.84,0.92,1], ease: "easeOut" }} />
          <motion.rect x="324" width="20" rx="2" fill={c.acc} opacity="0.8" animate={{ y: [65,65,65,65,65,65,30,30,65], height: [0,0,0,0,0,0,35,35,0] }} transition={{ duration: cycle, repeat: Infinity, times: [0,0.5,0.6,0.65,0.7,0.78,0.86,0.92,1], ease: "easeOut" }} />
          <motion.path d="M274 42 L304 28 L334 33" stroke="white" strokeWidth="1.5" strokeLinecap="round" strokeLinejoin="round" fill="none" animate={{ opacity: [0,0,0,0,0,0,0,0.5,0], pathLength: [0,0,0,0,0,0,0,1,0] }} transition={{ duration: cycle, repeat: Infinity, times: [0,0.5,0.6,0.65,0.7,0.78,0.84,0.9,1], ease: "easeOut" }} />
        </motion.g>
        {/* Flow particles */}
        {[0, 1, 2].map((i) => (
          <motion.circle key={`flow-${i}`} r="1.5" fill={c.f1} animate={{ cx: [50, 115, 170, 225, 310, 370], cy: [40, 38, 40, 38, 40, 40], opacity: [0, 0.5, 0.4, 0.5, 0.4, 0] }} transition={{ duration: cycle * 0.55, repeat: Infinity, ease: "linear", delay: i * 0.8 }} />
        ))}
      </svg>
    </motion.div>
  );
}

/* ── App card ──────────────────────────────────────────────────── */

function AppCard({ app, idx }: { app: typeof APPS[number]; idx: number }) {
  const cardRef = useRef<HTMLDivElement>(null);
  const mouseX = useMotionValue(0);
  const mouseY = useMotionValue(0);
  const spotlightBg = useMotionTemplate`radial-gradient(600px circle at ${mouseX}px ${mouseY}px, rgba(${app.accentRgb}, 0.12), transparent 80%)`;

  function handleMouseMove(e: React.MouseEvent) {
    if (!cardRef.current) return;
    const rect = cardRef.current.getBoundingClientRect();
    mouseX.set(e.clientX - rect.left);
    mouseY.set(e.clientY - rect.top);
  }

  return (
    <motion.div
      initial={{ opacity: 0, y: 40, rotateX: 15 }}
      animate={{ opacity: 1, y: 0, rotateX: 0 }}
      transition={{ type: "spring", stiffness: 100, damping: 20, delay: 0.6 + idx * 0.2 }}
    >
      <motion.div
        ref={cardRef}
        onMouseMove={handleMouseMove}
        whileHover={{ y: -4, scale: 1.01 }}
        whileTap={{ scale: 0.99 }}
        className="group block relative cursor-pointer"
      >
        <div className={`absolute inset-0 rounded-2xl bg-gradient-to-br ${app.gradient} opacity-30 group-hover:opacity-70 transition-opacity duration-500`} />
        <div className="relative m-[1px] rounded-2xl bg-white/60 dark:bg-neutral-900/60 backdrop-blur-2xl border-t border-white/20 p-8 overflow-hidden">
          <motion.div
            className="pointer-events-none absolute inset-0 rounded-2xl z-0 opacity-0 group-hover:opacity-100 transition-opacity duration-300"
            style={{ background: spotlightBg }}
          />
          <div className="shimmer-sweep" />
          <div className="flex items-start gap-5 relative z-10">
            <div className="relative shrink-0">
              <motion.div
                animate={{ y: [0, -6, 0] }}
                transition={{ duration: 3, repeat: Infinity, ease: "easeInOut" }}
                className={`w-14 h-14 rounded-2xl bg-gradient-to-br ${app.gradient} flex items-center justify-center shadow-lg ${app.shadowColor}`}
              >
                {app.icon}
              </motion.div>
            </div>
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-3 mb-2">
                <h2 className="text-xl font-bold tracking-tight text-neutral-900 dark:text-white">
                  {app.title}
                </h2>
                <span className={`inline-flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-widest px-2.5 py-1 rounded-full border-pulse ${app.accentBg} ${app.accentText}`}>
                  <motion.span
                    className="w-1.5 h-1.5 rounded-full bg-current"
                    animate={{ opacity: [1, 0.3, 1] }}
                    transition={{ duration: 2, repeat: Infinity, ease: "easeInOut" }}
                  />
                  {app.tag}
                </span>
              </div>
              <p className="text-sm text-neutral-500 dark:text-neutral-400 leading-relaxed">
                {app.description}
              </p>
            </div>
            <div className="shrink-0 mt-2">
              <motion.div whileHover={{ scale: 1.1 }} className="w-10 h-10 rounded-xl flex items-center justify-center relative overflow-hidden">
                <div className="absolute inset-0 bg-neutral-100 dark:bg-neutral-800 transition-opacity duration-300 group-hover:opacity-0" />
                <div className={`absolute inset-0 bg-gradient-to-br ${app.gradient} opacity-0 group-hover:opacity-100 transition-opacity duration-300`} />
                <ArrowRight className={`w-5 h-5 relative z-10 ${app.accentText} group-hover:text-white transition-all duration-300 group-hover:-rotate-45`} />
              </motion.div>
            </div>
          </div>
        </div>
      </motion.div>
    </motion.div>
  );
}

/* ── Hero section ──────────────────────────────────────────────── */

interface HeroSectionProps {
  theme: "light" | "dark";
  onToggleTheme: () => void;
}

export default function HeroSection({ theme, onToggleTheme }: HeroSectionProps) {
  const [toggleHover, setToggleHover] = useState(false);

  return (
    <section className="relative min-h-screen flex flex-col items-center justify-center px-6 py-16 overflow-hidden">
      {/* Orbs */}
      <div className="pointer-events-none absolute inset-0 z-0">
        {ORBS.map((orb, i) => (
          <motion.div
            key={i}
            className={`absolute rounded-full ${orb.className} blur-3xl`}
            style={{ width: orb.size, height: orb.size, ...orb.style }}
            animate={{ x: orb.x, y: orb.y, opacity: [0.6, 1, 0.6] }}
            transition={{ duration: orb.duration, repeat: Infinity, repeatType: "mirror", ease: "easeInOut" }}
          />
        ))}
        <FloatingParticles />
      </div>

      {/* Theme toggle */}
      <div className="fixed top-6 right-6 z-50 flex items-center gap-3">
        <AnimatePresence>
          {toggleHover && (
            <motion.span
              initial={{ opacity: 0, x: 8 }}
              animate={{ opacity: 1, x: 0 }}
              exit={{ opacity: 0, x: 8 }}
              transition={{ duration: 0.2 }}
              className="text-xs font-medium text-neutral-500 dark:text-neutral-400 whitespace-nowrap select-none"
            >
              {theme === "dark" ? "Light mode" : "Dark mode"}
            </motion.span>
          )}
        </AnimatePresence>
        <motion.button
          whileHover={{ scale: 1.05 }}
          whileTap={{ scale: 0.9 }}
          onMouseEnter={() => setToggleHover(true)}
          onMouseLeave={() => setToggleHover(false)}
          onClick={onToggleTheme}
          aria-label="Toggle theme"
          className="w-12 h-12 flex items-center justify-center rounded-2xl bg-white/80 dark:bg-neutral-800/80 backdrop-blur-sm border border-neutral-200 dark:border-neutral-700 text-neutral-500 hover:text-neutral-900 dark:hover:text-white transition-all shadow-sm hover:shadow-[0_0_20px_rgba(239,68,68,0.3)] dark:hover:shadow-[0_0_20px_rgba(239,68,68,0.25)]"
        >
          <AnimatePresence mode="wait" initial={false}>
            {theme === "dark" ? (
              <motion.span key="sun" initial={{ scale: 0, rotate: 180 }} animate={{ scale: [0, 1.2, 1], rotate: [180, 0, 0] }} exit={{ scale: 0, rotate: -180 }} transition={{ type: "spring", stiffness: 200, damping: 15 }} className="flex items-center justify-center">
                <Sun className="w-5 h-5" />
              </motion.span>
            ) : (
              <motion.span key="moon" initial={{ scale: 0, rotate: 180 }} animate={{ scale: [0, 1.2, 1], rotate: [180, 0, 0] }} exit={{ scale: 0, rotate: -180 }} transition={{ type: "spring", stiffness: 200, damping: 15 }} className="flex items-center justify-center">
                <Moon className="w-5 h-5" />
              </motion.span>
            )}
          </AnimatePresence>
        </motion.button>
      </div>

      {/* Main content */}
      <div className="relative z-10 flex flex-col items-center w-full max-w-2xl text-center mb-12">
        {/* Badge */}
        <motion.div
          initial={{ opacity: 0, scale: 0.9 }}
          animate={{ opacity: 1, scale: 1 }}
          transition={{ type: "spring", stiffness: 150, damping: 20 }}
          className="inline-block mb-6"
        >
          <div className="relative p-[1px] rounded-full bg-gradient-to-r from-red-500 via-rose-500 to-amber-500 overflow-hidden">
            <div className="shimmer-sweep rounded-full" />
            <div className="relative rounded-full bg-white/90 dark:bg-neutral-900/90 px-4 py-1.5 text-xs font-semibold tracking-wider text-neutral-700 dark:text-neutral-300">
              ✦ Data Processing Suite
            </div>
          </div>
        </motion.div>

        {/* Headline */}
        <motion.h1
          initial={{ opacity: 0, y: 20 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ type: "spring", stiffness: 100, damping: 20, delay: 0.15 }}
          className="text-5xl font-bold tracking-tight mb-4 gradient-text text-balance"
        >
          Process. Normalize. Analyze. Deliver.
        </motion.h1>

        {/* Subheadline */}
        <motion.p
          initial={{ opacity: 0, y: 15 }}
          animate={{ opacity: 1, y: 0 }}
          transition={{ type: "spring", stiffness: 100, damping: 20, delay: 0.3 }}
          className="text-lg text-neutral-500 dark:text-neutral-400 mb-2"
        >
          Your end-to-end procurement data pipeline
        </motion.p>

        <DataFlowAnimation />

        {/* Divider */}
        <motion.div
          initial={{ scaleX: 0 }}
          animate={{ scaleX: 1 }}
          transition={{ duration: 0.8, ease: "easeOut", delay: 0.5 }}
          className="h-[1px] bg-gradient-to-r from-transparent via-red-500/50 to-transparent mx-auto max-w-xs origin-left mt-4"
        />
      </div>

      {/* Module cards */}
      <div className="relative z-10 w-full max-w-2xl space-y-6" style={{ perspective: 800 }}>
        {APPS.map((app, idx) => (
          <AppCard key={app.title} app={app} idx={idx} />
        ))}
      </div>

      {/* Footer */}
      <motion.p
        initial={{ opacity: 0 }}
        animate={{ opacity: [0.4, 0.7, 0.4] }}
        transition={{ duration: 4, repeat: Infinity, ease: "easeInOut", delay: 1 }}
        className="relative z-10 mt-12 text-xs text-neutral-400 dark:text-neutral-600 tracking-wide"
      >
        ProcIP Data Processing Suite
      </motion.p>
    </section>
  );
}
