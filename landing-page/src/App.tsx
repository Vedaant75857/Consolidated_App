import { useState, useEffect, useRef } from "react";
import { Database, Layers, BarChart3, ArrowRight, Sun, Moon } from "lucide-react";
import {
  motion,
  AnimatePresence,
  useMotionValue,
  useMotionTemplate,
} from "motion/react";

/* ─── Data ────────────────────────────────────────────────────────── */

const APPS = [
  {
    title: "Data Stitcher",
    description:
      "Consolidate and merge multiple data sources into a unified dataset. Upload, append, normalize headers, clean, and merge — all in one guided pipeline.",
    url: import.meta.env.VITE_STITCHER_FE ?? "http://localhost:3002",
    icon: Database,
    gradient: "from-red-600 to-rose-600",
    shadowColor: "shadow-red-200/40 dark:shadow-red-900/30",
    accentText: "text-red-600 dark:text-red-400",
    accentBg: "bg-red-50 dark:bg-red-950/30",
    accentRgb: "239, 68, 68",
    tag: "Module 1",
  },
  {
    title: "Data Normalizer",
    description:
      "Normalize supplier names, countries, dates, payment terms, regions, plants, and currencies with AI-powered transformations.",
    url: import.meta.env.VITE_NORMALIZER_FE ?? "http://localhost:3003",
    icon: Layers,
    gradient: "from-rose-600 to-red-700",
    shadowColor: "shadow-rose-200/40 dark:shadow-rose-900/30",
    accentText: "text-rose-600 dark:text-rose-400",
    accentBg: "bg-rose-50 dark:bg-rose-950/30",
    accentRgb: "225, 29, 72",
    tag: "Module 2",
  },
  {
    title: "Spend Summarizer",
    description:
      "Upload procurement data, map columns with AI, and generate interactive spend dashboards with charts, Pareto analysis, and exportable PDF reports.",
    url: import.meta.env.VITE_ANALYZER_FE ?? "http://localhost:3004",
    icon: BarChart3,
    gradient: "from-amber-600 to-orange-600",
    shadowColor: "shadow-amber-200/40 dark:shadow-amber-900/30",
    accentText: "text-amber-600 dark:text-amber-400",
    accentBg: "bg-amber-50 dark:bg-amber-950/30",
    accentRgb: "217, 119, 6",
    tag: "Module 3",
  },
];

const ORBS = [
  {
    size: 700,
    x: [0, 100, -50, 0],
    y: [0, -80, 60, 0],
    className: "bg-red-300/30 dark:bg-red-800/15",
    duration: 22,
    style: { left: "-10%", top: "-15%" },
  },
  {
    size: 500,
    x: [0, -120, 80, 0],
    y: [0, 60, -100, 0],
    className: "bg-rose-400/25 dark:bg-rose-800/10",
    duration: 18,
    style: { right: "-5%", top: "20%" },
  },
  {
    size: 400,
    x: [0, 60, -80, 0],
    y: [0, 100, -40, 0],
    className: "bg-amber-300/15 dark:bg-amber-800/8",
    duration: 25,
    style: { left: "15%", bottom: "-10%" },
  },
  {
    size: 300,
    x: [0, -50, 100, 0],
    y: [0, -60, 80, 0],
    className: "bg-red-500/20 dark:bg-red-900/10",
    duration: 20,
    style: { right: "20%", bottom: "5%" },
  },
];

const PARTICLES = Array.from({ length: 6 }, (_, i) => {
  const angle = (i / 6) * Math.PI * 2;
  return { x: Math.cos(angle) * 30, y: Math.sin(angle) * 30 };
});

/* ─── Card Component ──────────────────────────────────────────────── */

function AppCard({ app, idx }: { app: (typeof APPS)[number]; idx: number }) {
  const cardRef = useRef<HTMLAnchorElement>(null);
  const mouseX = useMotionValue(0);
  const mouseY = useMotionValue(0);
  const [showParticles, setShowParticles] = useState(false);

  const spotlightBg = useMotionTemplate`radial-gradient(600px circle at ${mouseX}px ${mouseY}px, rgba(${app.accentRgb}, 0.12), transparent 80%)`;

  function handleMouseMove(e: React.MouseEvent) {
    if (!cardRef.current) return;
    const rect = cardRef.current.getBoundingClientRect();
    mouseX.set(e.clientX - rect.left);
    mouseY.set(e.clientY - rect.top);
  }

  const Icon = app.icon;

  return (
    <motion.div
      initial={{ opacity: 0, y: 40, rotateX: 15 }}
      animate={{ opacity: 1, y: 0, rotateX: 0 }}
      transition={{
        type: "spring",
        stiffness: 100,
        damping: 20,
        delay: 0.6 + idx * 0.2,
      }}
    >
      <motion.a
        href={app.url}
        ref={cardRef}
        onMouseMove={handleMouseMove}
        whileHover={{ y: -4, scale: 1.01 }}
        whileTap={{ scale: 0.99 }}
        className="group block relative"
      >
        {/* Gradient border wrapper */}
        <div
          className={`absolute inset-0 rounded-2xl bg-gradient-to-br ${app.gradient} opacity-30 group-hover:opacity-70 transition-opacity duration-500`}
        />

        {/* Card body (glassmorphism) */}
        <div className="relative m-[1px] rounded-2xl bg-white/60 dark:bg-neutral-900/60 backdrop-blur-2xl border-t border-white/20 p-8 overflow-hidden">
          {/* Cursor spotlight overlay */}
          <motion.div
            className="pointer-events-none absolute inset-0 rounded-2xl z-0 opacity-0 group-hover:opacity-100 transition-opacity duration-300"
            style={{ background: spotlightBg }}
          />

          {/* Shimmer sweep */}
          <div className="shimmer-sweep" />

          <div className="flex items-start gap-5 relative z-10">
            {/* Floating icon with particle burst */}
            <div className="relative shrink-0">
              <motion.div
                animate={{ y: [0, -6, 0] }}
                transition={{
                  duration: 3,
                  repeat: Infinity,
                  ease: "easeInOut",
                }}
                onHoverStart={() => setShowParticles(true)}
                onHoverEnd={() =>
                  setTimeout(() => setShowParticles(false), 600)
                }
                className={`w-14 h-14 rounded-2xl bg-gradient-to-br ${app.gradient} flex items-center justify-center shadow-lg ${app.shadowColor}`}
              >
                <Icon className="w-7 h-7 text-white" />
              </motion.div>

              <AnimatePresence>
                {showParticles &&
                  PARTICLES.map((p, i) => (
                    <motion.div
                      key={i}
                      className={`absolute top-1/2 left-1/2 w-1.5 h-1.5 rounded-full bg-gradient-to-br ${app.gradient}`}
                      initial={{ x: 0, y: 0, scale: 0, opacity: 1 }}
                      animate={{ x: p.x, y: p.y, scale: 1, opacity: 0 }}
                      exit={{ opacity: 0 }}
                      transition={{
                        duration: 0.6,
                        ease: "easeOut",
                        delay: i * 0.03,
                      }}
                    />
                  ))}
              </AnimatePresence>
            </div>

            {/* Text content */}
            <div className="flex-1 min-w-0">
              <div className="flex items-center gap-3 mb-2">
                <h2 className="text-xl font-bold tracking-tight text-neutral-900 dark:text-white">
                  {app.title}
                </h2>
                <span
                  className={`inline-flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-widest px-2.5 py-1 rounded-full border-pulse ${app.accentBg} ${app.accentText}`}
                >
                  <motion.span
                    className="w-1.5 h-1.5 rounded-full bg-current"
                    animate={{ opacity: [1, 0.3, 1] }}
                    transition={{
                      duration: 2,
                      repeat: Infinity,
                      ease: "easeInOut",
                    }}
                  />
                  {app.tag}
                </span>
              </div>
              <p className="text-sm text-neutral-500 dark:text-neutral-400 leading-relaxed">
                {app.description}
              </p>
            </div>

            {/* Arrow button with gradient morph */}
            <div className="shrink-0 mt-2">
              <motion.div
                whileHover={{ scale: 1.1 }}
                className="w-10 h-10 rounded-xl flex items-center justify-center relative overflow-hidden"
              >
                <div className="absolute inset-0 bg-neutral-100 dark:bg-neutral-800 transition-opacity duration-300 group-hover:opacity-0" />
                <div
                  className={`absolute inset-0 bg-gradient-to-br ${app.gradient} opacity-0 group-hover:opacity-100 transition-opacity duration-300`}
                />
                <ArrowRight
                  className={`w-5 h-5 relative z-10 ${app.accentText} group-hover:text-white transition-all duration-300 group-hover:-rotate-45`}
                />
              </motion.div>
            </div>
          </div>
        </div>
      </motion.a>
    </motion.div>
  );
}

/* ─── Main App ────────────────────────────────────────────────────── */

export default function App() {
  const [theme, setTheme] = useState<"light" | "dark">(() => {
    if (typeof window !== "undefined") {
      const saved = localStorage.getItem("procip-theme");
      if (saved === "dark" || saved === "light") return saved;
      return window.matchMedia("(prefers-color-scheme: dark)").matches
        ? "dark"
        : "light";
    }
    return "light";
  });

  const [toggleHover, setToggleHover] = useState(false);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", theme === "dark");
    localStorage.setItem("procip-theme", theme);
  }, [theme]);

  const toggleTheme = () =>
    setTheme((prev) => (prev === "dark" ? "light" : "dark"));

  return (
    <div className="min-h-screen bg-gradient-to-br from-neutral-50 via-white to-neutral-100 dark:from-neutral-950 dark:via-neutral-900 dark:to-neutral-950 text-neutral-900 dark:text-neutral-100 font-sans relative overflow-hidden">
      {/* ── Animated floating orbs ──────────────────────────────── */}
      <div className="pointer-events-none fixed inset-0 z-0">
        {ORBS.map((orb, i) => (
          <motion.div
            key={i}
            className={`absolute rounded-full ${orb.className} blur-3xl`}
            style={{ width: orb.size, height: orb.size, ...orb.style }}
            animate={{ x: orb.x, y: orb.y, opacity: [0.6, 1, 0.6] }}
            transition={{
              duration: orb.duration,
              repeat: Infinity,
              repeatType: "mirror",
              ease: "easeInOut",
            }}
          />
        ))}
      </div>

      {/* ── Dot grid texture ───────────────────────────────────── */}
      <div className="dot-grid pointer-events-none fixed inset-0 z-[1]" />

      {/* ── Noise grain overlay ────────────────────────────────── */}
      <div className="noise-overlay pointer-events-none fixed inset-0 z-[1]" />

      {/* ── Theme toggle ───────────────────────────────────────── */}
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
          onClick={toggleTheme}
          className="w-12 h-12 flex items-center justify-center rounded-2xl bg-white/80 dark:bg-neutral-800/80 backdrop-blur-sm border border-neutral-200 dark:border-neutral-700 text-neutral-500 hover:text-neutral-900 dark:hover:text-white transition-all shadow-sm hover:shadow-[0_0_20px_rgba(239,68,68,0.3)] dark:hover:shadow-[0_0_20px_rgba(239,68,68,0.25)]"
        >
          <AnimatePresence mode="wait" initial={false}>
            {theme === "dark" ? (
              <motion.span
                key="sun"
                initial={{ scale: 0, rotate: 180 }}
                animate={{ scale: [0, 1.2, 1], rotate: [180, 0, 0] }}
                exit={{ scale: 0, rotate: -180 }}
                transition={{ type: "spring", stiffness: 200, damping: 15 }}
                className="flex items-center justify-center"
              >
                <Sun className="w-5 h-5" />
              </motion.span>
            ) : (
              <motion.span
                key="moon"
                initial={{ scale: 0, rotate: 180 }}
                animate={{ scale: [0, 1.2, 1], rotate: [180, 0, 0] }}
                exit={{ scale: 0, rotate: -180 }}
                transition={{ type: "spring", stiffness: 200, damping: 15 }}
                className="flex items-center justify-center"
              >
                <Moon className="w-5 h-5" />
              </motion.span>
            )}
          </AnimatePresence>
        </motion.button>
      </div>

      {/* ── Main content ───────────────────────────────────────── */}
      <div className="relative z-10 flex flex-col items-center justify-center min-h-screen px-6 py-16">
        {/* Hero section */}
        <div className="w-full max-w-2xl text-center mb-12">
          {/* Animated badge */}
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

          {/* Gradient headline */}
          <motion.h1
            initial={{ opacity: 0, y: 20 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{
              type: "spring",
              stiffness: 100,
              damping: 20,
              delay: 0.15,
            }}
            className="text-5xl font-bold tracking-tight mb-4 gradient-text"
          >
            Process. Normalize. Analyze. Deliver.
          </motion.h1>

          {/* Subheadline */}
          <motion.p
            initial={{ opacity: 0, y: 15 }}
            animate={{ opacity: 1, y: 0 }}
            transition={{
              type: "spring",
              stiffness: 100,
              damping: 20,
              delay: 0.3,
            }}
            className="text-lg text-neutral-500 dark:text-neutral-400 mb-8"
          >
            Your end-to-end procurement data pipeline
          </motion.p>

          {/* Self-drawing divider */}
          <motion.div
            initial={{ scaleX: 0 }}
            animate={{ scaleX: 1 }}
            transition={{ duration: 0.8, ease: "easeOut", delay: 0.5 }}
            className="h-[1px] bg-gradient-to-r from-transparent via-red-500/50 to-transparent mx-auto max-w-xs origin-left"
          />
        </div>

        {/* Cards */}
        <div
          className="w-full max-w-2xl space-y-6"
          style={{ perspective: 800 }}
        >
          {APPS.map((app, idx) => (
            <AppCard key={app.title} app={app} idx={idx} />
          ))}
        </div>

        {/* Footer with breathing opacity */}
        <motion.p
          initial={{ opacity: 0 }}
          animate={{ opacity: [0.4, 0.7, 0.4] }}
          transition={{
            duration: 4,
            repeat: Infinity,
            ease: "easeInOut",
            delay: 1,
          }}
          className="mt-12 text-xs text-neutral-400 dark:text-neutral-600 tracking-wide"
        >
          ProcIP Data Processing Suite
        </motion.p>
      </div>
    </div>
  );
}
