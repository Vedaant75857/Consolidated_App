import React, { useState, useEffect } from "react";
import { motion, AnimatePresence } from "motion/react";
import { ArrowLeft, ArrowRight } from "lucide-react";
import Lottie from "lottie-react";
import layersAnimation from "../animations/layersAnimation.json";
import funnelAnimation from "../animations/funnelAnimation.json";
import chartAnimation from "../animations/chartAnimation.json";

/* ── Shared progress bar ───────────────────────────────────────── */

function ScreenProgress({
  total,
  active,
  accentColor,
}: {
  total: number;
  active: number;
  accentColor: string;
}) {
  return (
    <div className="flex gap-1 items-center mb-2.5 shrink-0">
      {Array.from({ length: total }).map((_, i) => (
        <div key={i} className="h-[2px] flex-1 rounded-full overflow-hidden bg-neutral-700/50">
          {i < active && (
            <div className={`h-full w-full rounded-full ${accentColor}`} style={{ opacity: 0.5 }} />
          )}
          {i === active && (
            <motion.div
              key={active}
              initial={{ width: "0%" }}
              animate={{ width: "100%" }}
              transition={{ duration: 5, ease: "linear" }}
              className={`h-full rounded-full ${accentColor}`}
            />
          )}
        </div>
      ))}
      <span className="text-[7px] text-neutral-600 shrink-0 ml-0.5 tabular-nums">
        {active + 1}/{total}
      </span>
    </div>
  );
}

/* ── Shared screen slide wrapper ───────────────────────────────── */
/* Fixed h-[280px] container so all states in all modules are
   identical in height. motion.div is absolute so transitions
   never cause layout shift.                                        */

function ScreenSlider({
  screen,
  children,
}: {
  screen: number;
  children: React.ReactElement;
}) {
  return (
    <div className="relative h-[200px] overflow-hidden">
      <AnimatePresence mode="wait">
        <motion.div
          key={screen}
          initial={{ x: 18, opacity: 0 }}
          animate={{ x: 0, opacity: 1 }}
          exit={{ x: -18, opacity: 0 }}
          transition={{ duration: 0.3, ease: "easeOut" }}
          className="absolute inset-0 flex flex-col overflow-hidden"
        >
          {children}
        </motion.div>
      </AnimatePresence>
    </div>
  );
}

/* ── Mockup: Data Stitcher ─────────────────────────────────────── */

function StitcherMockup() {
  const [screen, setScreen] = useState(0);
  useEffect(() => {
    const t = setInterval(() => setScreen((s) => (s + 1) % 3), 5000);
    return () => clearInterval(t);
  }, []);

  return (
    <div className="rounded-2xl bg-neutral-900/80 border border-white/10 p-3.5 flex flex-col overflow-hidden">
      <ScreenProgress total={3} active={screen} accentColor="bg-red-400" />
      <ScreenSlider screen={screen}>
        <>
          {/* ── Screen 0: Source files ── */}
          {screen === 0 && (
            <div className="flex flex-col justify-between h-full">
              <div className="flex items-center justify-between">
                <span className="text-[8px] font-semibold uppercase tracking-wider text-neutral-500">Source Files</span>
                <span className="text-[7px] bg-red-500/15 text-red-400 px-1.5 py-0.5 rounded-full">Step 1 / 3</span>
              </div>
              <div className="flex gap-2">
                {[
                  { name: "orders_Q1.csv", rows: "12 rows", bars: [60, 45, 70, 50] },
                  { name: "orders_Q2.csv", rows: "18 rows", bars: [75, 55, 40, 65] },
                ].map((f, i) => (
                  <motion.div
                    key={f.name}
                    initial={{ opacity: 0, y: -6 }}
                    animate={{ opacity: 1, y: 0 }}
                    transition={{ delay: i * 0.1 }}
                    className="flex-1 rounded-lg bg-red-500/10 border border-red-500/20 p-2"
                  >
                    <div className="flex items-center gap-1.5 mb-1.5">
                      <div className="w-2.5 h-2.5 rounded-sm bg-red-500/70 shrink-0" />
                      <span className="text-[8px] text-neutral-300 truncate font-medium">{f.name}</span>
                    </div>
                    <span className="text-[7px] text-neutral-600 mb-1.5 block">{f.rows}</span>
                    {f.bars.map((w, ri) => (
                      <motion.div
                        key={ri}
                        initial={{ width: 0 }}
                        animate={{ width: `${w}%` }}
                        transition={{ delay: 0.15 + ri * 0.05, duration: 0.4 }}
                        className="h-[2.5px] rounded-full bg-neutral-600 mb-1"
                      />
                    ))}
                  </motion.div>
                ))}
              </div>
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.4 }}
                className="flex items-center gap-1.5 bg-white/[0.03] rounded-lg px-2.5 py-2 border border-white/5"
              >
                <div className="w-1.5 h-1.5 rounded-full bg-amber-400 shrink-0" />
                <span className="text-[8px] text-neutral-400">2 files ready · 30 rows total detected</span>
              </motion.div>
            </div>
          )}

          {/* ── Screen 1: Column alignment ── */}
          {screen === 1 && (
            <div className="flex flex-col justify-between h-full">
              <div className="flex items-center justify-between">
                <span className="text-[8px] font-semibold uppercase tracking-wider text-neutral-500">Aligning Headers</span>
                <span className="text-[7px] bg-red-500/15 text-red-400 px-1.5 py-0.5 rounded-full">Step 2 / 3</span>
              </div>
              <div className="rounded-xl border border-white/10 overflow-hidden">
                <div className="grid grid-cols-[1fr_16px_1fr] gap-x-1 bg-red-500/15 px-3 py-1.5">
                  {["Source Column", "", "Target Field"].map((h, i) => (
                    <span key={i} className="text-[8px] font-bold text-red-300">{h}</span>
                  ))}
                </div>
                {[
                  ["SUPPLIER_NAME", "supplier"],
                  ["DATE_ISSUED", "date"],
                  ["AMOUNT_USD", "amount"],
                  ["COUNTRY_CODE", "country"],
                ].map(([src, tgt], i) => (
                  <motion.div
                    key={src}
                    initial={{ opacity: 0, x: -8 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: i * 0.1, duration: 0.25 }}
                    className="grid grid-cols-[1fr_16px_1fr] gap-x-1 items-center px-3 py-1.5 border-t border-white/5"
                  >
                    <span className="text-[8px] text-neutral-400 font-mono">{src}</span>
                    <span className="text-[8px] text-neutral-600 text-center">→</span>
                    <span className="text-[8px] text-emerald-400 font-medium">{tgt}</span>
                  </motion.div>
                ))}
              </div>
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.5 }}
                className="space-y-1"
              >
                <div className="flex justify-between">
                  <span className="text-[7px] text-neutral-500">Columns mapped</span>
                  <span className="text-[7px] font-bold text-red-400">4 / 4</span>
                </div>
                <div className="h-1 rounded-full bg-white/10 overflow-hidden">
                  <motion.div
                    initial={{ width: 0 }}
                    animate={{ width: "100%" }}
                    transition={{ delay: 0.55, duration: 0.6, ease: "easeOut" }}
                    className="h-full rounded-full bg-gradient-to-r from-red-600 to-rose-500"
                  />
                </div>
              </motion.div>
            </div>
          )}

          {/* ── Screen 2: Merged result ── */}
          {screen === 2 && (
            <div className="flex flex-col justify-between h-full">
              <div className="flex items-center justify-between">
                <span className="text-[8px] font-semibold uppercase tracking-wider text-neutral-500">Merged Dataset</span>
                <span className="text-[7px] bg-emerald-500/15 text-emerald-400 px-1.5 py-0.5 rounded-full">Step 3 / 3</span>
              </div>
              <div className="rounded-xl border border-white/10 overflow-hidden">
                <div className="grid grid-cols-3 bg-red-500/15 px-3 py-1.5">
                  {["Supplier", "Date", "Amount"].map((h) => (
                    <span key={h} className="text-[8px] font-bold text-red-300">{h}</span>
                  ))}
                </div>
                {[
                  ["Acme Corp", "12-Jan-24", "$4,200"],
                  ["TechSupply", "03-Feb-24", "$1,850"],
                  ["GlobalMfg", "28-Feb-24", "$9,100"],
                  ["NovaTech", "15-Mar-24", "$3,450"],
                ].map((r, i) => (
                  <motion.div
                    key={i}
                    initial={{ opacity: 0, x: -10 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: i * 0.08, duration: 0.28 }}
                    className="grid grid-cols-3 px-3 py-1.5 border-t border-white/5"
                  >
                    <span className="text-[9px] text-neutral-200">{r[0]}</span>
                    <span className="text-[9px] text-neutral-500">{r[1]}</span>
                    <span className="text-[9px] text-neutral-300 font-medium">{r[2]}</span>
                  </motion.div>
                ))}
              </div>
              <motion.div
                initial={{ opacity: 0, y: 4 }}
                animate={{ opacity: 1, y: 0 }}
                transition={{ delay: 0.4 }}
                className="flex items-center gap-2"
              >
                <div className="w-1.5 h-1.5 rounded-full bg-emerald-400 shrink-0" />
                <span className="text-[8px] text-neutral-400">30 rows merged · 2 duplicates removed · Quality 94%</span>
              </motion.div>
            </div>
          )}
        </>
      </ScreenSlider>
    </div>
  );
}

/* ── Mockup: Data Normalizer ───────────────────────────────────── */

function NormalizerMockup() {
  const [screen, setScreen] = useState(0);
  useEffect(() => {
    const t = setInterval(() => setScreen((s) => (s + 1) % 3), 5000);
    return () => clearInterval(t);
  }, []);

  return (
    <div className="rounded-2xl bg-neutral-900/80 border border-white/10 p-3.5 flex flex-col overflow-hidden">
      <ScreenProgress total={3} active={screen} accentColor="bg-rose-400" />
      <ScreenSlider screen={screen}>
        <>
          {/* ── Screen 0: Raw input ── */}
          {screen === 0 && (
            <div className="flex flex-col justify-between h-full">
              <div className="flex items-center justify-between">
                <span className="text-[8px] font-semibold uppercase tracking-wider text-neutral-500">Raw Input</span>
                <span className="text-[7px] bg-rose-500/15 text-rose-400 px-1.5 py-0.5 rounded-full">Before</span>
              </div>
              <div className="rounded-xl border border-white/10 overflow-hidden">
                <div className="grid grid-cols-3 bg-rose-500/15 px-3 py-1.5">
                  {["Supplier", "Date", "Amount"].map((h) => (
                    <span key={h} className="text-[8px] font-bold text-rose-300">{h}</span>
                  ))}
                </div>
                {[
                  ["ACME Corp.", "03/15/2024", "USD 4500"],
                  ["apple inc", "2024-01-20", "EUR 1200"],
                  ["Siemens AG", "Jan 5, 2024", "8900 CHF"],
                  ["IBM Corp", "15-02-24", "GBP 7200"],
                ].map((r, i) => (
                  <motion.div
                    key={i}
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    transition={{ delay: i * 0.08 }}
                    className="grid grid-cols-3 px-3 py-1.5 border-t border-white/5"
                  >
                    <span className="text-[8px] text-amber-300/80">{r[0]}</span>
                    <span className="text-[8px] text-amber-300/80">{r[1]}</span>
                    <span className="text-[8px] text-amber-300/80">{r[2]}</span>
                  </motion.div>
                ))}
              </div>
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.4 }}
                className="flex items-center gap-1.5 bg-amber-500/10 rounded-lg px-2.5 py-2 border border-amber-500/15"
              >
                <span className="text-amber-400 text-[10px] shrink-0">⚠</span>
                <span className="text-[8px] text-neutral-400">4 format inconsistencies detected</span>
              </motion.div>
            </div>
          )}

          {/* ── Screen 1: AI agents running ── */}
          {screen === 1 && (
            <div className="flex flex-col justify-between h-full">
              <div className="flex items-center justify-between">
                <span className="text-[8px] font-semibold uppercase tracking-wider text-neutral-500">AI Agents Running</span>
                <span className="text-[7px] bg-rose-500/15 text-rose-400 px-1.5 py-0.5 rounded-full">Processing</span>
              </div>
              <div className="flex flex-col gap-2">
                {[
                  { label: "Supplier Names", pct: 100, done: true },
                  { label: "Country / ISO", pct: 100, done: true },
                  { label: "Date Formats", pct: 75, done: false },
                  { label: "Currency Conv.", pct: 0, done: false },
                  { label: "Payment Terms", pct: 0, done: false },
                ].map((agent, i) => (
                  <motion.div
                    key={agent.label}
                    initial={{ opacity: 0, x: 8 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: i * 0.1 }}
                    className="flex items-center gap-2"
                  >
                    <span className="text-[9px] w-[10px] shrink-0 text-center">
                      {agent.done ? (
                        <span className="text-emerald-400">✓</span>
                      ) : agent.pct > 0 ? (
                        <motion.span
                          animate={{ rotate: 360 }}
                          transition={{ duration: 1, repeat: Infinity, ease: "linear" }}
                          className="inline-block text-rose-400"
                        >⟳</motion.span>
                      ) : (
                        <span className="text-neutral-700">○</span>
                      )}
                    </span>
                    <span className={`text-[8px] w-[90px] shrink-0 ${agent.done ? "text-neutral-300" : agent.pct > 0 ? "text-neutral-300" : "text-neutral-600"}`}>
                      {agent.label}
                    </span>
                    <div className="flex-1 h-1 rounded-full bg-white/10 overflow-hidden">
                      <motion.div
                        initial={{ width: 0 }}
                        animate={{ width: `${agent.pct}%` }}
                        transition={{ delay: 0.2 + i * 0.1, duration: 0.5, ease: "easeOut" }}
                        className={`h-full rounded-full ${agent.done ? "bg-emerald-500" : "bg-rose-500"}`}
                      />
                    </div>
                    <span className={`text-[7px] w-[22px] text-right shrink-0 tabular-nums ${agent.done ? "text-emerald-400" : "text-neutral-500"}`}>
                      {agent.pct}%
                    </span>
                  </motion.div>
                ))}
              </div>
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.6 }}
                className="flex items-center gap-1.5 bg-white/[0.03] rounded-lg px-2.5 py-2 border border-white/5"
              >
                <motion.div
                  animate={{ opacity: [1, 0.3, 1] }}
                  transition={{ duration: 1.2, repeat: Infinity }}
                  className="w-1.5 h-1.5 rounded-full bg-rose-400 shrink-0"
                />
                <span className="text-[8px] text-neutral-400">Processing field 3 of 7…</span>
              </motion.div>
            </div>
          )}

          {/* ── Screen 2: Normalized output ── */}
          {screen === 2 && (
            <div className="flex flex-col justify-between h-full">
              <div className="flex items-center justify-between">
                <span className="text-[8px] font-semibold uppercase tracking-wider text-neutral-500">Normalized Output</span>
                <span className="text-[7px] bg-emerald-500/15 text-emerald-400 px-1.5 py-0.5 rounded-full">After</span>
              </div>
              <div className="rounded-xl border border-white/10 overflow-hidden">
                <div className="grid grid-cols-[1fr_1fr_30px] gap-x-1 bg-rose-500/15 px-3 py-1.5">
                  {["Supplier", "Date", ""].map((h, i) => (
                    <span key={i} className="text-[8px] font-bold text-rose-300">{h}</span>
                  ))}
                </div>
                {[
                  ["Acme Corporation", "15-03-2024", 98],
                  ["Apple Inc.", "20-01-2024", 99],
                  ["Siemens AG", "05-01-2024", 97],
                  ["IBM Corporation", "15-02-2024", 96],
                ].map((r, i) => (
                  <motion.div
                    key={i}
                    initial={{ opacity: 0, x: 8 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: i * 0.08, duration: 0.28 }}
                    className="grid grid-cols-[1fr_1fr_30px] gap-x-1 items-center px-3 py-1.5 border-t border-white/5"
                  >
                    <span className="text-[8px] text-neutral-200 truncate">{r[0]}</span>
                    <span className="text-[8px] text-neutral-400">{r[1]}</span>
                    <span className="text-[8px] font-bold text-emerald-400 text-right">{r[2]}%</span>
                  </motion.div>
                ))}
              </div>
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.4 }}
                className="space-y-1"
              >
                <div className="flex justify-between items-center">
                  <span className="text-[7px] text-neutral-500">Avg confidence</span>
                  <span className="text-[8px] font-bold text-rose-400">98.5%</span>
                </div>
                <div className="h-1 rounded-full bg-white/10 overflow-hidden">
                  <motion.div
                    initial={{ width: 0 }}
                    animate={{ width: "98.5%" }}
                    transition={{ delay: 0.45, duration: 0.7, ease: "easeOut" }}
                    className="h-full rounded-full bg-gradient-to-r from-rose-600 to-red-500"
                  />
                </div>
              </motion.div>
            </div>
          )}
        </>
      </ScreenSlider>
    </div>
  );
}

/* ── Mockup: Spend Summarizer ──────────────────────────────────── */

function SummarizerMockup() {
  const [screen, setScreen] = useState(0);
  useEffect(() => {
    const t = setInterval(() => setScreen((s) => (s + 1) % 3), 5000);
    return () => clearInterval(t);
  }, []);

  return (
    <div className="rounded-2xl bg-neutral-900/80 border border-white/10 p-3.5 flex flex-col overflow-hidden">
      <ScreenProgress total={3} active={screen} accentColor="bg-amber-400" />
      <ScreenSlider screen={screen}>
        <>
          {/* ── Screen 0: Column mapping ── */}
          {screen === 0 && (
            <div className="flex flex-col justify-between h-full">
              <div className="flex items-center justify-between">
                <span className="text-[8px] font-semibold uppercase tracking-wider text-neutral-500">AI Column Mapping</span>
                <span className="text-[7px] bg-amber-500/15 text-amber-400 px-1.5 py-0.5 rounded-full">Step 1 / 3</span>
              </div>
              <div className="rounded-xl border border-white/10 overflow-hidden">
                <div className="grid grid-cols-[1fr_1fr] gap-x-1 bg-amber-500/15 px-3 py-1.5">
                  {["Source Column", "Mapped To"].map((h) => (
                    <span key={h} className="text-[8px] font-bold text-amber-300">{h}</span>
                  ))}
                </div>
                {[
                  ["VENDOR_NAME", "Supplier", 99],
                  ["INVOICE_DATE", "Date", 97],
                  ["TOTAL_USD", "Spend Amount", 100],
                  ["DEPT_CODE", "Category", 95],
                  ["REGION_CODE", "Region", 93],
                ].map(([src, tgt, conf], i) => (
                  <motion.div
                    key={String(src)}
                    initial={{ opacity: 0, x: -8 }}
                    animate={{ opacity: 1, x: 0 }}
                    transition={{ delay: i * 0.08, duration: 0.25 }}
                    className="grid grid-cols-[1fr_1fr] gap-x-1 items-center px-3 py-1.5 border-t border-white/5"
                  >
                    <span className="text-[8px] text-neutral-400 font-mono truncate">{src}</span>
                    <div className="flex items-center gap-1.5 min-w-0">
                      <span className="text-[8px] text-emerald-400 font-medium truncate">{tgt}</span>
                      <span className="text-[7px] text-neutral-600 shrink-0">{conf}%</span>
                    </div>
                  </motion.div>
                ))}
              </div>
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.5 }}
                className="flex items-center gap-1.5 bg-white/[0.03] rounded-lg px-2.5 py-2 border border-white/5"
              >
                <div className="w-1.5 h-1.5 rounded-full bg-emerald-400 shrink-0" />
                <span className="text-[8px] text-neutral-400">5 columns mapped · avg 96.8% confidence</span>
              </motion.div>
            </div>
          )}

          {/* ── Screen 1: Spend bar chart ── */}
          {screen === 1 && (
            <div className="flex flex-col justify-between h-full">
              <div className="flex items-center justify-between">
                <span className="text-[8px] font-semibold uppercase tracking-wider text-neutral-500">Spend by Supplier</span>
                <span className="text-[7px] bg-amber-500/15 text-amber-400 px-1.5 py-0.5 rounded-full">Step 2 / 3</span>
              </div>
              <div className="flex items-end gap-1.5 flex-1 px-1 py-2">
                {[
                  { label: "Acme", h: 110, amt: "$4.2M" },
                  { label: "TechCo", h: 82, amt: "$3.1M" },
                  { label: "SupplyX", h: 60, amt: "$2.2M" },
                  { label: "Global", h: 40, amt: "$1.5M" },
                  { label: "Others", h: 24, amt: "$0.9M" },
                ].map((bar, i) => (
                  <div key={bar.label} className="flex-1 flex flex-col items-center gap-0.5 h-full justify-end">
                    <span className="text-[7px] text-neutral-500 leading-none">{bar.amt}</span>
                    <motion.div
                      initial={{ height: 0 }}
                      animate={{ height: bar.h }}
                      transition={{ delay: 0.1 + i * 0.08, duration: 0.45, ease: "easeOut" }}
                      className="w-full rounded-t-md bg-gradient-to-t from-amber-600 to-orange-400"
                    />
                    <span className="text-[7px] text-neutral-600 truncate w-full text-center leading-none">{bar.label}</span>
                  </div>
                ))}
              </div>
              <motion.div
                initial={{ opacity: 0 }}
                animate={{ opacity: 1 }}
                transition={{ delay: 0.6 }}
                className="flex items-center gap-1.5 bg-amber-500/10 rounded-lg px-2.5 py-2 border border-amber-500/15"
              >
                <span className="text-[8px] font-bold text-amber-400">$11.9M</span>
                <span className="text-[8px] text-neutral-500">total spend · 5 suppliers</span>
              </motion.div>
            </div>
          )}

          {/* ── Screen 2: Dashboard stats ── */}
          {screen === 2 && (
            <div className="flex flex-col justify-between h-full">
              <div className="flex items-center justify-between">
                <span className="text-[8px] font-semibold uppercase tracking-wider text-neutral-500">Spend Dashboard</span>
                <span className="text-[7px] bg-emerald-500/15 text-emerald-400 px-1.5 py-0.5 rounded-full">Step 3 / 3</span>
              </div>
              <motion.div
                initial={{ opacity: 0, y: 4 }}
                animate={{ opacity: 1, y: 0 }}
                className="grid grid-cols-4 gap-1"
              >
                {[
                  { v: "$11.9M", l: "Total" },
                  { v: "12", l: "Categories" },
                  { v: "35%", l: "Top Supp." },
                  { v: "+8.2%", l: "YoY" },
                ].map((s, i) => (
                  <motion.div
                    key={s.l}
                    initial={{ opacity: 0, scale: 0.9 }}
                    animate={{ opacity: 1, scale: 1 }}
                    transition={{ delay: i * 0.08 }}
                    className="rounded-lg bg-white/[0.04] border border-white/5 p-1.5 text-center"
                  >
                    <div className="text-[10px] font-bold text-amber-400 leading-tight">{s.v}</div>
                    <div className="text-[6px] text-neutral-600 mt-0.5">{s.l}</div>
                  </motion.div>
                ))}
              </motion.div>
              <div className="flex flex-col gap-2">
                {[
                  { name: "Acme Corp", pct: "35%", w: "35%" },
                  { name: "TechCo Supply", pct: "26%", w: "26%" },
                  { name: "SupplyX Global", pct: "18%", w: "18%" },
                  { name: "GlobalMfg Ltd", pct: "12%", w: "12%" },
                ].map((row, i) => (
                  <motion.div
                    key={row.name}
                    initial={{ opacity: 0 }}
                    animate={{ opacity: 1 }}
                    transition={{ delay: 0.25 + i * 0.08 }}
                    className="flex items-center gap-2"
                  >
                    <span className="text-[8px] text-neutral-400 w-[80px] shrink-0 truncate">{row.name}</span>
                    <div className="flex-1 h-1.5 rounded-full bg-white/10 overflow-hidden">
                      <motion.div
                        initial={{ width: 0 }}
                        animate={{ width: row.w }}
                        transition={{ delay: 0.3 + i * 0.08, duration: 0.5, ease: "easeOut" }}
                        className="h-full rounded-full bg-gradient-to-r from-amber-500 to-orange-500"
                      />
                    </div>
                    <span className="text-[8px] font-medium text-amber-400 w-[22px] text-right shrink-0">{row.pct}</span>
                  </motion.div>
                ))}
              </div>
            </div>
          )}
        </>
      </ScreenSlider>
    </div>
  );
}

/* ── Device frame ──────────────────────────────────────────────── */

function DeviceFrame({ children }: { children: React.ReactElement }) {
  return (
    <div className="w-full select-none">
      <div
        className="relative rounded-xl p-[8px] border border-[#3a3a3c]"
        style={{
          background: "linear-gradient(145deg, #2a2a2c, #1c1c1e)",
          boxShadow:
            "0 25px 50px rgba(0,0,0,0.55), 0 8px 20px rgba(0,0,0,0.35), inset 0 1px 0 rgba(255,255,255,0.05)",
        }}
      >
        <div className="absolute top-[4px] left-1/2 -translate-x-1/2 w-[4px] h-[4px] rounded-full bg-[#48484a]" />
        <div className="rounded-lg overflow-hidden ring-1 ring-black/40">
          {children}
        </div>
      </div>
      <div
        className="mx-auto h-[3px] rounded-b-sm"
        style={{ width: "72%", background: "linear-gradient(to bottom, #3a3a3c, #2c2c2e)" }}
      />
      <div
        className="relative h-[8px] rounded-b-xl border border-t-0 border-[#3a3a3c]"
        style={{ background: "linear-gradient(to bottom, #2c2c2e, #1c1c1e)" }}
      />
      <div className="mx-8 h-[2px] rounded-full bg-black/25 blur-sm mt-0.5" />
    </div>
  );
}

/* ── Module data ───────────────────────────────────────────────── */

const MODULES = [
  {
    title: "Data Stitcher",
    tag: "Module 1",
    tagColor: "text-red-500 dark:text-red-400",
    tagBg: "bg-red-50 dark:bg-red-950/40",
    gradient: "from-red-600 to-rose-600",
    glowColor: "rgba(239,68,68,0.12)",
    lottieData: layersAnimation,
    Mockup: StitcherMockup,
    featureParagraph:
      "Built for procurement teams dealing with data spread across multiple ERP systems, regional offices, and file formats. The Data Stitcher ingests any number of CSVs, aligns mismatched column schemas, and merges them into one clean dataset — with full audit visibility at every step.",
    bullets: [
      "Schema auto-detection across files",
      "Row-level duplicate scoring",
      "Custom merge rules & priority",
      "Preview before final export",
    ],
  },
  {
    title: "Data Normalizer",
    tag: "Module 2",
    tagColor: "text-rose-500 dark:text-rose-400",
    tagBg: "bg-rose-50 dark:bg-rose-950/40",
    gradient: "from-rose-600 to-red-700",
    glowColor: "rgba(225,29,72,0.12)",
    lottieData: funnelAnimation,
    Mockup: NormalizerMockup,
    featureParagraph:
      "Procurement data from different systems rarely speaks the same language. AI agents resolve inconsistencies in supplier names, geographies, dates, and currencies — delivering a standardized dataset you can trust, with a confidence score on every transformation.",
    bullets: [
      "AI entity resolution for suppliers",
      "Multi-currency conversion & standardization",
      "Region & plant hierarchy mapping",
      "Human-in-the-loop review mode",
    ],
  },
  {
    title: "Spend Summarizer",
    tag: "Module 3",
    tagColor: "text-amber-600 dark:text-amber-400",
    tagBg: "bg-amber-50 dark:bg-amber-950/40",
    gradient: "from-amber-600 to-orange-600",
    glowColor: "rgba(217,119,6,0.12)",
    lottieData: chartAnimation,
    Mockup: SummarizerMockup,
    featureParagraph:
      "Transform raw spend data into executive-ready intelligence. Combines AI-driven column mapping with powerful analytics to surface top suppliers, category breakdowns, and spend trends — delivered through interactive dashboards and one-click PDF exports.",
    bullets: [
      "Category taxonomy auto-assignment",
      "Year-over-year trend analysis",
      "Top supplier & Pareto charts",
      "Branded PDF report generation",
    ],
  },
];

/* ── Slide variants ────────────────────────────────────────────── */

const variants = {
  enter: (dir: number) => ({ x: dir > 0 ? 120 : -120, opacity: 0, scale: 0.92 }),
  center: { x: 0, opacity: 1, scale: 1 },
  exit: (dir: number) => ({ x: dir < 0 ? 120 : -120, opacity: 0, scale: 0.92 }),
};

/* ── Module icon ───────────────────────────────────────────────── */

function ModuleIcon({ gradient, lottieData }: { gradient: string; lottieData: object }) {
  return (
    <div className={`w-14 h-14 rounded-2xl bg-gradient-to-br ${gradient} flex items-center justify-center shadow-lg shrink-0`}>
      <Lottie animationData={lottieData} loop autoplay style={{ width: 32, height: 32 }} />
    </div>
  );
}

/* ── Main component ────────────────────────────────────────────── */

export default function ModulesCarousel() {
  const [active, setActive] = useState(0);
  const [direction, setDirection] = useState(0);

  function navigate(delta: number) {
    setDirection(delta);
    setActive((prev) => (prev + delta + MODULES.length) % MODULES.length);
  }

  const mod = MODULES[active];

  return (
    <section className="relative py-24 px-6">
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

      {/* Thumbnail cards */}
      <div className="max-w-5xl mx-auto flex items-center justify-center gap-4 relative">
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
              className={`relative flex-1 max-w-[220px] rounded-2xl p-5 text-left transition-all duration-300 ${
                isActive
                  ? "glass-card shadow-xl"
                  : "bg-white/30 dark:bg-neutral-900/30 backdrop-blur-md border border-white/20 dark:border-neutral-700/30 shadow-sm opacity-60"
              }`}
              style={isActive ? { boxShadow: `0 0 40px ${m.glowColor}, 0 8px 32px rgba(0,0,0,0.08)` } : {}}
              aria-label={`View ${m.title}`}
            >
              <div className="mb-4">
                <ModuleIcon gradient={m.gradient} lottieData={m.lottieData} />
              </div>
              <span className={`inline-flex items-center gap-1 text-[10px] font-semibold uppercase tracking-widest px-2 py-0.5 rounded-full mb-2 ${m.tagBg} ${m.tagColor}`}>
                <span className="w-1 h-1 rounded-full bg-current" />
                {m.tag}
              </span>
              <h3 className="text-sm font-bold text-neutral-900 dark:text-white leading-tight mt-1">{m.title}</h3>
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
      <div className="max-w-4xl mx-auto mt-10 relative overflow-hidden">
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
            <div className={`absolute top-0 left-0 w-48 h-48 rounded-full bg-gradient-to-br ${mod.gradient} opacity-10 blur-3xl pointer-events-none`} />

            <div className="relative z-10 flex flex-col md:flex-row gap-8 items-start">
              {/* LEFT: content */}
              <div className="flex-1 flex flex-col min-w-0">
                <div className="flex items-start gap-4 mb-5">
                  <ModuleIcon gradient={mod.gradient} lottieData={mod.lottieData} />
                  <div>
                    <span className={`inline-flex items-center gap-1.5 text-[10px] font-semibold uppercase tracking-widest px-2.5 py-1 rounded-full ${mod.tagBg} ${mod.tagColor} mb-2`}>
                      <motion.span
                        className="w-1.5 h-1.5 rounded-full bg-current"
                        animate={{ opacity: [1, 0.3, 1] }}
                        transition={{ duration: 2, repeat: Infinity }}
                      />
                      {mod.tag}
                    </span>
                    <h3 className="text-2xl font-bold text-neutral-900 dark:text-white">{mod.title}</h3>
                  </div>
                </div>
                <p className="text-neutral-600 dark:text-neutral-400 leading-relaxed mb-5 text-sm">
                  {mod.featureParagraph}
                </p>
                <ul className="grid grid-cols-1 gap-2">
                  {mod.bullets.map((b) => (
                    <li key={b} className="flex items-center gap-2 text-sm text-neutral-700 dark:text-neutral-300">
                      <span className={`w-1.5 h-1.5 rounded-full bg-gradient-to-br ${mod.gradient} shrink-0`} />
                      {b}
                    </li>
                  ))}
                </ul>
              </div>

              {/* RIGHT: device frame */}
              <div className="w-full md:w-[45%] shrink-0 flex items-center">
                <DeviceFrame>
                  <mod.Mockup />
                </DeviceFrame>
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
