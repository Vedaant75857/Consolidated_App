import React, { useState } from "react";
import { motion } from "motion/react";
import { type LucideIcon, Info } from "lucide-react";

/* ─── Motion Variants ─── */

export const pageVariants = {
  initial: { opacity: 0, y: 20, filter: "blur(6px)" },
  animate: {
    opacity: 1,
    y: 0,
    filter: "blur(0px)",
    transition: {
      duration: 0.35,
      ease: [0.22, 1, 0.36, 1],
      staggerChildren: 0.06,
    },
  },
  exit: {
    opacity: 0,
    y: -10,
    filter: "blur(4px)",
    transition: { duration: 0.2 },
  },
};

export const horizontalVariants = {
  initial: (dir: number) => ({ opacity: 0, x: dir > 0 ? 60 : -60, filter: "blur(4px)" }),
  animate: { opacity: 1, x: 0, filter: "blur(0px)", transition: { duration: 0.3, ease: [0.22, 1, 0.36, 1] } },
  exit: (dir: number) => ({ opacity: 0, x: dir > 0 ? -60 : 60, filter: "blur(4px)", transition: { duration: 0.2 } }),
};

export const itemVariants = {
  initial: { opacity: 0, y: 12 },
  animate: { opacity: 1, y: 0, transition: { duration: 0.25, ease: [0.22, 1, 0.36, 1] } },
};

export const softSpring = { type: "spring" as const, stiffness: 280, damping: 24 };

export const entranceEasing = { duration: 0.32, ease: [0.22, 1, 0.36, 1] };

/* ─── SurfaceCard ─── */

interface SurfaceCardProps {
  title?: string;
  subtitle?: string;
  icon?: LucideIcon;
  right?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
  noPadding?: boolean;
}

export function SurfaceCard({ title, subtitle, icon: Icon, right, children, className = "", noPadding }: SurfaceCardProps) {
  return (
    <motion.div
      variants={itemVariants}
      className={`rounded-3xl border border-neutral-200 bg-white shadow-sm dark:border-neutral-700 dark:bg-neutral-900 ${className}`}
    >
      {(title || right) && (
        <div className="flex items-start justify-between gap-4 px-6 pt-6 pb-0">
          <div className="min-w-0">
            <h3 className="text-lg font-semibold tracking-tight text-neutral-900 dark:text-white flex items-center gap-2">
              {Icon && <Icon className="w-5 h-5 text-red-600 shrink-0" />}
              {title}
            </h3>
            {subtitle && <p className="mt-1 text-sm text-neutral-500 dark:text-neutral-400">{subtitle}</p>}
          </div>
          {right && <div className="shrink-0">{right}</div>}
        </div>
      )}
      <div className={noPadding ? "" : "p-6"}>{children}</div>
    </motion.div>
  );
}

/* ─── SkeletonBlock ─── */

export function SkeletonBlock({ className = "" }: { key?: React.Key; className?: string }) {
  return (
    <div
      className={`animate-pulse rounded-xl bg-gradient-to-r from-neutral-100 via-neutral-50 to-neutral-100 dark:from-neutral-800 dark:via-neutral-700 dark:to-neutral-800 ${className}`}
    />
  );
}

export function SkeletonRows({ rows = 3 }: { rows?: number }) {
  return (
    <div className="space-y-3">
      <SkeletonBlock className="h-5 w-40" />
      {Array.from({ length: rows }).map((_, i) => (
        <SkeletonBlock key={i} className="h-20 w-full" />
      ))}
    </div>
  );
}

/* ─── EmptyState ─── */

interface EmptyStateProps {
  icon: LucideIcon;
  title: string;
  description: string;
  action?: React.ReactNode;
}

export function EmptyState({ icon: Icon, title, description, action }: EmptyStateProps) {
  return (
    <div className="py-16 text-center">
      <div className="mx-auto mb-5 flex h-16 w-16 items-center justify-center rounded-2xl bg-red-50 dark:bg-red-950/40">
        <Icon className="w-7 h-7 text-red-400 dark:text-red-500" />
      </div>
      <h3 className="text-lg font-semibold tracking-tight text-neutral-400 dark:text-neutral-500">{title}</h3>
      <p className="mt-2 text-sm text-neutral-400 dark:text-neutral-500 max-w-xs mx-auto leading-relaxed">{description}</p>
      {action && <div className="mt-6">{action}</div>}
    </div>
  );
}

/* ─── StepHero ─── */

const STEP_META: Record<number, { title: string; description: string }> = {
  1: { title: "Upload + Settings", description: "Start by uploading your data files and configuring your API key." },
  2: { title: "Data Inventory", description: "Review extracted tables, adjust headers, and remove unwanted files." },
  3: { title: "Append Strategy", description: "Group related tables for stacking into unified datasets." },
  4: { title: "Header Normalisation", description: "Map each table's columns to the standard procurement schema." },
  5: { title: "Data Cleaning", description: "Clean and prepare individual tables before grouping." },
  6: { title: "Merge", description: "Select join keys and merge your tables with guided column matching." },
  7: { title: "Merge Results", description: "Review merge quality and download your unified dataset." },
};

interface StepHeroProps {
  step: number;
  displayStep?: number;
  totalSteps?: number;
  isAi?: boolean;
}

export function StepHero({ step, displayStep, totalSteps = 9, isAi }: StepHeroProps) {
  const meta = STEP_META[step] || { title: `Step ${step}`, description: "" };
  const shownStep = displayStep ?? step;
  return (
    <motion.div
      key={step}
      initial={{ opacity: 0, y: 10 }}
      animate={{ opacity: 1, y: 0 }}
      transition={entranceEasing}
      className="mb-8 rounded-3xl border border-red-200/60 dark:border-red-900/60 bg-gradient-to-r from-red-600 to-rose-600 p-7 text-white shadow-xl shadow-red-200/20 dark:shadow-red-900/20"
    >
      <div className="flex items-start justify-between gap-6">
        <div className="min-w-0">
          <p className="text-xs font-semibold uppercase tracking-[0.2em] text-red-200">
            Data pipeline assistant
          </p>
          <h2 className="mt-2 text-2xl font-semibold tracking-tight">{meta.title}</h2>
          <p className="mt-2 max-w-2xl text-sm text-red-50/90 leading-relaxed">{meta.description}</p>
        </div>
        <div className="rounded-2xl bg-white/10 px-4 py-3 backdrop-blur shrink-0">
          <p className="text-[10px] uppercase tracking-wider text-red-200">Current step</p>
          <p className="mt-1 text-lg font-semibold tabular-nums">
            {shownStep} <span className="text-red-200/70 text-sm font-normal">of {totalSteps}</span>
          </p>
          {isAi && (
            <span className="mt-1.5 inline-block text-[9px] font-bold uppercase tracking-wider bg-white/15 px-2 py-0.5 rounded-full">
              AI-assisted
            </span>
          )}
        </div>
      </div>
    </motion.div>
  );
}

/* ─── ResultCapsules ─── */

export function ResultCapsules({ items }: { items: string[] }) {
  if (items.length === 0) return null;
  return (
    <motion.div
      initial={{ opacity: 0, y: 6 }}
      animate={{ opacity: 1, y: 0 }}
      transition={entranceEasing}
      className="flex flex-wrap gap-2"
    >
      {items.map((item) => (
        <span
          key={item}
          className="rounded-full border border-emerald-200 bg-emerald-50 px-3 py-1 text-xs font-medium text-emerald-700 dark:border-emerald-800 dark:bg-emerald-950/40 dark:text-emerald-400"
        >
          {item}
        </span>
      ))}
    </motion.div>
  );
}

/* ─── PrimaryButton ─── */

interface PrimaryButtonProps {
  children: React.ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  className?: string;
  type?: "button" | "submit";
}

export function PrimaryButton({ children, onClick, disabled, className = "", type = "button" }: PrimaryButtonProps) {
  return (
    <motion.button
      type={type}
      whileHover={disabled ? undefined : { y: -1, scale: 1.01 }}
      whileTap={disabled ? undefined : { scale: 0.98 }}
      onClick={onClick}
      disabled={disabled}
      className={`inline-flex items-center justify-center gap-2 rounded-xl bg-red-600 px-5 py-2.5 text-sm font-bold text-white shadow-md shadow-red-200 dark:shadow-red-900/30 transition-colors hover:bg-red-700 disabled:opacity-50 disabled:cursor-not-allowed disabled:shadow-none ${className}`}
    >
      {children}
    </motion.button>
  );
}

/* ─── Tooltip ─── */

export function Tooltip({ text }: { text: string }) {
  const [show, setShow] = useState(false);
  return (
    <span className="relative inline-flex items-center">
      <Info
        className="w-3.5 h-3.5 text-neutral-400 dark:text-neutral-500 cursor-help"
        onMouseEnter={() => setShow(true)}
        onMouseLeave={() => setShow(false)}
      />
      {show && (
        <span className="absolute z-50 bottom-full left-1/2 -translate-x-1/2 mb-2 w-64 p-2.5 text-xs text-neutral-700 dark:text-neutral-300 bg-white dark:bg-neutral-900 border border-neutral-200 dark:border-neutral-700 rounded-xl shadow-lg leading-relaxed">
          {text}
        </span>
      )}
    </span>
  );
}

/* ─── FillBar ─── */

export function FillBar({ rate }: { rate: number }) {
  const pct = Math.round((rate ?? 0) * 100);
  const color =
    pct >= 90 ? "bg-emerald-500" : pct >= 60 ? "bg-amber-400" : "bg-red-400";
  return (
    <div className="flex items-center gap-2 min-w-[120px]">
      <div className="flex-1 h-2 bg-neutral-100 dark:bg-neutral-700 rounded-full overflow-hidden">
        <div className={`h-full rounded-full ${color}`} style={{ width: `${pct}%` }} />
      </div>
      <span className="text-xs tabular-nums font-bold text-neutral-600 dark:text-neutral-300 w-10 text-right">{pct}%</span>
    </div>
  );
}

/* ─── SecondaryButton ─── */

interface SecondaryButtonProps {
  children: React.ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  className?: string;
}

export function SecondaryButton({ children, onClick, disabled, className = "" }: SecondaryButtonProps) {
  return (
    <motion.button
      type="button"
      whileHover={disabled ? undefined : { y: -1, scale: 1.01 }}
      whileTap={disabled ? undefined : { scale: 0.98 }}
      onClick={onClick}
      disabled={disabled}
      className={`inline-flex items-center justify-center gap-2 rounded-xl border border-neutral-200 bg-white px-5 py-2.5 text-sm font-bold text-neutral-700 shadow-sm transition-colors hover:bg-neutral-50 disabled:opacity-50 disabled:cursor-not-allowed dark:border-neutral-700 dark:bg-neutral-800 dark:text-neutral-200 dark:hover:bg-neutral-700 ${className}`}
    >
      {children}
    </motion.button>
  );
}
