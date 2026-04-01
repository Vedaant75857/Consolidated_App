import type { QualityMetrics } from "./types";
import IntersectionMatrix from "./IntersectionMatrix";

interface Props {
  metrics: QualityMetrics;
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex items-baseline justify-between py-1.5">
      <span className="text-xs text-neutral-500 dark:text-neutral-400">
        {label}
      </span>
      <span className="text-sm font-semibold text-neutral-800 dark:text-neutral-200 tabular-nums">
        {value}
      </span>
    </div>
  );
}

function pct(val: number) {
  return `${val.toFixed(1)}%`;
}

function formatRows(n: number) {
  return n.toLocaleString();
}

export default function MetricsPanel({ metrics }: Props) {
  const d = metrics.description;

  return (
    <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
      {/* Description Quality */}
      <div className="space-y-1">
        <h4 className="text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400 mb-2">
          Description Quality
        </h4>
        <div className="divide-y divide-neutral-100 dark:divide-neutral-800">
          <Stat label="Completion Rate" value={pct(d.completionRate)} />
          <Stat label="Coded / Alphanumeric" value={pct(d.codedAlphanumericPct)} />
          <Stat label="Multi-word (A)" value={pct(d.multiWordPct)} />
          <Stat label="Single-word (B)" value={pct(d.singleWordPct)} />
          <Stat label="Long >10 chars (C)" value={pct(d.longPct)} />
          <Stat label="Short ≤10 chars (D)" value={pct(d.shortPct)} />
        </div>
      </div>

      {/* Supplier Quality + General */}
      <div className="space-y-6">
        <div className="space-y-1">
          <h4 className="text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400 mb-2">
            Supplier Quality
          </h4>
          <div className="divide-y divide-neutral-100 dark:divide-neutral-800">
            <Stat label="Fill Rate" value={pct(metrics.supplierFillRate)} />
          </div>
        </div>

        <div className="space-y-1">
          <h4 className="text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400 mb-2">
            General
          </h4>
          <div className="divide-y divide-neutral-100 dark:divide-neutral-800">
            <Stat label="Total Rows" value={formatRows(metrics.totalRows)} />
          </div>
        </div>
      </div>

      {/* Intersection Matrix */}
      <IntersectionMatrix intersections={d.intersections} />
    </div>
  );
}
