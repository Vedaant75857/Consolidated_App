import { useState, useCallback } from "react";
import { ChevronRight, ChevronDown } from "lucide-react";
import type { TreeNode } from "../../types";

interface Props {
  data: TreeNode[];
}

const LEVEL_COLORS: Record<string, string> = {
  l1: "bg-primary text-white",
  l2: "bg-primary-200 text-primary-800 dark:bg-primary-900/40 dark:text-primary-200",
  l3: "bg-primary-100 text-primary-700 dark:bg-primary-900/20 dark:text-primary-300",
};

function formatCurrency(v: number): string {
  if (v >= 1e9) return `$${(v / 1e9).toFixed(1)}B`;
  if (v >= 1e6) return `$${(v / 1e6).toFixed(1)}M`;
  if (v >= 1e3) return `$${(v / 1e3).toFixed(0)}K`;
  return `$${v.toFixed(0)}`;
}

function TreeRow({
  node,
  depth,
  expanded,
  onToggle,
}: {
  node: TreeNode;
  depth: number;
  expanded: Set<string>;
  onToggle: (key: string) => void;
}) {
  const nodeKey = `${node.level}::${node.name}::${depth}`;
  const isExpanded = expanded.has(nodeKey);
  const hasChildren = node.children && node.children.length > 0;

  return (
    <>
      <tr
        className="border-b border-neutral-100 dark:border-neutral-800 hover:bg-primary-50/30 dark:hover:bg-primary-900/10 transition-colors cursor-pointer"
        onClick={() => hasChildren && onToggle(nodeKey)}
      >
        <td className="py-2 px-3" style={{ paddingLeft: `${depth * 24 + 12}px` }}>
          <div className="flex items-center gap-2">
            {hasChildren ? (
              isExpanded ? (
                <ChevronDown className="w-4 h-4 text-neutral-400 shrink-0" />
              ) : (
                <ChevronRight className="w-4 h-4 text-neutral-400 shrink-0" />
              )
            ) : (
              <span className="w-4 shrink-0" />
            )}
            <span
              className={`inline-block px-2 py-0.5 rounded text-xs font-medium ${
                LEVEL_COLORS[node.level] || LEVEL_COLORS.l3
              }`}
            >
              {node.level.toUpperCase()}
            </span>
            <span className="text-sm font-medium text-neutral-900 dark:text-neutral-100 truncate">
              {node.name}
            </span>
          </div>
        </td>
        <td className="py-2 px-3 text-right text-sm tabular-nums font-medium text-neutral-900 dark:text-neutral-100">
          {formatCurrency(node.totalSpend)}
        </td>
        <td className="py-2 px-3 text-right text-sm tabular-nums text-neutral-600 dark:text-neutral-400">
          {node.percentOfParent.toFixed(1)}%
        </td>
        <td className="py-2 px-3 text-right text-sm tabular-nums text-neutral-600 dark:text-neutral-400">
          {node.percentOfTotal.toFixed(1)}%
        </td>
        <td className="py-2 px-3 w-32">
          <div className="w-full h-1.5 rounded-full bg-neutral-200 dark:bg-neutral-700 overflow-hidden">
            <div
              className="h-full rounded-full bg-primary transition-all"
              style={{ width: `${Math.min(100, node.percentOfTotal)}%` }}
            />
          </div>
        </td>
      </tr>
      {isExpanded &&
        hasChildren &&
        node.children.map((child, i) => (
          <TreeRow
            key={`${child.level}-${child.name}-${i}`}
            node={child}
            depth={depth + 1}
            expanded={expanded}
            onToggle={onToggle}
          />
        ))}
    </>
  );
}

export default function TreePivotTable({ data }: Props) {
  const [expanded, setExpanded] = useState<Set<string>>(new Set());

  const onToggle = useCallback((key: string) => {
    setExpanded((prev) => {
      const next = new Set(prev);
      if (next.has(key)) next.delete(key);
      else next.add(key);
      return next;
    });
  }, []);

  const expandAll = () => {
    const keys = new Set<string>();
    function walk(nodes: TreeNode[], depth: number) {
      nodes.forEach((n) => {
        keys.add(`${n.level}::${n.name}::${depth}`);
        if (n.children) walk(n.children, depth + 1);
      });
    }
    walk(data, 0);
    setExpanded(keys);
  };

  const collapseAll = () => setExpanded(new Set());

  if (!data.length) {
    return <p className="text-sm text-neutral-400 p-4">No drill-down data available</p>;
  }

  return (
    <div>
      <div className="flex gap-2 mb-2">
        <button
          onClick={expandAll}
          className="text-xs px-3 py-1 rounded-lg border border-neutral-200 dark:border-neutral-700 text-neutral-600 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
        >
          Expand All
        </button>
        <button
          onClick={collapseAll}
          className="text-xs px-3 py-1 rounded-lg border border-neutral-200 dark:border-neutral-700 text-neutral-600 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-800 transition-colors"
        >
          Collapse All
        </button>
      </div>
      <div className="overflow-auto max-h-[500px] rounded-lg border border-neutral-200 dark:border-neutral-700">
        <table className="w-full text-xs">
          <thead className="sticky top-0 z-10">
            <tr className="bg-neutral-100 dark:bg-neutral-800">
              <th className="text-left px-3 py-2 font-semibold text-neutral-600 dark:text-neutral-400">Category</th>
              <th className="text-right px-3 py-2 font-semibold text-neutral-600 dark:text-neutral-400">Total Spend</th>
              <th className="text-right px-3 py-2 font-semibold text-neutral-600 dark:text-neutral-400">% of Parent</th>
              <th className="text-right px-3 py-2 font-semibold text-neutral-600 dark:text-neutral-400">% of Total</th>
              <th className="px-3 py-2 font-semibold text-neutral-600 dark:text-neutral-400">Share</th>
            </tr>
          </thead>
          <tbody>
            {data.map((node, i) => (
              <TreeRow
                key={`${node.level}-${node.name}-${i}`}
                node={node}
                depth={0}
                expanded={expanded}
                onToggle={onToggle}
              />
            ))}
          </tbody>
        </table>
      </div>
    </div>
  );
}
