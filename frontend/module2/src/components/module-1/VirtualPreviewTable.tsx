import { useRef } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";

const COL_VIRTUALIZATION_THRESHOLD = 30;
const ESTIMATED_COL_WIDTH = 160;

interface VirtualPreviewTableProps {
  columns: string[];
  rows: Record<string, any>[];
  selectedRowIds: Set<string | number>;
  onToggleRow: (rowId: string | number) => void;
}

/**
 * Preview table with horizontal column virtualization for wide datasets.
 * Falls back to a plain <table> when column count is below the threshold.
 */
export default function VirtualPreviewTable({
  columns,
  rows,
  selectedRowIds,
  onToggleRow,
}: VirtualPreviewTableProps) {
  const scrollRef = useRef<HTMLDivElement>(null);
  const useVirtual = columns.length > COL_VIRTUALIZATION_THRESHOLD;

  const colVirtualizer = useVirtualizer({
    count: columns.length,
    getScrollElement: () => scrollRef.current,
    estimateSize: () => ESTIMATED_COL_WIDTH,
    horizontal: true,
    overscan: 5,
    enabled: useVirtual,
  });

  if (!useVirtual) {
    return (
      <div ref={scrollRef} className="overflow-x-auto border border-neutral-200 rounded-lg max-h-[500px] shadow-sm">
        <table className="min-w-full text-xs font-mono bg-white">
          <thead className="bg-neutral-50 sticky top-0 z-10 shadow-sm border-b border-neutral-200">
            <tr>
              <th className="px-2 py-2 text-center w-8 border-r border-neutral-200 bg-neutral-50 sticky left-0 z-20 shadow-[1px_0_0_#e5e5e5]" />
              <th className="px-2 py-2 text-center font-bold text-neutral-500 border-r border-neutral-200 w-12 whitespace-nowrap hidden md:table-cell">#</th>
              {columns.map((col, i) => (
                <th key={col + i} className="px-3 py-2 text-left font-bold text-neutral-500 whitespace-nowrap border-r border-neutral-200 last:border-r-0">
                  {col}
                </th>
              ))}
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-100">
            {rows.map((row, ri) => (
              <tr key={ri} className={`transition-colors ${selectedRowIds.has(ri) ? "bg-red-50/50" : "hover:bg-neutral-50"}`}>
                <td className="px-2 py-1.5 text-center border-r border-neutral-100 bg-inherit sticky left-0 z-10 shadow-[1px_0_0_#f5f5f5]">
                  <input type="checkbox" checked={selectedRowIds.has(ri)} onChange={() => onToggleRow(ri)} className="w-3.5 h-3.5 text-red-500 rounded border-neutral-300 focus:ring-red-500 cursor-pointer" />
                </td>
                <td className="px-2 py-1.5 text-center text-[10px] text-neutral-400 border-r border-neutral-100 hidden md:table-cell bg-inherit">
                  {ri}
                </td>
                {columns.map((col, ci) => (
                  <td key={col + ci} className="px-3 py-1.5 whitespace-nowrap text-neutral-700 max-w-[200px] truncate border-r border-neutral-100 last:border-r-0">
                    {row[col] != null && row[col] !== "" ? String(row[col]) : <span className="text-neutral-300 italic">null</span>}
                  </td>
                ))}
              </tr>
            ))}
            {rows.length === 0 && (
              <tr>
                <td colSpan={columns.length + 2} className="px-3 py-8 text-center text-neutral-400 italic">
                  No rows currently exist.
                </td>
              </tr>
            )}
          </tbody>
        </table>
      </div>
    );
  }

  /* ── Virtualized path (30+ columns) ── */

  const virtualCols = colVirtualizer.getVirtualItems();
  const totalWidth = colVirtualizer.getTotalSize();

  return (
    <div ref={scrollRef} className="overflow-x-auto overflow-y-auto border border-neutral-200 rounded-lg max-h-[500px] shadow-sm">
      <div style={{ width: totalWidth + 80, minWidth: "100%" }} className="text-xs font-mono bg-white">
        {/* Header */}
        <div className="flex bg-neutral-50 sticky top-0 z-10 shadow-sm border-b border-neutral-200">
          <div className="w-8 px-2 py-2 text-center border-r border-neutral-200 bg-neutral-50 sticky left-0 z-20 shadow-[1px_0_0_#e5e5e5] shrink-0" />
          <div className="w-12 px-2 py-2 text-center font-bold text-neutral-500 border-r border-neutral-200 shrink-0 hidden md:flex items-center justify-center">#</div>
          <div className="relative" style={{ width: totalWidth, height: 32 }}>
            {virtualCols.map((vc) => (
              <div
                key={vc.key}
                className="absolute top-0 h-full flex items-center px-3 font-bold text-neutral-500 whitespace-nowrap"
                style={{ left: vc.start, width: vc.size }}
              >
                <span className="truncate">{columns[vc.index]}</span>
              </div>
            ))}
          </div>
        </div>

        {/* Rows */}
        {rows.map((row, ri) => {
          const isSelected = selectedRowIds.has(ri);
          return (
            <div
              key={ri}
              className={`flex border-b border-neutral-100 transition-colors ${isSelected ? "bg-red-50/50" : "hover:bg-neutral-50"}`}
            >
              <div className="w-8 px-2 py-1.5 text-center border-r border-neutral-100 bg-inherit sticky left-0 z-10 shadow-[1px_0_0_#f5f5f5] shrink-0 flex items-center justify-center">
                <input type="checkbox" checked={isSelected} onChange={() => onToggleRow(ri)} className="w-3.5 h-3.5 text-red-500 rounded border-neutral-300 focus:ring-red-500 cursor-pointer" />
              </div>
              <div className="w-12 px-2 py-1.5 text-center text-[10px] text-neutral-400 border-r border-neutral-100 bg-inherit hidden md:flex items-center justify-center shrink-0">
                {ri}
              </div>
              <div className="relative" style={{ width: totalWidth, height: 28 }}>
                {virtualCols.map((vc) => {
                  const col = columns[vc.index];
                  return (
                    <div
                      key={vc.key}
                      className="absolute top-0 h-full flex items-center px-3 text-neutral-700"
                      style={{ left: vc.start, width: vc.size }}
                    >
                      <span className="truncate">
                        {row[col] != null && row[col] !== "" ? String(row[col]) : <span className="text-neutral-300 italic">null</span>}
                      </span>
                    </div>
                  );
                })}
              </div>
            </div>
          );
        })}
      </div>
    </div>
  );
}
