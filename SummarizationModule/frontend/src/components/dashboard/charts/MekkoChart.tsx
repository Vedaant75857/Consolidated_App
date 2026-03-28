import { useRef, useEffect, useMemo } from "react";
import * as d3 from "d3";
import { useTheme } from "../../../theme/ThemeProvider";
import type { MekkoData } from "../../../types";

interface Props {
  data: MekkoData;
  title: string;
}

const RED_PALETTE = [
  "#CC0000", "#E53E3E", "#FC8181", "#FEB2B2",
  "#4A5568", "#718096", "#A0AEC0", "#CBD5E0",
  "#C53030", "#F56565", "#FED7D7", "#9B2C2C",
  "#E2E8F0", "#A0AEC0", "#2D3748", "#EDF2F7",
];

export default function MekkoChart({ data, title }: Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const { theme } = useTheme();

  const allSegLabels = useMemo(() => {
    const s = new Set<string>();
    data.columns.forEach((c) => c.segments.forEach((seg) => s.add(seg.label)));
    return Array.from(s);
  }, [data]);

  const colorScale = useMemo(() => {
    return d3.scaleOrdinal<string>().domain(allSegLabels).range(RED_PALETTE);
  }, [allSegLabels]);

  useEffect(() => {
    if (!svgRef.current || !data.columns.length) return;

    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const width = svgRef.current.clientWidth;
    const height = 360;
    const margin = { top: 20, right: 20, bottom: 60, left: 20 };
    const chartW = width - margin.left - margin.right;
    const chartH = height - margin.top - margin.bottom;

    svg.attr("viewBox", `0 0 ${width} ${height}`);

    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const textColor = theme === "dark" ? "#9CA3AF" : "#6B7280";
    const tooltip = d3.select(tooltipRef.current);

    let xOffset = 0;
    const sorted = [...data.columns].sort((a, b) => b.totalSpend - a.totalSpend);

    sorted.forEach((col) => {
      const colW = col.width * chartW;
      const sortedSegs = [...col.segments].sort((a, b) => b.value - a.value);
      let yOffset = chartH;

      sortedSegs.forEach((seg) => {
        const segH = seg.share * chartH;
        yOffset -= segH;

        g.append("rect")
          .attr("x", xOffset)
          .attr("y", yOffset)
          .attr("width", Math.max(0, colW - 1))
          .attr("height", Math.max(0, segH))
          .attr("fill", colorScale(seg.label))
          .attr("stroke", theme === "dark" ? "#1A1A1A" : "#fff")
          .attr("stroke-width", 1)
          .style("cursor", "pointer")
          .on("mouseover", (event) => {
            tooltip
              .style("opacity", 1)
              .html(
                `<div style="font-weight:600">${col.label}</div>
                 <div>${seg.label}</div>
                 <div>$${seg.value.toLocaleString("en-US", { maximumFractionDigits: 0 })}</div>
                 <div>${(seg.share * 100).toFixed(1)}% of column</div>`
              );
          })
          .on("mousemove", (event) => {
            tooltip
              .style("left", event.offsetX + 12 + "px")
              .style("top", event.offsetY - 10 + "px");
          })
          .on("mouseout", () => {
            tooltip.style("opacity", 0);
          });

        if (segH > 16 && colW > 40) {
          g.append("text")
            .attr("x", xOffset + colW / 2)
            .attr("y", yOffset + segH / 2 + 4)
            .attr("text-anchor", "middle")
            .attr("fill", "#fff")
            .attr("font-size", 9)
            .attr("pointer-events", "none")
            .text(seg.label.length > 12 ? seg.label.slice(0, 11) + "…" : seg.label);
        }
      });

      g.append("text")
        .attr("x", xOffset + colW / 2)
        .attr("y", chartH + 14)
        .attr("text-anchor", "middle")
        .attr("fill", textColor)
        .attr("font-size", 10)
        .attr("transform", `rotate(-30, ${xOffset + colW / 2}, ${chartH + 14})`)
        .text(col.label.length > 16 ? col.label.slice(0, 15) + "…" : col.label);

      xOffset += colW;
    });
  }, [data, theme, colorScale]);

  if (!data.columns.length) {
    return <p className="text-sm text-neutral-400 p-4">No Mekko data available</p>;
  }

  return (
    <div className="relative">
      <svg ref={svgRef} className="w-full" style={{ height: 360 }} />
      <div
        ref={tooltipRef}
        className="absolute pointer-events-none opacity-0 bg-white dark:bg-neutral-800 text-neutral-900 dark:text-neutral-100 text-xs px-3 py-2 rounded-lg shadow-lg border border-neutral-200 dark:border-neutral-700 transition-opacity z-20"
        style={{ maxWidth: 200 }}
      />
      <div className="flex flex-wrap gap-2 px-2 pt-2">
        {allSegLabels.slice(0, 12).map((label) => (
          <div key={label} className="flex items-center gap-1.5 text-xs text-neutral-600 dark:text-neutral-400">
            <div className="w-3 h-3 rounded-sm shrink-0" style={{ backgroundColor: colorScale(label) }} />
            <span className="truncate max-w-[100px]">{label}</span>
          </div>
        ))}
        {allSegLabels.length > 12 && (
          <span className="text-xs text-neutral-400">+{allSegLabels.length - 12} more</span>
        )}
      </div>
    </div>
  );
}
