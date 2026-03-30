import { useRef, useEffect } from "react";
import * as d3 from "d3";
import { useTheme } from "../../../theme/ThemeProvider";

interface Props {
  labels: string[];
  spendValues: number[];
  cumulativePercent: number[];
  title: string;
}

export default function ParetoChart({ labels, spendValues, cumulativePercent, title }: Props) {
  const svgRef = useRef<SVGSVGElement>(null);
  const tooltipRef = useRef<HTMLDivElement>(null);
  const { theme } = useTheme();

  useEffect(() => {
    if (!svgRef.current || !labels.length) return;

    const svg = d3.select(svgRef.current);
    svg.selectAll("*").remove();

    const width = svgRef.current.clientWidth;
    const height = 360;
    const margin = { top: 20, right: 55, bottom: 20, left: 70 };
    const chartW = width - margin.left - margin.right;
    const chartH = height - margin.top - margin.bottom;

    svg.attr("viewBox", `0 0 ${width} ${height}`);

    const g = svg.append("g").attr("transform", `translate(${margin.left},${margin.top})`);

    const textColor = theme === "dark" ? "#9CA3AF" : "#6B7280";
    const gridColor = theme === "dark" ? "#374151" : "#E5E7EB";
    const tooltip = d3.select(tooltipRef.current);

    const maxSpend = d3.max(spendValues) || 1;

    const xScale = d3
      .scaleBand()
      .domain(labels)
      .range([0, chartW])
      .padding(0.25);

    const yLeft = d3
      .scaleLinear()
      .domain([0, maxSpend * 1.05])
      .range([chartH, 0]);

    const yRight = d3.scaleLinear().domain([0, 100]).range([chartH, 0]);

    const gridLines = g.append("g").attr("class", "grid");
    yLeft.ticks(5).forEach((tick) => {
      gridLines
        .append("line")
        .attr("x1", 0)
        .attr("x2", chartW)
        .attr("y1", yLeft(tick))
        .attr("y2", yLeft(tick))
        .attr("stroke", gridColor)
        .attr("stroke-dasharray", "3,3")
        .attr("opacity", 0.6);
    });

    const formatVal = (v: number) =>
      v >= 1e6 ? `$${(v / 1e6).toFixed(1)}M` : v >= 1e3 ? `$${(v / 1e3).toFixed(0)}K` : `$${v.toFixed(0)}`;

    const leftAxis = g.append("g").call(
      d3.axisLeft(yLeft).ticks(5).tickFormat((d) => formatVal(d as number))
    );
    leftAxis.selectAll("text").attr("fill", textColor).attr("font-size", 10);
    leftAxis.selectAll("line").attr("stroke", textColor).attr("opacity", 0.4);
    leftAxis.select(".domain").attr("stroke", textColor).attr("opacity", 0.4);

    const rightAxis = g
      .append("g")
      .attr("transform", `translate(${chartW},0)`)
      .call(d3.axisRight(yRight).ticks(5).tickFormat((d) => `${d}%`));
    rightAxis.selectAll("text").attr("fill", textColor).attr("font-size", 10);
    rightAxis.selectAll("line").attr("stroke", textColor).attr("opacity", 0.4);
    rightAxis.select(".domain").attr("stroke", textColor).attr("opacity", 0.4);

    g.selectAll(".bar")
      .data(labels)
      .enter()
      .append("rect")
      .attr("x", (d) => xScale(d)!)
      .attr("y", (_, i) => yLeft(spendValues[i]))
      .attr("width", xScale.bandwidth())
      .attr("height", (_, i) => chartH - yLeft(spendValues[i]))
      .attr("fill", "#CC0000")
      .attr("rx", 2)
      .style("cursor", "pointer")
      .on("mouseover", (event, d) => {
        const i = labels.indexOf(d);
        tooltip.style("opacity", 1).html(
          `<div style="font-weight:600">${d}</div>
           <div>Spend: ${formatVal(spendValues[i])}</div>
           <div>Cumulative: ${cumulativePercent[i]?.toFixed(1)}%</div>`
        );
      })
      .on("mousemove", (event) => {
        const [mx, my] = d3.pointer(event, svgRef.current);
        tooltip.style("left", mx + 12 + "px").style("top", my - 10 + "px");
      })
      .on("mouseout", () => {
        tooltip.style("opacity", 0);
      });

    const lineGen = d3
      .line<number>()
      .x((_, i) => xScale(labels[i])! + xScale.bandwidth() / 2)
      .y((d) => yRight(d))
      .curve(d3.curveMonotoneX);

    g.append("path")
      .datum(cumulativePercent)
      .attr("fill", "none")
      .attr("stroke", "#E53E3E")
      .attr("stroke-width", 2)
      .attr("d", lineGen);

    g.selectAll(".dot")
      .data(cumulativePercent)
      .enter()
      .append("circle")
      .attr("cx", (_, i) => xScale(labels[i])! + xScale.bandwidth() / 2)
      .attr("cy", (d) => yRight(d))
      .attr("r", 3)
      .attr("fill", "#E53E3E")
      .attr("stroke", theme === "dark" ? "#1A1A1A" : "#fff")
      .attr("stroke-width", 1.5);
  }, [labels, spendValues, cumulativePercent, theme]);

  if (!labels.length) {
    return <p className="text-sm text-neutral-400 p-4">No chart data</p>;
  }

  return (
    <div className="relative">
      <svg ref={svgRef} className="w-full" style={{ height: 360 }} />
      <div
        ref={tooltipRef}
        className="absolute pointer-events-none opacity-0 bg-white dark:bg-neutral-800 text-neutral-900 dark:text-neutral-100 text-xs px-3 py-2 rounded-lg shadow-lg border border-neutral-200 dark:border-neutral-700 transition-opacity z-20"
        style={{ maxWidth: 220 }}
      />
    </div>
  );
}
