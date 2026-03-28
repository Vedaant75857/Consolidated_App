import { ResponsiveBar } from "@nivo/bar";
import { ResponsiveLine } from "@nivo/line";
import { useTheme } from "../../../theme/ThemeProvider";

interface Props {
  labels: string[];
  spendValues: number[];
  cumulativePercent: number[];
  title: string;
}

export default function ParetoChart({ labels, spendValues, cumulativePercent, title }: Props) {
  const { theme } = useTheme();

  const barData = labels.map((label, i) => ({
    label,
    spend: spendValues[i] || 0,
  }));

  const lineData = [
    {
      id: "Cumulative %",
      data: labels.map((label, i) => ({
        x: label,
        y: cumulativePercent[i] || 0,
      })),
    },
  ];

  if (!barData.length) return <p className="text-sm text-neutral-400 p-4">No chart data</p>;

  const textColor = theme === "dark" ? "#9CA3AF" : "#6B7280";
  const gridColor = theme === "dark" ? "#374151" : "#E5E7EB";

  return (
    <div className="space-y-2">
      <div style={{ height: 280 }}>
        <ResponsiveBar
          data={barData}
          keys={["spend"]}
          indexBy="label"
          margin={{ top: 20, right: 60, bottom: 70, left: 80 }}
          padding={0.25}
          colors={["#CC0000"]}
          borderRadius={3}
          theme={{
            text: { fill: textColor, fontSize: 11 },
            axis: {
              ticks: { text: { fill: textColor, fontSize: 9 } },
            },
            grid: { line: { stroke: gridColor } },
            tooltip: {
              container: {
                background: theme === "dark" ? "#262626" : "#fff",
                color: theme === "dark" ? "#F9FAFB" : "#111827",
                borderRadius: "8px",
                boxShadow: "0 4px 12px rgba(0,0,0,0.15)",
                fontSize: 12,
              },
            },
          }}
          axisBottom={{
            tickRotation: -45,
            truncateTickAt: 12,
          }}
          axisLeft={{
            format: (v) =>
              typeof v === "number"
                ? v >= 1e6
                  ? `$${(v / 1e6).toFixed(1)}M`
                  : v >= 1e3
                    ? `$${(v / 1e3).toFixed(0)}K`
                    : `$${v}`
                : v,
          }}
          enableLabel={false}
          animate={true}
          motionConfig="gentle"
        />
      </div>
      <div style={{ height: 120 }}>
        <ResponsiveLine
          data={lineData}
          margin={{ top: 10, right: 60, bottom: 30, left: 80 }}
          xScale={{ type: "point" }}
          yScale={{ type: "linear", min: 0, max: 100 }}
          colors={["#E53E3E"]}
          pointSize={4}
          pointColor="#E53E3E"
          pointBorderWidth={2}
          pointBorderColor="#fff"
          enableArea={true}
          areaBaselineValue={0}
          areaOpacity={0.1}
          theme={{
            text: { fill: textColor, fontSize: 10 },
            axis: { ticks: { text: { fill: textColor, fontSize: 9 } } },
            grid: { line: { stroke: gridColor } },
            tooltip: {
              container: {
                background: theme === "dark" ? "#262626" : "#fff",
                color: theme === "dark" ? "#F9FAFB" : "#111827",
                borderRadius: "8px",
                fontSize: 12,
              },
            },
          }}
          axisLeft={{ format: (v) => `${v}%` }}
          axisBottom={{ tickRotation: -45, truncateTickAt: 8 }}
          animate={true}
          motionConfig="gentle"
        />
      </div>
    </div>
  );
}
