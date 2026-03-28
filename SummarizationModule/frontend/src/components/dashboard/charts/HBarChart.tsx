import { ResponsiveBar } from "@nivo/bar";
import { useTheme } from "../../../theme/ThemeProvider";

interface Props {
  labels: string[];
  values: number[];
  title: string;
}

export default function HBarChart({ labels, values, title }: Props) {
  const { theme } = useTheme();

  const data = labels
    .map((label, i) => ({ label, value: values[i] || 0 }))
    .reverse();

  if (!data.length) return <p className="text-sm text-neutral-400 p-4">No chart data</p>;

  const textColor = theme === "dark" ? "#9CA3AF" : "#6B7280";
  const gridColor = theme === "dark" ? "#374151" : "#E5E7EB";
  const height = Math.max(300, data.length * 28 + 60);

  return (
    <div style={{ height }}>
      <ResponsiveBar
        data={data}
        keys={["value"]}
        indexBy="label"
        layout="horizontal"
        margin={{ top: 10, right: 30, bottom: 40, left: 160 }}
        padding={0.25}
        colors={["#CC0000"]}
        borderRadius={3}
        theme={{
          text: { fill: textColor, fontSize: 11 },
          axis: {
            ticks: { text: { fill: textColor, fontSize: 10 } },
            legend: { text: { fill: textColor, fontSize: 11 } },
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
          format: (v) =>
            typeof v === "number"
              ? v >= 1e6
                ? `$${(v / 1e6).toFixed(1)}M`
                : v >= 1e3
                  ? `$${(v / 1e3).toFixed(0)}K`
                  : `$${v}`
              : v,
        }}
        axisLeft={{
          truncateTickAt: 25,
        }}
        enableLabel={false}
        animate={true}
        motionConfig="gentle"
      />
    </div>
  );
}
