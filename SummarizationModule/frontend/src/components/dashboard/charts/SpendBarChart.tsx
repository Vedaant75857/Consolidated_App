import { ResponsiveBar } from "@nivo/bar";
import { useTheme } from "../../../theme/ThemeProvider";

interface Props {
  labels: string[];
  values: number[];
  title: string;
}

export default function SpendBarChart({ labels, values, title }: Props) {
  const { theme } = useTheme();

  const data = labels.map((label, i) => ({
    label,
    value: values[i] || 0,
  }));

  if (!data.length) return <p className="text-sm text-neutral-400 p-4">No chart data</p>;

  const textColor = theme === "dark" ? "#9CA3AF" : "#6B7280";
  const gridColor = theme === "dark" ? "#374151" : "#E5E7EB";

  return (
    <div style={{ height: 320 }}>
      <ResponsiveBar
        data={data}
        keys={["value"]}
        indexBy="label"
        margin={{ top: 20, right: 20, bottom: 70, left: 80 }}
        padding={0.3}
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
          tickRotation: -45,
          truncateTickAt: 16,
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
        labelSkipWidth={20}
        labelSkipHeight={12}
        labelTextColor="#ffffff"
        enableLabel={false}
        animate={true}
        motionConfig="gentle"
      />
    </div>
  );
}
