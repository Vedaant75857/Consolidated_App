import type { QualityIntersections } from "./types";

interface Props {
  intersections: QualityIntersections;
}

function fmt(val: number) {
  return `${val.toFixed(1)}%`;
}

export default function IntersectionMatrix({ intersections }: Props) {
  return (
    <div>
      <h4 className="text-xs font-semibold uppercase tracking-wider text-neutral-500 dark:text-neutral-400 mb-3">
        Word x Length Matrix
      </h4>
      <div className="overflow-hidden rounded-lg border border-neutral-200 dark:border-neutral-700">
        <table className="w-full text-sm">
          <thead>
            <tr className="bg-neutral-50 dark:bg-neutral-800/60">
              <th className="px-4 py-2 text-left text-xs font-medium text-neutral-500 dark:text-neutral-400" />
              <th className="px-4 py-2 text-center text-xs font-medium text-neutral-500 dark:text-neutral-400">
                Long &gt;10 chars (C)
              </th>
              <th className="px-4 py-2 text-center text-xs font-medium text-neutral-500 dark:text-neutral-400">
                Short &le;10 chars (D)
              </th>
            </tr>
          </thead>
          <tbody className="divide-y divide-neutral-200 dark:divide-neutral-700">
            <tr>
              <td className="px-4 py-2.5 text-xs font-medium text-neutral-600 dark:text-neutral-300 bg-neutral-50 dark:bg-neutral-800/60">
                Multi-word (A)
              </td>
              <td className="px-4 py-2.5 text-center font-semibold text-neutral-800 dark:text-neutral-200 tabular-nums">
                {fmt(intersections.AC)}
              </td>
              <td className="px-4 py-2.5 text-center font-semibold text-neutral-800 dark:text-neutral-200 tabular-nums">
                {fmt(intersections.AD)}
              </td>
            </tr>
            <tr>
              <td className="px-4 py-2.5 text-xs font-medium text-neutral-600 dark:text-neutral-300 bg-neutral-50 dark:bg-neutral-800/60">
                Single-word (B)
              </td>
              <td className="px-4 py-2.5 text-center font-semibold text-neutral-800 dark:text-neutral-200 tabular-nums">
                {fmt(intersections.BC)}
              </td>
              <td className="px-4 py-2.5 text-center font-semibold text-neutral-800 dark:text-neutral-200 tabular-nums">
                {fmt(intersections.BD)}
              </td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  );
}
