import React from "react";

const MONTH_ORDER = ["Jan","Feb","Mar","Apr","May","Jun","Jul","Aug","Sep","Oct","Nov","Dec"];

export interface UnsupportedCurrency {
  code: string;
  row_count: number;
  total_spend: number;
  year_month_breakdown: Record<string, string[]>; // e.g. {"2024": ["Jan","Mar"], "2025": ["Feb"]}
}

export type FxOverrideMode = "yearly" | "monthly";
export type YearlyOverrides = Record<string, Record<string, string>>;   // {ccy: {year: value}}
export type MonthlyOverrides = Record<string, Record<string, Record<string, string>>>; // {ccy: {year: {month: value}}}

interface Props {
  currencies: UnsupportedCurrency[];
  dateColumnSelected: boolean;
  scopeYear: string;
  overrideMode: FxOverrideMode;
  onOverrideModeChange: (mode: FxOverrideMode) => void;
  yearlyOverrides: YearlyOverrides;
  onYearlyOverridesChange: (v: YearlyOverrides) => void;
  monthlyOverrides: MonthlyOverrides;
  onMonthlyOverridesChange: (v: MonthlyOverrides) => void;
  showValidation: boolean;
  disabled: boolean;
}

const fmtSpend = (n: number) =>
  new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 }).format(Math.round(n));

/** Check if any required FX rate input is empty */
export function hasEmptyOverrides(
  mode: FxOverrideMode,
  currencies: UnsupportedCurrency[],
  yearlyOvr: YearlyOverrides,
  monthlyOvr: MonthlyOverrides,
  dateColSelected: boolean,
  scopeYear: string,
): boolean {
  for (const c of currencies) {
    if (mode === "yearly") {
      const years = dateColSelected
        ? Object.keys(c.year_month_breakdown)
        : [scopeYear];
      for (const yr of years) {
        const val = yearlyOvr[c.code]?.[yr] ?? "";
        if (!val.trim()) return true;
      }
    } else {
      for (const [yr, months] of Object.entries(c.year_month_breakdown)) {
        for (const mo of months) {
          const val = monthlyOvr[c.code]?.[yr]?.[mo] ?? "";
          if (!val.trim()) return true;
        }
      }
    }
  }
  return false;
}

export default function UnsupportedCurrencyPanel({
  currencies,
  dateColumnSelected,
  scopeYear,
  overrideMode,
  onOverrideModeChange,
  yearlyOverrides,
  onYearlyOverridesChange,
  monthlyOverrides,
  onMonthlyOverridesChange,
  showValidation,
  disabled,
}: Props) {
  if (!currencies.length) return null;

  // Collect all years across all currencies (descending)
  const allYears: number[] = dateColumnSelected
    ? Array.from(
        new Set(currencies.flatMap(c => Object.keys(c.year_month_breakdown).map(Number)))
      ).sort((a, b) => b - a)
    : [Number(scopeYear)];

  // --- Helpers ---
  const setYearly = (ccy: string, yr: string, val: string) => {
    onYearlyOverridesChange({
      ...yearlyOverrides,
      [ccy]: { ...(yearlyOverrides[ccy] || {}), [yr]: val },
    });
  };

  const setMonthly = (ccy: string, yr: string, mo: string, val: string) => {
    onMonthlyOverridesChange({
      ...monthlyOverrides,
      [ccy]: {
        ...(monthlyOverrides[ccy] || {}),
        [yr]: { ...(monthlyOverrides[ccy]?.[yr] || {}), [mo]: val },
      },
    });
  };

  const isEmpty = (val: string | undefined) => !val || !val.trim();

  const inputCls = (val: string | undefined) =>
    `w-24 text-sm rounded-lg border bg-white dark:bg-neutral-900 text-neutral-800 dark:text-neutral-200 px-2 py-1 focus:outline-none focus:ring-2 focus:ring-red-500/40 ${
      showValidation && isEmpty(val)
        ? "border-red-400 dark:border-red-500"
        : "border-neutral-300 dark:border-neutral-600"
    }`;

  // For monthly mode, find the max number of months any currency has under a given year
  const maxMonthsPerYear: Record<number, number> = {};
  if (overrideMode === "monthly") {
    for (const yr of allYears) {
      let max = 0;
      for (const c of currencies) {
        const months = c.year_month_breakdown[String(yr)] || [];
        if (months.length > max) max = months.length;
      }
      maxMonthsPerYear[yr] = max;
    }
  }

  return (
    <div className="space-y-4">
      {/* Section A: Summary Table */}
      <div>
        <p className="text-sm font-medium text-neutral-700 dark:text-neutral-300 mb-2">
          Unsupported currencies detected:
        </p>
        <div className="overflow-x-auto rounded-lg border border-neutral-200 dark:border-neutral-700">
          <table className="w-full text-sm">
            <thead className="bg-neutral-100 dark:bg-neutral-800">
              <tr className="text-left text-xs text-neutral-500">
                <th className="px-3 py-2 font-medium border-b border-neutral-200 dark:border-neutral-700">Unsupported Currency</th>
                <th className="px-3 py-2 font-medium border-b border-neutral-200 dark:border-neutral-700">No. of Rows</th>
                <th className="px-3 py-2 font-medium border-b border-neutral-200 dark:border-neutral-700">Total Spend</th>
              </tr>
            </thead>
            <tbody>
              {currencies.map(c => (
                <tr key={c.code} className="border-b border-neutral-100 dark:border-neutral-800 last:border-0">
                  <td className="px-3 py-2 font-mono font-semibold text-neutral-800 dark:text-neutral-200">{c.code}</td>
                  <td className="px-3 py-2 text-neutral-600 dark:text-neutral-400">{c.row_count.toLocaleString()}</td>
                  <td className="px-3 py-2 text-neutral-600 dark:text-neutral-400">{fmtSpend(c.total_spend)} {c.code}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </div>

      {/* Section B: Yearly/Monthly Toggle */}
      <div className="space-y-3">
        <div className="flex items-center gap-2">
          <span className="text-sm font-medium text-neutral-700 dark:text-neutral-300">FX Rate Mode:</span>
          <div className="inline-flex rounded-lg border border-neutral-300 dark:border-neutral-600 overflow-hidden">
            <button
              type="button"
              onClick={() => onOverrideModeChange("yearly")}
              disabled={disabled}
              className={`px-3 py-1 text-xs font-medium transition-colors ${
                overrideMode === "yearly"
                  ? "bg-red-600 text-white"
                  : "bg-white dark:bg-neutral-800 text-neutral-600 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-700"
              }`}
            >
              Yearly
            </button>
            <button
              type="button"
              onClick={() => dateColumnSelected && onOverrideModeChange("monthly")}
              disabled={disabled || !dateColumnSelected}
              title={!dateColumnSelected ? "Select a date column to enable monthly mode" : undefined}
              className={`px-3 py-1 text-xs font-medium transition-colors border-l border-neutral-300 dark:border-neutral-600 ${
                overrideMode === "monthly"
                  ? "bg-red-600 text-white"
                  : !dateColumnSelected
                    ? "bg-neutral-100 dark:bg-neutral-800 text-neutral-400 dark:text-neutral-600 cursor-not-allowed"
                    : "bg-white dark:bg-neutral-800 text-neutral-600 dark:text-neutral-400 hover:bg-neutral-50 dark:hover:bg-neutral-700"
              }`}
            >
              Monthly
            </button>
          </div>
          {!dateColumnSelected && (
            <span className="text-xs text-neutral-400 dark:text-neutral-500 italic">
              Select a date column to enable monthly mode
            </span>
          )}
        </div>

        <p className="text-xs text-neutral-500 dark:text-neutral-400">
          Enter rate as: 1 USD = X (e.g. 3.67 for AED). Leave blank to skip those rows.
        </p>

        {/* Section C: Yearly Mode Grid */}
        {overrideMode === "yearly" && (
          <div className="overflow-x-auto rounded-lg border border-neutral-200 dark:border-neutral-700">
            <table className="w-full text-sm">
              <thead className="bg-neutral-100 dark:bg-neutral-800">
                <tr className="text-left text-xs text-neutral-500">
                  <th className="px-3 py-2 font-medium border-b border-neutral-200 dark:border-neutral-700 sticky left-0 bg-neutral-100 dark:bg-neutral-800 z-10">Currency</th>
                  {allYears.map(yr => (
                    <th key={yr} className="px-3 py-2 font-medium border-b border-neutral-200 dark:border-neutral-700 text-center">{yr}</th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {currencies.map(c => {
                  const ccyYears = dateColumnSelected
                    ? new Set(Object.keys(c.year_month_breakdown).map(Number))
                    : new Set([Number(scopeYear)]);
                  return (
                    <tr key={c.code} className="border-b border-neutral-100 dark:border-neutral-800 last:border-0">
                      <td className="px-3 py-2 font-mono font-semibold text-neutral-800 dark:text-neutral-200 sticky left-0 bg-white dark:bg-neutral-900 z-10">{c.code}</td>
                      {allYears.map(yr => (
                        <td key={yr} className="px-3 py-2 text-center">
                          {ccyYears.has(yr) ? (
                            <input
                              type="number"
                              min="0"
                              step="any"
                              value={yearlyOverrides[c.code]?.[String(yr)] ?? ""}
                              onChange={e => setYearly(c.code, String(yr), e.target.value)}
                              placeholder="e.g. 3.67"
                              disabled={disabled}
                              className={inputCls(yearlyOverrides[c.code]?.[String(yr)])}
                            />
                          ) : (
                            <span className="text-neutral-300 dark:text-neutral-600">—</span>
                          )}
                        </td>
                      ))}
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        )}

        {/* Section D: Monthly Mode Grid */}
        {overrideMode === "monthly" && dateColumnSelected && (
          <div className="overflow-x-auto rounded-lg border border-neutral-200 dark:border-neutral-700">
            <table className="w-full text-sm border-collapse">
              <thead className="bg-neutral-100 dark:bg-neutral-800">
                <tr className="text-left text-xs text-neutral-500">
                  <th className="px-3 py-2 font-medium border-b border-neutral-200 dark:border-neutral-700 sticky left-0 bg-neutral-100 dark:bg-neutral-800 z-10">Currency</th>
                  {allYears.map(yr => (
                    <th
                      key={yr}
                      className="px-3 py-2 font-medium border-b border-neutral-200 dark:border-neutral-700 text-center"
                      colSpan={2}
                    >
                      {yr}
                    </th>
                  ))}
                </tr>
              </thead>
              <tbody>
                {currencies.map(c => {
                  // Find max rows needed for this currency across all years
                  const maxRows = Math.max(
                    1,
                    ...allYears.map(yr => (c.year_month_breakdown[String(yr)] || []).length)
                  );
                  return Array.from({ length: maxRows }, (_, rowIdx) => (
                    <tr
                      key={`${c.code}-${rowIdx}`}
                      className={`${rowIdx === maxRows - 1 ? "border-b border-neutral-200 dark:border-neutral-700" : ""}`}
                    >
                      {/* Currency label only in first row */}
                      <td
                        className={`px-3 py-1.5 font-mono font-semibold text-neutral-800 dark:text-neutral-200 sticky left-0 bg-white dark:bg-neutral-900 z-10 ${rowIdx === 0 ? "pt-2" : ""}`}
                      >
                        {rowIdx === 0 ? c.code : ""}
                      </td>
                      {allYears.map(yr => {
                        const months = c.year_month_breakdown[String(yr)] || [];
                        const mo = months[rowIdx];
                        return (
                          <React.Fragment key={yr}>
                            {/* Month label */}
                            <td className="pl-3 pr-1 py-1.5 text-xs text-neutral-500 dark:text-neutral-400 text-right whitespace-nowrap">
                              {mo || ""}
                            </td>
                            {/* Input */}
                            <td className="pr-3 pl-1 py-1.5">
                              {mo ? (
                                <input
                                  type="number"
                                  min="0"
                                  step="any"
                                  value={monthlyOverrides[c.code]?.[String(yr)]?.[mo] ?? ""}
                                  onChange={e => setMonthly(c.code, String(yr), mo, e.target.value)}
                                  placeholder="e.g. 3.67"
                                  disabled={disabled}
                                  className={inputCls(monthlyOverrides[c.code]?.[String(yr)]?.[mo])}
                                />
                              ) : null}
                            </td>
                          </React.Fragment>
                        );
                      })}
                    </tr>
                  ));
                })}
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}
