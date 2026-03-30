import { Moon, Sun, BarChart3 } from "lucide-react";
import { useTheme } from "../../theme/ThemeProvider";
import type { AppStep } from "../../types";

const STEPS = [
  { step: 1, label: "Upload" },
  { step: 2, label: "Inventory" },
  { step: 3, label: "Map Columns" },
  { step: 4, label: "Select Views" },
  { step: 5, label: "Dashboard" },
] as const;

export default function Header({ currentStep }: { currentStep: AppStep }) {
  const { theme, toggle } = useTheme();

  return (
    <header className="sticky top-0 z-50 border-b border-neutral-200 dark:border-neutral-800 bg-white/80 dark:bg-neutral-950/80 backdrop-blur-md">
      <div className="max-w-[1400px] mx-auto px-6 flex items-center justify-between h-14">
        <div className="flex items-center gap-3">
          <div className="w-8 h-8 rounded-lg bg-primary flex items-center justify-center">
            <BarChart3 className="w-4 h-4 text-white" />
          </div>
          <span className="font-semibold text-sm tracking-tight text-neutral-900 dark:text-neutral-100">
            Spend Analyzer
          </span>
        </div>

        <nav className="hidden sm:flex items-center gap-1">
          {STEPS.map(({ step, label }) => (
            <div key={step} className="flex items-center">
              <div
                className={`flex items-center gap-1.5 px-3 py-1.5 rounded-full text-xs font-medium transition-all ${
                  step === currentStep
                    ? "bg-primary text-white"
                    : step < currentStep
                      ? "bg-primary-100 text-primary-700 dark:bg-primary-900/30 dark:text-primary-300"
                      : "text-neutral-400 dark:text-neutral-600"
                }`}
              >
                <span className="w-4 h-4 rounded-full border border-current flex items-center justify-center text-[10px]">
                  {step < currentStep ? "✓" : step}
                </span>
                {label}
              </div>
              {step < 5 && (
                <div
                  className={`w-6 h-px mx-1 ${
                    step < currentStep
                      ? "bg-primary-300 dark:bg-primary-700"
                      : "bg-neutral-200 dark:bg-neutral-700"
                  }`}
                />
              )}
            </div>
          ))}
        </nav>

        <button
          onClick={toggle}
          className="w-8 h-8 rounded-lg flex items-center justify-center text-neutral-500 hover:bg-neutral-100 dark:hover:bg-neutral-800 transition-colors"
        >
          {theme === "dark" ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
        </button>
      </div>
    </header>
  );
}
