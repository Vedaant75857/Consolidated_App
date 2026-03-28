import type { AppStep } from "../../types";

const STEPS = [
  { step: 1, label: "Upload" },
  { step: 2, label: "Map Columns" },
  { step: 3, label: "Select Views" },
  { step: 4, label: "Dashboard" },
] as const;

interface Props {
  currentStep: AppStep;
}

export default function StepIndicator({ currentStep }: Props) {
  return (
    <nav className="flex items-center gap-1">
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
            <span className="hidden sm:inline">{label}</span>
          </div>
          {step < 4 && (
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
  );
}
