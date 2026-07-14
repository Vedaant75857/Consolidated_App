import React from "react";
import { AlertTriangle, X } from "lucide-react";
import { useTheme } from "../../theme/ThemeProvider";

interface StepChangeWarningDialogProps {
  open: boolean;
  onConfirm: () => void;
  onCancel: () => void;
}

const LIGHT_BG =
  "linear-gradient(135deg, rgba(255,255,255,0.85) 0%, rgba(255,243,224,0.55) 50%, rgba(255,248,240,0.40) 100%)";
const DARK_BG =
  "linear-gradient(135deg, rgba(38,38,38,0.90) 0%, rgba(60,30,10,0.65) 100%)";

export default function StepChangeWarningDialog({
  open,
  onConfirm,
  onCancel,
}: StepChangeWarningDialogProps) {
  const { theme } = useTheme();
  if (!open) return null;

  const isDark = theme === "dark";

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/30 dark:bg-black/50 backdrop-blur-sm">
      <div
        className="w-[420px] mx-4 rounded-3xl border border-white/40 dark:border-white/[0.12] backdrop-blur-2xl px-8 py-8 flex flex-col items-center gap-5"
        style={{ background: isDark ? DARK_BG : LIGHT_BG }}
      >
        {/* Icon */}
        <div className="w-14 h-14 rounded-full bg-amber-100 dark:bg-amber-900/40 flex items-center justify-center">
          <AlertTriangle className="w-7 h-7 text-amber-600 dark:text-amber-400" />
        </div>

        {/* Title */}
        <h3 className="text-lg font-bold text-neutral-800 dark:text-neutral-100 text-center">
          Reset Downstream Progress?
        </h3>

        {/* Message */}
        <p className="text-sm text-neutral-600 dark:text-neutral-400 text-center leading-relaxed">
          Making changes here will clear all progress after this step. This
          cannot be undone. Continue?
        </p>

        {/* Buttons */}
        <div className="flex items-center gap-3 mt-1 w-full">
          <button
            type="button"
            onClick={onCancel}
            className="flex-1 inline-flex items-center justify-center gap-1.5 px-5 py-2.5 text-sm font-medium rounded-xl border border-white/50 dark:border-white/[0.12] bg-white/40 dark:bg-white/[0.06] text-neutral-600 dark:text-neutral-300 hover:bg-white/60 dark:hover:bg-white/10 backdrop-blur-sm transition-colors"
          >
            <X className="w-3.5 h-3.5" />
            Cancel
          </button>
          <button
            type="button"
            onClick={onConfirm}
            className="flex-1 inline-flex items-center justify-center gap-1.5 px-5 py-2.5 text-sm font-semibold rounded-xl bg-amber-500 hover:bg-amber-600 text-white transition-colors shadow-md"
          >
            Continue
          </button>
        </div>
      </div>
    </div>
  );
}
