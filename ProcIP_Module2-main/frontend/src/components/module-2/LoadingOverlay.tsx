import React from "react";
import { X } from "lucide-react";
import { useTheme } from "../common/ThemeProvider";

interface LoadingOverlayProps {
  isLoading: boolean;
  message?: string;
  onCancel?: () => void;
  /** 0-100 upload progress; null/undefined = indeterminate spinner */
  progress?: number | null;
}

const LIGHT_BG =
  "linear-gradient(135deg, rgba(255,255,255,0.72) 0%, rgba(255,228,230,0.45) 50%, rgba(255,241,242,0.30) 100%)";
const DARK_BG =
  "linear-gradient(135deg, rgba(38,38,38,0.82) 0%, rgba(48,12,18,0.65) 100%)";

export default function LoadingOverlay({ isLoading, message, onCancel, progress }: LoadingOverlayProps) {
  const { theme } = useTheme();
  if (!isLoading) return null;

  const isDark = theme === "dark";

  return (
    <div className="absolute inset-0 z-40 flex items-center justify-center bg-black/15 dark:bg-black/35">
      <div
        className="w-[360px] mx-4 rounded-3xl border border-white/40 dark:border-white/[0.12] backdrop-blur-2xl px-10 py-10 flex flex-col items-center gap-5"
        style={{
          background: isDark ? DARK_BG : LIGHT_BG,
          animation: isDark
            ? "breathing-glow-dark 3s ease-in-out infinite"
            : "breathing-glow 3s ease-in-out infinite",
        }}
      >
        <div className="relative w-14 h-14">
          <svg
            className="w-14 h-14"
            viewBox="0 0 56 56"
            fill="none"
            style={{ animation: "smooth-spin 1.2s linear infinite" }}
          >
            <circle
              cx="28"
              cy="28"
              r="24"
              stroke="currentColor"
              strokeWidth="3.5"
              className="text-red-100 dark:text-red-900/40"
            />
            <path
              d="M28 4 a24 24 0 0 1 24 24"
              stroke={isDark ? "url(#spinner-grad-dark)" : "url(#spinner-grad)"}
              strokeWidth="3.5"
              strokeLinecap="round"
            />
            <defs>
              <linearGradient id="spinner-grad" x1="28" y1="4" x2="52" y2="28">
                <stop stopColor="#dc2626" />
                <stop offset="1" stopColor="#f43f5e" />
              </linearGradient>
              <linearGradient id="spinner-grad-dark" x1="28" y1="4" x2="52" y2="28">
                <stop stopColor="#ef4444" />
                <stop offset="1" stopColor="#fb7185" />
              </linearGradient>
            </defs>
          </svg>
        </div>

        <p
          className="text-lg font-bold text-red-600 dark:text-red-400 text-center leading-snug tracking-tight"
          style={isDark ? { textShadow: "0 0 12px rgba(239,68,68,0.45)" } : undefined}
        >
          {message || "Processing\u2026"}
        </p>

        {typeof progress === "number" && (
          <div className="w-full flex flex-col items-center gap-1.5">
            <div className="w-full h-2 rounded-full bg-red-100 dark:bg-red-900/40 overflow-hidden">
              <div
                className="h-full rounded-full bg-gradient-to-r from-red-500 to-rose-500 transition-all duration-300 ease-out"
                style={{ width: `${progress}%` }}
              />
            </div>
            <span className="text-xs tabular-nums font-medium text-red-500/80 dark:text-red-400/80">
              {progress}%
            </span>
          </div>
        )}

        {onCancel && (
          <button
            type="button"
            onClick={onCancel}
            className="mt-1 inline-flex items-center gap-1.5 px-5 py-2 text-sm font-medium rounded-xl border border-white/50 dark:border-white/[0.12] bg-white/40 dark:bg-white/[0.06] text-neutral-600 dark:text-neutral-300 hover:bg-white/60 dark:hover:bg-white/10 backdrop-blur-sm transition-colors"
          >
            <X className="w-3.5 h-3.5" />
            Cancel
          </button>
        )}
      </div>
    </div>
  );
}