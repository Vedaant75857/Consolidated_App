import React, { Component } from "react";
import { motion } from "framer-motion";
import type { LucideIcon } from "lucide-react";

interface ErrorBoundaryProps {
  children: React.ReactNode;
  fallbackMessage?: string;
}

interface ErrorBoundaryState {
  hasError: boolean;
  error: Error | null;
}

export class ErrorBoundary extends Component<ErrorBoundaryProps, ErrorBoundaryState> {
  state: ErrorBoundaryState = { hasError: false, error: null };

  static getDerivedStateFromError(error: Error) {
    return { hasError: true, error };
  }

  render() {
    if (this.state.hasError) {
      return (
        <div className="max-w-2xl mx-auto mt-12">
          <div className="bg-white dark:bg-neutral-900 rounded-2xl border border-red-200 dark:border-red-800 shadow-sm p-8 text-center">
            <h2 className="text-lg font-semibold text-red-600 dark:text-red-400 mb-2">
              Something went wrong
            </h2>
            <p className="text-sm text-neutral-500 mb-4">
              {this.props.fallbackMessage || "An unexpected error occurred while rendering this step."}
            </p>
            <pre className="text-xs text-left bg-neutral-50 dark:bg-neutral-800 rounded-lg p-4 mb-4 overflow-auto max-h-32 text-neutral-600 dark:text-neutral-400">
              {this.state.error?.message}
            </pre>
            <button
              onClick={() => this.setState({ hasError: false, error: null })}
              className="px-4 py-2 rounded-lg bg-primary text-white text-sm font-semibold hover:bg-primary-hover transition-colors"
            >
              Try Again
            </button>
          </div>
        </div>
      );
    }
    return this.props.children;
  }
}

export const itemVariants = {
  initial: { opacity: 0, y: 12 },
  animate: {
    opacity: 1,
    y: 0,
    transition: { duration: 0.25, ease: [0.22, 1, 0.36, 1] as [number, number, number, number] },
  },
};

interface SurfaceCardProps {
  title?: string;
  subtitle?: string;
  icon?: LucideIcon;
  right?: React.ReactNode;
  children: React.ReactNode;
  className?: string;
  noPadding?: boolean;
}

export function SurfaceCard({
  title,
  subtitle,
  icon: Icon,
  right,
  children,
  className = "",
  noPadding,
}: SurfaceCardProps) {
  return (
    <motion.div
      variants={itemVariants}
      className={`rounded-2xl border border-neutral-200 bg-white shadow-sm dark:border-neutral-700 dark:bg-neutral-900 ${className}`}
    >
      {(title || right) && (
        <div className="flex items-start justify-between gap-4 px-6 pt-6 pb-0">
          <div className="min-w-0">
            <h3 className="text-lg font-semibold tracking-tight text-neutral-900 dark:text-white flex items-center gap-2">
              {Icon && <Icon className="w-5 h-5 text-primary shrink-0" />}
              {title}
            </h3>
            {subtitle && (
              <p className="mt-1 text-sm text-neutral-500 dark:text-neutral-400">
                {subtitle}
              </p>
            )}
          </div>
          {right && <div className="shrink-0">{right}</div>}
        </div>
      )}
      <div className={noPadding ? "" : "p-6"}>{children}</div>
    </motion.div>
  );
}

interface PrimaryButtonProps {
  children: React.ReactNode;
  onClick?: () => void;
  disabled?: boolean;
  className?: string;
  type?: "button" | "submit";
}

export function PrimaryButton({
  children,
  onClick,
  disabled,
  className = "",
  type = "button",
}: PrimaryButtonProps) {
  return (
    <motion.button
      type={type}
      whileHover={disabled ? undefined : { y: -1, scale: 1.01 }}
      whileTap={disabled ? undefined : { scale: 0.98 }}
      onClick={onClick}
      disabled={disabled}
      className={`inline-flex items-center justify-center gap-2 rounded-xl bg-primary px-5 py-2.5 text-sm font-bold text-white shadow-md shadow-primary/20 transition-colors hover:bg-primary-hover disabled:opacity-50 disabled:cursor-not-allowed disabled:shadow-none ${className}`}
    >
      {children}
    </motion.button>
  );
}
