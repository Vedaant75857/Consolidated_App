"use client";

import { useState, useEffect } from "react";
import HeroSection from "./HeroSection";
import ModulesCarousel from "./ModulesCarousel";
import PipelineSection from "./PipelineSection";
import FeaturesSection from "./FeaturesSection";
import CTASection from "./CTASection";

function SectionDivider() {
  return (
    <div className="px-6">
      <div className="section-divider" />
    </div>
  );
}

export default function LandingPage() {
  const [theme, setTheme] = useState<"light" | "dark">("dark");

  useEffect(() => {
    const saved = localStorage.getItem("procip-theme");
    if (saved === "dark" || saved === "light") {
      setTheme(saved);
    } else if (window.matchMedia("(prefers-color-scheme: dark)").matches) {
      setTheme("dark");
    }
  }, []);

  useEffect(() => {
    document.documentElement.classList.toggle("dark", theme === "dark");
    localStorage.setItem("procip-theme", theme);
  }, [theme]);

  function toggleTheme() {
    setTheme((prev) => (prev === "dark" ? "light" : "dark"));
  }

  return (
    <main className="min-h-screen bg-gradient-to-br from-neutral-50 via-white to-neutral-100 dark:from-neutral-950 dark:via-neutral-900 dark:to-neutral-950 text-neutral-900 dark:text-neutral-100 font-sans relative overflow-x-hidden">
      {/* Dot grid texture */}
      <div className="dot-grid pointer-events-none fixed inset-0 z-[1]" />
      {/* Noise overlay */}
      <div className="noise-overlay pointer-events-none fixed inset-0 z-[1]" />

      {/* Section 1 — Hero */}
      <HeroSection theme={theme} onToggleTheme={toggleTheme} />

      <SectionDivider />

      {/* Section 2 — Explore Our Modules */}
      <ModulesCarousel />

      <SectionDivider />

      {/* Section 3 — How It Works */}
      <PipelineSection />

      <SectionDivider />

      {/* Section 4 — Powerful Features */}
      <FeaturesSection />

      <SectionDivider />

      {/* Section 5 — CTA */}
      <CTASection />

      {/* Footer */}
      <footer className="relative z-10 text-center pb-10">
        <p className="text-xs text-neutral-400 dark:text-neutral-600 tracking-wide">
          &copy; {new Date().getFullYear()} ProcIP Data Processing Suite. All rights reserved.
        </p>
      </footer>
    </main>
  );
}
