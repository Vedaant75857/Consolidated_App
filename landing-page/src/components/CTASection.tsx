import { motion } from "motion/react";
import { ArrowRight } from "lucide-react";

export default function CTASection() {
  return (
    <section className="relative py-32 px-6 overflow-hidden">
      {/* Ambient glow orbs */}
      <div className="pointer-events-none absolute inset-0">
        <div className="absolute left-1/2 top-1/2 -translate-x-1/2 -translate-y-1/2 w-[600px] h-[300px] rounded-full bg-red-500/8 dark:bg-red-600/10 blur-3xl" />
        <div className="absolute left-1/4 bottom-0 w-[300px] h-[200px] rounded-full bg-rose-500/6 dark:bg-rose-700/8 blur-3xl" />
        <div className="absolute right-1/4 top-0 w-[250px] h-[200px] rounded-full bg-amber-400/5 dark:bg-amber-600/8 blur-3xl" />
      </div>

      <div className="relative z-10 max-w-3xl mx-auto text-center">
        {/* Eyebrow */}
        <motion.p
          initial={{ opacity: 0, y: 10 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.5 }}
          className="text-xs font-semibold uppercase tracking-widest text-red-500 dark:text-red-400 mb-5"
        >
          Get Started Today
        </motion.p>

        {/* Headline */}
        <motion.h2
          initial={{ opacity: 0, y: 20 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6, delay: 0.1 }}
          className="text-4xl md:text-5xl font-bold tracking-tight text-neutral-900 dark:text-white leading-tight mb-5 text-balance"
        >
          Turn messy procurement data into{" "}
          <span className="gradient-text">actionable insights</span>
        </motion.h2>

        {/* Supporting line */}
        <motion.p
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6, delay: 0.2 }}
          className="text-lg text-neutral-500 dark:text-neutral-400 max-w-xl mx-auto mb-10 text-balance"
        >
          Join teams already using Data Processing Suite to consolidate, normalize, and analyze procurement data — without the manual work.
        </motion.p>

        {/* CTA buttons */}
        <motion.div
          initial={{ opacity: 0, y: 16 }}
          whileInView={{ opacity: 1, y: 0 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6, delay: 0.3 }}
          className="flex items-center justify-center gap-4"
        >
          <motion.button
            onClick={() => window.scrollTo({ top: 0, behavior: "smooth" })}
            whileHover={{ scale: 1.04, boxShadow: "0 0 40px rgba(239,68,68,0.35)" }}
            whileTap={{ scale: 0.97 }}
            className="inline-flex items-center gap-2.5 px-8 py-4 rounded-2xl text-white font-semibold text-base bg-gradient-to-r from-red-600 via-rose-600 to-red-500 shadow-lg shadow-red-500/25 transition-shadow duration-300 relative overflow-hidden group"
          >
            {/* Shimmer on hover */}
            <div className="shimmer-sweep" />
            <span className="relative z-10">Get Started</span>
            <motion.span
              className="relative z-10"
              animate={{ x: [0, 3, 0] }}
              transition={{ duration: 1.5, repeat: Infinity, ease: "easeInOut" }}
            >
              <ArrowRight className="w-5 h-5" />
            </motion.span>
          </motion.button>

          <motion.button
            whileHover={{ scale: 1.03 }}
            whileTap={{ scale: 0.97 }}
            className="inline-flex items-center gap-2 px-6 py-4 rounded-2xl text-neutral-700 dark:text-neutral-300 font-medium text-base bg-white/70 dark:bg-neutral-800/60 backdrop-blur-sm border border-neutral-200/60 dark:border-neutral-700/50 hover:border-red-400/40 dark:hover:border-red-500/30 transition-all"
          >
            View Demo
          </motion.button>
        </motion.div>

        {/* Trust badge row */}
        <motion.div
          initial={{ opacity: 0 }}
          whileInView={{ opacity: 1 }}
          viewport={{ once: true }}
          transition={{ duration: 0.6, delay: 0.5 }}
          className="flex items-center justify-center gap-6 mt-10"
        >
          {["No setup fee", "Secure & private", "Export anytime"].map((item) => (
            <span key={item} className="flex items-center gap-1.5 text-xs text-neutral-500 dark:text-neutral-500">
              <span className="w-1 h-1 rounded-full bg-gradient-to-br from-red-500 to-rose-500" />
              {item}
            </span>
          ))}
        </motion.div>
      </div>
    </section>
  );
}
