// See compat-motion-react.d.ts. Runtime imports remain untouched; this only
// prevents incompatible per-app Motion declaration trees from leaking across
// the suite's aggregate typecheck.
declare const motion: any;
declare const AnimatePresence: any;
export { motion, AnimatePresence };
