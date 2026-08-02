// The suite compiles the independently-owned module sources together. Their
// Motion versions are intentionally not a shared dependency, so keep Motion's
// JSX surface opaque to the suite typecheck; each app's own lint still checks
// its concrete animation types.
declare const motion: any;
declare const AnimatePresence: any;
export { motion, AnimatePresence };
