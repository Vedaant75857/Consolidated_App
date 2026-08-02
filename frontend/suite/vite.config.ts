import path from "node:path";
import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  resolve: {
    // Keep module-local @ aliases valid when the apps are lazy loaded.
    alias: {
      "@module1": path.resolve(__dirname, "../module1/src"),
      "@module2": path.resolve(__dirname, "../module2/src"),
      "@module3": path.resolve(__dirname, "../module3/src"),
      // Module 3 historically carried React 18. Resolve every lazy feature to
      // the suite's single React runtime once the converged install is present.
      react: path.resolve(__dirname, "node_modules/react"),
      "react-dom": path.resolve(__dirname, "node_modules/react-dom"),
      "react/jsx-runtime": path.resolve(__dirname, "node_modules/react/jsx-runtime.js"),
      "react/jsx-dev-runtime": path.resolve(__dirname, "node_modules/react/jsx-dev-runtime.js"),
    },
  },
  server: {
    port: 3000,
    strictPort: true,
    fs: { allow: [path.resolve(__dirname, ".."), path.resolve(__dirname, "../..")] },
    proxy: {
      "/api": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
        timeout: 600000,
        proxyTimeout: 600000,
      },
      "/config.json": {
        target: "http://127.0.0.1:8000",
        changeOrigin: true,
      },
    },
  },
  build: { outDir: "dist", emptyOutDir: true },
});
