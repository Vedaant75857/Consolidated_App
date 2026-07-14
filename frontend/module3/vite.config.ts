import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    port: 3004,
    proxy: {
      "/api": {
        target: "http://127.0.0.1:3005",
        changeOrigin: true,
        timeout: 600000,
        proxyTimeout: 600000,
        ws: true,
        onProxyReq: (proxyReq) => {
          proxyReq.setHeader('Connection', 'keep-alive');
        },
      },
    },
  },
});
