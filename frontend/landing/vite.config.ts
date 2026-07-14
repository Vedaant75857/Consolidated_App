import tailwindcss from '@tailwindcss/vite';
import react from '@vitejs/plugin-react';
import { defineConfig } from 'vite';

export default defineConfig({
  plugins: [react(), tailwindcss()],
  server: {
    // Use 3010 so Vite never falls through to 3001 (stitcher backend) when 3000 is busy.
    port: 3010,
    strictPort: true,
  },
});
