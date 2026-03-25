import { defineConfig } from 'vite';
import react from '@vitejs/plugin-react';

export default defineConfig({
  plugins: [react()],
  server: {
    port: 5173,
    proxy: {
      '/Feat_03': { target: 'http://localhost:8000', changeOrigin: true },
      '/Feat_04': { target: 'http://localhost:8000', changeOrigin: true },
    },
  },
});
