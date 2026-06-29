import path from "node:path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

export default defineConfig({
  base: "/skitter/",
  plugins: [react(), tailwindcss()],
  server: {
    port: 18084,
    strictPort: true,
    proxy: {
      "/mqtt": {
        target: "ws://162.14.117.182:8083",
        ws: true,
        changeOrigin: true,
      },
      "/api": {
        target: "http://162.14.117.182:3000",
        changeOrigin: true,
      },
    },
  },
  resolve: {
    alias: {
      "@": path.resolve(__dirname, "./src"),
    },
  },
});
