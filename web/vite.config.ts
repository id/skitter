import path from "node:path";
import tailwindcss from "@tailwindcss/vite";
import react from "@vitejs/plugin-react";
import { defineConfig } from "vite";

// Dev-proxy targets default to localhost; override per environment via env vars.
const mqttProxyTarget = process.env.VITE_PROXY_MQTT ?? "ws://localhost:8083";
const apiProxyTarget = process.env.VITE_PROXY_API ?? "http://localhost:3000";

export default defineConfig({
  base: "/skitter/",
  plugins: [react(), tailwindcss()],
  server: {
    port: 18084,
    strictPort: true,
    proxy: {
      "/mqtt": {
        target: mqttProxyTarget,
        ws: true,
        changeOrigin: true,
      },
      "/api": {
        target: apiProxyTarget,
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
