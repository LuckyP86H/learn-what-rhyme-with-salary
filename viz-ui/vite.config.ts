import react from "@vitejs/plugin-react";
import { defineConfig } from "vitest/config";

// The built UI is served by the Python API from celery_viz/ui_dist, so one process serves everything.
export default defineConfig({
  plugins: [react()],
  build: { outDir: process.env.VIZ_OUT_DIR ?? "../celery_viz/ui_dist", emptyOutDir: true },
  // `npm run dev` proxies the API to the running viz service, for hot-reload while editing.
  server: { proxy: { "/api": process.env.VIZ_API ?? "http://localhost:8200" } },
  test: { environment: "node", include: ["src/**/*.test.ts"] },
});
