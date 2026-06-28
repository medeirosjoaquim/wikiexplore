import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  server: {
    host: "0.0.0.0",
    port: 5173,
    proxy: {
      "/api": "http://backend-api:8000",
      "/health": "http://backend-api:8000",
      "/ws": { target: "http://backend-api:8000", ws: true },
    },
  },
});
