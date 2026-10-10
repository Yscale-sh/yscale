import { defineConfig } from "vite";
import react from "@vitejs/plugin-react";

export default defineConfig({
  plugins: [react()],
  // Preserve the prior build's transpilation targets across the tool upgrade.
  build: {
    target: ["es2020", "edge88", "firefox78", "chrome87", "safari14"],
    cssTarget: ["edge88", "firefox78", "chrome87", "safari14"],
  },
  server: {
    proxy: {
      "/api": "http://localhost:8080",
      // Kritter art (incl. the server-side-grayscaled finale) comes from the
      // Go server, so proxy it in dev too. Run the server on :8080 alongside.
      "/kritters": "http://localhost:8080",
      // Vanity redirect (yscale.sh/kubagachi -> the OSS repo) is server-side.
      "/kubagachi": "http://localhost:8080",
    },
  },
});
