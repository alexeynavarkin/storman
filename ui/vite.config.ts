import path from "node:path";
import { defineConfig, loadEnv } from "vite";
import react from "@vitejs/plugin-react";
import tailwindcss from "@tailwindcss/vite";

// Vite config: dev server proxies /api to the storman backend so cookies and
// CSRF work without CORS gymnastics. Override the upstream via STORMAN_API.
export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "STORMAN_");
  const upstream = env.STORMAN_API ?? "http://localhost:8443";
  return {
    plugins: [react(), tailwindcss()],
    resolve: {
      alias: {
        "@": path.resolve(__dirname, "./src"),
      },
    },
    server: {
      port: 5173,
      strictPort: false,
      proxy: {
        "/api": {
          target: upstream,
          changeOrigin: true,
          // Do NOT rewrite cookies — the backend issues them with Path=/ and
          // we want the browser to keep using them on /api calls.
          cookieDomainRewrite: "",
        },
      },
    },
    build: {
      outDir: "dist",
      sourcemap: true,
      rollupOptions: {
        output: {
          // Carve out the largest, most stable vendors so they cache across
          // releases. Everything else stays in the entry chunk — avoiding a
          // catch-all "vendor" prevents circular-chunk warnings without
          // hurting the initial payload meaningfully.
          manualChunks(id) {
            if (!id.includes("node_modules")) return undefined;
            if (id.includes("react-dom") || id.includes("/react/") || id.includes("scheduler")) {
              return "react";
            }
            if (id.includes("react-router")) return "react-router";
            if (id.includes("@radix-ui")) return "radix";
            if (id.includes("react-hook-form") || id.includes("@hookform") || id.includes("/zod/")) {
              return "rhf";
            }
            return undefined;
          },
        },
      },
    },
  };
});
