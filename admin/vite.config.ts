import { defineConfig } from 'vite'
import react from '@vitejs/plugin-react'
import { resolve } from 'node:path'
import { TanStackRouterVite } from '@tanstack/router-plugin/vite'

// https://vitejs.dev/config/
export default defineConfig({
  plugins: [
    TanStackRouterVite({
      routeFileIgnorePrefix: "-",
      routesDirectory: resolve(__dirname, "./client/routes"),
      generatedRouteTree: resolve(__dirname, "./client/routeTree.gen.ts"),
    }),
    react(),
  ],
  resolve: {
    alias: {
      "client": resolve(__dirname, "./client"),
    },
  },
  server: {
    port: 3000,
    strictPort: true,
    proxy: {
      '/api': {
        target: 'http://localhost:3001',
        changeOrigin: true,
        ws: true,
      },
    }
  }
})
