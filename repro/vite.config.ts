import { cloudflare } from '@cloudflare/vite-plugin'
import react from '@vitejs/plugin-react'
import { defineConfig } from 'vite'

export default defineConfig({
  server: {
    port: 60099,
    fs: { strict: false },
  },
  worker: { format: 'es' },
  plugins: [cloudflare(), react()],
})
