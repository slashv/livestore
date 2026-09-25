import path from 'node:path'

import { defineConfig } from '@playwright/test'

export default defineConfig({
  testDir: import.meta.dirname,
  testMatch: 'benchmark.test.ts',
  workers: 1,
  timeout: 600000,
  reporter: 'line',
  use: { browserName: 'chromium', headless: true },
  webServer: (['mailbox', 'owner'] as const).map((variant) => ({
    cwd: path.resolve(import.meta.dirname, '..'),
    command: `SESSION_SYNC_VARIANT=${variant} pnpm exec vite build --config session-sync/vite.config.ts && SESSION_SYNC_VARIANT=${variant} pnpm exec vite preview --config session-sync/vite.config.ts`,
    url: `http://localhost:${variant === 'mailbox' ? 4178 : 4179}`,
    reuseExistingServer: false,
    timeout: 120000,
  })),
})
