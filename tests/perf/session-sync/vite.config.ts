import { readFileSync, readdirSync } from 'node:fs'
import path from 'node:path'

import { defineConfig } from 'vite'

const variant = process.env.SESSION_SYNC_VARIANT
if (variant !== 'mailbox' && variant !== 'owner') throw new Error('Set SESSION_SYNC_VARIANT to mailbox or owner')
const sourceRoot =
  variant === 'mailbox' ? process.env.SESSION_SYNC_BASELINE_WORKTREE : path.resolve(import.meta.dirname, '../../..')
if (sourceRoot === undefined) throw new Error('Set SESSION_SYNC_BASELINE_WORKTREE to the clean 4ead601cd checkout')
const packages = path.join(sourceRoot, 'packages/@livestore')
// Both builds use exactly the same fixture, but resolve all LiveStore package exports from their own checkout.
// There is no runtime implementation selector, duplicated processor or compatibility bridge in Store.
const alias = readdirSync(packages).flatMap((directory) => {
  const packageRoot = path.join(packages, directory)
  const manifest: { name: string; exports?: Record<string, unknown> } = JSON.parse(
    readFileSync(path.join(packageRoot, 'package.json'), 'utf8'),
  )
  return Object.entries(manifest.exports ?? {}).flatMap(([key, target]) =>
    typeof target === 'string'
      ? [
          {
            find: new RegExp('^' + manifest.name + (key === '.' ? '' : key.slice(1)) + '$'),
            replacement: path.resolve(packageRoot, target),
          },
        ]
      : [],
  )
})

export default defineConfig({
  root: import.meta.dirname,
  resolve: { alias },
  define: { __SESSION_SYNC_VARIANT__: JSON.stringify(variant) },
  build: { outDir: `dist-${variant}`, sourcemap: true },
  optimizeDeps: { exclude: ['@livestore/wa-sqlite'] },
  preview: { port: variant === 'mailbox' ? 4178 : 4179, strictPort: true },
})
