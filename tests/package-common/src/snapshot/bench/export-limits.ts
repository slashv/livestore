/**
 * Snapshot export/import cost and ceiling vs database size, without a replay: fills an fs state database with
 * message-like rows to `--mib`, then times `export()` (the checkpointer's serialize), `sha256Hex`, and `import(bytes)`
 * into a fresh fs database (the client install path). One size per process: the wasm heap never shrinks.
 *
 *   node export-limits.ts --mib 512
 */
import fs from 'node:fs'
import path from 'node:path'

import { Snapshot } from '@livestore/common'
import { Effect } from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import { benchDataDir, hardware, makeSqlite, memorySnapshot, parseArgs, writeResult } from './lib.ts'

const args = parseArgs(process.argv.slice(2))
const targetMiB = Number(args.mib ?? 256)

const program = Effect.gen(function* () {
  const makeSqliteDb = yield* makeSqlite
  const directory = path.join(benchDataDir, 'export-limits', String(targetMiB))
  fs.rmSync(directory, { recursive: true, force: true })
  const source = yield* makeSqliteDb({ _tag: 'fs', directory, fileName: 'source.db' })
  source.execute('PRAGMA page_size=8192')
  source.execute('CREATE TABLE messages (id TEXT PRIMARY KEY, sessionId TEXT, text TEXT, createdAt INTEGER)')
  // ~400 bytes per row, inserted in 50k-row transactions until the file reaches the target size.
  let rows = 0
  while (fs.statSync(path.join(directory, 'source.db')).size < targetMiB * 2 ** 20) {
    source.execute('BEGIN')
    source.execute(
      `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 50000)
       INSERT INTO messages SELECT printf('msg_%012d', ${rows} + i), 'ses_' || (i % 97), hex(randomblob(170)), i FROM n`,
    )
    source.execute('COMMIT')
    rows += 50_000
  }
  const bytesOnDisk = fs.statSync(path.join(directory, 'source.db')).size

  const result: Record<string, unknown> = { command: 'export-limits', targetMiB, rows, bytesOnDisk }
  const step = <A>(label: string, fn: () => A): A | undefined => {
    const start = performance.now()
    try {
      const value = fn()
      result[`${label}Ms`] = performance.now() - start
      return value
    } catch (error) {
      result[`${label}Error`] = String(error).slice(0, 300)
      return undefined
    }
  }

  const bytes = step('export', () => source.export())
  result.afterExport = memorySnapshot()
  if (bytes !== undefined) {
    result.exportBytes = bytes.byteLength
    step('sha256', () => Snapshot.sha256Hex(bytes))
    const target = yield* makeSqliteDb({ _tag: 'fs', directory, fileName: 'target.db' })
    step('import', () => target.import(bytes))
    result.afterImport = memorySnapshot()
  }
  writeResult('export-limits', { ...result, hardware: hardware() })
  fs.rmSync(directory, { recursive: true, force: true })
})

await Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(PlatformNode.NodeFileSystem.layer)))
process.exit(0)
