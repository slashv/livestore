/**
 * Isolates the per-statement memory cost of `SqliteDb.execute` (wa-sqlite Node build): runs `--calls` executes of an
 * eventlog-sized INSERT and reports JS heap and wasm heap growth per call after a forced GC.
 *
 *   node --expose-gc leak-probe.ts --calls 100000
 *
 * Every `execute` prepares and finalizes a statement. `makeSqliteDb` keeps each prepared statement object in an
 * array until the database closes, and wa-sqlite's `sqlite3.statements` never frees the SQL text it copied into the
 * wasm heap, so both heaps grow with the number of calls.
 */
import { Effect } from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import { hardware, makeSqlite, memorySnapshot, parseArgs, writeResult } from './lib.ts'

const args = parseArgs(process.argv.slice(2))
const calls = Number(args.calls ?? 100_000)
const gc = () => (globalThis as { gc?: () => void }).gc?.()

const program = Effect.gen(function* () {
  const makeSqliteDb = yield* makeSqlite
  const db = yield* makeSqliteDb({ _tag: 'in-memory' })
  db.execute('CREATE TABLE t (id INTEGER PRIMARY KEY, a TEXT, b TEXT, c TEXT, d INTEGER, e INTEGER, f TEXT)')
  // Warm up, then measure from a collected heap.
  for (let i = 0; i < 1000; i++) db.execute(`INSERT INTO t (a, b, c, d, e, f) VALUES ('x', 'y', 'z', ${i}, ${i}, 'w')`)
  db.execute('DELETE FROM t')
  gc()
  const before = memorySnapshot()
  const sql = `INSERT OR REPLACE INTO t (id, a, b, c, d, e, f) VALUES ($id, $a, $b, $c, $d, $e, $f) -- ${'padding '.repeat(20)}`
  for (let i = 0; i < calls; i++) {
    db.execute(sql, {
      $id: i % 100,
      $a: 'agent-1',
      $b: 'agent-1-session',
      $c: 'v1.TaskUpdated',
      $d: i,
      $e: i,
      $f: '{}',
    } as never)
  }
  gc()
  const after = memorySnapshot()
  writeResult('leak-probe', {
    command: 'leak-probe',
    calls,
    sqlBytes: sql.length,
    jsHeapBytesPerCall: ((after.heapUsedMiB - before.heapUsedMiB) * 2 ** 20) / calls,
    wasmHeapBytesPerCall: ((after.wasmHeapMiB - before.wasmHeapMiB) * 2 ** 20) / calls,
    before,
    after,
    hardware: hardware(),
  })
})

await Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(PlatformNode.NodeFileSystem.layer)))
process.exit(0)
