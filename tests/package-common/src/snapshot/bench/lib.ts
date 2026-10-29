import fs from 'node:fs'
import os from 'node:os'
import path from 'node:path'

import {
  type PreparedStatement,
  type SqliteDb,
  type SqliteDbSession,
  type SyncOptions,
  type UnknownError,
} from '@livestore/common'
import { LeaderThreadCtx, makeLeaderThreadLayer, ShutdownChannel } from '@livestore/common/leader-thread'
import { type LiveStoreSchema, SystemTables } from '@livestore/common/schema'
import { loadSqlite3Wasm } from '@livestore/sqlite-wasm/load-wasm'
import { type MakeNodeSqliteDb, sqliteDbFactory } from '@livestore/sqlite-wasm/node'
import { Context, Effect, Exit, FetchHttpClient, Layer, Schema, Scope, WebChannel } from '@livestore/utils/effect'

import { makeSqliteServicesLayer } from '../../leader-thread/fixture.ts'

/** Where the benchmarks keep databases and cached snapshots. Large: a 10M-event eventlog is several GB. */
export const benchDataDir = process.env.SNAPSHOT_BENCH_DIR ?? path.join(os.tmpdir(), 'livestore-snapshot-bench')

export const resultsDir = process.env.SNAPSHOT_BENCH_RESULTS ?? path.join(import.meta.dirname, 'results')

export type Storage = 'fs' | 'memory'

/** The wasm module of the last loaded SQLite build, to report its heap size. */
let wasmModule: { HEAPU8: Uint8Array } | undefined

export const makeSqlite = Effect.gen(function* () {
  const sqlite3 = yield* Effect.promise(() => loadSqlite3Wasm())
  wasmModule = (sqlite3 as unknown as { module: { HEAPU8: Uint8Array } }).module
  return yield* sqliteDbFactory({ sqlite3 })
})

export interface ClientDbs {
  readonly dbState: SqliteDb
  readonly dbEventlog: SqliteDb
  /** Total on-disk bytes of both databases (fs storage), or their page bytes (memory storage). */
  readonly sizes: () => { stateBytes: number; eventlogBytes: number }
  readonly close: () => void
}

/**
 * A fresh client sandbox. `fs` uses the wa-sqlite Node file-system VFS, like the Node adapter, and is the only option
 * that holds a 10M-event eventlog: `:memory:` databases live inside the 2 GiB wasm heap.
 */
export const makeClientDbs = ({
  makeSqliteDb,
  storage,
  name,
  reopen = false,
}: {
  makeSqliteDb: MakeNodeSqliteDb
  storage: Storage
  name: string
  /** Open the existing fs databases of `name` instead of starting from empty ones (a persisted restart). */
  reopen?: boolean
}) =>
  Effect.gen(function* () {
    if (storage === 'memory') {
      const dbState = yield* makeSqliteDb({ _tag: 'in-memory' })
      const dbEventlog = yield* makeSqliteDb({ _tag: 'in-memory' })
      return {
        dbState,
        dbEventlog,
        sizes: () => ({ stateBytes: pageBytes(dbState), eventlogBytes: pageBytes(dbEventlog) }),
        close: () => {
          dbState.close()
          dbEventlog.close()
        },
      }
    }
    const directory = path.join(benchDataDir, 'dbs', name)
    if (reopen === false) fs.rmSync(directory, { recursive: true, force: true })
    fs.mkdirSync(directory, { recursive: true })
    const dbState = yield* makeSqliteDb({ _tag: 'fs', directory, fileName: 'state.db' })
    const dbEventlog = yield* makeSqliteDb({ _tag: 'fs', directory, fileName: 'eventlog.db' })
    return {
      dbState,
      dbEventlog,
      sizes: () => ({
        stateBytes: fileBytes(path.join(directory, 'state.db')),
        eventlogBytes: fileBytes(path.join(directory, 'eventlog.db')),
      }),
      close: () => {
        dbState.close()
        dbEventlog.close()
      },
    } satisfies ClientDbs
  }).pipe(Effect.orDie)

export interface BenchLeader {
  readonly ctx: LeaderThreadCtx['Service']
  readonly shutdown: Effect.Effect<void>
}

/** Boots a leader directly on the given databases (same wiring as the snapshot test harness). */
export const bootBenchLeader = ({
  makeSqliteDb,
  dbs,
  schema,
  syncOptions,
  clientId,
  storeId,
}: {
  makeSqliteDb: MakeNodeSqliteDb
  dbs: { dbState: SqliteDb; dbEventlog: SqliteDb }
  schema: LiveStoreSchema
  syncOptions: SyncOptions
  clientId: string
  storeId: string
}): Effect.Effect<BenchLeader, UnknownError, Scope.Scope> =>
  Effect.gen(function* () {
    const scope = yield* Scope.fork(yield* Effect.scope)
    const shutdownChannel = yield* WebChannel.queueChannelProxy({ schema: ShutdownChannel.All }).pipe(
      Scope.provide(scope),
    )
    const context = yield* Layer.buildWithScope(
      makeLeaderThreadLayer({
        schema,
        storeId,
        clientId,
        syncPayloadEncoded: undefined,
        syncPayloadSchema: undefined,
        makeSqliteDb,
        syncOptions,
        devtoolsOptions: { enabled: false },
        shutdownChannel: shutdownChannel.webChannel,
      }).pipe(Layer.provide(makeSqliteServicesLayer(dbs)), Layer.provide(FetchHttpClient.layer)),
      scope,
    )
    return { ctx: Context.get(context, LeaderThreadCtx), shutdown: Scope.close(scope, Exit.void) }
  })

/**
 * Polls until the leader's upstream head reaches `head`. Polling every 2ms adds at most 2ms to a measurement.
 * With `SNAPSHOT_BENCH_PROGRESS=1` it logs the head and memory every 10 seconds to stderr.
 */
export const waitForHead = (leader: BenchLeader, head: number) =>
  Effect.gen(function* () {
    const progress = process.env.SNAPSHOT_BENCH_PROGRESS === '1'
    let lastLog = performance.now()
    while (true) {
      const current = (yield* leader.ctx.syncProcessor.syncState.get).upstreamHead.global
      if (current >= head) return
      if (progress === true && performance.now() - lastLog > 10_000) {
        lastLog = performance.now()
        ;(globalThis as { gc?: () => void }).gc?.()
        console.error(toJson({ progress: current, target: head, ...memorySnapshot() }))
      }
      yield* Effect.sleep(2)
    }
  })

/** Time buckets for SQLite calls, filled by `timeDb`. */
export interface SqlTimings {
  /** User table writes issued by materializers. */
  stateWrites: number
  /** Materialization journal inserts/deletes (changeset blobs). */
  journalWrites: number
  /** `session()` creation, `changeset()` extraction and `finish()`: the journal's recording overhead outside writes. */
  sessionOps: number
  /** State head row updates. */
  stateHead: number
  /** Savepoints and transactions on the state database. */
  stateTx: number
  /** Everything on the eventlog database: event inserts, sync status, transactions. */
  eventlog: number
  calls: number
}

export const makeSqlTimings = (): SqlTimings => ({
  stateWrites: 0,
  journalWrites: 0,
  sessionOps: 0,
  stateHead: 0,
  stateTx: 0,
  eventlog: 0,
  calls: 0,
})

/**
 * Wraps a database so that every SQLite call is timed into a bucket. The wrapper forwards everything else, so the
 * leader behaves the same; the cost is one `performance.now()` pair per call (~0.1 µs).
 *
 * `disableSession` replaces changeset recording with a no-op session (the journal then stores `null`), which measures
 * what recording costs inside SQLite writes.
 */
export const timeDb = (
  db: SqliteDb,
  role: 'state' | 'eventlog',
  timings: SqlTimings,
  options?: { disableSession?: boolean },
): SqliteDb => {
  const classify = (sql: string): keyof Omit<SqlTimings, 'calls'> => {
    if (role === 'eventlog') return 'eventlog'
    if (sql.includes(SystemTables.MATERIALIZATION_JOURNAL_META_TABLE) === true) return 'journalWrites'
    if (sql.includes(SystemTables.STATE_HEAD_META_TABLE) === true) return 'stateHead'
    const head = sql.trimStart().slice(0, 16).toUpperCase()
    return TX_KEYWORDS.some((keyword) => head.startsWith(keyword)) === true ? 'stateTx' : 'stateWrites'
  }
  const timed = <T>(bucket: keyof Omit<SqlTimings, 'calls'>, fn: () => T): T => {
    const start = performance.now()
    try {
      return fn()
    } finally {
      timings[bucket] += performance.now() - start
      timings.calls++
    }
  }
  const sqlOf = (query: unknown) =>
    typeof query === 'string' ? query : ((query as { asSql?: () => { query: string } }).asSql?.().query ?? '')

  const wrapStatement = (statement: PreparedStatement): PreparedStatement => {
    const bucket = classify(statement.sql)
    return {
      sql: statement.sql,
      execute: (bindValues, options) => timed(bucket, () => statement.execute(bindValues, options)),
      select: (bindValues) => timed(bucket, () => statement.select(bindValues)),
      finalize: () => statement.finalize(),
    }
  }

  return new Proxy(db, {
    get(target, prop, receiver) {
      switch (prop) {
        case 'prepare':
          return (query: string) => wrapStatement(target.prepare(query))
        case 'execute':
          return (query: any, ...rest: any[]) =>
            timed(classify(sqlOf(query)), () => (target.execute as any)(query, ...rest))
        case 'select':
          return (query: any, ...rest: any[]) =>
            timed(classify(sqlOf(query)), () => (target.select as any)(query, ...rest))
        case 'session':
          if (role === 'eventlog') return () => target.session()
          return (): SqliteDbSession => {
            if (options?.disableSession === true) return { changeset: () => null, finish: () => {} }
            const session = timed('sessionOps', () => target.session())
            return {
              changeset: () => timed('sessionOps', () => session.changeset()),
              finish: () => timed('sessionOps', () => session.finish()),
            }
          }
        default:
          return Reflect.get(target, prop, receiver)
      }
    },
  })
}

export const hardware = () => ({
  cpu: os.cpus()[0]?.model ?? 'unknown',
  loadavg1m: Math.round(os.loadavg()[0]! * 10) / 10,
  cores: os.cpus().length,
  ramGiB: Math.round(os.totalmem() / 2 ** 30),
  os: `${os.type()} ${os.release()}`,
  node: process.version,
})

export const memorySnapshot = () => {
  const m = process.memoryUsage()
  return {
    rssMiB: mib(m.rss),
    heapUsedMiB: mib(m.heapUsed),
    externalMiB: mib(m.external),
    arrayBuffersMiB: mib(m.arrayBuffers),
    /** SQLite wasm linear memory; it only grows, and is capped at 2 GiB by the build. */
    wasmHeapMiB: mib(wasmModule?.HEAPU8.byteLength ?? 0),
  }
}

/** Tracks the peak RSS of this process while a measurement runs. */
export const trackPeakRss = () => {
  let peak = process.memoryUsage().rss
  const timer = setInterval(() => {
    peak = Math.max(peak, process.memoryUsage().rss)
  }, 100)
  return {
    peak: () => mib(Math.max(peak, process.memoryUsage().rss)),
    stop: () => {
      clearInterval(timer)
      return mib(Math.max(peak, process.memoryUsage().rss))
    },
  }
}

/** Appends one JSON result line to `results/<file>.jsonl` and echoes it. */
export const writeResult = (file: string, result: Record<string, unknown>) => {
  fs.mkdirSync(resultsDir, { recursive: true })
  const line = toJson({ ...result, at: new Date().toISOString() })
  fs.appendFileSync(path.join(resultsDir, `${file}.jsonl`), `${line}\n`)
  console.log(line)
}

export const parseArgs = (argv: ReadonlyArray<string>) => {
  const args: Record<string, string> = {}
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i]!
    if (arg.startsWith('--') === false) continue
    const [key, inline] = arg.slice(2).split('=')
    if (inline !== undefined) args[key!] = inline
    else if (argv[i + 1] !== undefined && argv[i + 1]!.startsWith('--') === false) args[key!] = argv[++i]!
    else args[key!] = 'true'
  }
  return args
}

const JsonString = Schema.fromJsonString(Schema.Unknown)
export const toJson: (value: unknown) => string = Schema.encodeSync(JsonString)
export const fromJson: (json: string) => unknown = Schema.decodeUnknownSync(JsonString)

export const mib = (bytes: number) => Math.round((bytes / 2 ** 20) * 10) / 10

const TX_KEYWORDS = ['SAVEPOINT', 'RELEASE', 'BEGIN', 'COMMIT', 'ROLLBACK']

const pageBytes = (db: SqliteDb) => {
  const [count] = db.select<{ page_count: number }>('PRAGMA page_count')
  const [size] = db.select<{ page_size: number }>('PRAGMA page_size')
  return (count?.page_count ?? 0) * (size?.page_size ?? 0)
}

const fileBytes = (file: string) => {
  try {
    return fs.statSync(file).size
  } catch {
    return 0
  }
}
