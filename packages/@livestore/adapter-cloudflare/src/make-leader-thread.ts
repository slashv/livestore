import {
  EventlogSqliteDb,
  liveStoreStorageFormatVersion,
  MaterializationJournal,
  type MakeSqliteDb,
  type SqliteDb,
  StateHead,
  StateSqliteDb,
  type SyncOptions,
  UnknownError,
} from '@livestore/common'
import type { CfTypes } from '@livestore/common-cf'
import {
  type LeaderThreadCtx,
  makeLeaderThreadLayer,
  type MakeLeaderThreadLayerParams,
  type ShutdownChannel,
} from '@livestore/common/leader-thread'
import { getStateDbBaseName, type LiveStoreSchema } from '@livestore/common/schema'
import { CF_SQL_VFS_REQUIRED_PRAGMAS, sqliteDbFactory } from '@livestore/sqlite-wasm/cf'
import { loadSqlite3Wasm } from '@livestore/sqlite-wasm/load-wasm'
import { omitUndefineds } from '@livestore/utils'
import {
  type Context,
  Effect,
  type HttpClient,
  Layer,
  Schedule,
  type Schema,
  type Scope,
  WebChannel,
} from '@livestore/utils/effect'

import { makeSqliteDb as makeDoSqliteDb } from './make-sqlite-db.ts'
import * as StateFiles from './state-files.ts'

export interface MakeDoLeaderThreadOptions {
  /** Storage of the Durable Object the leader persists into (state via the VFS, eventlog via SqlStorage). */
  storage: CfTypes.DurableObjectStorage
  schema: LiveStoreSchema
  storeId: string
  clientId: string
  syncOptions: SyncOptions
  /** Clears existing Durable Object persistence before booting. Development only. */
  resetPersistence?: boolean
  syncPayloadEncoded?: Schema.Json | undefined
  syncPayloadSchema?: Schema.Decoder<Schema.Json> | undefined
  params?: MakeLeaderThreadLayerParams['params']
  /**
   * Receives the leader's shutdown signals (e.g. after a backend reset cleared local storage).
   * Defaults to a channel that drops them.
   */
  shutdownChannel?: ShutdownChannel.ShutdownChannel
  /** No client session will connect (e.g. a checkpointer). See `MakeLeaderThreadLayerParams.headless`. */
  headless?: boolean
}

export type DoLeaderThreadServices =
  | LeaderThreadCtx
  | StateSqliteDb.StateSqliteDb
  | EventlogSqliteDb.EventlogSqliteDb
  | StateHead.StateHead
  | MaterializationJournal.MaterializationJournal

export interface DoLeaderThread {
  /** Leader services, e.g. to run `makeCheckpointer` or to build a client session on top. */
  readonly context: Context.Context<DoLeaderThreadServices>
  readonly dbState: SqliteDb
  readonly dbEventlog: SqliteDb
  readonly makeSqliteDb: MakeSqliteDb
  /** File name of the state database in the DO's VFS (e.g. for `stageVfsFile`). */
  readonly stateFileName: string
  /**
   * Current size of this leader's wasm linear memory. Wasm memory only grows, so this is the high-water mark of SQLite's
   * page cache, statement memory and snapshot exports. Diagnostics only.
   */
  readonly wasmHeapBytes: () => number
}

/**
 * Boots a LiveStore leader inside a Durable Object without a client session.
 *
 * State lives in a wa-sqlite database backed by the DO's SQL storage through a VFS, so only SQLite's page cache stays
 * in memory; the eventlog runs on DO SqlStorage directly. Unlike {@link makeAdapter} this does not keep a second,
 * in-memory copy of the state for a client session, which makes it the right building block for headless clients
 * such as the snapshot checkpointer. The leader lives until the surrounding scope closes.
 */
export const makeDoLeaderThread = ({
  storage,
  schema,
  storeId,
  clientId,
  syncOptions,
  resetPersistence = false,
  syncPayloadEncoded,
  syncPayloadSchema,
  params,
  shutdownChannel,
  headless,
}: MakeDoLeaderThreadOptions): Effect.Effect<DoLeaderThread, UnknownError, Scope.Scope | HttpClient.HttpClient> =>
  Effect.gen(function* () {
    const sqlite3 = yield* Effect.promise(() => loadSqlite3Wasm())
    const makeSqliteDb = sqliteDbFactory({ sqlite3 })

    if (resetPersistence === true) {
      yield* resetDurableObjectPersistence({ storage, storeId })
    }

    const stateFileName = `${getStateDbBaseName(schema)}@${liveStoreStorageFormatVersion}.db`
    yield* StateFiles.register(storage, stateFileName).pipe(UnknownError.mapToUnknownError)

    const dbState = yield* makeSqliteDb({
      _tag: 'storage',
      storage,
      fileName: stateFileName,
      configureDb: (db) =>
        db.execute([...CF_SQL_VFS_REQUIRED_PRAGMAS, 'cache_size=-8000'].map((p) => `PRAGMA ${p}`).join(';\n')),
    }).pipe(UnknownError.mapToUnknownError)

    // dbEventlog runs on DO SQLite directly (not through the VFS). SQL-level transaction
    // control (BEGIN/COMMIT/ROLLBACK) is silently dropped — see isTransactionControlStatement
    // in make-sqlite-db.ts for details on why this is safe.
    const dbEventlog = yield* makeDoSqliteDb({
      _tag: 'file',
      db: storage.sql,
      configureDb: () => {},
    }).pipe(UnknownError.mapToUnknownError)

    const sqliteDbLayer = Layer.mergeAll(StateSqliteDb.layer(dbState), EventlogSqliteDb.layer(dbEventlog))
    const stateServicesLayer = Layer.mergeAll(StateHead.layer, MaterializationJournal.layer).pipe(
      Layer.provide(sqliteDbLayer),
    )

    const context = yield* Layer.build(
      makeLeaderThreadLayer({
        schema,
        storeId,
        clientId,
        makeSqliteDb,
        syncOptions,
        devtoolsOptions: { enabled: false },
        shutdownChannel: shutdownChannel ?? (yield* WebChannel.noopChannel<any, any>()),
        syncPayloadEncoded,
        syncPayloadSchema,
        ...omitUndefineds({ params, headless }),
      }).pipe(Layer.provideMerge(Layer.mergeAll(sqliteDbLayer, stateServicesLayer))),
    )

    // The leader has opened (and possibly rebuilt) the current state file, so files of older schemas can go.
    yield* StateFiles.cleanup(storage, stateFileName).pipe(
      Effect.catch((cause) => Effect.logWarning('Failed to clean up obsolete state databases', cause)),
    )

    return {
      context,
      dbState,
      dbEventlog,
      makeSqliteDb,
      stateFileName,
      wasmHeapBytes: () => readWasmHeapBytes(sqlite3),
    } satisfies DoLeaderThread
  }).pipe(Effect.withSpan('@livestore/adapter-cloudflare:makeDoLeaderThread', { attributes: { storeId, clientId } }))

/** `loadSqlite3Wasm` attaches the Emscripten module as an untyped `module` property. */
const readWasmHeapBytes = (sqlite3: object): number => {
  const module: unknown = Reflect.get(sqlite3, 'module')
  const heap: unknown = typeof module === 'object' && module !== null ? Reflect.get(module, 'HEAPU8') : undefined
  return heap instanceof Uint8Array ? heap.byteLength : 0
}

const resetDurableObjectPersistence = ({
  storage,
  storeId,
}: {
  storage: CfTypes.DurableObjectStorage
  storeId: string
}) =>
  Effect.try({
    try: () =>
      // All these tables live in the DO's single storage.sql database but are
      // owned by different layers during normal operation:
      // - vfs_pages: written by the wa-sqlite VFS layer (backs dbState)
      // - eventlog, __livestore_sync_status, __livestore_eventlog_base: written directly by dbEventlog via storage.sql
      // - __livestore_state_files: adapter-owned state file registry
      storage.transactionSync(() => {
        safeSqlExec(storage, 'DELETE FROM vfs_pages')
        safeSqlExec(storage, 'DELETE FROM __livestore_state_files')
        safeSqlExec(storage, 'DELETE FROM eventlog')
        safeSqlExec(storage, 'DELETE FROM __livestore_sync_status')
        // A leftover snapshot base would send the next boot down the rebuild/backfill path instead of a fresh boot.
        safeSqlExec(storage, 'DELETE FROM __livestore_eventlog_base')
      }),
    catch: (cause) =>
      new UnknownError({
        cause,
        note: `@livestore/adapter-cloudflare: Failed to reset persistence for store ${storeId}`,
      }),
  }).pipe(
    Effect.retry({ schedule: Schedule.exponentialBackoff10Sec }),
    Effect.withSpan('@livestore/adapter-cloudflare:resetPersistence', { attributes: { storeId } }),
  )

const safeSqlExec = (storage: CfTypes.DurableObjectStorage, query: string, binding?: string) => {
  try {
    binding !== undefined ? storage.sql.exec(query, binding) : storage.sql.exec(query)
  } catch (error) {
    if (isMissingTableError(error) === true) {
      return
    }

    throw error
  }
}

const isMissingTableError = (error: unknown): boolean =>
  error instanceof Error && error.message.toLowerCase().includes('no such table')
