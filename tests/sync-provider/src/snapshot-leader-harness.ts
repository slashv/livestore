import {
  EventlogSqliteDb,
  MaterializationJournal,
  type MakeSqliteDb,
  Snapshot,
  type SqliteDb,
  sql,
  StateHead,
  StateSqliteDb,
  type SyncOptions,
  UnknownError,
} from '@livestore/common'
import { LeaderThreadCtx, makeLeaderThreadLayer, ShutdownChannel } from '@livestore/common/leader-thread'
import { type LiveStoreSchema, SystemTables } from '@livestore/common/schema'
import { loadSqlite3Wasm } from '@livestore/sqlite-wasm/load-wasm'
import { sqliteDbFactory } from '@livestore/sqlite-wasm/node'
import {
  Context,
  type Duration,
  Effect,
  Exit,
  FetchHttpClient,
  Layer,
  type Queue,
  Schema,
  Scope,
  WebChannel,
} from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

/**
 * Minimal leader-level client for snapshot boot tests against the local workerd sync backend: boots a LiveStore leader
 * on in-memory SQLite (Node wasm build) with arbitrary sync options, without a client session.
 * Mirrors `tests/package-common/src/snapshot/harness.ts`.
 */

export const makeSqlite = Effect.gen(function* () {
  const sqlite3 = yield* Effect.promise(() => loadSqlite3Wasm())
  return yield* sqliteDbFactory({ sqlite3 })
}).pipe(Effect.provide(PlatformNode.NodeServices.layer))

export interface BootedLeader {
  readonly ctx: LeaderThreadCtx['Service']
  readonly dbState: SqliteDb
  readonly dbEventlog: SqliteDb
  /** Messages the leader sent on its shutdown channel (e.g. after a backend reset). */
  readonly shutdownMessages: Queue.Dequeue<typeof ShutdownChannel.All.Type>
  readonly shutdown: Effect.Effect<void>
}

export const bootLeader = ({
  makeSqliteDb,
  schema,
  storeId,
  clientId,
  syncOptions,
  dbs,
}: {
  makeSqliteDb: MakeSqliteDb
  schema: LiveStoreSchema
  storeId: string
  clientId: string
  syncOptions: SyncOptions
  /** Databases of an earlier (shut down) leader, to model a restart. Fresh in-memory databases by default. */
  dbs?: { dbState: SqliteDb; dbEventlog: SqliteDb }
}): Effect.Effect<BootedLeader, UnknownError, Scope.Scope> =>
  Effect.gen(function* () {
    const scope = yield* Scope.fork(yield* Effect.scope)
    const dbState = dbs?.dbState ?? (yield* makeSqliteDb({ _tag: 'in-memory' }))
    const dbEventlog = dbs?.dbEventlog ?? (yield* makeSqliteDb({ _tag: 'in-memory' }))
    const shutdownChannel = yield* WebChannel.queueChannelProxy({ schema: ShutdownChannel.All }).pipe(
      Scope.provide(scope),
    )

    const sqliteDbLayer = Layer.mergeAll(StateSqliteDb.layer(dbState), EventlogSqliteDb.layer(dbEventlog))
    const stateServicesLayer = Layer.mergeAll(StateHead.layer, MaterializationJournal.layer).pipe(
      Layer.provide(sqliteDbLayer),
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
      }).pipe(Layer.provide(Layer.mergeAll(sqliteDbLayer, stateServicesLayer)), Layer.provide(FetchHttpClient.layer)),
      scope,
    )

    return {
      ctx: Context.get(context, LeaderThreadCtx),
      dbState,
      dbEventlog,
      shutdownMessages: shutdownChannel.sendQueue,
      shutdown: Scope.close(scope, Exit.void),
    }
  }).pipe(UnknownError.mapToUnknownError)

/** Waits until the leader has applied the backend log up to `head`. */
export const waitForUpstreamHead = (leader: BootedLeader, head: number, timeout: Duration.Input = '30 seconds') =>
  Effect.gen(function* () {
    while ((yield* leader.ctx.syncProcessor.syncState.get).upstreamHead.global < head) {
      yield* Effect.sleep(20)
    }
  }).pipe(Effect.timeout(timeout), Effect.orDie)

/** All user-visible state: every schema table's rows ordered by primary key. */
export const stateContent = (dbState: SqliteDb, schema: LiveStoreSchema) =>
  [...schema.state.sqlite.tables.keys()]
    .filter((name) => SystemTables.isStateSystemTable(name) === false)
    .toSorted()
    .map((name) => [name, dbState.select(sql`SELECT * FROM "${name}" ORDER BY 1`)] as const)

/** Canonical digest of {@link stateContent}. Two clients at the same position must produce the same digest. */
export const stateDigest = (dbState: SqliteDb, schema: LiveStoreSchema) =>
  Snapshot.sha256Hex(new TextEncoder().encode(encodeJson(stateContent(dbState, schema))))

export const eventlogRowCount = (dbEventlog: SqliteDb) =>
  dbEventlog.select<{ count: number }>(sql`SELECT COUNT(*) AS count FROM ${SystemTables.EVENTLOG_META_TABLE}`)[0]!.count

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))
