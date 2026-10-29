import { type SqliteDb, type MakeSqliteDb, Snapshot, sql, type SyncOptions } from '@livestore/common'
import { LeaderThreadCtx, makeLeaderThreadLayer, ShutdownChannel } from '@livestore/common/leader-thread'
import {
  type EventSequenceNumber,
  Events,
  LiveStoreEvent,
  type LiveStoreSchema,
  makeSchema,
  State,
  SystemTables,
} from '@livestore/common/schema'
import { EventFactory } from '@livestore/common/testing'
import { loadSqlite3Wasm } from '@livestore/sqlite-wasm/load-wasm'
import { sqliteDbFactory } from '@livestore/sqlite-wasm/node'
import {
  Context,
  Duration,
  Effect,
  Exit,
  FetchHttpClient,
  Layer,
  Schema,
  Scope,
  WebChannel,
} from '@livestore/utils/effect'

import { makeSqliteServicesLayer } from '../leader-thread/fixture.ts'

/**
 * Leader-level harness for snapshot boot tests and benchmarks: boots leaders directly on in-memory SQLite (Node wasm
 * build), so "killing" a client is closing its scope and a "fresh sandbox" is a new pair of databases.
 */

const todos = State.SQLite.table({
  name: 'todos',
  columns: {
    id: State.SQLite.text({ primaryKey: true }),
    text: State.SQLite.text({ default: '' }),
    completed: State.SQLite.boolean({ default: false }),
    author: State.SQLite.text({ default: '' }),
  },
})

const counters = State.SQLite.table({
  name: 'counters',
  columns: {
    id: State.SQLite.text({ primaryKey: true }),
    value: State.SQLite.integer({ default: 0 }),
  },
})

const uiState = State.SQLite.clientDocument({
  name: 'uiState',
  schema: Schema.Struct({ filter: Schema.String, cursor: Schema.Int }),
  default: { id: 'singleton', value: { filter: 'all', cursor: 0 } },
})

const uiStateSetEvent = (uiState as typeof uiState & State.SQLite.ClientDocumentTableDef<any, any, any, any>)[
  State.SQLite.ClientDocumentTableDefSymbol
].derived.setEventDef

export const events = {
  todoCreated: Events.synced({
    name: 'v1.TodoCreated',
    schema: Schema.Struct({ id: Schema.String, text: Schema.String, author: Schema.String }),
  }),
  todoCompleted: Events.synced({ name: 'v1.TodoCompleted', schema: Schema.Struct({ id: Schema.String }) }),
  counterIncremented: Events.synced({
    name: 'v1.CounterIncremented',
    schema: Schema.Struct({ id: Schema.String, by: Schema.Int }),
  }),
  uiStateSet: uiStateSetEvent,
}

const materializers = State.SQLite.materializers(events, {
  'v1.TodoCreated': ({ id, text, author }) => todos.insert({ id, text, author }).onConflict('id', 'replace'),
  'v1.TodoCompleted': ({ id }) => todos.update({ completed: true }).where({ id }),
  'v1.CounterIncremented': ({ id, by }) => [
    counters.insert({ id, value: 0 }).onConflict('id', 'ignore'),
    { sql: 'UPDATE counters SET value = value + $by WHERE id = $id', bindValues: { by, id } },
  ],
})

export const tables = { todos, counters, uiState }

export const schema = makeSchema({ state: State.SQLite.makeState({ tables, materializers }), events })

/** Same tables and events plus one new table: a state schema change that produces a new snapshot key. */
export const makeSchemaWithExtraTable = () => {
  const audit = State.SQLite.table({
    name: 'audit',
    columns: { id: State.SQLite.text({ primaryKey: true }) },
  })
  return makeSchema({
    state: State.SQLite.makeState({ tables: { ...tables, audit }, materializers }),
    events,
  })
}

export interface ClientDbs {
  readonly dbState: SqliteDb
  readonly dbEventlog: SqliteDb
}

export const makeSqlite = Effect.gen(function* () {
  const sqlite3 = yield* Effect.promise(() => loadSqlite3Wasm())
  return yield* sqliteDbFactory({ sqlite3 })
})

/** A fresh sandbox: empty in-memory state and eventlog databases. Reuse the result to model a persisted restart. */
export const makeClientDbs = (makeSqliteDb: MakeSqliteDb) =>
  Effect.all({
    dbState: makeSqliteDb({ _tag: 'in-memory' }),
    dbEventlog: makeSqliteDb({ _tag: 'in-memory' }),
  }).pipe(Effect.orDie)

export interface BootedLeader {
  readonly ctx: LeaderThreadCtx['Service']
  readonly dbs: ClientDbs
  readonly clientId: string
  /** Closes the leader (simulated kill); the databases stay usable for a restart. */
  readonly shutdown: Effect.Effect<void>
}

/**
 * Models a restart after a state schema change: adapters name the state database after the state schema hash, so the
 * new schema opens a fresh state database next to the existing eventlog.
 */
export const withFreshStateDb = (makeSqliteDb: MakeSqliteDb, dbs: ClientDbs) =>
  makeSqliteDb({ _tag: 'in-memory' }).pipe(
    Effect.orDie,
    Effect.map((dbState): ClientDbs => ({ dbState, dbEventlog: dbs.dbEventlog })),
  )

/** Boots a leader on the given databases. The leader lives until `shutdown` (or the surrounding scope) closes it. */
export const bootLeader = ({
  makeSqliteDb,
  dbs,
  clientId,
  storeId = 'snapshot-test-store',
  syncOptions,
  leaderSchema = schema,
}: {
  makeSqliteDb: MakeSqliteDb
  dbs: ClientDbs
  clientId: string
  storeId?: string
  syncOptions: SyncOptions | undefined
  leaderSchema?: LiveStoreSchema
}): Effect.Effect<BootedLeader, unknown, Scope.Scope> =>
  Effect.gen(function* () {
    const outer = yield* Effect.scope
    const scope = yield* Scope.fork(outer)
    const shutdownChannel = yield* WebChannel.queueChannelProxy({ schema: ShutdownChannel.All }).pipe(
      Scope.provide(scope),
    )
    const context = yield* Layer.buildWithScope(
      makeLeaderThreadLayer({
        schema: leaderSchema,
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

    return {
      ctx: Context.get(context, LeaderThreadCtx),
      dbs,
      clientId,
      shutdown: Scope.close(scope, Exit.void),
    }
  })

/** Waits until the leader has applied the backend log up to `head`. */
export const waitForUpstreamHead = (leader: BootedLeader, head: number, timeout: Duration.Input = '20 seconds') =>
  Effect.gen(function* () {
    while ((yield* leader.ctx.syncProcessor.syncState.get).upstreamHead.global < head) {
      yield* Effect.sleep(5)
    }
  }).pipe(
    Effect.timeoutOrElse({
      duration: timeout,
      onTimeout: () =>
        Effect.gen(function* () {
          const state = yield* leader.ctx.syncProcessor.syncState.get
          return yield* Effect.die(
            new Error(`${leader.clientId} stuck at upstream ${state.upstreamHead.global}, expected ${head}`),
          )
        }),
    }),
  )

/** Waits until every pending local event of the leader is confirmed by the backend. */
export const waitForNoPending = (leader: BootedLeader, timeout: Duration.Input = '20 seconds') =>
  Effect.gen(function* () {
    while ((yield* leader.ctx.syncProcessor.syncState.get).pending.length > 0) {
      yield* Effect.sleep(5)
    }
  }).pipe(Effect.timeout(timeout), Effect.orDie)

/**
 * Canonical digest of all user-visible state: every schema table's rows ordered by primary key. Two clients at the
 * same position must produce the same digest.
 */
export const stateDigest = (dbState: SqliteDb, digestSchema: LiveStoreSchema = schema) => {
  const tableNames = [...digestSchema.state.sqlite.tables.keys()]
    .filter((name) => SystemTables.isStateSystemTable(name) === false)
    .toSorted()
  const content = tableNames.map((name) => [name, dbState.select(sql`SELECT * FROM "${name}" ORDER BY 1`)] as const)
  return Snapshot.sha256Hex(new TextEncoder().encode(JSON.stringify(content)))
}

export const stateHeadOf = (dbState: SqliteDb) =>
  dbState.select<SystemTables.StateHeadMetaRow>(SystemTables.stateHeadMetaTable.where({ id: 1 }))[0]

export const eventlogRowCount = (dbEventlog: SqliteDb) =>
  dbEventlog.select<{ count: number }>(sql`SELECT COUNT(*) AS count FROM ${SystemTables.EVENTLOG_META_TABLE}`)[0]!.count

/** Generates `count` backend events from one writer, cycling through creates, completions and counter updates. */
export const makeWriterEvents = ({
  writer = 'writer',
  startSeq = 1,
  count,
  textBytes = 64,
}: {
  writer?: string
  startSeq?: number
  count: number
  textBytes?: number
}): LiveStoreEvent.Global.Encoded[] => {
  const factory = EventFactory.makeFactory(events)({
    client: EventFactory.clientIdentity(writer),
    startSeq,
    initialParent: startSeq === 1 ? 'root' : startSeq - 1,
  })
  const filler = 'x'.repeat(textBytes)
  return Array.from({ length: count }, (_, i) => {
    const n = startSeq + i
    switch (n % 4) {
      case 0:
        return factory.todoCompleted.next({ id: `todo-${n - 3}` })
      case 1:
        return factory.counterIncremented.next({ id: `counter-${n % 7}`, by: n % 5 })
      default:
        return factory.todoCreated.next({ id: `todo-${n}`, text: `${filler}-${n}`, author: writer })
    }
  })
}

/** Builds the next local event of a leader, as a client session would. */
export const nextLocalEvent = (
  leader: BootedLeader,
  event: { name: string; args: unknown },
  options?: { clientOnly?: boolean },
) =>
  Effect.gen(function* () {
    const { localHead } = yield* leader.ctx.syncProcessor.syncState.get
    const seqNum: EventSequenceNumber.Client.Composite =
      options?.clientOnly === true
        ? { global: localHead.global, client: localHead.client + 1, rebaseGeneration: localHead.rebaseGeneration }
        : { global: localHead.global + 1, client: 0, rebaseGeneration: localHead.rebaseGeneration }
    return LiveStoreEvent.Client.Encoded.make({
      name: event.name,
      args: event.args,
      seqNum,
      parentSeqNum: localHead,
      clientId: leader.clientId,
      sessionId: `${leader.clientId}-session`,
    })
  })

/** Encodes a typed event for `nextLocalEvent`. */
export const encodeArgs = <TName extends keyof typeof events>(
  name: TName,
  args: Parameters<(typeof events)[TName]>[0],
) => {
  const eventDef = events[name] as any
  return { name: eventDef.name as string, args: Schema.encodeUnknownSync(eventDef.schema)(args) }
}
