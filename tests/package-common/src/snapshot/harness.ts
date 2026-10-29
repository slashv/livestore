import { type SqliteDb, type MakeSqliteDb, Snapshot, sql, type SyncOptions, UnknownError } from '@livestore/common'
import { LeaderThreadCtx, makeLeaderThreadLayer, ShutdownChannel } from '@livestore/common/leader-thread'
import {
  EventSequenceNumber,
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
  type Duration,
  Context,
  Effect,
  Exit,
  FetchHttpClient,
  Layer,
  Schema,
  Scope,
  Stream,
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

const syncedEvents = {
  todoCreated: Events.synced({
    name: 'v1.TodoCreated',
    schema: Schema.Struct({ id: Schema.String, text: Schema.String, author: Schema.String }),
  }),
  todoCompleted: Events.synced({ name: 'v1.TodoCompleted', schema: Schema.Struct({ id: Schema.String }) }),
  counterIncremented: Events.synced({
    name: 'v1.CounterIncremented',
    schema: Schema.Struct({ id: Schema.String, by: Schema.Int }),
  }),
}

export const events = { ...syncedEvents, uiStateSet: uiStateSetEvent }

// The client document's set event gets its materializer from `makeState`; only the synced events need one here.
const materializers = State.SQLite.materializers(syncedEvents, {
  'v1.TodoCreated': ({ id, text, author }) => todos.insert({ id, text, author }).onConflict('id', 'replace'),
  'v1.TodoCompleted': ({ id }) => todos.update({ completed: true }).where({ id }),
  'v1.CounterIncremented': ({ id, by }) => [
    counters.insert({ id, value: 0 }).onConflict('id', 'ignore'),
    { sql: 'UPDATE counters SET value = value + $by WHERE id = $id', bindValues: { by, id } },
  ],
})

export const tables = { todos, counters, uiState }

export const schema = makeSchema({ state: State.SQLite.makeState({ tables, materializers }), events })

/** A synced event only the "v2" schema knows: older readers treat it as an unknown event. */
export const auditLogged = Events.synced({
  name: 'v1.AuditLogged',
  schema: Schema.Struct({ id: Schema.String, note: Schema.String }),
})

const auditLog = State.SQLite.table({
  name: 'auditLog',
  columns: {
    id: State.SQLite.text({ primaryKey: true }),
    note: State.SQLite.text({ default: '' }),
  },
})

/**
 * Two schema generations for long-running scenarios: `v2` adds a table and a new event (`v1.AuditLogged`), so it has a
 * different snapshot key, and `v1` readers see the new event as unknown. Both ignore unknown events silently.
 */
export const makeVersionedSchemas = (
  unknownEventHandling: { strategy: 'ignore' } | { strategy: 'warn' } = { strategy: 'ignore' },
) => {
  const v1 = makeSchema({ state: State.SQLite.makeState({ tables, materializers }), events, unknownEventHandling })
  const v2Materializers = {
    ...materializers,
    ...State.SQLite.materializers(
      { auditLogged },
      {
        'v1.AuditLogged': ({ id, note }) => auditLog.insert({ id, note }).onConflict('id', 'replace'),
      },
    ),
  }
  const v2 = makeSchema({
    state: State.SQLite.makeState({ tables: { ...tables, auditLog }, materializers: v2Materializers }),
    events: { ...events, auditLogged },
    unknownEventHandling,
  })
  return { v1, v2 }
}

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
  /** Messages the leader sent on its shutdown channel, e.g. a sync failure that stopped its sync processor. */
  readonly shutdownMessages: ReadonlyArray<unknown>
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
}): Effect.Effect<BootedLeader, UnknownError, Scope.Scope> =>
  Effect.gen(function* () {
    const outer = yield* Effect.scope
    const scope = yield* Scope.fork(outer)
    const shutdownChannel = yield* WebChannel.queueChannelProxy({ schema: ShutdownChannel.All }).pipe(
      Scope.provide(scope),
    )
    const shutdownMessages: unknown[] = []
    yield* Stream.fromQueue(shutdownChannel.sendQueue).pipe(
      Stream.runForEach((message) => Effect.sync(() => shutdownMessages.push(message))),
      Effect.forkIn(scope),
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
    ).pipe(UnknownError.mapToUnknownError)

    return {
      ctx: Context.get(context, LeaderThreadCtx),
      dbs,
      clientId,
      shutdown: Scope.close(scope, Exit.void),
      shutdownMessages,
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
      orElse: () =>
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
export const stateDigest = (
  dbState: SqliteDb,
  digestSchema: LiveStoreSchema = schema,
  options?: { readonly excludeTables?: ReadonlyArray<string> },
) => {
  const tableNames = [...digestSchema.state.sqlite.tables.keys()]
    .filter((name) => SystemTables.isStateSystemTable(name) === false)
    .filter((name) => options?.excludeTables?.includes(name) !== true)
    .toSorted()
  const content = tableNames.map((name) => [name, dbState.select(sql`SELECT * FROM "${name}" ORDER BY 1`)] as const)
  return Snapshot.sha256Hex(new TextEncoder().encode(JSON.stringify(content)))
}

export const stateHeadOf = (dbState: SqliteDb) =>
  dbState.select<SystemTables.StateHeadMetaRow>(
    sql`SELECT * FROM ${SystemTables.STATE_HEAD_META_TABLE} WHERE id = 1`,
  )[0]

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
    // A synced event's parent is always the last global position (`client: 0`), even after client-only events.
    const { seqNum, parentSeqNum } = EventSequenceNumber.Client.nextPair({
      seqNum: localHead,
      isClientOnly: options?.clientOnly === true,
    })
    return LiveStoreEvent.Client.Encoded.make({
      name: event.name,
      args: event.args,
      seqNum,
      parentSeqNum,
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
