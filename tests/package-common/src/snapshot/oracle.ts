import { expect } from 'vitest'

import {
  type MakeSqliteDb,
  makeMockSyncBackend,
  type MockSyncBackend,
  Snapshot,
  type SqliteDb,
  sql,
  StateSqliteDb,
  type SyncBackend,
  type SyncOptions,
  type UnknownError,
} from '@livestore/common'
import { type Checkpointer, Eventlog, LeaderThreadCtx, makeCheckpointer } from '@livestore/common/leader-thread'
import { EventSequenceNumber, LiveStoreEvent, type LiveStoreSchema, SystemTables } from '@livestore/common/schema'
import { type Duration, type Scope, Effect, Option, Semaphore, Stream, SubscriptionRef } from '@livestore/utils/effect'

import {
  bootLeader,
  type BootedLeader,
  type ClientDbs,
  makeClientDbs,
  makeSqlite,
  schema as defaultSchema,
  stateDigest,
  stateHeadOf,
  tables,
  waitForUpstreamHead,
} from './harness.ts'

/**
 * Correctness oracle for snapshot boot: helpers that compare a client against a full-replay reference and check the
 * eventlog invariants a snapshot base must preserve.
 */

/** Client-only tables differ per client by design, so the synced-state digest leaves them out. */
export const CLIENT_ONLY_TABLES = [tables.uiState.sqliteDef.name]

/** Digest of the synced (cross-client) state. Two converged clients on the same schema must agree on it. */
export const syncedDigest = (dbState: SqliteDb, digestSchema: LiveStoreSchema) =>
  stateDigest(dbState, digestSchema, { excludeTables: CLIENT_ONLY_TABLES })

/** Query results compared between clients, in addition to the digest, to exercise ordinary reads. */
export const probeQueries = (dbState: SqliteDb) => ({
  todos: dbState.select(sql`SELECT id, text, completed, author FROM todos ORDER BY id`),
  completedCount: dbState.select(sql`SELECT COUNT(*) AS n FROM todos WHERE completed = 1`),
  counters: dbState.select(sql`SELECT id, value FROM counters ORDER BY id`),
  counterSum: dbState.select(sql`SELECT COALESCE(SUM(value), 0) AS total FROM counters`),
})

/** Asserts that two clients on the same schema hold identical synced state at the same synced head. */
export const expectEquivalentState = (
  actual: SqliteDb,
  reference: SqliteDb,
  digestSchema: LiveStoreSchema,
  label = 'client',
) => {
  expect(syncedDigest(actual, digestSchema), `${label}: synced state digest`).toBe(
    syncedDigest(reference, digestSchema),
  )
  expect(stateHeadOf(actual)?.seqNumGlobal, `${label}: state head`).toBe(stateHeadOf(reference)?.seqNumGlobal)
  expect(probeQueries(actual), `${label}: query results`).toEqual(probeQueries(reference))
}

/**
 * Eventlog invariants of a converged client at backend head `head`:
 * - no synced row at or below the base (the snapshot stands in for those events), and no row of any kind below it;
 * - the synced rows above the base are exactly the backend events in `(base, head]`: no gaps, no duplicates, same order;
 * - the persisted backend head equals `head`.
 */
export const expectEventlogInvariants = ({
  dbEventlog,
  backendLog,
  head,
  label = 'client',
}: {
  dbEventlog: SqliteDb
  backendLog: ReadonlyArray<LiveStoreEvent.Global.Encoded>
  head: number
  label?: string
}) => {
  const base = Option.match(Eventlog.getEventlogBaseFromDb(dbEventlog), {
    onNone: () => 0,
    onSome: (b) => b.seqNumGlobal,
  })

  const belowBase = dbEventlog.select<{ n: number }>(
    sql`SELECT COUNT(*) AS n FROM ${SystemTables.EVENTLOG_META_TABLE}
        WHERE seqNumGlobal < ${base} OR (seqNumGlobal = ${base} AND seqNumClient = 0 AND ${base} > 0)`,
  )[0]!.n
  expect(belowBase, `${label}: eventlog rows at or below base ${base}`).toBe(0)

  const synced = dbEventlog.select<{ seqNumGlobal: number; name: string; clientId: string }>(
    sql`SELECT seqNumGlobal, name, clientId FROM ${SystemTables.EVENTLOG_META_TABLE}
        WHERE seqNumClient = 0 ORDER BY seqNumGlobal, seqNumRebaseGeneration`,
  )
  const expected = backendLog
    .filter((event) => event.seqNum > base && event.seqNum <= head)
    .map((event) => ({ seqNumGlobal: event.seqNum, name: event.name, clientId: event.clientId }))
  const key = (row: { seqNumGlobal: number; name: string; clientId: string }) =>
    `${row.seqNumGlobal}:${row.name}:${row.clientId}`
  const syncedKeys = synced.map(key)
  const expectedKeys = expected.map(key)
  const missing = expectedKeys.filter((k) => syncedKeys.includes(k) === false)
  const extra = syncedKeys.filter((k, i) => expectedKeys.includes(k) === false || syncedKeys.indexOf(k) !== i)
  expect(
    { missing, extra, order: syncedKeys.join() === expectedKeys.join() },
    `${label}: synced eventlog rows equal backend events in (${base}, ${head}] ` +
      `(missing: ${missing.join(' ')}; extra: ${extra.join(' ')})`,
  ).toEqual({ missing: [], extra: [], order: true })

  expect(Eventlog.getBackendHeadFromDb(dbEventlog), `${label}: persisted backend head`).toBe(head)
}

export const provideLeader =
  (leader: BootedLeader) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.provideService(LeaderThreadCtx, leader.ctx),
      Effect.provideService(StateSqliteDb.StateSqliteDb, leader.dbs.dbState),
    )

/** Boots a fresh full-replay leader (no snapshot) and waits until it reached `head`. */
export const bootReference = ({
  makeSqliteDb,
  syncOptions,
  head,
  leaderSchema,
  clientId = 'reference',
}: {
  makeSqliteDb: MakeSqliteDb
  syncOptions: SyncOptions
  head: number
  leaderSchema: LiveStoreSchema
  clientId?: string
}) =>
  Effect.gen(function* () {
    const { snapshot: _snapshot, ...withoutSnapshot } = syncOptions
    const reference = yield* bootLeader({
      makeSqliteDb,
      dbs: yield* makeClientDbs(makeSqliteDb),
      clientId,
      syncOptions: withoutSnapshot,
      leaderSchema,
    })
    expect(Option.isNone(reference.ctx.snapshotBoot)).toBe(true)
    yield* waitForUpstreamHead(reference, head)
    return reference
  })

/**
 * A network in front of the mock sync backend that adds what the mock lacks for long schedules:
 * - per-client connectivity (an offline client neither pushes nor receives pulls, and stays offline across restarts);
 * - `advance` at the true backend head, serialized with client pushes so concurrent writers never fork the log;
 * - an observer that reads the complete backend log for invariant checks.
 */
export interface Network {
  readonly backendFor: (clientId: string) => SyncOptions['backend']
  readonly setOnline: (clientId: string, online: boolean) => Effect.Effect<void>
  readonly isOnline: (clientId: string) => boolean
  /** Holds back pull results for a client while its pushes still go through (e.g. a slow downstream link). */
  readonly setPullsPaused: (clientId: string, paused: boolean) => Effect.Effect<void>
  /** Appends `build(head)` at the backend head. `build` must return events numbered from `head + 1`. */
  readonly advance: (build: (head: number) => LiveStoreEvent.Global.Encoded[]) => Effect.Effect<void>
  /** The complete backend log, re-read from the backend. */
  readonly log: Effect.Effect<ReadonlyArray<LiveStoreEvent.Global.Encoded>>
  readonly head: Effect.Effect<number>
}

export const makeNetwork = (backend: MockSyncBackend): Effect.Effect<Network, UnknownError, Scope.Scope> =>
  Effect.gen(function* () {
    const connectivity = new Map<string, SubscriptionRef.SubscriptionRef<boolean>>()
    const pullsPaused = new Map<string, SubscriptionRef.SubscriptionRef<boolean>>()
    const ensureIn = (map: typeof connectivity, initial: boolean) => (clientId: string) =>
      Effect.gen(function* () {
        const existing = map.get(clientId)
        if (existing !== undefined) return existing
        const ref = yield* SubscriptionRef.make(initial)
        map.set(clientId, ref)
        return ref
      })
    const ensure = ensureIn(connectivity, true)
    const ensurePaused = ensureIn(pullsPaused, false)

    // Serializes client pushes with `advance`, which must observe a stable head.
    const writeLock = yield* Semaphore.make(1)

    const observer = yield* backend.makeSyncBackend
    const backendLog: LiveStoreEvent.Global.Encoded[] = []
    const refreshLog = Effect.suspend(() => {
      const last = backendLog.at(-1)?.seqNum
      const cursor =
        last === undefined
          ? Option.none()
          : Option.some({ eventSequenceNumber: EventSequenceNumber.Global.make(last), metadata: Option.none() })
      return observer.pull(cursor, { live: false }).pipe(
        Stream.runForEach(({ batch }) =>
          Effect.sync(() => {
            for (const { eventEncoded } of batch) backendLog.push(eventEncoded)
          }),
        ),
        Effect.orDie,
      )
    })

    const backendFor =
      (clientId: string): SyncOptions['backend'] =>
      () =>
        Effect.gen(function* () {
          const inner = yield* backend.makeSyncBackend
          const online = yield* ensure(clientId)
          const paused = yield* ensurePaused(clientId)
          const waitOnline = SubscriptionRef.waitUntil(online, (isOnline) => isOnline === true)
          const waitPullable = waitOnline.pipe(
            Effect.andThen(SubscriptionRef.waitUntil(paused, (isPaused) => isPaused === false)),
          )
          const wrapped: SyncBackend.SyncBackend = {
            ...inner,
            isConnected: online,
            pull: (cursor, options) =>
              inner.pull(cursor, options).pipe(Stream.mapEffect((item) => waitPullable.pipe(Effect.as(item)))),
            // Wait for connectivity outside the lock, so an offline client never blocks other writers.
            push: (batch) => waitOnline.pipe(Effect.andThen(inner.push(batch).pipe(writeLock.withPermits(1)))),
          }
          return wrapped
        })

    return {
      backendFor,
      setOnline: (clientId, online) => ensure(clientId).pipe(Effect.flatMap((ref) => SubscriptionRef.set(ref, online))),
      setPullsPaused: (clientId, isPaused) =>
        ensurePaused(clientId).pipe(Effect.flatMap((ref) => SubscriptionRef.set(ref, isPaused))),
      isOnline: (clientId) => {
        const ref = connectivity.get(clientId)
        return ref === undefined ? true : SubscriptionRef.getUnsafe(ref)
      },
      advance: (build) =>
        Effect.gen(function* () {
          yield* refreshLog
          const batch = build(backendLog.at(-1)?.seqNum ?? 0)
          if (batch.length === 0) return
          yield* backend.advance(...batch)
          yield* refreshLog
        }).pipe(writeLock.withPermits(1)),
      log: refreshLog.pipe(Effect.as(backendLog as ReadonlyArray<LiveStoreEvent.Global.Encoded>)),
      head: refreshLog.pipe(Effect.map(() => backendLog.at(-1)?.seqNum ?? 0)),
    } satisfies Network
  })

/** Small, fast, seedable PRNG (mulberry32) so randomized schedules replay exactly from their seed. */
export const makeRng = (seed: number) => {
  let state = seed >>> 0
  const next = () => {
    state = (state + 0x6d2b79f5) >>> 0
    let t = state
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
  const int = (min: number, max: number) => min + Math.floor(next() * (max - min + 1))
  const chance = (p: number) => next() < p
  const pick = <T>(items: ReadonlyArray<T>): T => items[Math.floor(next() * items.length)]!
  /** Picks a key by relative weight; zero-weight entries are never chosen. */
  const weighted = <K extends string>(weights: Record<K, number>): K => {
    const entries = Object.entries(weights) as [K, number][]
    const total = entries.reduce((sum, [, w]) => sum + w, 0)
    let roll = next() * total
    for (const [key, weight] of entries) {
      roll -= weight
      if (roll < 0 && weight > 0) return key
    }
    return entries.findLast(([, w]) => w > 0)![0]
  }
  return { next, int, chance, pick, weighted }
}

export type Rng = ReturnType<typeof makeRng>

export const STORE_ID = 'snapshot-test-store'
export const BACKEND_ID = 'backend-1'

/** Kinds of backend events the builders produce. `unknown` is unknown to every schema, `audit` only to `v2`. */
export type EventKind = 'create' | 'complete' | 'counter' | 'unknown' | 'audit'

/** Builds one backend event at `seqNum` (parent `seqNum - 1`). Arguments are derived from `n` only. */
export const makeGlobalEvent = ({
  seqNum,
  kind,
  writer,
  n = seqNum,
}: {
  seqNum: number
  kind: EventKind
  writer: string
  n?: number
}): LiveStoreEvent.Global.Encoded => {
  const { name, args } = ((): { name: string; args: unknown } => {
    switch (kind) {
      case 'create':
        return { name: 'v1.TodoCreated', args: { id: `todo-${n}`, text: `text-${n}`, author: writer } }
      case 'complete':
        return { name: 'v1.TodoCompleted', args: { id: `todo-${Math.max(1, n - 3)}` } }
      case 'counter':
        return { name: 'v1.CounterIncremented', args: { id: `counter-${n % 5}`, by: (n % 7) - 2 } }
      case 'unknown':
        return { name: 'v9.FutureEvent', args: { payload: `future-${n}` } }
      case 'audit':
        return { name: 'v1.AuditLogged', args: { id: `audit-${n % 11}`, note: `note-${n}` } }
    }
  })()
  return LiveStoreEvent.Global.Encoded.make({
    name,
    args,
    seqNum: EventSequenceNumber.Global.make(seqNum),
    parentSeqNum: EventSequenceNumber.Global.make(seqNum - 1),
    clientId: writer,
    sessionId: `${writer}-session`,
  })
}

/** Default deterministic mix of known events. */
export const knownKinds = (n: number): EventKind => (['create', 'create', 'counter', 'complete'] as const)[n % 4]!

export interface CheckpointerHandle {
  readonly leader: BootedLeader
  readonly checkpointer: Checkpointer
  readonly dbs: ClientDbs
}

/**
 * One store's world: mock backend behind a {@link Network}, an in-memory snapshot store and helpers to append events,
 * run checkpointers and boot clients. All clients share one SQLite factory.
 */
export const makeSnapshotWorld = (options?: {
  readonly graceMs?: number
  readonly now?: () => number
  readonly backendId?: string
}) =>
  Effect.gen(function* () {
    const makeSqliteDb = yield* makeSqlite
    const backend = yield* makeMockSyncBackend({
      startConnected: true,
      backendId: options?.backendId ?? BACKEND_ID,
    })
    const network = yield* makeNetwork(backend)
    const store = Snapshot.makeInMemorySnapshotStore({
      storeId: STORE_ID,
      ...(options?.graceMs !== undefined ? { graceMs: options.graceMs } : {}),
      ...(options?.now !== undefined ? { now: options.now } : {}),
    })

    const sync = (clientId: string, extra?: Partial<SyncOptions>): SyncOptions => ({
      backend: network.backendFor(clientId),
      livePull: true,
      // Surface sync failures on the leader's shutdown channel (`BootedLeader.shutdownMessages`) instead of stopping
      // silently, so the oracle can report them.
      onSyncError: 'shutdown',
      ...extra,
    })

    /** Appends `count` events at the backend head; `kinds(n)` chooses each event's kind by its sequence number. */
    const append = (count: number, kinds: (n: number) => EventKind = knownKinds, writer = 'backend-writer') =>
      network.advance((head) =>
        Array.from({ length: count }, (_, i) =>
          makeGlobalEvent({ seqNum: head + 1 + i, kind: kinds(head + 1 + i), writer }),
        ),
      )

    const boot = ({
      clientId,
      dbs,
      leaderSchema = defaultSchema,
      snapshot = true,
      snapshotOptions,
    }: {
      clientId: string
      dbs?: ClientDbs
      leaderSchema?: LiveStoreSchema
      snapshot?: boolean
      snapshotOptions?: Partial<Snapshot.SnapshotBootOptions>
    }) =>
      Effect.gen(function* () {
        return yield* bootLeader({
          makeSqliteDb,
          dbs: dbs ?? (yield* makeClientDbs(makeSqliteDb)),
          clientId,
          storeId: STORE_ID,
          syncOptions: sync(
            clientId,
            snapshot === true ? { snapshot: { source: store.makeSource, ...snapshotOptions } } : {},
          ),
          leaderSchema,
        })
      })

    /**
     * Boots a never-pushing checkpointer leader and starts the checkpointer loop on it. With `snapshotBoot` the
     * checkpointer itself may boot from the latest snapshot of its key.
     */
    const startCheckpointer = ({
      leaderSchema = defaultSchema,
      dbs,
      clientId = 'checkpointer',
      trigger = { minIntervalMs: 60_000_000 },
      pollInterval,
      now,
      snapshotBoot = false,
      waitForHead,
    }: {
      leaderSchema?: LiveStoreSchema
      dbs?: ClientDbs
      clientId?: string
      trigger?: Partial<Snapshot.SnapshotTrigger.TriggerParams>
      pollInterval?: Duration.Input
      now?: () => number
      snapshotBoot?: boolean
      /** Waits until the checkpointer applied the backend log up to this head before starting the loop. */
      waitForHead?: number
    }) =>
      Effect.gen(function* () {
        const checkpointerDbs = dbs ?? (yield* makeClientDbs(makeSqliteDb))
        const leader = yield* boot({ clientId, dbs: checkpointerDbs, leaderSchema, snapshot: snapshotBoot })
        if (waitForHead !== undefined) yield* waitForUpstreamHead(leader, waitForHead)
        const checkpointer = yield* makeCheckpointer({
          schema: leaderSchema,
          storeId: STORE_ID,
          sink: store.sink,
          trigger,
          ...(pollInterval !== undefined ? { pollInterval } : {}),
          ...(now !== undefined ? { now } : {}),
        }).pipe(provideLeader(leader))
        return { leader, checkpointer, dbs: checkpointerDbs } satisfies CheckpointerHandle
      })

    /** Runs a checkpointer at the current backend head, publishes once and stops it. */
    const publishAtHead = (leaderSchema: LiveStoreSchema = defaultSchema) =>
      Effect.gen(function* () {
        const head = yield* network.head
        const handle = yield* startCheckpointer({ leaderSchema, waitForHead: head, clientId: `checkpointer-${head}` })
        const manifest = Option.getOrThrow(yield* handle.checkpointer.publishNow)
        yield* handle.leader.shutdown
        return manifest
      })

    return { makeSqliteDb, backend, network, store, sync, append, boot, startCheckpointer, publishAtHead }
  })

export type SnapshotWorld = Effect.Success<ReturnType<typeof makeSnapshotWorld>>
