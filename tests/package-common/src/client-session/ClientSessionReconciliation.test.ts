import { expect } from 'vitest'

import {
  type ClientSession,
  ClientSessionLeaderThreadProxy,
  LeaderAheadError,
  MATERIALIZATION_JOURNAL_META_TABLE,
  STATE_HEAD_META_TABLE,
} from '@livestore/common'
import {
  Events,
  EventSequenceNumber,
  LiveStoreEvent,
  type LiveStoreSchema,
  makeSchema,
  State,
} from '@livestore/common/schema'
import { createStore, StoreInternalsSymbol } from '@livestore/livestore'
import { Vitest } from '@livestore/utils-dev/node-vitest'
import {
  Deferred,
  Effect,
  Exit,
  FetchHttpClient,
  Fiber,
  Layer,
  Option,
  Queue,
  References,
  Schema,
  Stream,
} from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import { events, schema, tables } from '../leader-thread/fixture.ts'
import { makeTestAdapter } from '../test-adapter.ts'

const environment = Layer.mergeAll(PlatformNode.NodeFileSystem.layer, FetchHttpClient.layer)

Vitest.describe('Client session reconciliation through Store', () => {
  Vitest.live('rolls back the whole local batch before installing state or scheduling propagation', (test) =>
    Effect.gen(function* () {
      const propagated: LiveStoreEvent.Client.Encoded[] = []
      const { store } = yield* makeStoreHarness((batch) =>
        Effect.sync(() => {
          propagated.push(...batch)
        }),
      )
      const processor = store[StoreInternalsSymbol].syncProcessor
      const before = yield* processor.syncState.get
      const exit = yield* processor
        .commit([
          events.todoCreated({ id: 'duplicate', text: 'first' }),
          events.todoCreated({ id: 'duplicate', text: 'second' }),
        ])
        .pipe(Effect.exit)
      expect(Exit.isFailure(exit)).toBe(true)
      expect(store.query(tables.todos)).toEqual([])
      expect(readHead(store)).toEqual(before.localHead)
      expect(journalCount(store)).toBe(0)
      expect(yield* processor.syncState.get).toEqual(before)
      yield* processor.shutdown(Exit.void)
      expect(propagated).toEqual([])
    }).pipe(Effect.provide(environment), Vitest.withTestCtx(test)),
  )

  Vitest.live('aborts remaining pull steps when the active push fails after a coherent prefix', (test) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>()
      const failPush = yield* Deferred.make<void>()
      const { store, deliver, failed } = yield* makeStoreHarness(() =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Deferred.await(failPush)),
          Effect.andThen(Effect.die(new Error('fatal push during reconciliation'))),
        ),
      )
      store.commit(events.todoCreated({ id: 'local', text: 'local' }))
      yield* Deferred.await(started)
      const local = (yield* store[StoreInternalsSymbol].syncProcessor.syncState.get).pending[0]!
      const observed: number[] = []
      const unsubscribe = store.subscribe(tables.todos, (rows) => {
        observed.push(rows.length)
        if (rows.length === 32) Effect.runSync(Deferred.succeed(failPush, undefined))
      })
      // Confirm the in-flight local event so the pull can advance without cancelling that push first.
      yield* deliver([local, ...Array.from({ length: 63 }, (_, i) => remoteCreated(i + 2))])
      expect(Exit.isFailure(yield* Deferred.await(failed))).toBe(true)
      expect(observed).toEqual([1, 32])
      expect(readHead(store).global).toBe(32)
      expect(store.query(tables.todos)).toHaveLength(32)
      expect(journalCount(store)).toBe(32)
      expect((yield* store[StoreInternalsSymbol].syncProcessor.syncState.get).localHead).toEqual(readHead(store))
      unsubscribe()
    }).pipe(Effect.provide(environment), Vitest.withTestCtx(test)),
  )

  Vitest.live('starts propagation only with the final encodings of edits committed between pull steps', (test) =>
    Effect.gen(function* () {
      const propagated: LiveStoreEvent.Client.Encoded[] = []
      const { store, deliver } = yield* makeStoreHarness((batch) =>
        Effect.sync(() => {
          propagated.push(...batch)
        }),
      )
      const committedAt = new Set<number>()
      const unsubscribe = store.subscribe(tables.todos, () => {
        const head = Effect.runSync(store[StoreInternalsSymbol].syncProcessor.syncState).upstreamHead.global
        if ((head === 32 || head === 64) && committedAt.has(head) === false) {
          committedAt.add(head)
          store.commit(events.todoCreated({ id: `local-${head}`, text: 'local' }))
          expect(propagated).toEqual([])
        }
      })
      yield* yield* deliver(Array.from({ length: 96 }, (_, i) => remoteCreated(i + 1)))
      yield* store[StoreInternalsSymbol].syncProcessor.shutdown(Exit.void)
      expect(propagated.map((event) => event.seqNum.global)).toEqual([97, 98])
      expect(propagated.map((event) => event.args.id)).toEqual(['local-32', 'local-64'])
      expect(readHead(store).global).toBe(98)
      unsubscribe()
    }).pipe(Effect.provide(environment), Vitest.withTestCtx(test)),
  )

  for (const updateSameRow of [false, true]) {
    Vitest.live(`preserves rows and durable head during cancellation (same row: ${updateSameRow})`, (test) =>
      Effect.gen(function* () {
        const started = yield* Deferred.make<void>()
        const cancelling = yield* Deferred.make<void>()
        const release = yield* Deferred.make<void>()
        const propagated: LiveStoreEvent.Client.Encoded[] = []
        let first = true
        const { store, deliver } = yield* makeStoreHarness((batch) =>
          Effect.suspend(() => {
            if (first === false)
              return Effect.sync(() => {
                propagated.push(...batch)
              })
            first = false
            return Deferred.succeed(started, undefined).pipe(
              Effect.andThen(Effect.never),
              Effect.ensuring(Deferred.succeed(cancelling, undefined).pipe(Effect.andThen(Deferred.await(release)))),
            )
          }),
        )
        store.commit(events.todoCreated({ id: 'local', text: 'local' }))
        yield* Deferred.await(started)
        const completed = yield* deliver([remoteCreated(1)])
        yield* Deferred.await(cancelling)
        // Cancellation is a genuine async wait, not permission to advertise an unapplied new head.
        expect(readHead(store)).toEqual(Effect.runSync(store[StoreInternalsSymbol].syncProcessor.syncState).localHead)
        store.commit(
          updateSameRow === true
            ? events.todoCompleted({ id: 'local' })
            : events.todoCreated({ id: 'concurrent', text: 'concurrent' }),
        )
        yield* Deferred.succeed(release, undefined)
        yield* completed
        expect(readHead(store)).toEqual(Effect.runSync(store[StoreInternalsSymbol].syncProcessor.syncState).localHead)
        expect(store.query(tables.todos).find((row) => row.id === 'local')?.completed).toBe(updateSameRow)
        expect(store.query(tables.todos)).toHaveLength(updateSameRow === true ? 2 : 3)
        yield* store[StoreInternalsSymbol].syncProcessor.shutdown(Exit.void)
        expect(propagated.map((event) => event.seqNum.global)).toEqual([2, 3])
      }).pipe(Effect.provide(environment), Vitest.withTestCtx(test)),
    )
  }

  Vitest.live('yields with coherent rows, journal and heads, and replays a concurrent noncommutative update', (test) =>
    Effect.gen(function* () {
      const firstPrefix = yield* Deferred.make<void>()
      const propagated: LiveStoreEvent.Client.Encoded[] = []
      const { store, deliver } = yield* makeStoreHarness(
        (batch) =>
          Effect.sync(() => {
            propagated.push(...batch)
          }),
        counterSchema,
      )
      const input = yield* Deferred.await(firstPrefix).pipe(
        Effect.andThen(
          Effect.sync(() => {
            const before = Effect.runSync(store[StoreInternalsSymbol].syncProcessor.syncState)
            expect(before.upstreamHead.global).toBeGreaterThan(0)
            expect(before.upstreamHead.global).toBeLessThan(128)
            store.commit(counterEvents.multiply({ factor: 2 }))
            expect(store.query(counter)[0]!.value).toBe(before.upstreamHead.global * 2)
          }),
        ),
        Effect.forkChild,
      )
      const seen: number[] = []
      const unsubscribe = store.subscribe(counter, (rows) => {
        if (rows.length === 0) return
        const state = Effect.runSync(store[StoreInternalsSymbol].syncProcessor.syncState)
        expect(readHead(store)).toEqual(state.localHead)
        expect(rows[0]!.value).toBe(state.upstreamHead.global * (state.pending.length > 0 ? 2 : 1))
        seen.push(state.upstreamHead.global)
        if (state.upstreamHead.global < 128) expect(journalCount(store)).toBeGreaterThan(0)
        Effect.runSync(Deferred.succeed(firstPrefix, undefined))
      })
      yield* yield* deliver(counterBatch(128))
      yield* Fiber.join(input)
      expect(seen.some((head) => head > 0 && head < 128)).toBe(true)
      expect(store.query(counter)[0]!.value).toBe(256)
      expect(journalCount(store)).toBe(1)
      yield* store[StoreInternalsSymbol].syncProcessor.shutdown(Exit.void)
      expect(propagated).toHaveLength(1)
      expect(propagated[0]!.seqNum.global).toBe(129)
      unsubscribe()
    }).pipe(
      Effect.provide(environment),
      Effect.provideService(References.MaxOpsBeforeYield, 64),
      Vitest.withTestCtx(test),
    ),
  )

  Vitest.live('allows a subscriber commit after the durable prefix without publishing stale state', (test) =>
    Effect.gen(function* () {
      const { store, deliver } = yield* makeStoreHarness(() => Effect.void)
      let committed = false
      const unsubscribe = store.subscribe(tables.todos, (rows) => {
        if (rows.length === 0 || committed === true) return
        committed = true
        expect(readHead(store)).toEqual(Effect.runSync(store[StoreInternalsSymbol].syncProcessor.syncState).localHead)
        store.commit(events.todoCompleted({ id: 'remote-1' }))
      })
      yield* yield* deliver(Array.from({ length: 96 }, (_, i) => remoteCreated(i + 1)))
      expect(store.query(tables.todos).find((row) => row.id === 'remote-1')!.completed).toBe(true)
      expect(readHead(store).global).toBe(97)
      expect(journalCount(store)).toBe(1)
      unsubscribe()
    }).pipe(Effect.provide(environment), Vitest.withTestCtx(test)),
  )

  for (const failingEvent of [2, 34]) {
    Vitest.live(`rolls back a failed step, keeping only complete prefixes (failure at ${failingEvent})`, (test) =>
      Effect.gen(function* () {
        const { store, deliver, failed } = yield* makeStoreHarness(() => Effect.void)
        const batch = Array.from({ length: 64 }, (_, i) => remoteCreated(i + 1))
        batch[failingEvent - 1] = LiveStoreEvent.Client.Encoded.make({
          ...batch[failingEvent - 1]!,
          args: batch[failingEvent - 2]!.args,
        })
        const published: number[] = []
        const unsubscribe = store.subscribe(tables.todos, (rows) => published.push(rows.length))
        yield* deliver(batch)
        expect(Exit.isFailure(yield* Deferred.await(failed))).toBe(true)
        const prefix = failingEvent === 2 ? 0 : 32
        expect(store.query(tables.todos)).toHaveLength(prefix)
        expect(readHead(store).global).toBe(prefix)
        expect(readHead(store)).toEqual(Effect.runSync(store[StoreInternalsSymbol].syncProcessor.syncState).localHead)
        expect(journalCount(store)).toBe(prefix)
        expect(published).toEqual(prefix === 0 ? [0] : [0, 32])
        unsubscribe()
      }).pipe(Effect.provide(environment), Vitest.withTestCtx(test)),
    )
  }

  Vitest.live('rejects local commits before materialization while failure cleanup is awaiting cancellation', (test) =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>()
      const cancelling = yield* Deferred.make<void>()
      const release = yield* Deferred.make<void>()
      const { store, deliver } = yield* makeStoreHarness(() =>
        Deferred.succeed(started, undefined).pipe(
          Effect.andThen(Effect.never),
          Effect.ensuring(Deferred.succeed(cancelling, undefined).pipe(Effect.andThen(Deferred.await(release)))),
        ),
      )
      store.commit(events.todoCreated({ id: 'local', text: 'local' }))
      yield* Deferred.await(started)
      const state = Effect.runSync(store[StoreInternalsSymbol].syncProcessor.syncState)
      // Confirmation doesn't require cancellation, but the following duplicate INSERT fails materialization.
      yield* deliver([
        state.pending[0]!,
        LiveStoreEvent.Client.Encoded.make({
          ...remoteCreated(2),
          args: { id: 'local', text: 'duplicate' },
        }),
      ])
      yield* Deferred.await(cancelling)
      try {
        // Store may already mark itself disposed while reporting the defect. Inspect SQLite directly while
        // cleanup is held open; the important guarantee is that rejected admission did not materialize anything.
        Effect.runSyncExit(Effect.sync(() => store.commit(events.todoCreated({ id: 'too-late', text: 'too-late' }))))
        expect(store[StoreInternalsSymbol].clientSession.sqliteDb.select('SELECT * FROM todos')).toHaveLength(1)
        expect(readHead(store)).toEqual(state.localHead)
        expect(journalCount(store)).toBe(1)
      } finally {
        Effect.runSync(Deferred.succeed(release, undefined))
      }
    }).pipe(Effect.provide(environment), Vitest.withTestCtx(test)),
  )

  Vitest.live('refreshes tables changed only by rollback and applies an explicit rebase across steps', (test) =>
    Effect.gen(function* () {
      const { store, deliver } = yield* makeStoreHarness(() => Effect.void)
      const old = Array.from({ length: 64 }, (_, i) => remoteCreated(i + 1))
      yield* yield* deliver(old, { globalHead: EventSequenceNumber.Client.ROOT })
      const counts: number[] = []
      const unsubscribe = store.subscribe(tables.todos, (rows) => counts.push(rows.length))
      // Unknown replacement events still advance durable history but don't touch the todos table.
      const replacement = Array.from({ length: 96 }, (_, i) =>
        LiveStoreEvent.Client.Encoded.make({
          ...remoteCreated(i + 1),
          name: 'future-event',
          seqNum: EventSequenceNumber.Client.Composite.make({ global: i + 1, client: 0, rebaseGeneration: 1 }),
          parentSeqNum: EventSequenceNumber.Client.Composite.make({ global: i, client: 0, rebaseGeneration: 1 }),
        }),
      )
      yield* yield* deliver(replacement, { rollbackEvents: old })
      expect(counts).toEqual([64, 0])
      expect(store.query(tables.todos)).toEqual([])
      expect(readHead(store).global).toBe(96)
      expect(journalCount(store)).toBe(0)
      unsubscribe()
    }).pipe(Effect.provide(environment), Vitest.withTestCtx(test)),
  )

  Vitest.live('does not attach a future leader hash to intermediate pending replay', (test) =>
    Effect.gen(function* () {
      let first = true
      const { store, deliver } = yield* makeStoreHarness(() => {
        if (first === false) return Effect.void
        first = false
        return Effect.never
      })
      store.commit(events.todoCreated({ id: 'local', text: 'local' }))
      const batch = Array.from({ length: 64 }, (_, i) => remoteCreated(i + 1))
      // After the first prefix, pending local becomes e33r1. This unrelated future event has that same key.
      const key = EventSequenceNumber.Client.Composite.make({ global: 33, client: 0, rebaseGeneration: 1 })
      batch[32] = LiveStoreEvent.Client.Encoded.make({ ...batch[32]!, name: 'future-event', seqNum: key })
      yield* yield* deliver(batch, {
        materializerHashes: [
          LiveStoreEvent.Client.MaterializerHash.make({
            eventNum: key,
            hash: Option.some(99),
          }),
        ],
      })
      expect(store.query(tables.todos)).toHaveLength(64)
      expect(readHead(store).global).toBe(65)
      expect(journalCount(store)).toBe(1)
    }).pipe(Effect.provide(environment), Vitest.withTestCtx(test)),
  )

  Vitest.live('confirms a pending event acknowledged by a later prefix instead of duplicating it', (test) =>
    Effect.gen(function* () {
      const { store, deliver } = yield* makeStoreHarness(() => Effect.void)
      store.commit(events.todoCreated({ id: 'local', text: 'local' }))
      const pending = Effect.runSync(store[StoreInternalsSymbol].syncProcessor.syncState).pending[0]!
      const batch = Array.from({ length: 64 }, (_, i) => remoteCreated(i + 1))
      batch[32] = LiveStoreEvent.Client.Encoded.make({
        ...pending,
        seqNum: batch[32]!.seqNum,
        parentSeqNum: batch[32]!.parentSeqNum,
      })
      yield* yield* deliver(batch)
      expect(Effect.runSync(store[StoreInternalsSymbol].syncProcessor.syncState).pending).toEqual([])
      expect(store.query(tables.todos)).toHaveLength(64)
      expect(readHead(store).global).toBe(64)
      expect(journalCount(store)).toBe(0)
    }).pipe(Effect.provide(environment), Vitest.withTestCtx(test)),
  )
})

type StoreDb = { [StoreInternalsSymbol]: { clientSession: Pick<ClientSession, 'sqliteDb'> } }

const journalCount = (store: StoreDb) =>
  store[StoreInternalsSymbol].clientSession.sqliteDb.select<{ count: number }>(
    `SELECT COUNT(*) AS count FROM ${MATERIALIZATION_JOURNAL_META_TABLE}`,
  )[0]!.count

const readHead = (store: StoreDb) => {
  const [head] = store[StoreInternalsSymbol].clientSession.sqliteDb.select<{
    seqNumGlobal: number
    seqNumClient: number
    seqNumRebaseGeneration: number
  }>(`SELECT * FROM ${STATE_HEAD_META_TABLE}`)
  return head === undefined
    ? EventSequenceNumber.Client.ROOT
    : EventSequenceNumber.Client.Composite.make({
        global: head.seqNumGlobal,
        client: head.seqNumClient,
        rebaseGeneration: head.seqNumRebaseGeneration,
      })
}

const remoteCreated = (global: number) =>
  LiveStoreEvent.Client.Encoded.make({
    name: events.todoCreated.name,
    args: { id: `remote-${global}`, text: 'remote' },
    seqNum: EventSequenceNumber.Client.Composite.make({ global, client: 0 }),
    parentSeqNum: EventSequenceNumber.Client.Composite.make({ global: global - 1, client: 0 }),
    clientId: 'remote',
    sessionId: 'remote',
  })

// Real Store/SQLite, with only transport controlled. The next pull demand is the completion signal,
// because a concurrent local commit can publish syncState while the preceding pull is still running.
const makeStoreHarness = Effect.fn(function* (
  push: ClientSessionLeaderThreadProxy.ClientSessionLeaderThreadProxy['events']['push'],
  storeSchema: LiveStoreSchema = schema,
) {
  const failed = yield* Deferred.make<Exit.Exit<unknown, unknown>>()
  const pulls = yield* Queue.unbounded<{
    item: typeof ClientSessionLeaderThreadProxy.PullItem.Type
    completed: Deferred.Deferred<void>
  }>()
  let previous: Deferred.Deferred<void> | undefined
  const store = yield* createStore({
    schema: storeSchema,
    storeId: 'reconciliation',
    logLevel: 'Error',
    adapter: (args) =>
      makeTestAdapter({
        testing: {
          overrides: {
            clientSession: {
              leaderThreadProxy: (proxy) => ({
                events: {
                  ...proxy.events,
                  push,
                  pull: () =>
                    Stream.fromEffect(
                      Effect.gen(function* () {
                        if (previous !== undefined) yield* Deferred.succeed(previous, undefined)
                        const next = yield* Queue.take(pulls)
                        previous = next.completed
                        return next.item
                      }),
                    ).pipe(Stream.forever),
                },
              }),
            },
          },
        },
      })(args).pipe(
        Effect.map((session) => ({
          ...session,
          // Keep the test DB available after a processor failure so we can inspect the rolled-back transaction.
          shutdown: (exit) => Deferred.succeed(failed, exit).pipe(Effect.asVoid),
        })),
      ),
  })
  const deliver = Effect.fn(function* (
    newEvents: ReadonlyArray<LiveStoreEvent.Client.Encoded>,
    options?: {
      globalHead?: EventSequenceNumber.Client.Composite
      rollbackEvents?: ReadonlyArray<LiveStoreEvent.Client.Encoded>
      materializerHashes?: ReadonlyArray<LiveStoreEvent.Client.MaterializerHash>
    },
  ) {
    const completed = yield* Deferred.make<void>()
    yield* Queue.offer(pulls, {
      item: ClientSessionLeaderThreadProxy.PullItem.make({
        payload:
          options?.rollbackEvents === undefined
            ? { _tag: 'upstream-advance', newEvents }
            : { _tag: 'upstream-rebase', newEvents, rollbackEvents: options.rollbackEvents },
        globalHead: options?.globalHead ?? newEvents.at(-1)?.seqNum ?? EventSequenceNumber.Client.ROOT,
        materializerHashes: options?.materializerHashes ?? [],
      }),
      completed,
    })
    return Deferred.await(completed)
  })
  return { store, deliver, failed }
})

const counter = State.SQLite.table({
  name: 'counter',
  columns: {
    id: State.SQLite.integer({ primaryKey: true }),
    value: State.SQLite.integer(),
  },
})
const counterEvents = {
  increment: Events.synced({ name: 'increment', schema: Schema.Struct({}) }),
  multiply: Events.synced({ name: 'multiply', schema: Schema.Struct({ factor: Schema.Number }) }),
}
const counterSchema = makeSchema({
  events: counterEvents,
  state: State.SQLite.makeState({
    tables: { counter },
    materializers: State.SQLite.materializers(counterEvents, {
      increment: () => ({
        sql: 'INSERT INTO counter (id, value) VALUES (1, 1) ON CONFLICT(id) DO UPDATE SET value = value + 1',
        bindValues: [],
      }),
      multiply: ({ factor }) => ({ sql: 'UPDATE counter SET value = value * ?', bindValues: [factor] }),
    }),
  }),
})
const counterBatch = (count: number) =>
  Array.from({ length: count }, (_, i) =>
    LiveStoreEvent.Client.Encoded.make({
      ...remoteCreated(i + 1),
      name: counterEvents.increment.name,
      args: {},
    }),
  )
