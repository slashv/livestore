import { expect } from 'vitest'

import type { UnknownError } from '@livestore/common'
import type { LiveStoreSchema } from '@livestore/common/schema'
import { createStore, StoreInternalsSymbol } from '@livestore/livestore'
import { Vitest } from '@livestore/utils-dev/node-vitest'
import { type Scope, Effect, Option } from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import { makeTestAdapter } from '../test-adapter.ts'
import {
  type BootedLeader,
  encodeArgs,
  events,
  makeVersionedSchemas,
  nextLocalEvent,
  tables,
  waitForNoPending,
  waitForUpstreamHead,
} from './harness.ts'
import {
  bootReference,
  type EventKind,
  expectEquivalentState,
  expectEventlogInvariants,
  knownKinds,
  makeSnapshotWorld,
  type SnapshotWorld,
  STORE_ID,
} from './oracle.ts'

/**
 * Verifier 1, state equivalence oracle: in every scenario a client booted from snapshot + tail is indistinguishable
 * from a client that replayed the full history, at the same head and after further writes from both.
 */

const { v1, v2 } = makeVersionedSchemas()

const withUnknown = (n: number): EventKind => (n % 5 === 0 ? 'unknown' : knownKinds(n))
const withAudit = (n: number): EventKind => (n % 3 === 0 ? 'audit' : withUnknown(n))

interface Scenario {
  readonly name: string
  readonly schema: LiveStoreSchema
  /** Builds backend history and publishes snapshots; returns the expected installed snapshot position. */
  readonly build: (world: SnapshotWorld) => Effect.Effect<number, UnknownError, Scope.Scope>
}

const scenarios: ReadonlyArray<Scenario> = [
  {
    name: 'snapshot exactly at the head (empty tail)',
    schema: v1,
    build: (world) =>
      Effect.gen(function* () {
        yield* world.append(150)
        return (yield* world.publishAtHead(v1)).seqNum
      }),
  },
  {
    name: 'long tail of mixed events after the snapshot',
    schema: v1,
    build: (world) =>
      Effect.gen(function* () {
        yield* world.append(40)
        const manifest = yield* world.publishAtHead(v1)
        yield* world.append(260)
        return manifest.seqNum
      }),
  },
  {
    name: 'unknown events below and above the snapshot',
    schema: v1,
    build: (world) =>
      Effect.gen(function* () {
        yield* world.append(120, withAudit)
        const manifest = yield* world.publishAtHead(v1)
        yield* world.append(80, withAudit)
        return manifest.seqNum
      }),
  },
  {
    name: 'new-event schema materializes events the old schema ignored',
    schema: v2,
    build: (world) =>
      Effect.gen(function* () {
        yield* world.append(120, withAudit)
        const manifest = yield* world.publishAtHead(v2)
        yield* world.append(80, withAudit)
        return manifest.seqNum
      }),
  },
  {
    name: 'several snapshot generations (latest wins)',
    schema: v1,
    build: (world) =>
      Effect.gen(function* () {
        let latest = 0
        for (let i = 0; i < 4; i++) {
          yield* world.append(50, withUnknown)
          latest = (yield* world.publishAtHead(v1)).seqNum
        }
        yield* world.append(25)
        return latest
      }),
  },
  {
    name: 'checkpointer that itself booted from a snapshot publishes the next one',
    schema: v1,
    build: (world) =>
      Effect.gen(function* () {
        yield* world.append(100)
        yield* world.publishAtHead(v1)
        yield* world.append(70, withUnknown)
        const head = yield* world.network.head
        const chained = yield* world.startCheckpointer({
          leaderSchema: v1,
          snapshotBoot: true,
          clientId: 'chained-checkpointer',
          waitForHead: head,
        })
        expect(Option.map(chained.leader.ctx.snapshotBoot, (r) => r.manifest.seqNum)).toEqual(Option.some(100))
        const manifest = Option.getOrThrow(yield* chained.checkpointer.publishNow)
        expect(manifest.seqNum).toBe(170)
        yield* chained.leader.shutdown
        yield* world.append(30)
        return manifest.seqNum
      }),
  },
]

const runOracle = (scenario: Scenario) =>
  Effect.gen(function* () {
    const world = yield* makeSnapshotWorld()
    const snapshotAt = yield* scenario.build(world)
    const head = yield* world.network.head

    const booted = yield* world.boot({ clientId: 'snapshot-client', leaderSchema: scenario.schema })
    expect(Option.map(booted.ctx.snapshotBoot, (r) => r.manifest.seqNum)).toEqual(Option.some(snapshotAt))
    const reference = yield* bootReference({
      makeSqliteDb: world.makeSqliteDb,
      syncOptions: world.sync('reference'),
      head,
      leaderSchema: scenario.schema,
    })
    yield* waitForUpstreamHead(booted, head)

    expectEquivalentState(booted.dbs.dbState, reference.dbs.dbState, scenario.schema, 'at head')
    expectEventlogInvariants({ dbEventlog: booted.dbs.dbEventlog, backendLog: yield* world.network.log, head })

    // Further writes from both clients and the backend keep them equivalent.
    yield* pushTodo(booted, 'from-snapshot-client')
    yield* pushTodo(reference, 'from-reference')
    yield* pushCompletion(booted, 'from-reference')
    yield* world.append(15, scenario.schema === v2 ? withAudit : withUnknown)
    yield* Effect.all([waitForNoPending(booted), waitForNoPending(reference)])
    const finalHead = yield* world.network.head
    yield* Effect.all([waitForUpstreamHead(booted, finalHead), waitForUpstreamHead(reference, finalHead)])
    expectEquivalentState(booted.dbs.dbState, reference.dbs.dbState, scenario.schema, 'after further writes')
    expectEventlogInvariants({
      dbEventlog: booted.dbs.dbEventlog,
      backendLog: yield* world.network.log,
      head: finalHead,
    })

    // A restart on the persisted databases keeps the base and the state.
    yield* booted.shutdown
    const restarted = yield* world.boot({ clientId: 'snapshot-client', dbs: booted.dbs, leaderSchema: scenario.schema })
    expect(Option.isNone(restarted.ctx.snapshotBoot)).toBe(true)
    yield* waitForUpstreamHead(restarted, finalHead)
    expectEquivalentState(restarted.dbs.dbState, reference.dbs.dbState, scenario.schema, 'after restart')
  })

Vitest.describe('snapshot oracle: snapshot + tail equals full replay', () => {
  for (const scenario of scenarios) {
    Vitest.live(scenario.name, (test) =>
      runOracle(scenario).pipe(
        Effect.provide(PlatformNode.NodeFileSystem.layer),
        Vitest.withTestCtx(test, { timeout: 60_000 }),
      ),
    )
  }

  Vitest.live('Store API: snapshot-booted store answers queries like a full-replay store', (test) =>
    Effect.gen(function* () {
      const world = yield* makeSnapshotWorld()
      yield* world.append(200, withUnknown)
      const manifest = yield* world.publishAtHead(v1)
      yield* world.append(50)

      let downloads = 0
      const countingSource: typeof world.store.makeSource = (args) =>
        world.store.makeSource(args).pipe(
          Effect.map((source) => ({
            ...source,
            download: (m: typeof manifest) => source.download(m).pipe(Effect.tap(() => Effect.sync(() => downloads++))),
          })),
        )

      const snapshotStore = yield* createStore({
        schema: v1 as LiveStoreSchema,
        storeId: STORE_ID,
        adapter: makeTestAdapter({
          clientId: 'store-snapshot',
          sync: { ...world.sync('store-snapshot'), snapshot: { source: countingSource } },
        }),
      })
      const replayStore = yield* createStore({
        schema: v1 as LiveStoreSchema,
        storeId: STORE_ID,
        adapter: makeTestAdapter({ clientId: 'store-replay', sync: world.sync('store-replay') }),
      })
      expect(downloads).toBe(1)

      const waitForStoreHead = (store: typeof snapshotStore, head: number) =>
        Effect.gen(function* () {
          const syncState = store[StoreInternalsSymbol].syncProcessor.syncState
          while ((yield* syncState.get).upstreamHead.global < head || (yield* syncState.get).pending.length > 0) {
            yield* Effect.sleep(5)
          }
        }).pipe(Effect.timeout('20 seconds'), Effect.orDie)

      const snapshotQueries = () => ({
        todos: snapshotStore.query(tables.todos.orderBy('id', 'asc')),
        counters: snapshotStore.query(tables.counters.orderBy('id', 'asc')),
        completed: snapshotStore.query(tables.todos.where({ completed: true }).count()),
      })
      const replayQueries = () => ({
        todos: replayStore.query(tables.todos.orderBy('id', 'asc')),
        counters: replayStore.query(tables.counters.orderBy('id', 'asc')),
        completed: replayStore.query(tables.todos.where({ completed: true }).count()),
      })

      let head = yield* world.network.head
      yield* Effect.all([waitForStoreHead(snapshotStore, head), waitForStoreHead(replayStore, head)])
      expect(snapshotQueries().todos.length).toBeGreaterThan(0)
      expect(snapshotQueries()).toEqual(replayQueries())
      expect(manifest.seqNum).toBe(200)

      // Commits through both stores, including one on a row that only exists in the snapshot.
      snapshotStore.commit(events.todoCreated({ id: 'store-a', text: 'A', author: 'snapshot' }))
      snapshotStore.commit(events.todoCompleted({ id: 'todo-2' }))
      snapshotStore.commit(tables.uiState.set({ filter: 'done', cursor: 1 }, 'singleton'))
      replayStore.commit(events.counterIncremented({ id: 'counter-1', by: 40 }))
      replayStore.commit(events.todoCompleted({ id: 'store-a' }))
      yield* world.append(20, withUnknown)

      // Wait until both committed batches reached the backend, then until both stores applied everything.
      yield* Effect.gen(function* () {
        while ((yield* world.network.log).filter((e) => e.clientId.startsWith('store-')).length < 4) {
          yield* Effect.sleep(5)
        }
      }).pipe(Effect.timeout('20 seconds'), Effect.orDie)
      head = yield* world.network.head
      yield* Effect.all([waitForStoreHead(snapshotStore, head), waitForStoreHead(replayStore, head)])
      expect(snapshotQueries()).toEqual(replayQueries())
      expect(snapshotStore.query(tables.todos.where({ id: 'store-a' }))).toEqual([
        { id: 'store-a', text: 'A', completed: true, author: 'snapshot' },
      ])
      expect(snapshotStore.query(tables.uiState.get('singleton'))).toEqual({ filter: 'done', cursor: 1 })
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test, { timeout: 60_000 })),
  )
})

const pushTodo = (leader: BootedLeader, id: string) =>
  Effect.gen(function* () {
    const event = yield* nextLocalEvent(leader, encodeArgs('todoCreated', { id, text: id, author: leader.clientId }))
    yield* leader.ctx.syncProcessor.push([event])
  })

const pushCompletion = (leader: BootedLeader, id: string) =>
  Effect.gen(function* () {
    const event = yield* nextLocalEvent(leader, encodeArgs('todoCompleted', { id }))
    yield* leader.ctx.syncProcessor.push([event])
  })
