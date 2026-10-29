import { expect } from 'vitest'

import { makeMockSyncBackend, Snapshot, StateSqliteDb, type SyncOptions } from '@livestore/common'
import { Eventlog, LeaderThreadCtx, makeCheckpointer } from '@livestore/common/leader-thread'
import { LiveStoreEvent, SystemTables } from '@livestore/common/schema'
import { Vitest } from '@livestore/utils-dev/node-vitest'
import { Effect, Exit, Option, Schema } from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import {
  bootLeader,
  type BootedLeader,
  makeClientDbs,
  makeSchemaWithExtraTable,
  makeSqlite,
  makeWriterEvents,
  schema,
  stateDigest,
  waitForUpstreamHead,
  withFreshStateDb,
  nextLocalEvent,
  encodeArgs,
} from './harness.ts'

const storeId = 'snapshot-test-store'

const provideLeader =
  (leader: BootedLeader) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.provideService(LeaderThreadCtx, leader.ctx),
      Effect.provideService(StateSqliteDb.StateSqliteDb, leader.dbs.dbState),
    )

const setup = Effect.gen(function* () {
  const makeSqliteDb = yield* makeSqlite
  const backend = yield* makeMockSyncBackend({ startConnected: true, backendId: 'backend-1' })
  const store = Snapshot.makeInMemorySnapshotStore({ storeId })
  const sync = (extra?: Partial<SyncOptions>): SyncOptions => ({
    backend: () => backend.makeSyncBackend,
    livePull: true,
    ...extra,
  })
  const runCheckpointer = (head: number, leaderSchema = schema) =>
    Effect.gen(function* () {
      const leader = yield* bootLeader({
        makeSqliteDb,
        dbs: yield* makeClientDbs(makeSqliteDb),
        clientId: 'checkpointer',
        syncOptions: sync(),
        leaderSchema,
      })
      yield* waitForUpstreamHead(leader, head)
      const checkpointer = yield* makeCheckpointer({
        schema: leaderSchema,
        storeId,
        sink: store.sink,
        trigger: { minIntervalMs: 60_000 },
      }).pipe(provideLeader(leader))
      const manifest = yield* checkpointer.publishNow
      return { leader, manifest: Option.getOrThrow(manifest) }
    })
  return { makeSqliteDb, backend, store, sync, runCheckpointer }
})

/** Regressions for crash windows and checkpointer edge cases found in review. */
Vitest.describe('snapshot boot regressions', () => {
  Vitest.live('a crash between state install and eventlog positioning is repaired on the next boot', (test) =>
    Effect.gen(function* () {
      const { makeSqliteDb, backend, store, sync, runCheckpointer } = yield* setup
      yield* backend.advance(...makeWriterEvents({ count: 100 }))
      const { manifest } = yield* runCheckpointer(100)
      const bytes = store.testing.blobs.get(manifest.objectKey)!.bytes

      // What a crash in the window leaves behind: the snapshot pages without the completion marker, eventlog untouched.
      const dbs = yield* makeClientDbs(makeSqliteDb)
      dbs.dbState.import(bytes)
      dbs.dbState.execute(`DELETE FROM ${SystemTables.REBUILD_META_TABLE}`)

      // Next boot without a usable snapshot (option removed / manifest gone).
      const client = yield* bootLeader({ makeSqliteDb, dbs, clientId: 'crashed', syncOptions: sync() })
      const reference = yield* bootLeader({
        makeSqliteDb,
        dbs: yield* makeClientDbs(makeSqliteDb),
        clientId: 'reference',
        syncOptions: sync(),
      })
      yield* Effect.all([waitForUpstreamHead(client, 100), waitForUpstreamHead(reference, 100)])
      expect(stateDigest(client.dbs.dbState)).toBe(stateDigest(reference.dbs.dbState))
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test, { timeout: 60_000 })),
  )

  Vitest.live('an interrupted backfill is resumed by the next boot', (test) =>
    Effect.gen(function* () {
      const { makeSqliteDb, backend, store, sync, runCheckpointer } = yield* setup
      const nextSchema = makeSchemaWithExtraTable()
      const writerEvents = makeWriterEvents({ count: 100 })
      yield* backend.advance(...writerEvents)
      yield* runCheckpointer(100)

      const dbs = yield* makeClientDbs(makeSqliteDb)
      const agent = yield* bootLeader({
        makeSqliteDb,
        dbs,
        clientId: 'agent',
        syncOptions: sync({ snapshot: { source: store.makeSource } }),
      })
      expect(Option.isSome(agent.ctx.snapshotBoot)).toBe(true)
      yield* backend.advance(...makeWriterEvents({ startSeq: 101, count: 10 }))
      yield* waitForUpstreamHead(agent, 110)
      yield* agent.shutdown

      // Simulate a backfill that committed its first batches (events 1..30) and then was killed.
      for (const ev of writerEvents.slice(0, 30)) {
        const event = LiveStoreEvent.Client.fromGlobal(ev)
        const def = nextSchema.eventsDefsMap.get(event.name)!
        yield* Eventlog.insertIntoEventlog(
          event,
          dbs.dbEventlog,
          Schema.hash(def.schema),
          event.clientId,
          event.sessionId,
        )
      }

      const upgradedDbs = yield* withFreshStateDb(makeSqliteDb, dbs)
      const exit = yield* bootLeader({
        makeSqliteDb,
        dbs: upgradedDbs,
        clientId: 'agent',
        syncOptions: sync(),
        leaderSchema: nextSchema,
      }).pipe(Effect.exit)
      expect(Exit.isSuccess(exit)).toBe(true)
      if (Exit.isSuccess(exit) === true) {
        yield* waitForUpstreamHead(exit.value, 110)
        const reference = yield* bootLeader({
          makeSqliteDb,
          dbs: yield* makeClientDbs(makeSqliteDb),
          clientId: 'reference',
          syncOptions: sync(),
          leaderSchema: nextSchema,
        })
        yield* waitForUpstreamHead(reference, 110)
        expect(stateDigest(upgradedDbs.dbState, nextSchema)).toBe(stateDigest(reference.dbs.dbState, nextSchema))
        expect(Option.isNone(Eventlog.getEventlogBaseFromDb(upgradedDbs.dbEventlog))).toBe(true)
      }
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test, { timeout: 60_000 })),
  )

  Vitest.live('the in-memory sink orders snapshots per backend identity', (test) =>
    Effect.gen(function* () {
      const { store } = yield* setup
      // Publish at 500 for backend-1, then a new backend identity at 200.
      const base = {
        manifestVersion: 1 as const,
        storeId,
        key: 'k',
        byteLength: 1,
        sha256: 'x',
        createdAt: 0,
      }
      yield* store.sink.publish({
        manifest: { ...base, backendId: 'backend-1', seqNum: 500 },
        bytes: new Uint8Array(1),
      })
      const res = yield* store.sink
        .publish({ manifest: { ...base, backendId: 'backend-2', seqNum: 200 }, bytes: new Uint8Array(1) })
        .pipe(Effect.exit)
      expect(Exit.isSuccess(res)).toBe(true)
      const stale = yield* store.sink
        .publish({ manifest: { ...base, backendId: 'backend-2', seqNum: 100 }, bytes: new Uint8Array(1) })
        .pipe(Effect.exit)
      expect(Exit.isFailure(stale)).toBe(true)
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test, { timeout: 60_000 })),
  )
  Vitest.live('a leader with client-only events never exports a snapshot', (test) =>
    Effect.gen(function* () {
      const { makeSqliteDb, backend, sync } = yield* setup
      yield* backend.advance(...makeWriterEvents({ count: 10 }))
      const leader = yield* bootLeader({
        makeSqliteDb,
        dbs: yield* makeClientDbs(makeSqliteDb),
        clientId: 'cp',
        syncOptions: sync(),
      })
      yield* waitForUpstreamHead(leader, 10)
      const ui = yield* nextLocalEvent(
        leader,
        encodeArgs('uiStateSet', { id: 'singleton', value: { filter: 'LEAK', cursor: 9 } }),
        { clientOnly: true },
      )
      yield* leader.ctx.syncProcessor.push([ui])
      yield* Effect.sleep(100)
      expect(Exit.isFailure(yield* leader.ctx.exportSnapshot.pipe(Effect.exit))).toBe(true)
      yield* backend.advance(...makeWriterEvents({ startSeq: 11, count: 5 }))
      yield* waitForUpstreamHead(leader, 15)
      // The client-only event left the pending set, but its effect is still in state: export must keep refusing.
      expect(Exit.isFailure(yield* leader.ctx.exportSnapshot.pipe(Effect.exit))).toBe(true)

      // Also after a restart, when the refusal must come from the eventlog rather than in-memory tracking.
      yield* leader.shutdown
      const restarted = yield* bootLeader({ makeSqliteDb, dbs: leader.dbs, clientId: 'cp', syncOptions: sync() })
      yield* waitForUpstreamHead(restarted, 15)
      expect(Exit.isFailure(yield* restarted.ctx.exportSnapshot.pipe(Effect.exit))).toBe(true)
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test, { timeout: 60_000 })),
  )
})
