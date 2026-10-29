import { expect } from 'vitest'

import { makeMockSyncBackend, Snapshot, StateSqliteDb, type SyncOptions } from '@livestore/common'
import { Eventlog, LeaderThreadCtx, makeCheckpointer } from '@livestore/common/leader-thread'
import { Vitest } from '@livestore/utils-dev/node-vitest'
import { Effect, Option } from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import {
  bootLeader,
  type BootedLeader,
  encodeArgs,
  eventlogRowCount,
  makeClientDbs,
  makeSchemaWithExtraTable,
  makeSqlite,
  makeWriterEvents,
  nextLocalEvent,
  schema,
  stateDigest,
  stateHeadOf,
  waitForNoPending,
  waitForUpstreamHead,
  withFreshStateDb,
} from './harness.ts'

const storeId = 'snapshot-test-store'

const setup = Effect.gen(function* () {
  const makeSqliteDb = yield* makeSqlite
  const backend = yield* makeMockSyncBackend({ startConnected: true, backendId: 'backend-1' })
  const store = Snapshot.makeInMemorySnapshotStore({ storeId })
  const sync = (extra?: Partial<SyncOptions>): SyncOptions => ({
    backend: () => backend.makeSyncBackend,
    livePull: true,
    ...extra,
  })

  /** Boots a never-pushing checkpointer leader at the backend head and publishes one snapshot. */
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
      return { leader, checkpointer, manifest: Option.getOrThrow(manifest) }
    })

  return { makeSqliteDb, backend, store, sync, runCheckpointer }
})

const provideLeader =
  (leader: BootedLeader) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.provideService(LeaderThreadCtx, leader.ctx),
      Effect.provideService(StateSqliteDb.StateSqliteDb, leader.dbs.dbState),
    )

Vitest.describe('snapshot boot', () => {
  Vitest.live('a fresh client boots from snapshot + tail and matches a full replay', (test) =>
    Effect.gen(function* () {
      const { makeSqliteDb, backend, store, sync, runCheckpointer } = yield* setup
      yield* backend.advance(...makeWriterEvents({ count: 400 }))

      const { manifest } = yield* runCheckpointer(400)
      expect(manifest.seqNum).toBe(400)
      expect(manifest.backendId).toBe('backend-1')

      yield* backend.advance(...makeWriterEvents({ startSeq: 401, count: 60 }))

      const booted = yield* bootLeader({
        makeSqliteDb,
        dbs: yield* makeClientDbs(makeSqliteDb),
        clientId: 'fresh-agent',
        syncOptions: sync({ snapshot: { source: store.makeSource } }),
      })
      expect(Option.map(booted.ctx.snapshotBoot, (result) => result.manifest.seqNum)).toEqual(Option.some(400))

      const replayed = yield* bootLeader({
        makeSqliteDb,
        dbs: yield* makeClientDbs(makeSqliteDb),
        clientId: 'full-replay',
        syncOptions: sync(),
      })
      expect(Option.isNone(replayed.ctx.snapshotBoot)).toBe(true)

      yield* Effect.all([waitForUpstreamHead(booted, 460), waitForUpstreamHead(replayed, 460)])

      // Only the tail is stored locally; the base stands in for everything below it.
      expect(eventlogRowCount(booted.dbs.dbEventlog)).toBe(60)
      expect(eventlogRowCount(replayed.dbs.dbEventlog)).toBe(460)
      expect(Eventlog.getEventlogBaseFromDb(booted.dbs.dbEventlog).pipe(Option.map((b) => b.seqNumGlobal))).toEqual(
        Option.some(400),
      )
      expect(stateDigest(booted.dbs.dbState)).toBe(stateDigest(replayed.dbs.dbState))
      expect(stateHeadOf(booted.dbs.dbState)).toEqual(stateHeadOf(replayed.dbs.dbState))

      // The booted client keeps working: its own events reach the backend and every other client.
      const created = yield* nextLocalEvent(
        booted,
        encodeArgs('todoCreated', { id: 'from-booted', text: 'hi', author: 'fresh-agent' }),
      )
      yield* booted.ctx.syncProcessor.push([created])
      const uiSet = yield* nextLocalEvent(
        booted,
        encodeArgs('uiStateSet', { id: 'singleton', value: { filter: 'done', cursor: 3 } }),
        {
          clientOnly: true,
        },
      )
      yield* booted.ctx.syncProcessor.push([uiSet])
      yield* waitForNoPending(booted)
      yield* waitForUpstreamHead(replayed, 461)
      expect(booted.dbs.dbState.select(`SELECT text FROM todos WHERE id = 'from-booted'`)).toEqual([{ text: 'hi' }])
      expect(replayed.dbs.dbState.select(`SELECT text FROM todos WHERE id = 'from-booted'`)).toEqual([{ text: 'hi' }])
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test, { timeout: 60_000 })),
  )

  Vitest.live('a client with a non-empty eventlog keeps its local state across restarts', (test) =>
    Effect.gen(function* () {
      const { makeSqliteDb, backend, store, sync, runCheckpointer } = yield* setup
      yield* backend.advance(...makeWriterEvents({ count: 50 }))

      const dbs = yield* makeClientDbs(makeSqliteDb)
      const first = yield* bootLeader({ makeSqliteDb, dbs, clientId: 'agent', syncOptions: sync() })
      yield* waitForUpstreamHead(first, 50)
      yield* first.shutdown

      yield* backend.advance(...makeWriterEvents({ startSeq: 51, count: 250 }))
      yield* runCheckpointer(300)

      const restarted = yield* bootLeader({
        makeSqliteDb,
        dbs,
        clientId: 'agent',
        syncOptions: sync({ snapshot: { source: store.makeSource } }),
      })
      expect(Option.isNone(restarted.ctx.snapshotBoot)).toBe(true)
      yield* waitForUpstreamHead(restarted, 300)
      expect(eventlogRowCount(dbs.dbEventlog)).toBe(300)
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test, { timeout: 60_000 })),
  )

  for (const fault of ['none-published', 'corrupt-blob', 'missing-blob', 'wrong-backend'] as const) {
    Vitest.live(`falls back to full replay when the snapshot is unusable (${fault})`, (test) =>
      Effect.gen(function* () {
        const { makeSqliteDb, backend, store, sync, runCheckpointer } = yield* setup
        yield* backend.advance(...makeWriterEvents({ count: 120 }))

        if (fault !== 'none-published') {
          const { manifest } = yield* runCheckpointer(120)
          if (fault === 'corrupt-blob') store.testing.corruptBlob(manifest.objectKey)
          if (fault === 'missing-blob') store.testing.deleteBlob(manifest.objectKey)
          if (fault === 'wrong-backend') store.testing.setManifest({ ...manifest, backendId: 'backend-0' })
        }

        const client = yield* bootLeader({
          makeSqliteDb,
          dbs: yield* makeClientDbs(makeSqliteDb),
          clientId: 'agent',
          syncOptions: sync({ snapshot: { source: store.makeSource } }),
        })
        const reference = yield* bootLeader({
          makeSqliteDb,
          dbs: yield* makeClientDbs(makeSqliteDb),
          clientId: 'reference',
          syncOptions: sync(),
        })
        yield* Effect.all([waitForUpstreamHead(client, 120), waitForUpstreamHead(reference, 120)])

        if (fault === 'wrong-backend') {
          // A manifest from another backend identity installs, then the provider's identity check resets the client
          // (default `onBackendIdMismatch: 'reset'`). The mock backend does not check identities, so we only assert the
          // install happened; the Cloudflare suite covers the reset.
          expect(Option.isSome(client.ctx.snapshotBoot)).toBe(true)
        } else {
          expect(Option.isNone(client.ctx.snapshotBoot)).toBe(true)
          expect(eventlogRowCount(client.dbs.dbEventlog)).toBe(120)
        }
        expect(stateDigest(client.dbs.dbState)).toBe(stateDigest(reference.dbs.dbState))
      }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test, { timeout: 60_000 })),
    )
  }

  Vitest.live('a schema change re-seeds from the new key snapshot or backfills from the backend', (test) =>
    Effect.gen(function* () {
      const { makeSqliteDb, backend, store, sync, runCheckpointer } = yield* setup
      const nextSchema = makeSchemaWithExtraTable()
      expect(Snapshot.SnapshotKey.toString(Snapshot.SnapshotKey.make(nextSchema))).not.toBe(
        Snapshot.SnapshotKey.toString(Snapshot.SnapshotKey.make(schema)),
      )

      yield* backend.advance(...makeWriterEvents({ count: 200 }))
      yield* runCheckpointer(200)

      // Two snapshot-booted agents on the old schema.
      const reseedDbs = yield* makeClientDbs(makeSqliteDb)
      const backfillDbs = yield* makeClientDbs(makeSqliteDb)
      for (const [dbs, clientId] of [
        [reseedDbs, 'reseed'],
        [backfillDbs, 'backfill'],
      ] as const) {
        const agent = yield* bootLeader({
          makeSqliteDb,
          dbs,
          clientId,
          syncOptions: sync({ snapshot: { source: store.makeSource } }),
        })
        expect(Option.isSome(agent.ctx.snapshotBoot)).toBe(true)
        yield* backend.advance(...makeWriterEvents({ startSeq: clientId === 'reseed' ? 201 : 251, count: 50 }))
        yield* waitForUpstreamHead(agent, clientId === 'reseed' ? 250 : 300)
        yield* agent.shutdown
      }

      // The new schema's checkpointer publishes at 300 (>= both bases, <= reseed agent's head only after it catches up).
      yield* backend.advance(...makeWriterEvents({ startSeq: 301, count: 20 }))
      yield* runCheckpointer(320, nextSchema)
      const reference = yield* bootLeader({
        makeSqliteDb,
        dbs: yield* makeClientDbs(makeSqliteDb),
        clientId: 'reference',
        syncOptions: sync(),
        leaderSchema: nextSchema,
      })
      yield* waitForUpstreamHead(reference, 320)

      // The reseed agent's backend head (250) is below the new snapshot (320), so it cannot re-seed and backfills.
      // The backfill agent is at 300 < 320 as well. Both must still converge exactly.
      for (const dbs of [reseedDbs, backfillDbs]) {
        const upgradedDbs = yield* withFreshStateDb(makeSqliteDb, dbs)
        const upgraded = yield* bootLeader({
          makeSqliteDb,
          dbs: upgradedDbs,
          clientId: 'upgraded',
          syncOptions: sync({ snapshot: { source: store.makeSource } }),
          leaderSchema: nextSchema,
        })
        yield* waitForUpstreamHead(upgraded, 320)
        expect(stateDigest(upgradedDbs.dbState, nextSchema)).toBe(stateDigest(reference.dbs.dbState, nextSchema))
      }
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test, { timeout: 60_000 })),
  )

  Vitest.live('re-seeds from a new-key snapshot inside the local window', (test) =>
    Effect.gen(function* () {
      const { makeSqliteDb, backend, store, sync, runCheckpointer } = yield* setup
      const nextSchema = makeSchemaWithExtraTable()
      yield* backend.advance(...makeWriterEvents({ count: 100 }))
      yield* runCheckpointer(100)

      const dbs = yield* makeClientDbs(makeSqliteDb)
      const agent = yield* bootLeader({
        makeSqliteDb,
        dbs,
        clientId: 'agent',
        syncOptions: sync({ snapshot: { source: store.makeSource } }),
      })
      yield* backend.advance(...makeWriterEvents({ startSeq: 101, count: 100 }))
      yield* waitForUpstreamHead(agent, 200)
      // A pending local event that must survive the rebuild.
      yield* backend.disconnect
      const local = yield* nextLocalEvent(
        agent,
        encodeArgs('todoCreated', { id: 'pending', text: 'p', author: 'agent' }),
      )
      yield* agent.ctx.syncProcessor.push([local])
      yield* agent.shutdown
      yield* backend.connect

      // New-key snapshot at 200: base 100 <= 200 <= the agent's backend head 200.
      yield* runCheckpointer(200, nextSchema).pipe(Effect.ignore)
      const manifests = [...store.testing.manifests.values()].filter(
        (m) => m.key === Snapshot.SnapshotKey.toString(Snapshot.SnapshotKey.make(nextSchema)),
      )
      expect(manifests).toHaveLength(1)

      const upgradedDbs = yield* withFreshStateDb(makeSqliteDb, dbs)
      const upgraded = yield* bootLeader({
        makeSqliteDb,
        dbs: upgradedDbs,
        clientId: 'agent',
        syncOptions: sync({ snapshot: { source: store.makeSource } }),
        leaderSchema: nextSchema,
      })
      // Re-seeding keeps the base: no backfill happened.
      expect(Eventlog.getEventlogBaseFromDb(dbs.dbEventlog).pipe(Option.map((b) => b.seqNumGlobal))).toEqual(
        Option.some(100),
      )
      yield* waitForNoPending(upgraded)
      yield* waitForUpstreamHead(upgraded, 201)
      const reference = yield* bootLeader({
        makeSqliteDb,
        dbs: yield* makeClientDbs(makeSqliteDb),
        clientId: 'reference',
        syncOptions: sync(),
        leaderSchema: nextSchema,
      })
      yield* waitForUpstreamHead(reference, 201)
      expect(stateDigest(upgradedDbs.dbState, nextSchema)).toBe(stateDigest(reference.dbs.dbState, nextSchema))
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test, { timeout: 60_000 })),
  )
})
