import { expect } from 'vitest'

import { Snapshot, UnknownError } from '@livestore/common'
import { Eventlog } from '@livestore/common/leader-thread'
import { Vitest } from '@livestore/utils-dev/node-vitest'
import { Effect, Option, Result, SubscriptionRef } from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import { eventlogRowCount, makeVersionedSchemas, waitForUpstreamHead, withFreshStateDb } from './harness.ts'
import {
  bootReference,
  type EventKind,
  expectEquivalentState,
  expectEventlogInvariants,
  makeSnapshotWorld,
  type SnapshotWorld,
} from './oracle.ts'

/**
 * Verifier 3, fault injection: every unusable snapshot (missing, corrupt, stale, foreign, slow) must leave the client
 * in exactly the state of a full replay, and checkpointer restarts and the grace period must behave as specified.
 */

const { v1, v2 } = makeVersionedSchemas()

const layer = PlatformNode.NodeFileSystem.layer

/** Boots a snapshot-enabled client and a reference, waits for the head and checks they are equivalent. */
const bootAndCompare = (
  world: SnapshotWorld,
  options?: { snapshotOptions?: Partial<Snapshot.SnapshotBootOptions>; schema?: typeof v1 },
) =>
  Effect.gen(function* () {
    const leaderSchema = options?.schema ?? v1
    const client = yield* world.boot({
      clientId: 'client',
      leaderSchema,
      ...(options?.snapshotOptions !== undefined ? { snapshotOptions: options.snapshotOptions } : {}),
    })
    // Read after boot: some sources below let the backend move on while the client boots.
    const head = yield* world.network.head
    const reference = yield* bootReference({
      makeSqliteDb: world.makeSqliteDb,
      syncOptions: world.sync('reference'),
      head,
      leaderSchema,
    })
    yield* waitForUpstreamHead(client, head)
    expectEquivalentState(client.dbs.dbState, reference.dbs.dbState, leaderSchema)
    expectEventlogInvariants({ dbEventlog: client.dbs.dbEventlog, backendLog: yield* world.network.log, head })
    return { client, reference, head }
  })

const expectFullReplay = (client: { ctx: { snapshotBoot: Option.Option<unknown> }; dbs: any }, head: number) => {
  expect(Option.isNone(client.ctx.snapshotBoot), 'client fell back to full replay').toBe(true)
  expect(Option.isNone(Eventlog.getEventlogBaseFromDb(client.dbs.dbEventlog))).toBe(true)
  expect(eventlogRowCount(client.dbs.dbEventlog)).toBe(head)
}

/** Replaces the published manifest for v1 with `edit(manifest)`, keeping the blob. */
const editManifest = (world: SnapshotWorld, edit: (m: Snapshot.SnapshotManifest) => Snapshot.SnapshotManifest) => {
  const key = Snapshot.SnapshotKey.toString(Snapshot.SnapshotKey.make(v1))
  const manifest = world.store.testing.manifests.get(key)!
  world.store.testing.setManifest(edit(manifest))
}

Vitest.describe('snapshot faults', () => {
  type BlobFault = {
    readonly name: string
    readonly inject: (world: SnapshotWorld, manifest: Snapshot.SnapshotManifest) => Effect.Effect<void>
  }

  const blobFaults: ReadonlyArray<BlobFault> = [
    {
      name: 'truncated blob',
      inject: (world, m) =>
        Effect.sync(() => {
          const blob = world.store.testing.blobs.get(m.objectKey)!
          world.store.testing.blobs.set(m.objectKey, { ...blob, bytes: blob.bytes.slice(0, blob.bytes.length - 100) })
        }),
    },
    {
      name: 'manifest checksum mismatch',
      inject: (world) => Effect.sync(() => editManifest(world, (m) => ({ ...m, sha256: '0'.repeat(64) }))),
    },
    {
      name: 'blob is not a SQLite database (consistent manifest)',
      inject: (world, m) =>
        Effect.sync(() => {
          const bytes = new Uint8Array(4096).fill(7)
          world.store.testing.blobs.set(m.objectKey, { bytes, replacedAt: undefined })
          editManifest(world, (x) => ({ ...x, byteLength: bytes.byteLength, sha256: Snapshot.sha256Hex(bytes) }))
        }),
    },
    {
      name: 'manifest seqNum below the blob state head',
      inject: (world) => Effect.sync(() => editManifest(world, (m) => ({ ...m, seqNum: m.seqNum - 10 }))),
    },
    {
      name: 'manifest seqNum past the blob state head (and past the backend head)',
      inject: (world) => Effect.sync(() => editManifest(world, (m) => ({ ...m, seqNum: m.seqNum + 500 }))),
    },
    {
      name: 'manifest for another store',
      inject: (world) => Effect.sync(() => editManifest(world, (m) => ({ ...m, storeId: 'other-store' }))),
    },
    {
      name: 'source cannot be constructed',
      inject: () => Effect.void,
    },
    {
      name: 'manifest lookup fails',
      inject: () => Effect.void,
    },
  ]

  for (const fault of blobFaults) {
    Vitest.live(`falls back to full replay: ${fault.name}`, (test) =>
      Effect.gen(function* () {
        const world = yield* makeSnapshotWorld()
        yield* world.append(150)
        const manifest = yield* world.publishAtHead(v1)
        yield* world.append(30)
        yield* fault.inject(world, manifest)

        const source: Snapshot.SnapshotSourceConstructor =
          fault.name === 'source cannot be constructed'
            ? () => Effect.fail(new UnknownError({ cause: 'source offline' }))
            : fault.name === 'manifest lookup fails'
              ? (args) =>
                  world.store.makeSource(args).pipe(
                    Effect.map((s) => ({
                      ...s,
                      getManifest: () =>
                        Effect.fail(new Snapshot.SnapshotError({ reason: 'unavailable', message: 'down' })),
                    })),
                  )
              : world.store.makeSource

        const { client, head } = yield* bootAndCompare(world, { snapshotOptions: { source } })
        expectFullReplay(client, head)
      }).pipe(Effect.provide(layer), Vitest.withTestCtx(test, { timeout: 30_000 })),
    )
  }

  Vitest.live('stale manifest whose blob is still retained boots from it and pulls the longer tail', (test) =>
    Effect.gen(function* () {
      let now = 1_000
      const world = yield* makeSnapshotWorld({ graceMs: 10_000, now: () => now })
      yield* world.append(100)
      const old = yield* world.publishAtHead(v1)
      yield* world.append(100)
      yield* world.publishAtHead(v1)
      yield* world.append(20)
      now += 5_000
      // A lagging cache or replica still serves the older pointer.
      world.store.testing.setManifest(old)

      const { client } = yield* bootAndCompare(world)
      expect(Option.map(client.ctx.snapshotBoot, (r) => r.manifest.seqNum)).toEqual(Option.some(100))
      expect(eventlogRowCount(client.dbs.dbEventlog)).toBe(120)
    }).pipe(Effect.provide(layer), Vitest.withTestCtx(test, { timeout: 30_000 })),
  )

  Vitest.live('stale manifest whose blob was pruned after the grace period falls back', (test) =>
    Effect.gen(function* () {
      let now = 1_000
      const world = yield* makeSnapshotWorld({ graceMs: 10_000, now: () => now })
      yield* world.append(100)
      const old = yield* world.publishAtHead(v1)
      yield* world.append(100)
      yield* world.publishAtHead(v1)
      now += 10_001
      world.store.testing.setManifest(old)

      const { client, head } = yield* bootAndCompare(world)
      expectFullReplay(client, head)
    }).pipe(Effect.provide(layer), Vitest.withTestCtx(test, { timeout: 30_000 })),
  )

  Vitest.live('download slower than the timeout falls back without waiting for it', (test) =>
    Effect.gen(function* () {
      const world = yield* makeSnapshotWorld()
      yield* world.append(120)
      yield* world.publishAtHead(v1)
      yield* world.append(10)

      const slowSource: Snapshot.SnapshotSourceConstructor = (args) =>
        world.store
          .makeSource(args)
          .pipe(Effect.map((s) => ({ ...s, download: (m) => s.download(m).pipe(Effect.delay('30 seconds')) })))
      const started = Date.now()
      const { client, head } = yield* bootAndCompare(world, {
        snapshotOptions: { source: slowSource, timeout: '200 millis' },
      })
      expect(Date.now() - started).toBeLessThan(10_000)
      expectFullReplay(client, head)
    }).pipe(Effect.provide(layer), Vitest.withTestCtx(test, { timeout: 30_000 })),
  )

  Vitest.live('manifest lookup slower than the timeout falls back', (test) =>
    Effect.gen(function* () {
      const world = yield* makeSnapshotWorld()
      yield* world.append(60)
      yield* world.publishAtHead(v1)

      const slowSource: Snapshot.SnapshotSourceConstructor = (args) =>
        world.store
          .makeSource(args)
          .pipe(Effect.map((s) => ({ ...s, getManifest: (a) => s.getManifest(a).pipe(Effect.delay('30 seconds')) })))
      const { client, head } = yield* bootAndCompare(world, {
        snapshotOptions: { source: slowSource, timeout: '100 millis' },
      })
      expectFullReplay(client, head)
    }).pipe(Effect.provide(layer), Vitest.withTestCtx(test, { timeout: 30_000 })),
  )

  /**
   * A genuine snapshot whose position the backend does not have (e.g. the backend was restored from an older backup
   * under the same identity, or a misconfigured source serves another deployment's manifests). Installing it would
   * give the client events the backend never confirmed.
   */
  Vitest.live.fails('BUG: a genuine snapshot past the backend head (same backendId) falls back', (test) =>
    Effect.gen(function* () {
      const ahead = yield* makeSnapshotWorld()
      yield* ahead.append(300)
      const manifest = yield* ahead.publishAtHead(v1)

      const world = yield* makeSnapshotWorld()
      yield* world.append(200)
      copySnapshot(ahead, world, manifest)

      const { client, head } = yield* bootAndCompare(world)
      expectFullReplay(client, head)
    }).pipe(Effect.provide(layer), Vitest.withTestCtx(test, { timeout: 30_000 })),
  )

  Vitest.live.fails('BUG: a snapshot from a different backendId falls back', (test) =>
    Effect.gen(function* () {
      const other = yield* makeSnapshotWorld({ backendId: 'backend-0' })
      yield* other.append(150, (): EventKind => 'counter', 'other-writer')
      const manifest = yield* other.publishAtHead(v1)
      expect(manifest.backendId).toBe('backend-0')

      const world = yield* makeSnapshotWorld()
      yield* world.append(200)
      copySnapshot(other, world, manifest)

      const { client, head } = yield* bootAndCompare(world)
      expectFullReplay(client, head)
    }).pipe(Effect.provide(layer), Vitest.withTestCtx(test, { timeout: 30_000 })),
  )

  Vitest.live('a corrupt new-key snapshot during a schema upgrade falls back to backfill', (test) =>
    Effect.gen(function* () {
      const world = yield* makeSnapshotWorld()
      yield* world.append(100)
      yield* world.publishAtHead(v1)
      const agent = yield* world.boot({ clientId: 'agent', leaderSchema: v1 })
      yield* world.append(50)
      yield* waitForUpstreamHead(agent, 150)
      yield* agent.shutdown

      const v2Manifest = yield* world.publishAtHead(v2)
      world.store.testing.corruptBlob(v2Manifest.objectKey)

      const upgraded = yield* world.boot({
        clientId: 'agent',
        dbs: yield* withFreshStateDb(world.makeSqliteDb, agent.dbs),
        leaderSchema: v2,
      })
      const reference = yield* bootReference({
        makeSqliteDb: world.makeSqliteDb,
        syncOptions: world.sync('reference'),
        head: 150,
        leaderSchema: v2,
      })
      yield* waitForUpstreamHead(upgraded, 150)
      expectEquivalentState(upgraded.dbs.dbState, reference.dbs.dbState, v2)
      // Backfill removed the base and completed the eventlog.
      expect(Option.isNone(Eventlog.getEventlogBaseFromDb(upgraded.dbs.dbEventlog))).toBe(true)
      expectEventlogInvariants({ dbEventlog: upgraded.dbs.dbEventlog, backendLog: yield* world.network.log, head: 150 })
    }).pipe(Effect.provide(layer), Vitest.withTestCtx(test, { timeout: 30_000 })),
  )

  Vitest.live('a restarted checkpointer resumes from the latest manifest and never regresses seqNum', (test) =>
    Effect.gen(function* () {
      const world = yield* makeSnapshotWorld()
      const key = Snapshot.SnapshotKey.toString(Snapshot.SnapshotKey.make(v1))
      const publishedSeqNum = () => world.store.testing.manifests.get(key)?.seqNum ?? 0
      yield* world.append(120)

      const first = yield* world.startCheckpointer({ leaderSchema: v1, waitForHead: 120 })
      expect(Option.map(yield* first.checkpointer.publishNow, (m) => m.seqNum)).toEqual(Option.some(120))
      yield* first.leader.shutdown

      yield* world.append(80)
      // Restart on the persisted databases: the trigger state is resumed from the sink.
      const restarted = yield* world.startCheckpointer({ leaderSchema: v1, dbs: first.dbs })
      const resumed = yield* SubscriptionRef.get(restarted.checkpointer.stats)
      expect(Option.map(resumed.latestManifest, (m) => m.seqNum)).toEqual(Option.some(120))
      yield* waitForUpstreamHead(restarted.leader, 200)
      expect(Option.map(yield* restarted.checkpointer.publishNow, (m) => m.seqNum)).toEqual(Option.some(200))
      // Nothing new: no publication.
      expect(Option.isNone(yield* restarted.checkpointer.publishNow)).toBe(true)
      yield* restarted.leader.shutdown

      // A checkpointer that lost its databases starts behind the published snapshot and must not publish over it.
      const amnesiac = yield* world.startCheckpointer({ leaderSchema: v1, clientId: 'amnesiac' })
      for (let i = 0; i < 5; i++) {
        const result = yield* amnesiac.checkpointer.publishNow.pipe(Effect.result)
        if (Result.isSuccess(result) === true && Option.isSome(result.success) === true) {
          expect(result.success.value.seqNum).toBeGreaterThan(200)
        }
        expect(publishedSeqNum()).toBeGreaterThanOrEqual(200)
        yield* Effect.sleep(1)
      }
      yield* waitForUpstreamHead(amnesiac.leader, 200)
      yield* world.append(10)
      yield* waitForUpstreamHead(amnesiac.leader, 210)
      expect(Option.map(yield* amnesiac.checkpointer.publishNow, (m) => m.seqNum)).toEqual(Option.some(210))

      // The sink itself refuses to move a key backwards.
      const regress = yield* world.store.sink
        .publish({
          manifest: {
            manifestVersion: 1,
            storeId: 'snapshot-test-store',
            key,
            backendId: 'backend-1',
            seqNum: 150,
            byteLength: 1,
            sha256: Snapshot.sha256Hex(new Uint8Array(1)),
            createdAt: 0,
          },
          bytes: new Uint8Array(1),
        })
        .pipe(Effect.flip)
      expect(regress.reason).toBe('conflict')
      expect(publishedSeqNum()).toBe(210)
    }).pipe(Effect.provide(layer), Vitest.withTestCtx(test, { timeout: 30_000 })),
  )

  Vitest.live('grace period: a fetched manifest stays downloadable after a newer one, until graceMs', (test) =>
    Effect.gen(function* () {
      let now = 1_000
      const graceMs = 30_000
      const world = yield* makeSnapshotWorld({ graceMs, now: () => now })
      yield* world.append(100)
      const checkpointer = yield* world.startCheckpointer({ leaderSchema: v1, waitForHead: 100 })
      yield* checkpointer.checkpointer.publishNow

      /** A source whose download first lets the world move on: a newer snapshot appears and time passes. */
      const racingSource =
        (elapsedMs: number): Snapshot.SnapshotSourceConstructor =>
        (args) =>
          world.store.makeSource(args).pipe(
            Effect.map((s) => ({
              ...s,
              download: (m: Snapshot.SnapshotManifest) =>
                Effect.gen(function* () {
                  yield* world.append(50).pipe(Effect.orDie)
                  yield* waitForUpstreamHead(checkpointer.leader, yield* world.network.head)
                  const newer = yield* checkpointer.checkpointer.publishNow.pipe(Effect.orDie)
                  expect(Option.map(newer, (n) => n.seqNum > m.seqNum)).toEqual(Option.some(true))
                  now += elapsedMs
                  return yield* s.download(m)
                }),
            })),
          )

      // Within the grace period the old blob is still served: the client installs snapshot 100.
      const within = yield* bootAndCompare(world, { snapshotOptions: { source: racingSource(graceMs - 1) } })
      expect(Option.map(within.client.ctx.snapshotBoot, (r) => r.manifest.seqNum)).toEqual(Option.some(100))
      yield* within.client.shutdown
      yield* within.reference.shutdown

      // After the grace period the old blob is gone: the client falls back to full replay.
      const after = yield* bootAndCompare(world, { snapshotOptions: { source: racingSource(graceMs) } })
      expectFullReplay(after.client, after.head)
    }).pipe(Effect.provide(layer), Vitest.withTestCtx(test, { timeout: 30_000 })),
  )
})

/** Serves `manifest` (and its blob) of `from` through `to`'s snapshot store. */
const copySnapshot = (from: SnapshotWorld, to: SnapshotWorld, manifest: Snapshot.SnapshotManifest) => {
  to.store.testing.blobs.set(manifest.objectKey, from.store.testing.blobs.get(manifest.objectKey)!)
  to.store.testing.setManifest(manifest)
}
