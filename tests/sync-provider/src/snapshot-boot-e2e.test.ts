import { expect } from 'vitest'

import type { CheckpointerDoStats } from '@livestore/adapter-cloudflare'
import { type MakeSqliteDb, Snapshot } from '@livestore/common'
import { nanoid } from '@livestore/livestore'
import { schema } from '@livestore/livestore/internal/testing-utils'
import { makeHttpSnapshotSource, makeHttpSync } from '@livestore/sync-cf/client'
import { objectToString } from '@livestore/utils'
import { Vitest } from '@livestore/utils-dev/node-vitest'
import { type Duration, Effect, Option, Queue, Schedule, Schema } from '@livestore/utils/effect'

import { makeEventFactory, setupProviderRuntime, SyncDoProbeError } from './do-idle-helpers.ts'
import * as CloudflareHttpProvider from './providers/cloudflare-http-rpc.ts'
import { isProviderSelected } from './providers/registry.ts'
import {
  bootLeader,
  eventlogRowCount,
  makeSqlite,
  stateContent,
  stateDigest,
  waitForUpstreamHead,
} from './snapshot-leader-harness.ts'
import { SyncProviderImpl } from './types.ts'

/**
 * End-to-end snapshot boot on local workerd (experimental, LS.SYS-R06): a writer pushes to the sync DO, which wakes
 * the checkpointer DO; the checkpointer follows the log via DO-RPC live pull and publishes snapshots to R2; fresh Node
 * clients boot from the snapshot through the worker's HTTP routes and pull only the tail.
 *
 * The test worker's `CheckpointerDO` publishes once 100 events accumulated (see `test-worker.ts`).
 */

// Selected by provider key, not by suite title, so renaming this suite cannot drop it from CI.
const describeHttpDo = Vitest.describe.skipIf(isProviderSelected('cf-http-do') === false)

describeHttpDo(`${CloudflareHttpProvider.doSqlite.name} — snapshot boot end to end`, { timeout: 120_000 }, () => {
  const getContext = setupProviderRuntime(CloudflareHttpProvider.doSqlite.layer)

  Vitest.live('a fresh client boots from the checkpointer snapshot and pulls only the tail', () =>
    Effect.gen(function* () {
      const { syncUrl, baseUrl } = yield* urls
      const storeId = `snapshot-e2e-${nanoid()}`
      const writer = yield* makeWriter({ syncUrl, storeId })

      // The sync DO wakes the checkpointer on push; it publishes on its own once 100 events accumulated.
      yield* writer.push(150)
      const auto = yield* awaitStats(
        { baseUrl, storeId },
        (stats) => stats.latestManifest !== null && stats.head === 150,
      )
      expect(auto.publishedCount).toBeGreaterThanOrEqual(1)

      const flushed = yield* getJson<Snapshot.SnapshotManifest | null>(
        `${baseUrl}/snapshot-e2e/checkpointer/publish-now?storeId=${storeId}`,
      )
      const manifest = flushed ?? auto.latestManifest!
      expect(manifest.seqNum).toBe(150)

      // The test checkpointer streams its export (multipart upload from staged pages): the blob must be a complete
      // rollback-journal SQLite file of exactly `page count × page size` bytes that passes an integrity check.
      const makeSqliteDb = yield* makeSqlite
      const blob = yield* (yield* makeHttpSnapshotSource({ url: syncUrl })({
        storeId,
        clientId: 'blob-check',
        payload: undefined,
      })).download(manifest)
      const header = new DataView(blob.buffer, blob.byteOffset, 100)
      expect([blob[18], blob[19]]).toEqual([1, 1])
      expect(blob.byteLength).toBe(header.getUint16(16) * header.getUint32(28))
      expect(Snapshot.sha256Hex(blob)).toBe(manifest.sha256)
      const imported = yield* makeSqliteDb({ _tag: 'in-memory' })
      imported.import(blob)
      expect(imported.select<{ integrity_check: string }>('PRAGMA integrity_check')).toEqual([
        { integrity_check: 'ok' },
      ])

      yield* writer.push(20)

      const snapshotClient = yield* bootClient({ makeSqliteDb, syncUrl, storeId, clientId: 'snapshot', snapshot: true })
      const replayClient = yield* bootClient({ makeSqliteDb, syncUrl, storeId, clientId: 'replay', snapshot: false })
      yield* waitForUpstreamHead(snapshotClient, 170)
      yield* waitForUpstreamHead(replayClient, 170)

      const installed = Option.getOrUndefined(snapshotClient.ctx.snapshotBoot)
      expect(installed?.manifest.seqNum).toBe(150)
      expect(installed?.manifest.backendId).toBe(manifest.backendId)
      expect(Option.isNone(replayClient.ctx.snapshotBoot)).toBe(true)

      expect(eventlogRowCount(snapshotClient.dbEventlog)).toBe(20)
      expect(eventlogRowCount(replayClient.dbEventlog)).toBe(170)
      expect(stateContent(snapshotClient.dbState, schema)).toEqual(stateContent(replayClient.dbState, schema))
      expect(stateDigest(snapshotClient.dbState, schema)).toBe(stateDigest(replayClient.dbState, schema))
    }).pipe(Effect.provide(getContext())),
  )

  Vitest.live('a backend reset stops serving the snapshot, resets booted clients and restarts the checkpointer', () =>
    Effect.gen(function* () {
      const { syncUrl, baseUrl } = yield* urls
      const storeId = `snapshot-e2e-reset-${nanoid()}`
      const writer = yield* makeWriter({ syncUrl, storeId })

      yield* writer.push(120)
      yield* awaitStats({ baseUrl, storeId }, (stats) => stats.head === 120)
      const before = yield* getJson<Snapshot.SnapshotManifest | null>(
        `${baseUrl}/snapshot-e2e/checkpointer/publish-now?storeId=${storeId}`,
      )
      const oldManifest =
        before ?? (yield* awaitStats({ baseUrl, storeId }, (stats) => stats.latestManifest !== null)).latestManifest!

      const makeSqliteDb = yield* makeSqlite
      const booted = yield* bootClient({ makeSqliteDb, syncUrl, storeId, clientId: 'booted', snapshot: true })
      yield* waitForUpstreamHead(booted, 120)
      expect(Option.isSome(booted.ctx.snapshotBoot)).toBe(true)

      yield* Effect.promise(() => fetch(`${baseUrl}/snapshot-e2e/reset-backend?storeId=${storeId}`))

      // The reset backend has a new identity and no manifests, so the old snapshot is never served.
      const source = yield* makeHttpSnapshotSource({ url: syncUrl })({ storeId, clientId: 'probe', payload: undefined })
      const afterReset = yield* source.getManifest({ key: oldManifest.key })
      expect(Option.isNone(afterReset)).toBe(true)

      // The booted client's next live pull hits the identity mismatch: it clears its storage and shuts down.
      const shutdownMessage = yield* Queue.take(booted.shutdownMessages).pipe(Effect.timeout('20 seconds'))
      expect(shutdownMessage._tag).toMatch(/BackendIdMismatch|IntentionalShutdown/)

      // New history on the reset backend: the push wake tells the checkpointer about the new identity, it starts over
      // and publishes a snapshot of the new backend.
      const newWriter = yield* makeWriter({ syncUrl, storeId, clientId: 'writer-after-reset' })
      yield* newWriter.push(110)
      const restarted = yield* awaitStats(
        { baseUrl, storeId },
        (stats) =>
          stats.head === 110 &&
          stats.latestManifest !== null &&
          stats.latestManifest.backendId !== oldManifest.backendId,
      )
      expect(restarted.latestManifest!.seqNum).toBeLessThanOrEqual(110)

      const fresh = yield* bootClient({ makeSqliteDb, syncUrl, storeId, clientId: 'fresh', snapshot: true })
      yield* waitForUpstreamHead(fresh, 110)
      expect(Option.getOrUndefined(fresh.ctx.snapshotBoot)?.manifest.backendId).toBe(
        restarted.latestManifest!.backendId,
      )
      const replay = yield* bootClient({ makeSqliteDb, syncUrl, storeId, clientId: 'replay', snapshot: false })
      yield* waitForUpstreamHead(replay, 110)
      expect(stateDigest(fresh.dbState, schema)).toBe(stateDigest(replay.dbState, schema))
    }).pipe(Effect.provide(getContext())),
  )
})

/** Sync URL routed to the checkpointer-enabled sync DO of the test worker. */
const urls = Effect.gen(function* () {
  const { makeProvider } = yield* SyncProviderImpl
  const probe = yield* makeProvider({ storeId: `url-probe-${nanoid()}`, clientId: 'probe', payload: undefined })
  const url = probe.metadata.url
  const baseUrl = typeof url === 'string' ? url : objectToString(url)
  return { baseUrl, syncUrl: `${baseUrl}/snapshot-e2e/sync` }
})

const makeWriter = ({
  syncUrl,
  storeId,
  clientId = 'writer',
}: {
  syncUrl: string
  storeId: string
  clientId?: string
}) =>
  Effect.gen(function* () {
    const backend = yield* makeHttpSync({ url: syncUrl, ping: { enabled: false } })({
      storeId,
      clientId,
      payload: undefined,
    })
    const factory = makeEventFactory({
      client: { clientId, sessionId: `${clientId}-session` },
      startSeq: 1,
      initialParent: 'root',
    })
    let next = 0
    const push = (count: number) =>
      Effect.gen(function* () {
        const events = Array.from({ length: count }, () => {
          const n = next++
          return factory.todoCreated.next({ id: `${clientId}-${n}`, text: `todo ${n}`, completed: n % 3 === 0 })
        })
        for (let i = 0; i < events.length; i += 50) yield* backend.push(events.slice(i, i + 50))
      })
    return { push }
  })

const bootClient = ({
  makeSqliteDb,
  syncUrl,
  storeId,
  clientId,
  snapshot,
}: {
  makeSqliteDb: MakeSqliteDb
  syncUrl: string
  storeId: string
  clientId: string
  snapshot: boolean
}) =>
  bootLeader({
    makeSqliteDb,
    schema,
    storeId,
    clientId,
    syncOptions: {
      backend: makeHttpSync({ url: syncUrl, livePull: { pollInterval: 200 }, ping: { enabled: false } }),
      livePull: true,
      ...(snapshot === true ? { snapshot: { source: makeHttpSnapshotSource({ url: syncUrl }) } } : {}),
    },
  })

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))

/** Polls the checkpointer's stats until `predicate` holds. */
const awaitStats = (
  { baseUrl, storeId }: { baseUrl: string; storeId: string },
  predicate: (stats: CheckpointerDoStats) => boolean,
  timeout: Duration.Input = '60 seconds',
) =>
  getJson<CheckpointerDoStats>(`${baseUrl}/snapshot-e2e/checkpointer/stats?storeId=${storeId}`).pipe(
    Effect.filterOrFail(predicate, (stats) => new SyncDoProbeError({ message: encodeJson(stats) })),
    Effect.retry(Schedule.spaced('250 millis')),
    Effect.timeout(timeout),
    Effect.orDie,
  )

const getJson = <T>(url: string) =>
  Effect.promise(async () => {
    const res = await fetch(url)
    if (res.ok === false) throw new Error(`${url} failed with ${res.status}: ${await res.text()}`)
    return (await res.json()) as T
  })
