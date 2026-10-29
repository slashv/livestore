import { expect } from 'vitest'

import { Snapshot } from '@livestore/common'
import { nanoid } from '@livestore/livestore'
import { makeHttpSnapshotSource } from '@livestore/sync-cf/client'
import { objectToString } from '@livestore/utils'
import { Vitest } from '@livestore/utils-dev/node-vitest'
import { Effect, Option, Schema } from '@livestore/utils/effect'

import { makeEventFactory, setupProviderRuntime } from './do-idle-helpers.ts'
import * as CloudflareHttpProvider from './providers/cloudflare-http-rpc.ts'
import { isProviderSelected } from './providers/registry.ts'
import { SyncProviderImpl } from './types.ts'

/**
 * Cloudflare realization of the experimental snapshot boot (LS.SYS-R06) on local workerd: manifest storage and
 * validation in the sync DO, the R2 snapshot sink, the worker's snapshot routes and both client sources.
 */

// Selected by provider key, not by suite title, so renaming this suite cannot drop it from CI.
const describeHttpDo = Vitest.describe.skipIf(isProviderSelected('cf-http-do') === false)

const snapshotKey = 'v1.test-state-hash.test-event-defs-hash'

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))

describeHttpDo(`${CloudflareHttpProvider.doSqlite.name} — snapshot manifests`, { timeout: 60_000 }, () => {
  const getContext = setupProviderRuntime(CloudflareHttpProvider.doSqlite.layer)

  Vitest.live('publishes a snapshot and serves it via the HTTP routes and the DO-RPC source', () =>
    Effect.gen(function* () {
      const { storeId, baseUrl, backendId, head } = yield* setupStore({ eventCount: 5 })
      expect(head).toBe(5)

      const bytes = randomBytes(64 * 1024)
      const published = yield* publish({
        baseUrl,
        storeId,
        manifest: makeManifest({ storeId, backendId, seqNum: 3, bytes }),
        bytes,
      })
      expect(published._tag).toBe('ok')
      if (published._tag !== 'ok') return
      expect(
        published.manifest.objectKey.startsWith(`snapshots/${encodeURIComponent(storeId)}/${snapshotKey}/3-`),
      ).toBe(true)

      const source = yield* makeHttpSnapshotSource({ url: baseUrl })({
        storeId,
        clientId: 'snapshot-test',
        payload: undefined,
      })

      const manifest = yield* source.getManifest({ key: snapshotKey })
      expect(Option.getOrUndefined(manifest)).toEqual(published.manifest)

      const downloaded = yield* source.download(published.manifest)
      expect(downloaded.byteLength).toBe(bytes.byteLength)
      expect(Snapshot.sha256Hex(downloaded)).toBe(published.manifest.sha256)

      // Unknown keys have no manifest.
      expect(Option.isNone(yield* source.getManifest({ key: 'v1.unknown.unknown' }))).toBe(true)

      // Clients inside Durable Objects read the manifest via RPC and the blob straight from R2.
      const viaDoRpc = yield* getJson<{ manifest: Snapshot.SnapshotManifest | null; sha256: string | null }>(
        `${baseUrl}/snapshot-test/do-rpc-source?storeId=${storeId}&key=${encodeURIComponent(snapshotKey)}`,
      )
      expect(viaDoRpc).toEqual({ manifest: published.manifest, sha256: published.manifest.sha256 })
    }).pipe(Effect.provide(getContext())),
  )

  Vitest.live('rejects manifests from another backend, ahead of the head, or older than the current one', () =>
    Effect.gen(function* () {
      const { storeId, baseUrl, backendId } = yield* setupStore({ eventCount: 5 })
      const bytes = randomBytes(1024)
      const valid = { ...makeManifest({ storeId, backendId, seqNum: 4, bytes }), objectKey: `snapshots/${storeId}/x` }

      const putManifest = (manifest: Snapshot.SnapshotManifest) =>
        postJson<{ _tag: string; reason?: string }>(
          `${baseUrl}/snapshot-test/put-manifest?storeId=${storeId}`,
          manifest,
        ).pipe(Effect.map(({ body }) => body))

      expect(yield* putManifest({ ...valid, backendId: 'other-backend' })).toMatchObject({
        _tag: 'Rejected',
        reason: 'incompatible',
      })
      expect(yield* putManifest({ ...valid, storeId: 'other-store' })).toMatchObject({
        _tag: 'Rejected',
        reason: 'incompatible',
      })
      expect(yield* putManifest({ ...valid, seqNum: 6 })).toMatchObject({ _tag: 'Rejected', reason: 'incompatible' })

      // A cold sync DO must not adopt the store id of the first manifest it sees.
      const coldStoreId = `snapshot-cold-${nanoid()}`
      const cold = yield* postJson<{ _tag: string; reason?: string }>(
        `${baseUrl}/snapshot-test/put-manifest?storeId=${coldStoreId}`,
        { ...valid, storeId: coldStoreId },
      )
      expect(cold.body).toMatchObject({ _tag: 'Rejected', reason: 'incompatible' })
      expect(yield* putManifest(valid)).toEqual({ _tag: 'Accepted', previousObjectKey: null })
      // Same seqNum again is allowed (e.g. a restarted checkpointer republishing), older is not.
      expect(yield* putManifest(valid)).toEqual({ _tag: 'Accepted', previousObjectKey: valid.objectKey })
      expect(yield* putManifest({ ...valid, seqNum: 2 })).toMatchObject({ _tag: 'Rejected', reason: 'conflict' })

      // Through the sink, a rejected manifest's blob is deleted right away.
      const blobsBefore = yield* listBlobs({ baseUrl, storeId })
      const rejected = yield* publish({
        baseUrl,
        storeId,
        manifest: makeManifest({ storeId, backendId, seqNum: 2, bytes }),
        bytes,
      })
      expect(rejected).toMatchObject({ _tag: 'error', reason: 'conflict' })
      expect(yield* listBlobs({ baseUrl, storeId })).toEqual(blobsBefore)

      // R2 verifies the uploaded bytes against the manifest digest.
      const corrupt = yield* publish({
        baseUrl,
        storeId,
        manifest: { ...makeManifest({ storeId, backendId, seqNum: 5, bytes }), sha256: '0'.repeat(64) },
        bytes,
      })
      expect(corrupt).toMatchObject({ _tag: 'error', reason: 'unavailable' })
      expect(corrupt._tag === 'error' ? corrupt.message : '').toContain('Failed to upload snapshot blob')
      expect(yield* listBlobs({ baseUrl, storeId })).toEqual(blobsBefore)
    }).pipe(Effect.provide(getContext())),
  )

  Vitest.live('applies validatePayload and the store prefix to the snapshot routes', () =>
    Effect.gen(function* () {
      const { storeId, baseUrl, backendId } = yield* setupStore({ eventCount: 2 })
      const bytes = randomBytes(1024)
      const published = yield* publish({
        baseUrl,
        storeId,
        manifest: makeManifest({ storeId, backendId, seqNum: 2, bytes }),
        bytes,
      })
      if (published._tag !== 'ok') return expect.unreachable('publish failed')

      const deniedSource = yield* makeHttpSnapshotSource({ url: baseUrl })({
        storeId,
        clientId: 'snapshot-test',
        payload: { deny: true },
      })
      const denied = yield* deniedSource.getManifest({ key: snapshotKey }).pipe(Effect.flip)
      expect(denied.message).toContain('status 400')
      const deniedBlob = yield* deniedSource.download(published.manifest).pipe(Effect.flip)
      expect(deniedBlob.message).toContain('status 400')

      const blobStatus = (requestStoreId: string, objectKey: string) =>
        Effect.promise(() =>
          fetch(
            `${baseUrl}/snapshot/blob?storeId=${encodeURIComponent(requestStoreId)}&objectKey=${encodeURIComponent(objectKey)}`,
          ).then(async (res) => {
            await res.arrayBuffer()
            return res.status
          }),
        )

      // A payload valid for this store must not read another store's blobs, including a store whose id extends this
      // one (`<storeId>/victim` used to share the `snapshots/<storeId>/` prefix).
      const victimStoreId = `${storeId}/victim`
      const victim = yield* setupStore({ eventCount: 2, storeId: victimStoreId })
      const victimPublished = yield* publish({
        baseUrl,
        storeId: victimStoreId,
        manifest: makeManifest({ storeId: victimStoreId, backendId: victim.backendId, seqNum: 2, bytes }),
        bytes,
      })
      if (victimPublished._tag !== 'ok') return expect.unreachable('victim publish failed')
      expect(yield* blobStatus(storeId, victimPublished.manifest.objectKey)).toBe(403)
      expect(yield* blobStatus(victimStoreId, victimPublished.manifest.objectKey)).toBe(200)

      // Only blobs a manifest points at are served, not arbitrary objects under the store's prefix.
      const orphan = yield* getJson<{ objectKey: string }>(`${baseUrl}/snapshot-test/put-orphan?storeId=${storeId}`)
      expect(yield* blobStatus(storeId, orphan.objectKey)).toBe(404)

      // The blob route sets content-length from R2.
      const blob = yield* Effect.promise(() =>
        fetch(
          `${baseUrl}/snapshot/blob?storeId=${storeId}&objectKey=${encodeURIComponent(published.manifest.objectKey)}`,
        ),
      )
      expect(blob.status).toBe(200)
      expect(blob.headers.get('content-length')).toBe(String(bytes.byteLength))
      yield* Effect.promise(() => blob.arrayBuffer())
    }).pipe(Effect.provide(getContext())),
  )

  Vitest.live('never serves manifests recorded under a previous backend identity', () =>
    Effect.gen(function* () {
      const { storeId, baseUrl, backendId } = yield* setupStore({ eventCount: 3 })
      const bytes = randomBytes(1024)
      yield* publish({ baseUrl, storeId, manifest: makeManifest({ storeId, backendId, seqNum: 3, bytes }), bytes })

      const source = yield* makeHttpSnapshotSource({ url: baseUrl })({
        storeId,
        clientId: 'snapshot-test',
        payload: undefined,
      })
      expect(Option.isSome(yield* source.getManifest({ key: snapshotKey }))).toBe(true)

      const { rowsWritten } = yield* getJson<{ rowsWritten: number }>(
        `${baseUrl}/snapshot-test/mark-stale?storeId=${storeId}`,
      )
      expect(rowsWritten).toBe(1)
      expect(Option.isNone(yield* source.getManifest({ key: snapshotKey }))).toBe(true)

      // A stale row neither blocks an older seqNum nor counts as `previous`.
      const republished = yield* publish({
        baseUrl,
        storeId,
        manifest: makeManifest({ storeId, backendId, seqNum: 1, bytes }),
        bytes,
      })
      expect(republished._tag).toBe('ok')
      expect(Option.getOrUndefined(yield* source.getManifest({ key: snapshotKey }))?.seqNum).toBe(1)
    }).pipe(Effect.provide(getContext())),
  )

  Vitest.live('keeps the current and previous blobs and prunes blobs replaced longer than the grace period ago', () =>
    Effect.gen(function* () {
      const { storeId, baseUrl, backendId } = yield* setupStore({ eventCount: 4 })
      const objectKeys: string[] = []

      for (const [seqNum, graceMs] of [
        [1, 60_000],
        [2, 60_000],
        [3, 60_000],
      ] as const) {
        const bytes = randomBytes(1024)
        const published = yield* publish({
          baseUrl,
          storeId,
          manifest: makeManifest({ storeId, backendId, seqNum, bytes }),
          bytes,
          graceMs,
        })
        if (published._tag !== 'ok') return expect.unreachable(`publish ${seqNum} failed`)
        objectKeys.push(published.manifest.objectKey)
      }

      // Within the grace period every replaced blob stays stored, but only the current and previous one are served.
      expect(yield* listBlobs({ baseUrl, storeId })).toEqual(objectKeys.toSorted())
      const servedStatus = (objectKey: string) =>
        Effect.promise(() =>
          fetch(`${baseUrl}/snapshot/blob?storeId=${storeId}&objectKey=${encodeURIComponent(objectKey)}`).then(
            async (res) => {
              await res.arrayBuffer()
              return res.status
            },
          ),
        )
      expect([
        yield* servedStatus(objectKeys[0]!),
        yield* servedStatus(objectKeys[1]!),
        yield* servedStatus(objectKeys[2]!),
      ]).toEqual([404, 200, 200])

      const bytes = randomBytes(1024)
      const latest = yield* publish({
        baseUrl,
        storeId,
        manifest: makeManifest({ storeId, backendId, seqNum: 4, bytes }),
        bytes,
        graceMs: 0,
      })
      if (latest._tag !== 'ok') return expect.unreachable('publish 4 failed')

      // With the grace period elapsed only the current and the previous manifest's blobs remain.
      expect(yield* listBlobs({ baseUrl, storeId })).toEqual([objectKeys[2]!, latest.manifest.objectKey].toSorted())
    }).pipe(Effect.provide(getContext())),
  )
})

/** Creates a fresh store, pushes `eventCount` events over HTTP and reads the sync DO's backend identity and head. */
const setupStore = ({ eventCount, storeId = `snapshot-${nanoid()}` }: { eventCount: number; storeId?: string }) =>
  Effect.gen(function* () {
    const { makeProvider } = yield* SyncProviderImpl
    const syncBackend = yield* makeProvider({ storeId, clientId: 'snapshot-test', payload: undefined })
    const url = syncBackend.metadata.url
    const baseUrl = typeof url === 'string' ? url : objectToString(url)

    const factory = makeEventFactory({
      client: { clientId: 'snapshot-test', sessionId: 'snapshot-test-session' },
      startSeq: 1,
      initialParent: 'root',
    })
    yield* syncBackend.push(
      Array.from({ length: eventCount }, (_, i) =>
        factory.todoCreated.next({ id: `todo-${i}`, text: `Todo ${i}`, completed: false }),
      ),
    )

    const { backendId, head } = yield* getJson<{ backendId: string; head: number }>(
      `${baseUrl}/snapshot-test/context?storeId=${encodeURIComponent(storeId)}`,
    )
    return { storeId, baseUrl, backendId, head }
  })

const makeManifest = ({
  storeId,
  backendId,
  seqNum,
  bytes,
}: {
  storeId: string
  backendId: string
  seqNum: number
  bytes: Uint8Array
}): Omit<Snapshot.SnapshotManifest, 'objectKey'> => ({
  manifestVersion: 1,
  storeId,
  key: snapshotKey,
  backendId,
  seqNum,
  byteLength: bytes.byteLength,
  sha256: Snapshot.sha256Hex(bytes),
  createdAt: Date.now(),
})

/** Publishes through the R2 snapshot sink running in the test worker. */
const publish = ({
  baseUrl,
  storeId,
  manifest,
  bytes,
  graceMs,
}: {
  baseUrl: string
  storeId: string
  manifest: Omit<Snapshot.SnapshotManifest, 'objectKey'>
  bytes: Uint8Array
  graceMs?: number
}) =>
  postJson<Snapshot.SnapshotManifest | { reason: string; message: string }>(
    `${baseUrl}/snapshot-test/publish?storeId=${encodeURIComponent(storeId)}${graceMs !== undefined ? `&graceMs=${graceMs}` : ''}`,
    { manifest, bytesBase64: Buffer.from(bytes).toString('base64') },
  ).pipe(
    Effect.map(({ status, body }) =>
      status === 200
        ? { _tag: 'ok' as const, manifest: body as Snapshot.SnapshotManifest }
        : { _tag: 'error' as const, ...(body as { reason: string; message: string }) },
    ),
  )

const listBlobs = ({ baseUrl, storeId }: { baseUrl: string; storeId: string }) =>
  getJson<string[]>(`${baseUrl}/snapshot-test/blobs?storeId=${storeId}`)

const getJson = <T>(url: string) => Effect.promise(() => fetch(url).then((res) => res.json() as Promise<T>))

const postJson = <T>(url: string, body: unknown) =>
  Effect.promise(async () => {
    const res = await fetch(url, { method: 'POST', body: encodeJson(body) })
    return { status: res.status, body: (await res.json()) as T }
  })

const randomBytes = (length: number) => {
  const bytes = new Uint8Array(length)
  // getRandomValues caps at 65536 bytes per call.
  for (let offset = 0; offset < length; offset += 65_536) {
    crypto.getRandomValues(bytes.subarray(offset, Math.min(offset + 65_536, length)))
  }
  return bytes
}
