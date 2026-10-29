import { Snapshot, type SqliteDb } from '@livestore/common'
import type { CfTypes } from '@livestore/common-cf'
import { Effect, Exit } from '@livestore/utils/effect'
import { nanoid } from '@livestore/utils/nanoid'

import { getManifestViaRpc, type SnapshotManifestRpcStub, trySnapshotPromise } from '../common/snapshot-rpc.ts'
import { encodeSnapshotManifestJson, SNAPSHOT_BLOB_CONTENT_TYPE, snapshotKeyObjectPrefix } from '../common/snapshot.ts'

export type { SnapshotManifestRpcStub } from '../common/snapshot-rpc.ts'

export interface R2SnapshotSinkOptions {
  /** R2 bucket holding snapshot blobs. Must be the bucket the worker's snapshot routes serve from. */
  bucket: CfTypes.R2Bucket
  storeId: string
  /** Returns a stub of the store's sync Durable Object. Called per request, since CF stubs can break after errors. */
  getSyncBackendStub: () => SnapshotManifestRpcStub
  /**
   * How long a replaced blob stays readable, so clients that fetched the previous manifest can finish downloading.
   * The blob of the previous manifest is always kept, regardless of this value.
   *
   * @default 5 minutes
   */
  graceMs?: number
  /** Clock used for grace-period pruning. Injectable for tests. */
  now?: () => number
  /**
   * Enables streaming publication for state too large to serialize in memory (e.g. `stageVfsFile` from
   * `@livestore/sqlite-wasm/cf`). Called synchronously under the leader's persistence lock; it must take a consistent
   * copy that outlives the lock. The sink then streams it to R2 as a multipart upload, hashing as it goes, so memory
   * stays around one part regardless of the state size.
   */
  stage?: (dbState: SqliteDb) => SnapshotBlobSource
  /**
   * Multipart part size for streamed publication. R2 requires equal parts of at least 5 MiB (except the last), and it
   * must be a multiple of the staged copy's `pageSize`.
   *
   * @default 5 MiB
   */
  partSizeBytes?: number
}

/** A staged, consistent copy of a snapshot blob that can be read in ranges. */
export interface SnapshotBlobSource {
  readonly byteLength: number
  /** Reads must start at multiples of this; the part size has to be a multiple of it. */
  readonly pageSize: number
  /** Reads `length` bytes at `offset` into a new buffer. Offsets are multiples of the part size. */
  readonly read: (offset: number, length: number) => Uint8Array<ArrayBuffer>
  /** Drops the staged copy. Called exactly once, after publication or discard. */
  readonly release: () => void
}

/**
 * Snapshot sink for a checkpointer running on Cloudflare (experimental snapshot boot, LS.SYS-R06).
 *
 * `publish` follows the "blob first, pointer second" contract of {@link Snapshot.SnapshotSink}:
 *
 * 1. Uploads the blob to `snapshots/<storeId>/<key>/<seqNum>-<id>.sqlite` (R2 verifies its SHA-256).
 * 2. Publishes the manifest through the sync Durable Object, which validates it against the backend identity and head.
 *    A rejected manifest's blob is deleted right away.
 * 3. Prunes blobs of the same key that were replaced more than `graceMs` ago. The current and previous manifests' blobs
 *    are always kept. Pruning failures are logged and never fail the publish.
 *
 * Blobs of other snapshot keys (e.g. an older schema) are never touched; use an R2 lifecycle rule for those.
 *
 * With `stage`, the sink also implements {@link Snapshot.SnapshotSink.capture}: the checkpointer then never serializes
 * the state database, and step 1 becomes a multipart upload streamed from the staged copy (sha256 computed on the fly
 * with `crypto.DigestStream`).
 *
 * R2 does not verify a SHA-256 for multipart uploads (unlike the single `put` path), so streamed blobs rely on the
 * manifest's digest alone: booting clients verify it and fall back to full replay on a mismatch.
 *
 * @experimental
 */
export const makeR2SnapshotSink = ({
  bucket,
  storeId,
  getSyncBackendStub,
  graceMs = 5 * 60_000,
  now = () => Date.now(),
  stage,
  partSizeBytes = 5 * 1024 * 1024,
}: R2SnapshotSinkOptions): Snapshot.SnapshotSink => {
  if (stage !== undefined && (Number.isInteger(partSizeBytes) === false || partSizeBytes < R2_MIN_PART_BYTES)) {
    throw new Error(`partSizeBytes must be an integer of at least ${R2_MIN_PART_BYTES} bytes, got ${partSizeBytes}`)
  }

  const prune = Effect.fn('@livestore/sync-cf:r2-snapshot-sink:prune')(function* ({
    key,
    keep,
  }: {
    key: string
    keep: ReadonlySet<string>
  }) {
    const objects = yield* trySnapshotPromise('unavailable', 'Failed to list snapshot blobs', () =>
      listAllObjects(bucket, snapshotKeyObjectPrefix({ storeId, key })),
    )
    const toDelete = selectReplacedObjects({ objects, keep, cutoff: now() - graceMs })

    for (let i = 0; i < toDelete.length; i += R2_MAX_DELETE_KEYS) {
      const chunk = toDelete.slice(i, i + R2_MAX_DELETE_KEYS)
      yield* trySnapshotPromise('unavailable', 'Failed to delete replaced snapshot blobs', () => bucket.delete(chunk))
    }

    return toDelete.length
  })

  const publish: Snapshot.SnapshotSink['publish'] = Effect.fn('@livestore/sync-cf:r2-snapshot-sink:publish')(
    function* ({ manifest, bytes }) {
      if (manifest.storeId !== storeId) {
        return yield* new Snapshot.SnapshotError({
          reason: 'incompatible',
          message: `Manifest storeId ${manifest.storeId} does not match sink storeId ${storeId}`,
        })
      }

      const objectKey = makeObjectKey(manifest)
      const published: Snapshot.SnapshotManifest = { ...manifest, objectKey }

      yield* trySnapshotPromise('unavailable', `Failed to upload snapshot blob ${objectKey}`, () =>
        bucket.put(objectKey, bytes, {
          // R2 rejects the upload if the received bytes don't hash to the manifest's digest.
          sha256: manifest.sha256,
          httpMetadata: { contentType: SNAPSHOT_BLOB_CONTENT_TYPE },
          customMetadata: customMetadataFor(manifest),
        }),
      )

      return yield* movePointer(published)
    },
  )

  /** Second half of every publish: move the pointer to the uploaded blob, then prune replaced blobs. */
  const movePointer = Effect.fn('@livestore/sync-cf:r2-snapshot-sink:movePointer')(function* (
    published: Snapshot.SnapshotManifest,
  ) {
    const result = yield* trySnapshotPromise('unavailable', 'Failed to publish snapshot manifest', () =>
      getSyncBackendStub().putSnapshotManifest(encodeSnapshotManifestJson(published)),
    )

    if (result._tag === 'Rejected') {
      // The pointer never moved to this blob, so nobody can be reading it.
      yield* trySnapshotPromise('unavailable', 'Failed to delete rejected snapshot blob', () =>
        bucket.delete(published.objectKey),
      ).pipe(Effect.ignore({ log: 'Warn' }))
      return yield* new Snapshot.SnapshotError({ reason: result.reason, message: result.message })
    }

    const keep = new Set([
      published.objectKey,
      ...(result.previousObjectKey === null ? [] : [result.previousObjectKey]),
    ])
    yield* prune({ key: published.key, keep }).pipe(Effect.ignore({ log: 'Warn' }))

    return published
  })

  /** Streams a staged copy to R2 as a multipart upload, hashing incrementally, then moves the pointer. */
  const publishStaged = Effect.fn('@livestore/sync-cf:r2-snapshot-sink:publishStaged')(function* (
    source: SnapshotBlobSource,
    draft: Snapshot.SnapshotManifestDraft,
  ) {
    if (draft.storeId !== storeId) {
      return yield* new Snapshot.SnapshotError({
        reason: 'incompatible',
        message: `Manifest storeId ${draft.storeId} does not match sink storeId ${storeId}`,
      })
    }
    if (source.byteLength === 0) {
      return yield* new Snapshot.SnapshotError({ reason: 'internal', message: 'Staged snapshot is empty' })
    }
    if (partSizeBytes % source.pageSize !== 0) {
      return yield* new Snapshot.SnapshotError({
        reason: 'internal',
        message: `partSizeBytes ${partSizeBytes} is not a multiple of the staged page size ${source.pageSize}`,
      })
    }

    const objectKey = makeObjectKey(draft)
    const upload = yield* trySnapshotPromise('unavailable', `Failed to start snapshot upload ${objectKey}`, () =>
      bucket.createMultipartUpload(objectKey, {
        httpMetadata: { contentType: SNAPSHOT_BLOB_CONTENT_TYPE },
        customMetadata: customMetadataFor(draft),
      }),
    )

    const sha256 = yield* Effect.gen(function* () {
      const digest = new (cfCrypto().DigestStream)('SHA-256')
      const hasher = digest.getWriter()
      const parts: CfTypes.R2UploadedPart[] = []
      for (let offset = 0, partNumber = 1; offset < source.byteLength; offset += partSizeBytes, partNumber++) {
        // One part in memory at a time: read, hash, upload, drop.
        const part = yield* Effect.try({
          try: () => source.read(offset, Math.min(partSizeBytes, source.byteLength - offset)),
          catch: (cause) =>
            new Snapshot.SnapshotError({ reason: 'internal', message: 'Failed to read staged snapshot', cause }),
        })
        yield* trySnapshotPromise('internal', 'Failed to hash snapshot part', () => hasher.write(part))
        parts.push(
          yield* trySnapshotPromise('unavailable', `Failed to upload snapshot part ${partNumber}`, () =>
            upload.uploadPart(partNumber, part),
          ),
        )
      }
      yield* trySnapshotPromise('internal', 'Failed to finish snapshot hash', () => hasher.close())
      const hash = yield* trySnapshotPromise('internal', 'Failed to finish snapshot hash', () => digest.digest)
      yield* trySnapshotPromise('unavailable', `Failed to complete snapshot upload ${objectKey}`, () =>
        upload.complete(parts),
      )
      return toHex(new Uint8Array(hash))
    }).pipe(
      // Any non-success (failure, defect or interruption) leaves no half-finished multipart upload behind.
      Effect.onExit((exit) =>
        Exit.isSuccess(exit) === true
          ? Effect.void
          : trySnapshotPromise('unavailable', 'Failed to abort snapshot upload', () => upload.abort()).pipe(
              Effect.ignore({ log: 'Warn' }),
            ),
      ),
    )

    return yield* movePointer({ ...draft, objectKey, byteLength: source.byteLength, sha256 })
  })

  const capture: Snapshot.SnapshotSink['capture'] =
    stage === undefined
      ? undefined
      : (dbState) => {
          const source = stage(dbState)
          return {
            publish: (draft) => publishStaged(source, draft).pipe(Effect.ensuring(Effect.sync(source.release))),
            discard: Effect.sync(source.release),
          }
        }

  const getManifest: Snapshot.SnapshotSink['getManifest'] = ({ key }) =>
    getManifestViaRpc({ getSyncBackendStub, storeId, key })

  const makeObjectKey = (manifest: Pick<Snapshot.SnapshotManifest, 'key' | 'seqNum'>) =>
    `${snapshotKeyObjectPrefix({ storeId, key: manifest.key })}${manifest.seqNum}-${nanoid()}.sqlite`

  const customMetadataFor = (manifest: Snapshot.SnapshotManifestDraft) => ({
    storeId,
    snapshotKey: manifest.key,
    backendId: manifest.backendId,
    seqNum: String(manifest.seqNum),
    createdAt: String(manifest.createdAt),
  })

  return capture === undefined ? { publish, getManifest } : { publish, getManifest, capture }
}

/** Workers' `crypto` has the non-standard `DigestStream`; the ambient type may be the DOM one. */
// oxlint-disable-next-line typescript-eslint(no-unsafe-type-assertion) -- workerd global with Cloudflare-specific members
const cfCrypto = () => Reflect.get(globalThis, 'crypto') as CfTypes.Crypto

const toHex = (bytes: Uint8Array) => {
  let hex = ''
  for (const byte of bytes) hex += byte.toString(16).padStart(2, '0')
  return hex
}

/** R2 multipart uploads need parts of at least 5 MiB, except the last. */
const R2_MIN_PART_BYTES = 5 * 1024 * 1024

/** R2 deletes at most 1000 keys per call. */
const R2_MAX_DELETE_KEYS = 1000

const listAllObjects = async (bucket: CfTypes.R2Bucket, prefix: string): Promise<CfTypes.R2Object[]> => {
  const objects: CfTypes.R2Object[] = []
  let cursor: string | undefined
  do {
    const page = await bucket.list({ prefix, ...(cursor !== undefined ? { cursor } : {}) })
    objects.push(...page.objects)
    cursor = page.truncated === true ? page.cursor : undefined
  } while (cursor !== undefined)
  return objects
}

/**
 * Picks blobs whose replacement is older than `cutoff`.
 *
 * A blob stops being current at the latest when its successor's manifest is published, which happens right after the
 * successor's upload. The successor's upload time therefore approximates the replacement time (it can only be earlier
 * by the latency of one manifest RPC). The newest blob has no successor and is never selected, and blobs in `keep`
 * (current and previous manifests) are always retained. Orphans from failed publishes age out the same way.
 */
const selectReplacedObjects = ({
  objects,
  keep,
  cutoff,
}: {
  objects: ReadonlyArray<CfTypes.R2Object>
  keep: ReadonlySet<string>
  cutoff: number
}): string[] => {
  const byUpload = objects.toSorted((a, b) => a.uploaded.getTime() - b.uploaded.getTime())
  const selected: string[] = []
  for (let i = 0; i < byUpload.length - 1; i++) {
    const object = byUpload[i]!
    const successor = byUpload[i + 1]!
    if (keep.has(object.key) === false && successor.uploaded.getTime() <= cutoff) selected.push(object.key)
  }
  return selected
}
