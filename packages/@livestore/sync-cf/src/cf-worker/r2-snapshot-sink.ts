import { Snapshot } from '@livestore/common'
import type { CfTypes } from '@livestore/common-cf'
import { Effect } from '@livestore/utils/effect'
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
 * @experimental
 */
export const makeR2SnapshotSink = ({
  bucket,
  storeId,
  getSyncBackendStub,
  graceMs = 5 * 60_000,
  now = () => Date.now(),
}: R2SnapshotSinkOptions): Snapshot.SnapshotSink => {
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

      const objectKey = `${snapshotKeyObjectPrefix({ storeId, key: manifest.key })}${manifest.seqNum}-${nanoid()}.sqlite`
      const published: Snapshot.SnapshotManifest = { ...manifest, objectKey }

      yield* trySnapshotPromise('unavailable', `Failed to upload snapshot blob ${objectKey}`, () =>
        bucket.put(objectKey, bytes, {
          // R2 rejects the upload if the received bytes don't hash to the manifest's digest.
          sha256: manifest.sha256,
          httpMetadata: { contentType: SNAPSHOT_BLOB_CONTENT_TYPE },
          customMetadata: {
            storeId,
            snapshotKey: manifest.key,
            backendId: manifest.backendId,
            seqNum: String(manifest.seqNum),
            createdAt: String(manifest.createdAt),
          },
        }),
      )

      const result = yield* trySnapshotPromise('unavailable', 'Failed to publish snapshot manifest', () =>
        getSyncBackendStub().putSnapshotManifest(encodeSnapshotManifestJson(published)),
      )

      if (result._tag === 'Rejected') {
        // The pointer never moved to this blob, so nobody can be reading it.
        yield* trySnapshotPromise('unavailable', 'Failed to delete rejected snapshot blob', () =>
          bucket.delete(objectKey),
        ).pipe(Effect.ignore({ log: 'Warn' }))
        return yield* new Snapshot.SnapshotError({ reason: result.reason, message: result.message })
      }

      const keep = new Set([objectKey, ...(result.previousObjectKey === null ? [] : [result.previousObjectKey])])
      yield* prune({ key: manifest.key, keep }).pipe(Effect.ignore({ log: 'Warn' }))

      return published
    },
  )

  const getManifest: Snapshot.SnapshotSink['getManifest'] = ({ key }) =>
    getManifestViaRpc({ getSyncBackendStub, storeId, key })

  return { publish, getManifest }
}

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
