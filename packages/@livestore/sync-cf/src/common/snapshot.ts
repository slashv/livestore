import { Snapshot } from '@livestore/common'
import type { CfTypes } from '@livestore/common-cf'
import { Schema } from '@livestore/utils/effect'

/**
 * Shared pieces of the Cloudflare snapshot boot realization (experimental, LS.SYS-R06).
 *
 * The sync Durable Object only stores the small {@link Snapshot.SnapshotManifest}; blobs live in R2 under
 * {@link snapshotObjectPrefix}. Everything in this module is runtime-agnostic so both the worker and clients can use it.
 *
 * @experimental
 * @module
 */

/** Path suffixes of the opt-in worker routes. Matched as suffixes so the sync endpoint can be mounted under a prefix. */
export const SNAPSHOT_ROUTES = {
  manifest: '/snapshot/manifest',
  blob: '/snapshot/blob',
} as const

const payloadParam = Schema.optional(
  Schema.StringFromUriComponent.pipe(Schema.decodeTo(Schema.fromJsonString(Schema.Json))),
)

/** Search params of `GET …/snapshot/manifest`. `payload` is encoded like the sync transports encode it. */
export const SnapshotManifestSearchParamsSchema = Schema.Struct({
  storeId: Schema.String,
  key: Schema.String,
  payload: payloadParam,
})

export type SnapshotManifestSearchParams = typeof SnapshotManifestSearchParamsSchema.Type

/** Search params of `GET …/snapshot/blob`. */
export const SnapshotBlobSearchParamsSchema = Schema.Struct({
  storeId: Schema.String,
  objectKey: Schema.String,
  payload: payloadParam,
})

export type SnapshotBlobSearchParams = typeof SnapshotBlobSearchParamsSchema.Type

/**
 * Result of `SyncBackendRpcInterface.putSnapshotManifest`.
 *
 * A plain object (not a thrown error) so the reason survives Durable Object RPC structured cloning.
 */
export type SnapshotManifestPutResult =
  | {
      readonly _tag: 'Accepted'
      /** Blob of the manifest that was current before this put (same backend only); the sink keeps it readable. */
      readonly previousObjectKey: string | null
    }
  | {
      readonly _tag: 'Rejected'
      readonly reason: Extract<Snapshot.SnapshotError['reason'], 'incompatible' | 'conflict'>
      readonly message: string
    }

/**
 * Every snapshot blob of a store lives under this R2 prefix. The store id is URI-encoded so that one store's prefix
 * is never a prefix of another's (`a` vs `a/b`).
 */
export const snapshotObjectPrefix = (storeId: string) => `snapshots/${encodeURIComponent(storeId)}/`

/** R2 prefix of all blobs published for one snapshot key. */
export const snapshotKeyObjectPrefix = ({ storeId, key }: { storeId: string; key: string }) =>
  `${snapshotObjectPrefix(storeId)}${encodeURIComponent(key)}/`

export const SNAPSHOT_BLOB_CONTENT_TYPE = 'application/vnd.sqlite3'

/** Durable Object name of the checkpointer for a store and snapshot key: one checkpointer per (store, schema). */
export const checkpointerDurableObjectName = ({ storeId, key }: { storeId: string; key: string }) => `${storeId}/${key}`

export const encodeSnapshotManifestJson = Schema.encodeSync(Schema.fromJsonString(Snapshot.SnapshotManifest))
export const decodeSnapshotManifestJson = Schema.decodeUnknownEffect(Schema.fromJsonString(Snapshot.SnapshotManifest))

/**
 * Reads a snapshot blob from R2, refusing object keys outside the store's prefix and blobs whose size does not
 * match the manifest (a mismatch means the pointer and the blob diverged, so the caller should fall back).
 */
export const readSnapshotBlobFromR2 = async ({
  bucket,
  storeId,
  manifest,
}: {
  bucket: CfTypes.R2Bucket
  storeId: string
  manifest: Snapshot.SnapshotManifest
}): Promise<
  | { _tag: 'Ok'; bytes: Uint8Array<ArrayBuffer> }
  | { _tag: 'Error'; reason: Snapshot.SnapshotError['reason']; message: string }
> => {
  if (manifest.objectKey.startsWith(snapshotObjectPrefix(storeId)) === false) {
    return {
      _tag: 'Error',
      reason: 'incompatible',
      message: `Object key outside of store prefix: ${manifest.objectKey}`,
    }
  }

  const object = await bucket.get(manifest.objectKey)
  // The sink records the owning store on every blob; a mismatch means the key does not belong to this store.
  if (object === null || object.customMetadata?.storeId !== storeId) {
    object?.body.cancel().catch(() => {})
    return { _tag: 'Error', reason: 'not-found', message: `No snapshot blob at ${manifest.objectKey}` }
  }
  if (object.size !== manifest.byteLength) {
    object.body.cancel().catch(() => {})
    return {
      _tag: 'Error',
      reason: 'integrity',
      message: `Snapshot blob has ${object.size} bytes, manifest expects ${manifest.byteLength}`,
    }
  }

  const bytes = new Uint8Array(await object.arrayBuffer())
  if (bytes.byteLength !== manifest.byteLength) {
    return {
      _tag: 'Error',
      reason: 'integrity',
      message: `Snapshot blob has ${bytes.byteLength} bytes, manifest expects ${manifest.byteLength}`,
    }
  }

  return { _tag: 'Ok', bytes }
}
