import { Effect, Option } from '@livestore/utils/effect'

import {
  SnapshotError,
  type SnapshotManifest,
  type SnapshotSink,
  type SnapshotSource,
  type SnapshotSourceConstructor,
} from './snapshot.ts'

/**
 * Process-local snapshot store implementing both the checkpointer sink and the client source.
 *
 * Used by tests and Node benchmarks. Mirrors the object-storage contract: blobs are written before the manifest
 * pointer moves, and a replaced blob stays readable for `graceMs`.
 *
 * @experimental
 */
export const makeInMemorySnapshotStore = ({
  storeId,
  graceMs = 60_000,
  now = () => Date.now(),
}: {
  storeId: string
  graceMs?: number
  now?: () => number
}) => {
  const blobs = new Map<string, { bytes: Uint8Array<ArrayBuffer>; replacedAt: number | undefined }>()
  const manifests = new Map<string, SnapshotManifest>()
  let nextObjectId = 0

  const pruneReplaced = () => {
    const t = now()
    for (const [objectKey, blob] of blobs) {
      if (blob.replacedAt !== undefined && t - blob.replacedAt >= graceMs) blobs.delete(objectKey)
    }
  }

  const sink: SnapshotSink = {
    publish: ({ manifest, bytes }) =>
      Effect.gen(function* () {
        if (manifest.storeId !== storeId) {
          return yield* new SnapshotError({ reason: 'incompatible', message: `Wrong storeId ${manifest.storeId}` })
        }
        const previous = manifests.get(manifest.key)
        // A reset backend restarts its log, so only snapshots of the same backend are ordered.
        if (previous !== undefined && previous.backendId === manifest.backendId && previous.seqNum > manifest.seqNum) {
          return yield* new SnapshotError({
            reason: 'conflict',
            message: `Snapshot at ${manifest.seqNum} is older than the published one at ${previous.seqNum}`,
          })
        }

        const objectKey = `snapshots/${storeId}/${manifest.key}/${manifest.seqNum}-${nextObjectId++}.sqlite`
        // Copy so later mutations by the caller cannot change the published blob.
        blobs.set(objectKey, { bytes: bytes.slice(), replacedAt: undefined })
        const published: SnapshotManifest = { ...manifest, objectKey }
        manifests.set(manifest.key, published)

        if (previous !== undefined) {
          const previousBlob = blobs.get(previous.objectKey)
          if (previousBlob !== undefined) previousBlob.replacedAt = now()
        }
        pruneReplaced()

        return published
      }),
    getManifest: ({ key }) => Effect.sync(() => Option.fromUndefinedOr(manifests.get(key))),
  }

  const source: SnapshotSource = {
    getManifest: ({ key }) => Effect.sync(() => Option.fromUndefinedOr(manifests.get(key))),
    download: (manifest) =>
      Effect.gen(function* () {
        pruneReplaced()
        const blob = blobs.get(manifest.objectKey)
        if (blob === undefined) {
          return yield* new SnapshotError({ reason: 'not-found', message: `No blob at ${manifest.objectKey}` })
        }
        return blob.bytes.slice()
      }),
  }

  const makeSource: SnapshotSourceConstructor = () => Effect.succeed(source)

  return {
    sink,
    source,
    makeSource,
    /** Test helpers to simulate storage faults. */
    testing: {
      manifests,
      blobs,
      setManifest: (manifest: SnapshotManifest) => manifests.set(manifest.key, manifest),
      deleteBlob: (objectKey: string) => blobs.delete(objectKey),
      corruptBlob: (objectKey: string) => {
        const blob = blobs.get(objectKey)
        if (blob !== undefined && blob.bytes.length > 0) {
          const corrupted = blob.bytes.slice()
          corrupted[Math.floor(corrupted.length / 2)] = corrupted[Math.floor(corrupted.length / 2)]! ^ 0xff
          blobs.set(objectKey, { ...blob, bytes: corrupted })
        }
      },
    },
  }
}

export type InMemorySnapshotStore = ReturnType<typeof makeInMemorySnapshotStore>
