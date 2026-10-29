import { Snapshot } from '@livestore/common'
import { Effect, Option } from '@livestore/utils/effect'

import type { SyncBackendRpcInterface } from '../cf-worker/shared.ts'
import { decodeSnapshotManifestJson } from './snapshot.ts'

/**
 * Internal helpers shared by the R2 snapshot sink (`cf-worker`) and the DO-RPC snapshot source (`client`).
 * Kept free of `cloudflare:workers` imports so both entry points can use it.
 */

/** The part of the sync Durable Object stub the snapshot sink and the DO-RPC snapshot source need. */
export type SnapshotManifestRpcStub = Pick<SyncBackendRpcInterface, 'putSnapshotManifest' | 'getSnapshotManifest'>

/**
 * Reads the current manifest of a key from the sync Durable Object via RPC.
 * Shared by the R2 sink and the DO-RPC snapshot source.
 */
export const getManifestViaRpc = ({
  getSyncBackendStub,
  storeId,
  key,
}: {
  getSyncBackendStub: () => SnapshotManifestRpcStub
  storeId: string
  key: string
}): Effect.Effect<Option.Option<Snapshot.SnapshotManifest>, Snapshot.SnapshotError> =>
  Effect.gen(function* () {
    const manifestJson = yield* trySnapshotPromise('unavailable', 'Failed to read snapshot manifest', () =>
      getSyncBackendStub().getSnapshotManifest({ storeId, key }),
    )
    if (manifestJson === null) return Option.none()

    const manifest = yield* decodeSnapshotManifestJson(manifestJson).pipe(
      Effect.mapError(
        (cause) => new Snapshot.SnapshotError({ reason: 'internal', message: 'Invalid snapshot manifest', cause }),
      ),
    )
    return Option.some(manifest)
  }).pipe(Effect.withSpan('@livestore/sync-cf:snapshot:getManifestViaRpc', { attributes: { storeId, key } }))

export const trySnapshotPromise = <A>(
  reason: Snapshot.SnapshotError['reason'],
  message: string,
  try_: () => Promise<A>,
): Effect.Effect<A, Snapshot.SnapshotError> =>
  Effect.tryPromise({ try: try_, catch: (cause) => new Snapshot.SnapshotError({ reason, message, cause }) })
