import { makeCheckpointerDurableObject } from '@livestore/adapter-cloudflare'
import { Snapshot } from '@livestore/livestore'
import { makeDurableObject, makeWorker } from '@livestore/sync-cf/cf-worker'

import { schema } from '../cloudflare/schema.ts'
import type { Env } from './env.ts'

// A headless LiveStore client that follows the log and publishes state snapshots to R2.
export class CheckpointerDO extends makeCheckpointerDurableObject<Env>({
  schema,
  syncBackendBinding: 'SYNC_BACKEND_DO',
  bucketBinding: 'SNAPSHOT_BUCKET',
  bindingName: 'CHECKPOINTER_DO',
}) {}

// The sync backend stays schema-blind: it stores the manifest pointer and wakes the checkpointer after pushes.
export class SyncBackendDO extends makeDurableObject({
  snapshots: {
    checkpointerBinding: 'CHECKPOINTER_DO',
    snapshotKeys: [Snapshot.SnapshotKey.toString(Snapshot.SnapshotKey.make(schema))],
  },
}) {}

// Serves `GET …/snapshot/manifest` and `GET …/snapshot/blob` next to the sync routes, behind the same `validatePayload`.
export default makeWorker<Env>({
  syncBackendBinding: 'SYNC_BACKEND_DO',
  snapshots: { bucketBinding: 'SNAPSHOT_BUCKET' },
})
