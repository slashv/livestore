import { Snapshot, UnknownError } from '@livestore/common'
import { Effect, Option, Schema } from '@livestore/utils/effect'

import { encodeSnapshotManifestJson, type SnapshotManifestPutResult } from '../../common/snapshot.ts'
import { PERSISTENCE_FORMAT_VERSION } from '../shared.ts'
import { DoCtx, type Service as DoCtxService } from './layer.ts'
import { contextTable } from './sqlite.ts'

/**
 * Snapshot manifest storage of the sync Durable Object (experimental snapshot boot, LS.SYS-R06).
 *
 * The sync backend stays schema-blind: it only keeps the small manifest pointer per snapshot key and validates it
 * against its own log (backend identity and head). Blobs live in object storage and are managed by the snapshot sink.
 *
 * Adding this table does not require bumping {@link PERSISTENCE_FORMAT_VERSION}. The name still carries the version so a
 * soft reset (which also rotates the backendId) starts with an empty manifest table.
 */
export const snapshotManifestsTableName = `snapshot_manifests_${PERSISTENCE_FORMAT_VERSION}`

/**
 * Validates and stores a manifest published by a checkpointer.
 *
 * Accepted only when it belongs to this store and backend identity, does not claim events the backend has not
 * committed, and does not move the key's pointer backwards. The current manifest becomes `previous`, so the sink knows
 * which replaced blob in-flight downloads may still read.
 */
export const putSnapshotManifest = Effect.fn('@livestore/sync-cf:durable-object:putSnapshotManifest')(function* (
  manifest: Snapshot.SnapshotManifest,
) {
  const { storeId, backendId, currentHeadRef, ctx } = yield* DoCtx

  const reject = (reason: 'incompatible' | 'conflict', message: string): SnapshotManifestPutResult => ({
    _tag: 'Rejected',
    reason,
    message,
  })

  if (manifest.storeId !== storeId) {
    return reject('incompatible', `Manifest storeId ${manifest.storeId} does not match ${storeId}`)
  }
  if (manifest.backendId !== backendId) {
    return reject('incompatible', `Manifest backendId ${manifest.backendId} does not match current ${backendId}`)
  }

  ensureTable(ctx.storage.sql)

  // Synchronous read-check-write so a concurrent put cannot interleave between the monotonicity check and the write.
  return yield* Effect.try({
    try: (): SnapshotManifestPutResult =>
      ctx.storage.transactionSync(() => {
        const head = currentHeadRef.current
        if (manifest.seqNum > head) {
          return reject('incompatible', `Manifest seqNum ${manifest.seqNum} is ahead of the backend head ${head}`)
        }

        const existing = readRow(ctx.storage.sql, { storeId, key: manifest.key })
        // A row from an earlier backend identity is stale: it neither blocks the new manifest nor becomes `previous`.
        const sameBackend = existing !== undefined && existing.backendId === backendId
        if (sameBackend === true && manifest.seqNum < existing.seqNum) {
          return reject(
            'conflict',
            `Manifest seqNum ${manifest.seqNum} is older than the published one at ${existing.seqNum}`,
          )
        }

        const previousManifest = sameBackend === true ? existing.manifest : null
        ctx.storage.sql.exec(
          `INSERT OR REPLACE INTO "${snapshotManifestsTableName}" (storeId, key, backendId, seqNum, manifest, previousManifest, updatedAt) VALUES (?, ?, ?, ?, ?, ?, ?)`,
          storeId,
          manifest.key,
          backendId,
          manifest.seqNum,
          encodeSnapshotManifestJson(manifest),
          previousManifest,
          Date.now(),
        )

        return {
          _tag: 'Accepted',
          previousObjectKey: previousManifest === null ? null : decodeObjectKey(previousManifest),
        }
      }),
    catch: (cause) => new UnknownError({ cause, note: 'Failed to store snapshot manifest' }),
  })
})

/**
 * Returns the current manifest JSON for a key, or `None`.
 *
 * Manifests recorded under a different backendId are never served: a reset backend must not hand out snapshots of a
 * history it no longer has.
 */
export const getSnapshotManifest = Effect.fn('@livestore/sync-cf:durable-object:getSnapshotManifest')(function* (
  key: string,
) {
  const { storeId, backendId, ctx } = yield* DoCtx

  return yield* Effect.try({
    try: () => {
      // Reads never run DDL: a missing table simply means nothing was published yet.
      const row = orIfMissingTable(() => readRow(ctx.storage.sql, { storeId, key }), undefined)
      return row !== undefined && row.backendId === backendId ? Option.some(row.manifest) : Option.none<string>()
    },
    catch: (cause) => new UnknownError({ cause, note: 'Failed to read snapshot manifest' }),
  })
})

/**
 * Whether `objectKey` is the blob of the current or previous manifest of any key of this store and backend identity.
 * The blob route serves nothing else: no orphans, rejected uploads, blobs of a reset backend or of other stores.
 */
export const isSnapshotObjectServable = Effect.fn('@livestore/sync-cf:durable-object:isSnapshotObjectServable')(
  function* (objectKey: string) {
    const { storeId, backendId, ctx } = yield* DoCtx

    return yield* Effect.try({
      try: () =>
        orIfMissingTable(
          () =>
            ctx.storage.sql
              .exec(
                `SELECT 1 FROM "${snapshotManifestsTableName}" WHERE storeId = ? AND backendId = ?
                AND (json_extract(manifest, '$.objectKey') = ? OR json_extract(previousManifest, '$.objectKey') = ?)
                LIMIT 1`,
                storeId,
                backendId,
                objectKey,
                objectKey,
              )
              .toArray().length > 0,
          false,
        ),
      catch: (cause) => new UnknownError({ cause, note: 'Failed to check snapshot object' }),
    })
  },
)

/**
 * Whether the sync backend has ever been initialized for `storeId` in this Durable Object. Used to reject manifests
 * for unknown stores on a cold instance, which would otherwise initialize its context from the manifest's store id.
 */
export const hasStoreContext = (sql: DoCtxService['ctx']['storage']['sql'], storeId: string): boolean =>
  // No context table yet means nothing was ever pushed or pulled here. Any other error is transient and propagates,
  // so callers report it as unavailable rather than as a permanent rejection.
  orIfMissingTable(
    () => sql.exec(`SELECT 1 FROM "${contextTable.sqliteDef.name}" WHERE storeId = ?`, storeId).toArray().length > 0,
    false,
  )

type ManifestRow = { backendId: string; seqNum: number; manifest: string }

/** Only writes create the table (rare); reads treat a missing table as empty, which also holds after `deleteAll`. */
const ensureTable = (sql: DoCtxService['ctx']['storage']['sql']) => {
  sql.exec(
    `CREATE TABLE IF NOT EXISTS "${snapshotManifestsTableName}" (
      storeId TEXT NOT NULL,
      key TEXT NOT NULL,
      backendId TEXT NOT NULL,
      seqNum INTEGER NOT NULL,
      manifest TEXT NOT NULL,
      previousManifest TEXT,
      updatedAt INTEGER NOT NULL,
      PRIMARY KEY (storeId, key)
    ) strict`,
  )
}

const orIfMissingTable = <A, B>(read: () => A, fallback: B): A | B => {
  try {
    return read()
  } catch (error) {
    if (error instanceof Error && error.message.toLowerCase().includes('no such table') === true) return fallback
    throw error
  }
}

const readRow = (
  sql: DoCtxService['ctx']['storage']['sql'],
  { storeId, key }: { storeId: string; key: string },
): ManifestRow | undefined =>
  sql
    .exec<ManifestRow>(
      `SELECT backendId, seqNum, manifest FROM "${snapshotManifestsTableName}" WHERE storeId = ? AND key = ?`,
      storeId,
      key,
    )
    .toArray()[0]

/** Rows were validated on write, so only the object key needs to be read back. */
const decodeObjectKey = (manifestJson: string): string | null => {
  const result = decodeObjectKeyResult(manifestJson)
  return result._tag === 'Some' ? result.value.objectKey : null
}

const decodeObjectKeyResult = Schema.decodeUnknownOption(
  Schema.fromJsonString(Schema.Struct({ objectKey: Snapshot.SnapshotManifest.fields.objectKey })),
)
