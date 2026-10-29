import { sha256 } from '@noble/hashes/sha2.js'

import { textEncodeToArrayBuffer } from '@livestore/utils'
import { Base64Url, Schema } from '@livestore/utils/effect'

import type { LiveStoreSchema } from '../schema/mod.ts'
import { liveStoreStorageFormatVersion } from '../version.ts'

/**
 * Identifies which clients can use a state snapshot.
 *
 * A snapshot is a SQLite state database, so a client can only install it when its state schema, its event-definition
 * set and its storage format all match the checkpointer that produced it.
 *
 * @remarks
 * The key deliberately does not cover materializer code. LiveStore's convention is that changing a materializer's
 * behaviour requires a new event name (e.g. `todoCreated-v2`), which changes `eventDefsHash`. Under this assumption
 * materializer changes are forward-only: events already materialized under the old name keep their old effect.
 */
export interface SnapshotKey {
  /** `schema.state.sqlite.hash`: fingerprint of all state table ASTs. */
  readonly stateSchemaHash: string
  /** Fingerprint of all event names, their arg-schema hashes and their `clientOnly` flag. */
  readonly eventDefsHash: string
  /** {@link liveStoreStorageFormatVersion} of the producing client. */
  readonly storageFormatVersion: number
}

/** Derives the snapshot key for a schema. Pure and deterministic across platforms. */
export const make = (schema: LiveStoreSchema): SnapshotKey => ({
  stateSchemaHash: schema.state.sqlite.hash,
  eventDefsHash: hashEventDefs(schema),
  storageFormatVersion: liveStoreStorageFormatVersion,
})

/**
 * Stable string form, safe for object-storage keys and Durable Object names.
 * Both hashes are base64url, so `.` is an unambiguous separator.
 */
export const toString = (key: SnapshotKey): string =>
  `v${key.storageFormatVersion}.${key.stateSchemaHash}.${key.eventDefsHash}`

const domain = 'livestore/snapshot-event-defs'

const hashEventDefs = (schema: LiveStoreSchema): string => {
  const entries = [...schema.eventsDefsMap.entries()]
    .map(([name, eventDef]) => [name, Schema.hash(eventDef.schema), eventDef.options.clientOnly] as const)
    .toSorted(([a], [b]) => (a < b ? -1 : a > b ? 1 : 0))

  return Base64Url.encode(sha256(textEncodeToArrayBuffer(JSON.stringify([domain, entries]))))
}
