import { sha256 } from '@noble/hashes/sha2.js'

import type { Duration, Effect, HttpClient, Option, Scope } from '@livestore/utils/effect'
import { Schema } from '@livestore/utils/effect'

import type { UnknownError } from '../errors.ts'

/**
 * Describes one published state snapshot.
 *
 * The manifest is the small pointer a sync backend may serve; the blob it points to lives in object storage.
 * `seqNum` is the global sequence number of the last event reflected by the snapshot's state (its state head),
 * so a client that installs the snapshot continues pulling with `seqNum` as its backend cursor.
 */
export const SnapshotManifest = Schema.Struct({
  manifestVersion: Schema.Literal(1),
  storeId: Schema.String,
  /** {@link SnapshotKey.toString} of the producing schema. */
  key: Schema.String,
  /** Sync backend identity the snapshot was derived from. A reset backend invalidates the snapshot. */
  backendId: Schema.String,
  /** Global sequence number of the last event reflected by the snapshot. */
  seqNum: Schema.Int,
  /**
   * Provider-specific sync metadata of the event at `seqNum`, used as the pull cursor metadata.
   * Absent when the provider does not attach metadata.
   */
  cursorMetadata: Schema.optionalKey(Schema.Json),
  /** Exact size of the snapshot blob in bytes. */
  byteLength: Schema.Int,
  /** Lowercase hex SHA-256 of the snapshot blob. */
  sha256: Schema.String,
  /** Epoch milliseconds at which the checkpointer exported the snapshot. */
  createdAt: Schema.Int,
  /** Opaque, provider-specific location of the blob. */
  objectKey: Schema.String,
}).annotate({ title: 'SnapshotManifest' })

export type SnapshotManifest = typeof SnapshotManifest.Type

export class SnapshotError extends Schema.TaggedError<SnapshotError>('~@livestore/common/SnapshotError')(
  'SnapshotError',
  {
    reason: Schema.Literals(['unavailable', 'not-found', 'integrity', 'incompatible', 'conflict', 'internal']),
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

/**
 * Read side used by booting clients. Implementations must never return a manifest whose blob they cannot serve.
 *
 * @experimental
 */
export interface SnapshotSource {
  /** Returns the latest snapshot for the key, or `None` when none has been published (or it no longer applies). */
  readonly getManifest: (args: {
    readonly key: string
  }) => Effect.Effect<Option.Option<SnapshotManifest>, SnapshotError>
  /** Downloads the blob. Callers verify `byteLength` and `sha256` themselves. */
  readonly download: (manifest: SnapshotManifest) => Effect.Effect<Uint8Array<ArrayBuffer>, SnapshotError>
}

export type MakeSnapshotSourceArgs<TPayload = Schema.Json> = {
  readonly storeId: string
  readonly clientId: string
  readonly payload: TPayload | undefined
}

/** @experimental */
export type SnapshotSourceConstructor<TPayload = Schema.Json> = (
  args: MakeSnapshotSourceArgs<TPayload>,
) => Effect.Effect<SnapshotSource, UnknownError, Scope.Scope | HttpClient.HttpClient>

/**
 * Write side used by the checkpointer.
 *
 * `publish` must upload the blob before it makes the manifest visible, so readers never see a manifest without a blob.
 * Implementations keep the previously published snapshot readable for a grace period, so in-flight downloads finish.
 *
 * @experimental
 */
export interface SnapshotSink {
  readonly publish: (args: {
    readonly manifest: Omit<SnapshotManifest, 'objectKey'>
    readonly bytes: Uint8Array<ArrayBuffer>
  }) => Effect.Effect<SnapshotManifest, SnapshotError>
  /** Latest manifest published for the key, so a restarted checkpointer resumes its trigger state. */
  readonly getManifest: (args: {
    readonly key: string
  }) => Effect.Effect<Option.Option<SnapshotManifest>, SnapshotError>
}

/**
 * Client-side snapshot boot options. Opt-in and experimental (LS.SYS-R06).
 *
 * @experimental
 */
export type SnapshotBootOptions<TPayload = Schema.Json> = {
  readonly source: SnapshotSourceConstructor<TPayload>
  /**
   * Upper bound for fetching the manifest and downloading the blob. On timeout the client falls back to full replay.
   *
   * @default 60 seconds
   */
  readonly timeout?: Duration.Input
}

/** Lowercase hex SHA-256, used for manifest integrity checks on every platform. */
export const sha256Hex = (bytes: Uint8Array): string => {
  const digest = sha256(bytes)
  let hex = ''
  for (let i = 0; i < digest.length; i++) hex += digest[i]!.toString(16).padStart(2, '0')
  return hex
}
