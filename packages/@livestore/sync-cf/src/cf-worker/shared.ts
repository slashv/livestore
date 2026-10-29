import type { UnknownError } from '@livestore/common'
import type { CfTypes } from '@livestore/common-cf'
import type { Effect, Layer } from '@livestore/utils/effect'
import { Result, Schema } from '@livestore/utils/effect'

import type { SearchParams } from '../common/mod.ts'
import { SearchParamsSchema, SyncMessage } from '../common/mod.ts'
import type { SnapshotManifestPutResult } from '../common/snapshot.ts'

export type Env = {}

/** Headers forwarded from the request to callbacks */
export type ForwardedHeaders = ReadonlyMap<string, string>

/**
 * Configuration for forwarding request headers to DO callbacks.
 * - `string[]`: List of header names to forward (case-insensitive)
 * - `(request) => Record<string, string>`: Custom extraction function (sync)
 */
export type ForwardHeadersOption = readonly string[] | ((request: CfTypes.Request) => Record<string, string>)

/** Context passed to onPush/onPull callbacks */
export type CallbackContext = {
  storeId: StoreId
  payload?: Schema.Json
  /** Headers forwarded from the request (only present if `forwardHeaders` is configured) */
  headers?: ForwardedHeaders
}

export type MakeDurableObjectClassOptions = {
  onPush?: (message: SyncMessage.PushRequest, context: CallbackContext) => Effect.SyncOrPromiseOrEffect<void>
  onPushRes?: (message: SyncMessage.PushAck | UnknownError) => Effect.SyncOrPromiseOrEffect<void>
  onPull?: (message: SyncMessage.PullRequest, context: CallbackContext) => Effect.SyncOrPromiseOrEffect<void>
  onPullRes?: (message: SyncMessage.PullResponse | UnknownError) => Effect.SyncOrPromiseOrEffect<void>

  /**
   * Forward request headers to `onPush`/`onPull` callbacks for authentication.
   *
   * This enables cookie-based or header-based authentication patterns where
   * you need access to request headers inside the Durable Object.
   *
   * @example Forward specific headers by name (case-insensitive)
   * ```ts
   * makeDurableObject({
   *   forwardHeaders: ['cookie', 'authorization'],
   *   onPush: async (message, { headers }) => {
   *     const cookie = headers?.get('cookie')
   *     const session = await validateSession(cookie)
   *   },
   * })
   * ```
   *
   * @example Custom extraction function for derived values
   * ```ts
   * makeDurableObject({
   *   forwardHeaders: (request) => ({
   *     'x-user-id': request.headers.get('x-user-id') ?? '',
   *     'x-session': request.headers.get('cookie')?.split('session=')[1]?.split(';')[0] ?? '',
   *   }),
   *   onPush: async (message, { headers }) => {
   *     const userId = headers?.get('x-user-id')
   *   },
   * })
   * ```
   */
  forwardHeaders?: ForwardHeadersOption
  /**
   * Storage engine for event persistence.
   * - Default: `{ _tag: 'do-sqlite' }` (Durable Object SQLite)
   * - D1: `{ _tag: 'd1', binding: string }` where `binding` is the D1 binding name in wrangler.toml.
   *
   * If omitted, the runtime defaults to DO SQLite. For backwards-compatibility, if an env binding named
   * `DB` exists and looks like a D1Database, D1 will be used.
   *
   * Trade-offs:
   * - DO SQLite: simpler deploy, data co-located with DO, not externally queryable
   * - D1: centralized DB, inspectable with DB tools, extra network hop and JSON size limits
   */
  storage?: { _tag: 'do-sqlite' } | { _tag: 'd1'; binding: string }

  /**
   * Enabled transports for sync backend
   * - `http`: HTTP JSON-RPC
   * - `ws`: WebSocket
   * - `do-rpc`: Durable Object RPC calls (only works in combination with `@livestore/adapter-cf`)
   *
   * @default Set(['http', 'ws', 'do-rpc'])
   */
  enabledTransports?: Set<'http' | 'ws' | 'do-rpc'>

  /**
   * Custom HTTP response headers for HTTP transport
   * These headers will be added to all HTTP RPC responses (Pull, Push, Ping)
   *
   * @example
   * ```ts
   * {
   *   http: {
   *     responseHeaders: {
   *       'Access-Control-Allow-Origin': '*',
   *       'Cache-Control': 'no-cache'
   *     }
   *   }
   * }
   * ```
   */
  http?: {
    responseHeaders?: Record<string, string>
  }

  /** Optional telemetry. Omit to avoid creating an exporter or sending telemetry. */
  otel?: SyncBackendOtelOptions

  /**
   * Wakes snapshot checkpointers after successful pushes (experimental snapshot boot, LS.SYS-R06).
   *
   * A running checkpointer follows the log via live pull, and an evicted one is woken by that pull's callback. This
   * option covers the remaining case: a checkpointer that has never run for a store (or lost its subscription) starts
   * as soon as the store receives events. The sync backend stays schema-blind; it only knows the snapshot key strings.
   *
   * @experimental
   */
  snapshots?: {
    /** Binding name of the checkpointer Durable Object namespace (e.g. `makeCheckpointerDurableObject`). */
    checkpointerBinding: string
    /** Snapshot keys to keep checkpointed: `SnapshotKey.toString(SnapshotKey.make(schema))`, one checkpointer each. */
    snapshotKeys: ReadonlyArray<string>
    /**
     * Minimum time between two wake-up calls to the same checkpointer from this Durable Object instance.
     *
     * @default 10 seconds
     */
    wakeIntervalMs?: number
  }
}

/**
 * The part of a snapshot checkpointer Durable Object the sync backend calls (experimental snapshot boot).
 * Checkpointer instances are named by {@link checkpointerDurableObjectName}.
 */
export interface CheckpointerWakeInterface {
  /**
   * Boots the checkpointer for `storeId` if it is not running and keeps it awake while the log moves.
   *
   * `backendId` is the caller's current backend identity. A checkpointer that followed a different identity (the
   * backend was reset) discards its local copy and starts over, instead of waiting for its next pull to notice.
   * `head` is the backend head at the time of the call; a checkpointer that stays behind it (e.g. because a live-pull
   * callback was lost and no later push revealed the gap) re-pulls from its cursor.
   */
  ensureRunning(storeId: string, options?: { backendId?: string; head?: number }): Promise<void>
}

export type SyncBackendOtelOptions =
  /** Application-supplied tracer layer; LiveStore builds and finalizes its resources per sync operation. */
  | Layer.Layer<never>
  | {
      /** Convenience OTLP/HTTP JSON exporter owned by LiveStore for each operation. */
      baseUrl?: string
      serviceName?: string
    }

export type StoreId = string
export type DurableObjectId = string

/**
 * CRITICAL: Increment this version whenever you modify the database schema structure.
 *
 * Bump required when:
 * - Adding/removing/renaming columns in eventlogTable or contextTable (see sqlite.ts)
 * - Changing column types or constraints
 * - Modifying primary keys or indexes
 *
 * Bump NOT required when:
 * - Changing query patterns, pagination logic, or streaming behavior
 * - Adding new tables (as long as existing table schemas remain unchanged)
 * - Updating implementation details in sync-storage.ts
 *
 * Impact: Changing this version triggers a "soft reset" - new table names are created
 * and old data becomes inaccessible (but remains in storage).
 */
export const PERSISTENCE_FORMAT_VERSION = 7

export const encodeOutgoingMessage = Schema.encodeSync(Schema.fromJsonString(SyncMessage.BackendToClientMessage))
export const encodeIncomingMessage = Schema.encodeSync(Schema.fromJsonString(SyncMessage.ClientToBackendMessage))

const SearchParamsFromUrlSearchParams = Schema.fromURLSearchParams(SearchParamsSchema)

/**
 * Extracts the LiveStore sync search parameters from a request. Returns
 * `undefined` when the request does not carry valid sync metadata so callers
 * can fall back to custom routing.
 */
export const matchSyncRequest = (request: CfTypes.Request): SearchParams | undefined => {
  const url = new URL(request.url)
  const paramsResult = Schema.decodeResult(SearchParamsFromUrlSearchParams)(url.searchParams)

  if (Result.isFailure(paramsResult) === true) {
    return undefined
  }

  return paramsResult.success
}

// RPC subscription storage (TODO refactor)
export type RpcSubscription = {
  storeId: StoreId
  payload?: Schema.Json
  subscribedAt: number
  /** Effect RPC request ID */
  requestId: string
  callerContext: {
    bindingName: string
    durableObjectId: string
  }
}

/** Key prefix for DO-RPC subscriptions persisted in the DO's synchronous KV storage. */
export const rpcSubscriptionKeyPrefix = 'rpc-sub:'

/**
 * Durable Object interface supporting the DO RPC protocol for DO <> DO syncing.
 */
export interface SyncBackendRpcInterface {
  __DURABLE_OBJECT_BRAND: never
  rpc(payload: Uint8Array): Promise<Uint8Array | CfTypes.ReadableStream>
  /**
   * Publishes a snapshot manifest (JSON-encoded `Snapshot.SnapshotManifest`) for the experimental snapshot boot.
   * Called by the checkpointer's snapshot sink after the blob has been uploaded.
   *
   * @experimental
   */
  putSnapshotManifest(manifestJson: string): Promise<SnapshotManifestPutResult>
  /**
   * Returns the JSON-encoded current manifest for the snapshot key, or `null` when none applies to the current
   * backend identity.
   *
   * @experimental
   */
  getSnapshotManifest(args: { storeId: string; key: string }): Promise<string | null>
  /**
   * Whether `objectKey` is the blob of the current or previous manifest of some snapshot key of this store (and backend
   * identity). The worker's blob route serves nothing else.
   *
   * @experimental
   */
  isSnapshotObjectServable(args: { storeId: string; objectKey: string }): Promise<boolean>
}

export const WebSocketAttachmentSchema = Schema.fromJsonString(
  Schema.Struct({
    // Same across all websocket connections
    storeId: Schema.String,
    // Different for each websocket connection
    payload: Schema.optional(Schema.Json),
    pullRequestIds: Schema.Array(Schema.Union([Schema.String, Schema.Finite])),
    // Headers forwarded from the initial request (via forwardHeaders option)
    headers: Schema.optional(Schema.Record(Schema.String, Schema.String)),
  }),
)

/** Helper to extract headers from a request based on the forwardHeaders option */
export const extractForwardedHeaders = (
  request: CfTypes.Request,
  forwardHeaders: ForwardHeadersOption | undefined,
): Record<string, string> | undefined => {
  if (forwardHeaders === undefined) {
    return undefined
  }

  if (typeof forwardHeaders === 'function') {
    return forwardHeaders(request)
  }

  // Array of header names - extract them case-insensitively
  const result: Record<string, string> = {}
  for (const name of forwardHeaders) {
    const value = request.headers.get(name)
    if (value !== null) {
      result[name.toLowerCase()] = value
    }
  }
  return Object.keys(result).length > 0 ? result : undefined
}

/** Convert a headers record to a ReadonlyMap */
export const headersRecordToMap = (headers: Record<string, string> | undefined): ForwardedHeaders | undefined => {
  if (headers === undefined) {
    return undefined
  }
  return new Map(Object.entries(headers).map(([key, value]) => [key.toLowerCase(), value]))
}
