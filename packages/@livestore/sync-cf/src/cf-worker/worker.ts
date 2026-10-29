import { env as importedEnv } from 'cloudflare:workers'

import { UnknownError } from '@livestore/common'
import type { HelperTypes } from '@livestore/common-cf'
import { CfDeclare } from '@livestore/common-cf/declare'
import { Effect, Result, Schema } from '@livestore/utils/effect'

import type { CfTypes, SearchParams } from '../common/mod.ts'
import {
  SNAPSHOT_BLOB_CONTENT_TYPE,
  SNAPSHOT_ROUTES,
  type SnapshotBlobSearchParams,
  SnapshotBlobSearchParamsSchema,
  type SnapshotManifestSearchParams,
  SnapshotManifestSearchParamsSchema,
  snapshotObjectPrefix,
} from '../common/snapshot.ts'
import { type Env, type ForwardedHeaders, matchSyncRequest, type SyncBackendRpcInterface } from './shared.ts'

const Response = CfDeclare.Response

// HINT: If we ever extend user's custom worker RPC, type T can help here with expected return type safety. Currently unused.
export type CFWorker<TEnv extends Env = Env, _T extends CfTypes.Rpc.DurableObjectBranded | undefined = undefined> = {
  fetch: <CFHostMetada = unknown>(
    request: CfTypes.Request<CFHostMetada>,
    env: TEnv,
    ctx: CfTypes.ExecutionContext,
  ) => Promise<CfTypes.Response>
}

/** Context passed to validatePayload callback */
export type ValidatePayloadContext = {
  storeId: string
  /** Request headers (raw, not filtered by forwardHeaders) */
  headers: ForwardedHeaders
}

/**
 * Options accepted by {@link makeWorker}. The Durable Object binding has to be
 * supplied explicitly so we never fall back to deprecated defaults when Cloudflare config changes.
 */
export type MakeWorkerOptions<TEnv extends Env = Env, TSyncPayload = Schema.Json> = {
  /**
   * Binding name of the sync Durable Object declared in wrangler config.
   */
  syncBackendBinding: HelperTypes.ExtractDurableObjectKeys<TEnv>
  /**
   * Optionally pass a schema to decode the client-provided payload into a typed object
   * before calling {@link validatePayload}. If omitted, the raw JSON value is forwarded.
   */
  syncPayloadSchema?: Schema.Decoder<TSyncPayload>
  /**
   * Validates the (optionally decoded) payload during WebSocket connection establishment.
   * If {@link syncPayloadSchema} is provided, `payload` will be of the schema's inferred type.
   *
   * The context includes request headers for cookie-based or header-based authentication.
   *
   * @example Cookie-based authentication
   * ```ts
   * validatePayload: async (payload, { storeId, headers }) => {
   *   const cookie = headers.get('cookie')
   *   const session = await validateSessionFromCookie(cookie)
   *   if (!session) throw new Error('Unauthorized')
   * }
   * ```
   *
   * Note: This runs only at connection time, not for individual push events.
   * For push event validation, use the `onPush` callback in the Durable Object.
   */
  validatePayload?: (payload: TSyncPayload, context: ValidatePayloadContext) => void | Promise<void>
  /** @default false */
  enableCORS?: boolean
  /**
   * Enables the read-only snapshot routes of the experimental snapshot boot (LS.SYS-R06):
   *
   * - `GET …/snapshot/manifest?storeId=…&key=…&payload=…` returns the current manifest of a snapshot key (404 if none)
   * - `GET …/snapshot/blob?storeId=…&objectKey=…&payload=…` streams a snapshot blob from the R2 bucket
   *
   * Both apply {@link validatePayload} exactly like sync requests. Blobs are written by a checkpointer through
   * `makeR2SnapshotSink`; clients read them with `makeHttpSnapshotSource` from `@livestore/sync-cf/client`.
   *
   * @experimental
   */
  snapshots?: {
    /** Binding name of the R2 bucket holding snapshot blobs. */
    bucketBinding: HelperTypes.ExtractR2BucketKeys<TEnv>
  }
}

/**
 * Produces a Cloudflare Worker `fetch` handler that delegates sync traffic to the
 * Durable Object identified by `syncBackendBinding`.
 *
 * For more complex setups prefer implementing a custom `fetch` and call {@link handleSyncRequest}
 * from the branch that handles LiveStore sync requests.
 */
export const makeWorker = <
  TEnv extends Env = Env,
  TDurableObjectRpc extends CfTypes.Rpc.DurableObjectBranded | undefined = undefined,
  TSyncPayload = Schema.Json,
>(
  options: MakeWorkerOptions<TEnv, TSyncPayload>,
): CFWorker<TEnv, TDurableObjectRpc> => {
  return {
    fetch: async (request, env, _ctx) => {
      const url = new URL(request.url)

      const corsHeaders: CfTypes.HeadersInit =
        options.enableCORS === true
          ? {
              'Access-Control-Allow-Origin': '*',
              'Access-Control-Allow-Methods': 'GET, POST, OPTIONS',
              'Access-Control-Allow-Headers': request.headers.get('Access-Control-Request-Headers') ?? '*',
            }
          : {}

      if (request.method === 'OPTIONS' && options.enableCORS === true) {
        return new Response(null, {
          status: 204,
          headers: corsHeaders,
        })
      }

      if (options.snapshots !== undefined) {
        const snapshotRequest = matchSnapshotRequest(request)
        if (snapshotRequest !== undefined) {
          return handleSnapshotRequest<TEnv, unknown, TSyncPayload>({
            request,
            snapshotRequest,
            env,
            syncBackendBinding: options.syncBackendBinding,
            bucketBinding: options.snapshots.bucketBinding,
            headers: corsHeaders,
            validatePayload: options.validatePayload,
            syncPayloadSchema: options.syncPayloadSchema,
          })
        }
      }

      const searchParams = matchSyncRequest(request)

      // Check if this is a sync request first, before showing info message
      if (searchParams !== undefined) {
        return handleSyncRequest<TEnv, TDurableObjectRpc, unknown, TSyncPayload>({
          request,
          searchParams,
          env,
          ctx: _ctx,
          syncBackendBinding: options.syncBackendBinding,
          headers: corsHeaders,
          validatePayload: options.validatePayload,
          syncPayloadSchema: options.syncPayloadSchema,
        })
      }

      // Only show info message for GET requests to / without sync parameters
      if (request.method === 'GET' && url.pathname === '/') {
        return new Response('Info: Sync backend endpoint for @livestore/sync-cf.', {
          status: 200,
          headers: { 'Content-Type': 'text/plain' },
        })
      }

      console.error('Invalid path', url.pathname)

      return new Response('Invalid path', {
        status: 400,
        statusText: 'Bad Request',
        headers: {
          ...corsHeaders,
          'Content-Type': 'text/plain',
        },
      })
    },
  }
}

/** Convert CF Request headers to a ForwardedHeaders map */
const requestHeadersToMap = (request: CfTypes.Request): ForwardedHeaders => {
  const result = new Map<string, string>()
  request.headers.forEach((value, key) => {
    result.set(key.toLowerCase(), value)
  })
  return result
}

/**
 * Handles LiveStore sync requests (e.g. with search params `?storeId=...&transport=...`).
 *
 * @example Token-based authentication
 * ```ts
 * const validatePayload = (payload: Schema.Json | undefined, context: { storeId: string }) => {
 *   if (payload?.authToken !== 'insecure-token-change-me') {
 *     throw new Error('Invalid auth token')
 *   }
 * }
 * ```
 *
 * @example Cookie-based authentication
 * ```ts
 * const validatePayload = async (payload: Schema.Json | undefined, { storeId, headers }) => {
 *   const cookie = headers.get('cookie')
 *   const session = await validateSessionFromCookie(cookie)
 *   if (!session) throw new Error('Unauthorized')
 * }
 * ```
 *
 * @throws {UnknownError} If the payload is invalid
 */
export const handleSyncRequest = <
  TEnv extends Env = Env,
  TDurableObjectRpc extends CfTypes.Rpc.DurableObjectBranded | undefined = undefined,
  CFHostMetada = unknown,
  TSyncPayload = Schema.Json,
>({
  request,
  searchParams: { storeId, payload, transport },
  env: explicitlyProvidedEnv,
  syncBackendBinding,
  headers,
  validatePayload,
  syncPayloadSchema,
}: {
  request: CfTypes.Request<CFHostMetada>
  searchParams: SearchParams
  env?: TEnv | undefined
  /** Only there for type-level reasons */
  ctx: CfTypes.ExecutionContext
  /** Binding name of the sync backend Durable Object */
  syncBackendBinding: MakeWorkerOptions<TEnv, TSyncPayload>['syncBackendBinding']
  headers?: CfTypes.HeadersInit | undefined
  validatePayload?: MakeWorkerOptions<TEnv, TSyncPayload>['validatePayload']
  syncPayloadSchema?: MakeWorkerOptions<TEnv, TSyncPayload>['syncPayloadSchema']
}): Promise<CfTypes.Response> =>
  Effect.gen(function* () {
    const invalidPayloadResponse = yield* validateRequestPayload({
      request,
      storeId,
      payload,
      headers,
      validatePayload,
      syncPayloadSchema,
    })
    if (invalidPayloadResponse !== undefined) return invalidPayloadResponse

    const env = explicitlyProvidedEnv ?? (importedEnv as TEnv)

    if (!(syncBackendBinding in env)) {
      return new Response(
        `Failed dependency: Required Durable Object binding '${syncBackendBinding as string}' not available`,
        {
          status: 424,
          ...(headers !== undefined ? { headers } : {}),
        },
      )
    }

    const durableObjectNamespace = env[
      syncBackendBinding as keyof TEnv
    ] as CfTypes.DurableObjectNamespace<TDurableObjectRpc>

    const id = durableObjectNamespace.idFromName(storeId)
    const durableObject = durableObjectNamespace.get(id)

    // Handle WebSocket upgrade request
    const upgradeHeader = request.headers.get('Upgrade')
    if (transport === 'ws' && (upgradeHeader === null || upgradeHeader !== 'websocket')) {
      return new Response('Durable Object expected Upgrade: websocket', {
        status: 426,
        ...(headers !== undefined ? { headers } : {}),
      })
    }

    return yield* Effect.promise(() => durableObject.fetch(request))
  }).pipe(Effect.tapCauseLogPretty, Effect.runPromise)

/** A request for one of the snapshot routes, see {@link MakeWorkerOptions.snapshots}. */
export type SnapshotRequest =
  | { readonly _tag: 'manifest'; readonly params: SnapshotManifestSearchParams }
  | { readonly _tag: 'blob'; readonly params: SnapshotBlobSearchParams }

/**
 * Matches the snapshot routes (`GET …/snapshot/manifest`, `GET …/snapshot/blob`). Returns `undefined` for any other
 * request (including malformed snapshot requests) so callers can fall back to custom routing.
 *
 * @experimental
 */
export const matchSnapshotRequest = (request: CfTypes.Request): SnapshotRequest | undefined => {
  if (request.method !== 'GET') return undefined

  const url = new URL(request.url)
  if (url.pathname.endsWith(SNAPSHOT_ROUTES.manifest) === true) {
    const result = decodeSnapshotManifestSearchParams(url.searchParams)
    return Result.isSuccess(result) === true ? { _tag: 'manifest', params: result.success } : undefined
  }
  if (url.pathname.endsWith(SNAPSHOT_ROUTES.blob) === true) {
    const result = decodeSnapshotBlobSearchParams(url.searchParams)
    return Result.isSuccess(result) === true ? { _tag: 'blob', params: result.success } : undefined
  }
  return undefined
}

/**
 * Serves the snapshot routes of the experimental snapshot boot. Use it from a custom `fetch` handler next to
 * {@link handleSyncRequest}:
 *
 * ```ts
 * const snapshotRequest = matchSnapshotRequest(request)
 * if (snapshotRequest !== undefined) {
 *   return handleSnapshotRequest({
 *     request,
 *     snapshotRequest,
 *     env,
 *     syncBackendBinding: 'SYNC_BACKEND_DO',
 *     bucketBinding: 'SNAPSHOT_BUCKET',
 *     validatePayload,
 *   })
 * }
 * ```
 *
 * The manifest is read from the sync Durable Object, which only serves manifests of its current backend identity.
 * Blobs are streamed from R2; object keys outside `snapshots/<storeId>/` are refused so a payload valid for one store
 * cannot read another store's snapshots.
 *
 * @experimental
 */
export const handleSnapshotRequest = <TEnv extends Env = Env, CFHostMetada = unknown, TSyncPayload = Schema.Json>({
  request,
  snapshotRequest,
  env: explicitlyProvidedEnv,
  syncBackendBinding,
  bucketBinding,
  headers,
  validatePayload,
  syncPayloadSchema,
}: {
  request: CfTypes.Request<CFHostMetada>
  snapshotRequest: SnapshotRequest
  env?: TEnv | undefined
  /** Binding name of the sync backend Durable Object */
  syncBackendBinding: MakeWorkerOptions<TEnv, TSyncPayload>['syncBackendBinding']
  /** Binding name of the R2 bucket holding snapshot blobs */
  bucketBinding: HelperTypes.ExtractR2BucketKeys<TEnv>
  headers?: CfTypes.HeadersInit | undefined
  validatePayload?: MakeWorkerOptions<TEnv, TSyncPayload>['validatePayload']
  syncPayloadSchema?: MakeWorkerOptions<TEnv, TSyncPayload>['syncPayloadSchema']
}): Promise<CfTypes.Response> =>
  Effect.gen(function* () {
    const { storeId, payload } = snapshotRequest.params

    const invalidPayloadResponse = yield* validateRequestPayload({
      request,
      storeId,
      payload,
      headers,
      validatePayload,
      syncPayloadSchema,
    })
    if (invalidPayloadResponse !== undefined) return invalidPayloadResponse

    const env = explicitlyProvidedEnv ?? (importedEnv as TEnv)
    const textResponse = (status: number, body: string) =>
      makeResponse(body, { status, headers, extraHeaders: { 'Content-Type': 'text/plain' } })

    if (!(syncBackendBinding in env)) {
      return textResponse(
        424,
        `Failed dependency: Required Durable Object binding '${syncBackendBinding as string}' not available`,
      )
    }
    const namespace = env[syncBackendBinding as keyof TEnv] as CfTypes.DurableObjectNamespace<SyncBackendRpcInterface>
    const stub = namespace.get(namespace.idFromName(storeId))

    if (snapshotRequest._tag === 'manifest') {
      const manifestJson = yield* Effect.tryPromise(() =>
        stub.getSnapshotManifest({ storeId, key: snapshotRequest.params.key }),
      )

      if (manifestJson === null) return textResponse(404, 'No snapshot manifest')

      return makeResponse(manifestJson, {
        status: 200,
        headers,
        // The pointer moves whenever the checkpointer publishes, so it must never be served from a cache.
        extraHeaders: { 'Content-Type': 'application/json', 'Cache-Control': 'no-store' },
      })
    }

    const { objectKey } = snapshotRequest.params
    if (objectKey.startsWith(snapshotObjectPrefix(storeId)) === false) {
      return textResponse(403, 'Snapshot object key outside of the store prefix')
    }

    const bucket = (env as Record<string, CfTypes.R2Bucket | undefined>)[bucketBinding]
    if (bucket === undefined) {
      return textResponse(424, `Failed dependency: Required R2 bucket binding '${bucketBinding}' not available`)
    }

    // Only blobs a manifest currently points at (current or previous) are served: never orphans, rejected uploads or
    // blobs of a reset backend, even under this store's prefix.
    const servable = yield* Effect.tryPromise(() => stub.isSnapshotObjectServable({ storeId, objectKey }))
    if (servable === false) return textResponse(404, 'No snapshot blob')

    const object = yield* Effect.tryPromise(() => bucket.get(objectKey))
    if (object === null) return textResponse(404, 'No snapshot blob')
    if (object.customMetadata?.storeId !== storeId) {
      yield* Effect.promise(() => object.body.cancel())
      return textResponse(404, 'No snapshot blob')
    }

    return makeResponse(object.body, {
      status: 200,
      headers,
      extraHeaders: {
        'Content-Type': SNAPSHOT_BLOB_CONTENT_TYPE,
        'Content-Length': String(object.size),
        ETag: object.httpEtag,
        // Object keys are never reused for different content, but access depends on the request payload.
        'Cache-Control': 'private, max-age=31536000, immutable',
      },
    })
  }).pipe(
    Effect.tapCauseLogPretty,
    // Storage or RPC failures are transient for the client (it falls back to full replay); keep CORS headers on them.
    Effect.catchCause(() =>
      Effect.succeed(
        makeResponse('Snapshot storage unavailable', {
          status: 503,
          headers,
          extraHeaders: { 'Content-Type': 'text/plain', 'Retry-After': '5' },
        }),
      ),
    ),
    Effect.runPromise,
  )

/**
 * Runs `validatePayload` (decoding with `syncPayloadSchema` first, when given) and returns a 400 response if the
 * payload is rejected, or `undefined` when the request may proceed.
 */
const validateRequestPayload = <TEnv extends Env, TSyncPayload, CFHostMetada>({
  request,
  storeId,
  payload,
  headers,
  validatePayload,
  syncPayloadSchema,
}: {
  request: CfTypes.Request<CFHostMetada>
  storeId: string
  payload: Schema.Json | undefined
  headers: CfTypes.HeadersInit | undefined
  validatePayload: MakeWorkerOptions<TEnv, TSyncPayload>['validatePayload']
  syncPayloadSchema: MakeWorkerOptions<TEnv, TSyncPayload>['syncPayloadSchema']
}): Effect.Effect<CfTypes.Response | undefined> =>
  Effect.gen(function* () {
    if (validatePayload === undefined) return undefined

    // Convert request headers to a Map for the validation context
    const requestHeaders = requestHeadersToMap(request)

    // Always decode with the supplied schema when present, even if payload is undefined.
    // This ensures required payloads are enforced by the schema.
    let decodedPayload: TSyncPayload
    if (syncPayloadSchema !== undefined) {
      const decodedResult = Schema.decodeResult(syncPayloadSchema)(payload)
      if (Result.isFailure(decodedResult) === true) {
        const message = decodedResult.failure.toString()
        console.error('Invalid payload (decode failed)', message)
        return new Response(message, { status: 400, ...(headers !== undefined ? { headers } : {}) })
      }
      decodedPayload = decodedResult.success
    } else {
      decodedPayload = payload as TSyncPayload
    }

    const result = yield* Effect.promise(async () =>
      validatePayload(decodedPayload, { storeId, headers: requestHeaders }),
    ).pipe(UnknownError.mapToUnknownError, Effect.result)

    if (Result.isFailure(result) === true) {
      console.error('Invalid payload (validation failed)', result.failure)
      return new Response(result.failure.toString(), { status: 400, ...(headers !== undefined ? { headers } : {}) })
    }

    return undefined
  })

/** Applies caller headers (e.g. CORS) first, then route-specific ones, regardless of the `HeadersInit` shape. */
const makeResponse = (
  body: CfTypes.BodyInit | null,
  {
    status,
    headers,
    extraHeaders,
  }: { status: number; headers: CfTypes.HeadersInit | undefined; extraHeaders: Record<string, string> },
): CfTypes.Response => {
  const response = new Response(body, { status, ...(headers !== undefined ? { headers } : {}) })
  for (const [name, value] of Object.entries(extraHeaders)) response.headers.set(name, value)
  return response
}

const decodeSnapshotManifestSearchParams = Schema.decodeResult(
  Schema.fromURLSearchParams(SnapshotManifestSearchParamsSchema),
)
const decodeSnapshotBlobSearchParams = Schema.decodeResult(Schema.fromURLSearchParams(SnapshotBlobSearchParamsSchema))
