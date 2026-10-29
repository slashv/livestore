import { Snapshot } from '@livestore/common'
import type { CfTypes } from '@livestore/common-cf'
import {
  Effect,
  HttpClient,
  HttpClientRequest,
  type HttpClientResponse,
  Option,
  Schema,
  UrlParams,
} from '@livestore/utils/effect'

import { getManifestViaRpc, type SnapshotManifestRpcStub, trySnapshotPromise } from '../common/snapshot-rpc.ts'
import {
  readSnapshotBlobFromR2,
  SNAPSHOT_ROUTES,
  SnapshotBlobSearchParamsSchema,
  SnapshotManifestSearchParamsSchema,
} from '../common/snapshot.ts'

export interface HttpSnapshotSourceOptions {
  /**
   * URL of the sync backend, the same one passed to `makeWsSync` / `makeHttpSync`. `ws(s)://` is mapped to
   * `http(s)://`, and the snapshot routes are resolved relative to its path.
   *
   * @example 'wss://sync.example.com' → 'https://sync.example.com/snapshot/manifest'
   */
  url: string
  headers?: Record<string, string>
}

/**
 * Snapshot source reading from the worker's snapshot routes (see `makeWorker({ snapshots })`).
 * The store id and the sync payload are sent like the sync transports send them, so `validatePayload` applies.
 *
 * @example
 * ```ts
 * const adapter = makePersistedAdapter({
 *   sync: {
 *     backend: makeWsSync({ url }),
 *     snapshot: { source: makeHttpSnapshotSource({ url }) },
 *   },
 * })
 * ```
 *
 * @experimental
 */
export const makeHttpSnapshotSource =
  (options: HttpSnapshotSourceOptions): Snapshot.SnapshotSourceConstructor =>
  ({ storeId, payload }) =>
    Effect.gen(function* () {
      const httpClient = yield* HttpClient.HttpClient
      const baseUrl = toHttpBaseUrl(options.url)

      const get = ({ path, urlParams }: { path: string; urlParams: UrlParams.UrlParams }) =>
        httpClient
          .execute(
            HttpClientRequest.get(`${baseUrl}${path}`).pipe(
              HttpClientRequest.appendUrlParams(urlParams),
              HttpClientRequest.setHeaders(options.headers ?? {}),
            ),
          )
          .pipe(
            Effect.mapError(
              (cause) =>
                new Snapshot.SnapshotError({ reason: 'unavailable', message: `Request to ${path} failed`, cause }),
            ),
          )

      const getManifest: Snapshot.SnapshotSource['getManifest'] = Effect.fn(
        '@livestore/sync-cf:http-snapshot-source:getManifest',
      )(function* ({ key }) {
        const urlParams = yield* Schema.encodeEffect(SnapshotManifestSearchParamsSchema)({
          storeId,
          key,
          payload,
        }).pipe(
          Effect.map(UrlParams.fromInput),
          Effect.mapError(
            (cause) => new Snapshot.SnapshotError({ reason: 'internal', message: 'Invalid snapshot request', cause }),
          ),
        )
        const response = yield* get({ path: SNAPSHOT_ROUTES.manifest, urlParams })
        if (response.status === 404) return Option.none()
        yield* failUnlessOk(response, 'manifest')

        const manifest = yield* response.json.pipe(
          Effect.flatMap(Schema.decodeUnknownEffect(Snapshot.SnapshotManifest)),
          Effect.mapError(
            (cause) => new Snapshot.SnapshotError({ reason: 'internal', message: 'Invalid snapshot manifest', cause }),
          ),
        )
        if (manifest.storeId !== storeId || manifest.key !== key) {
          return yield* new Snapshot.SnapshotError({
            reason: 'incompatible',
            message: `Received manifest for ${manifest.storeId}/${manifest.key}, expected ${storeId}/${key}`,
          })
        }
        return Option.some(manifest)
      })

      const download: Snapshot.SnapshotSource['download'] = Effect.fn(
        '@livestore/sync-cf:http-snapshot-source:download',
      )(function* (manifest) {
        const urlParams = yield* Schema.encodeEffect(SnapshotBlobSearchParamsSchema)({
          storeId,
          objectKey: manifest.objectKey,
          payload,
        }).pipe(
          Effect.map(UrlParams.fromInput),
          Effect.mapError(
            (cause) => new Snapshot.SnapshotError({ reason: 'internal', message: 'Invalid snapshot request', cause }),
          ),
        )
        const response = yield* get({ path: SNAPSHOT_ROUTES.blob, urlParams })
        if (response.status === 404) {
          return yield* new Snapshot.SnapshotError({
            reason: 'not-found',
            message: `No snapshot blob at ${manifest.objectKey}`,
          })
        }
        yield* failUnlessOk(response, 'blob')

        // Refuse a blob of the wrong size before buffering it.
        const contentLength = response.headers['content-length']
        if (contentLength !== undefined && Number(contentLength) !== manifest.byteLength) {
          return yield* new Snapshot.SnapshotError({
            reason: 'integrity',
            message: `Snapshot blob has ${contentLength} bytes, manifest expects ${manifest.byteLength}`,
          })
        }

        const buffer = yield* response.arrayBuffer.pipe(
          Effect.mapError(
            (cause) =>
              new Snapshot.SnapshotError({ reason: 'unavailable', message: 'Snapshot blob download failed', cause }),
          ),
        )
        return new Uint8Array(buffer)
      })

      return { getManifest, download } satisfies Snapshot.SnapshotSource
    })

export interface DoRpcSnapshotSourceOptions {
  /** Returns a stub of the store's sync Durable Object (manifests are read via RPC). */
  getSyncBackendStub: () => SnapshotManifestRpcStub
  /** R2 bucket the checkpointer's sink writes to. */
  bucket: CfTypes.R2Bucket
}

/**
 * Snapshot source for LiveStore clients running inside Durable Objects. Reads the manifest from the sync Durable
 * Object via RPC and the blob straight from the R2 binding, bypassing the worker (and therefore `validatePayload`,
 * which is fine since bindings are only reachable from within the account).
 *
 * @experimental
 */
export const makeDoRpcSnapshotSource =
  ({ getSyncBackendStub, bucket }: DoRpcSnapshotSourceOptions): Snapshot.SnapshotSourceConstructor =>
  ({ storeId }) =>
    Effect.succeed({
      getManifest: ({ key }) => getManifestViaRpc({ getSyncBackendStub, storeId, key }),
      download: (manifest) =>
        trySnapshotPromise('unavailable', 'Failed to read snapshot blob from R2', () =>
          readSnapshotBlobFromR2({ bucket, storeId, manifest }),
        ).pipe(
          Effect.flatMap((result) =>
            result._tag === 'Ok'
              ? Effect.succeed(result.bytes)
              : Effect.fail(new Snapshot.SnapshotError({ reason: result.reason, message: result.message })),
          ),
          Effect.withSpan('@livestore/sync-cf:do-rpc-snapshot-source:download'),
        ),
    } satisfies Snapshot.SnapshotSource)

/** Maps the sync URL to the HTTP base the snapshot route suffixes are appended to. */
const toHttpBaseUrl = (url: string): string => {
  const parsed = new URL(url)
  if (parsed.protocol === 'ws:') parsed.protocol = 'http:'
  if (parsed.protocol === 'wss:') parsed.protocol = 'https:'
  parsed.search = ''
  parsed.hash = ''
  return parsed.toString().replace(/\/+$/, '')
}

const failUnlessOk = (response: HttpClientResponse.HttpClientResponse, what: string) =>
  response.status >= 200 && response.status < 300
    ? Effect.void
    : Effect.fail(
        new Snapshot.SnapshotError({
          reason: 'unavailable',
          message: `Snapshot ${what} request failed with status ${response.status}`,
        }),
      )
