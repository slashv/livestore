/// <reference types="@cloudflare/workers-types" />

import { DurableObject } from 'cloudflare:workers'

import {
  type CheckpointerDoRpcInterface,
  createStoreDoPromise,
  getCheckpointerStub,
  makeCheckpointerDurableObject,
} from '@livestore/adapter-cloudflare'
import { Snapshot } from '@livestore/common'
import { type ClientDoWithRpcCallback, setupDurableObjectWebSocketRpc } from '@livestore/common-cf'
import type { CfDeclare } from '@livestore/common-cf/declare'
import { type Store } from '@livestore/livestore'
import { schema, tables } from '@livestore/livestore/internal/testing-utils'
import {
  type CfTypes,
  handleSnapshotRequest,
  handleSyncRequest,
  makeDurableObject,
  makeR2SnapshotSink,
  matchSnapshotRequest,
  matchSyncRequest,
  PERSISTENCE_FORMAT_VERSION,
  rpcSubscriptionKeyPrefix,
  type SyncBackendRpcInterface,
  WebSocketAttachmentSchema,
} from '@livestore/sync-cf/cf-worker'
import { handleSyncUpdateRpc, makeDoRpcSnapshotSource, makeDoRpcSync } from '@livestore/sync-cf/client'
import {
  Cache,
  Effect,
  FetchHttpClient,
  KeyValueStore,
  Layer,
  Result,
  RpcServer,
  Schema,
  Scope,
  Stream,
  SubscriptionRef,
} from '@livestore/utils/effect'

import { DoRpcProxyRpcs } from './do-rpc-proxy-schema.ts'

declare class Request extends CfDeclare.Request {}
declare class Response extends CfDeclare.Response {}
declare class WebSocketPair extends CfDeclare.WebSocketPair {}

/** Module-scoped JSON decoder; keeping the sync codec out of Effect generators avoids `schemaSyncInEffect`. */
const jsonParse = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown))

/**
 * Records what each `onPush` observes, keyed by storeId, so a test can assert a transport threaded
 * the client payload through. Push and probe hit the same `idFromName(storeId)` DO instance, so this
 * module-scoped map bridges them.
 */
const observedPushPayloads = new Map<string, Schema.Json | undefined>()

/** Test-only outage admission and counters, isolated by each test's store id. */
const webSocketOutages = new Map<string, { until: number; openAttempts: number }>()

interface SyncDoProbe {
  getHibernationProbe(): {
    instanceId: string
    webSocketCount: number
    pullRequestIds: ReadonlyArray<string | number>
  }
  getPushProbe(storeId: string): { observed: boolean; payload: Schema.Json | null }
  getRpcSubscriptionCount(storeId: string): number
  closeWebSockets(): number
  getSnapshotContextProbe(): { backendId: string; head: number }
  markSnapshotManifestsStale(): number
}

interface TestClientDoProbe {
  getClientProbe(): { instanceId: string }
}

interface StoreClientDoProbe {
  bootStore(storeId: string): Promise<void>
  disposeStore(storeId: string): Promise<void>
  getStoreProbe(storeId: string): Promise<{ instanceId: string; todoIds: string[] }>
}

export interface Env {
  SYNC_BACKEND_DO: CfTypes.DurableObjectNamespace<SyncBackendRpcInterface & SyncDoProbe>
  TEST_CLIENT_DO: CfTypes.DurableObjectNamespace<ClientDoWithRpcCallback & TestClientDoProbe>
  STORE_CLIENT_DO: CfTypes.DurableObjectNamespace<ClientDoWithRpcCallback & StoreClientDoProbe>
  /** Eventlog database */
  DB: CfTypes.D1Database
  /** Snapshot blobs (experimental snapshot boot) */
  SNAPSHOT_BUCKET: CfTypes.R2Bucket
  /** Sync DO that wakes {@link CheckpointerDO} on push (snapshot boot end-to-end tests). */
  SNAPSHOT_SYNC_BACKEND_DO: CfTypes.DurableObjectNamespace<SyncBackendRpcInterface & SnapshotSyncDoProbe>
  CHECKPOINTER_DO: CfTypes.DurableObjectNamespace<CheckpointerDoRpcInterface>
  /** Checkpointer that never publishes on its own, for DO limit measurements against `SYNC_BACKEND_DO`. */
  MEASURE_CHECKPOINTER_DO: CfTypes.DurableObjectNamespace<CheckpointerDoRpcInterface>
  /** Same as `MEASURE_CHECKPOINTER_DO` with the in-memory (buffered) export. */
  MEASURE_BUFFERED_CHECKPOINTER_DO: CfTypes.DurableObjectNamespace<CheckpointerDoRpcInterface>
}

interface SnapshotSyncDoProbe {
  resetBackendForTest(): Promise<void>
}

/** Snapshot key of the test schema; the snapshot sync DO wakes this key's checkpointer on push. */
const testSnapshotKey = Snapshot.SnapshotKey.toString(Snapshot.SnapshotKey.make(schema))

/**
 * Sync backend for snapshot boot end-to-end tests. Separate from {@link SyncBackendDO} so other suites don't boot
 * checkpointers (which would add live-pull subscriptions) for their stores.
 */
export class SnapshotSyncBackendDO extends makeDurableObject({
  snapshots: { checkpointerBinding: 'CHECKPOINTER_DO', snapshotKeys: [testSnapshotKey], wakeIntervalMs: 1_000 },
}) {
  #state: CfTypes.DurableObjectState

  constructor(state: CfTypes.DurableObjectState, env: Env) {
    super(state, env)
    this.#state = state
  }

  /**
   * Simulates a backend reset: wipes all storage (events, backend identity, snapshot manifests, subscriptions) and
   * aborts the instance so the next request starts with a fresh backendId.
   */
  async resetBackendForTest(): Promise<void> {
    // No other request may observe the wiped storage through this instance's cached backend identity.
    await this.#state.blockConcurrencyWhile(async () => {
      await this.#state.storage.deleteAll()
      this.#state.abort('backend reset (test)')
    })
  }
}

/** Publishes as soon as 100 events accumulated; tests flush the rest with `publishNow`. */
export class CheckpointerDO extends makeCheckpointerDurableObject<Env>({
  schema,
  syncBackendBinding: 'SNAPSHOT_SYNC_BACKEND_DO',
  bucketBinding: 'SNAPSHOT_BUCKET',
  bindingName: 'CHECKPOINTER_DO',
  trigger: { minTailEvents: 100, minIntervalMs: 0, alpha: 0 },
  pollInterval: 200,
  keepAlive: { alarmIntervalMs: 2_000, idleTimeoutMs: 10_000 },
}) {}

export class MeasureCheckpointerDO extends makeCheckpointerDurableObject<Env>({
  schema,
  syncBackendBinding: 'SYNC_BACKEND_DO',
  bucketBinding: 'SNAPSHOT_BUCKET',
  bindingName: 'MEASURE_CHECKPOINTER_DO',
  trigger: { minTailEvents: Number.MAX_SAFE_INTEGER },
}) {}

export class MeasureBufferedCheckpointerDO extends makeCheckpointerDurableObject<Env>({
  schema,
  syncBackendBinding: 'SYNC_BACKEND_DO',
  bucketBinding: 'SNAPSHOT_BUCKET',
  bindingName: 'MEASURE_BUFFERED_CHECKPOINTER_DO',
  trigger: { minTailEvents: Number.MAX_SAFE_INTEGER },
  exportMode: 'buffered',
}) {}

export class SyncBackendDO extends makeDurableObject({
  onPush: (_message, context) => {
    observedPushPayloads.set(context.storeId, context.payload)
  },
  http: {
    responseHeaders: {
      'X-Custom-Header': 'test-value',
      'X-LiveStore-Version': '1.0.0',
    },
  },
}) {
  /** Never persisted: a different id means this DO was evicted and rebuilt. */
  instanceId = crypto.randomUUID()
  #state: CfTypes.DurableObjectState

  constructor(state: CfTypes.DurableObjectState, env: Env) {
    super(state, env)
    this.#state = state
  }

  getHibernationProbe(): {
    instanceId: string
    webSocketCount: number
    pullRequestIds: ReadonlyArray<string | number>
  } {
    const webSockets = this.#state.getWebSockets()
    const pullRequestIds = webSockets.flatMap(
      (ws) => Schema.decodeUnknownSync(WebSocketAttachmentSchema)(ws.deserializeAttachment()).pullRequestIds,
    )
    return { instanceId: this.instanceId, webSocketCount: webSockets.length, pullRequestIds }
  }

  closeWebSockets(): number {
    const sockets = this.#state.getWebSockets()
    for (const socket of sockets) socket.close(1012, 'Test backend restart')
    return sockets.length
  }

  getPushProbe(storeId: string): { observed: boolean; payload: Schema.Json | null } {
    return {
      observed: observedPushPayloads.has(storeId),
      payload: observedPushPayloads.get(storeId) ?? null,
    }
  }

  /** Backend identity and head as persisted by the sync DO, so snapshot tests can build valid manifests. */
  getSnapshotContextProbe(): { backendId: string; head: number } {
    const row = this.#state.storage.sql
      .exec<{ backendId: string; currentHead: number }>(
        `SELECT backendId, currentHead FROM "context_${PERSISTENCE_FORMAT_VERSION}"`,
      )
      .one()
    return { backendId: row.backendId, head: row.currentHead }
  }

  /** Simulates manifests left over from an earlier backend identity (e.g. before a backend reset). */
  markSnapshotManifestsStale(): number {
    return this.#state.storage.sql.exec(
      `UPDATE "snapshot_manifests_${PERSISTENCE_FORMAT_VERSION}" SET backendId = 'stale-backend-id'`,
    ).rowsWritten
  }

  /** DO-RPC live-pull subscriptions persisted for this store (all rows belong to this per-store DO). */
  getRpcSubscriptionCount(_storeId: string): number {
    return Array.from(this.#state.storage.kv.list({ prefix: rpcSubscriptionKeyPrefix })).length
  }
}

const DurableObjectBase = DurableObject as any as new (
  state: CfTypes.DurableObjectState,
  env: Env,
) => CfTypes.DurableObject

export class TestClientDo extends DurableObjectBase implements ClientDoWithRpcCallback {
  __DURABLE_OBJECT_BRAND = 'ClientDO' as never
  env: Env
  ctx: CfTypes.DurableObjectState
  /** Never persisted: a different id means this DO was evicted and rebuilt. */
  instanceId = crypto.randomUUID()

  constructor(state: CfTypes.DurableObjectState, env: Env) {
    super(state, env)
    this.ctx = state
    this.env = env

    /**
     * RPC handlers run in request scopes, but cached clients must live for the longer-lived WebSocket server scope.
     */
    const handlersLayer = Layer.unwrap(
      Effect.gen({ self: this }, function* () {
        const syncBackendScope = yield* Effect.scope
        const syncBackendCache = yield* Cache.make({
          capacity: Number.POSITIVE_INFINITY,
          lookup: (key: string) =>
            Effect.gen({ self: this }, function* () {
              const { clientId, storeId, payload } = jsonParse(key) as SyncBackendArgs

              return yield* makeDoRpcSync({
                getSyncBackendStub: () => this.env.SYNC_BACKEND_DO.get(this.env.SYNC_BACKEND_DO.idFromName(storeId)),
                durableObjectState: this.ctx,
                durableObjectContext: { bindingName: 'TEST_CLIENT_DO', durableObjectId: this.ctx.id.toString() },
              })({ storeId, clientId, payload }).pipe(Scope.provide(syncBackendScope), Effect.orDie)
            }),
        })

        const getSyncBackend = (args: SyncBackendArgs) => Cache.get(syncBackendCache, JSON.stringify(args))

        return DoRpcProxyRpcs.toLayer({
          Connect: (args) =>
            Effect.gen(function* () {
              const syncBackend = yield* getSyncBackend(args)
              yield* syncBackend.connect
            }),
          Pull: (args) =>
            Effect.gen(function* () {
              const syncBackend = yield* getSyncBackend(args)
              return syncBackend.pull(args.cursor as any, { live: args.live })
            }).pipe(
              Stream.unwrap,
              Stream.map((msg) => ({ ...msg, backendId: 'TODO' })),
            ),
          Push: ({ batch, ...args }) =>
            Effect.gen(function* () {
              const syncBackend = yield* getSyncBackend(args)
              yield* syncBackend.push(batch)
            }),
          Ping: (args) =>
            Effect.gen(function* () {
              const syncBackend = yield* getSyncBackend(args)
              yield* syncBackend.ping
            }).pipe(Effect.orDie),
          IsConnected: (args) =>
            Effect.gen(function* () {
              const syncBackend = yield* getSyncBackend(args)
              return SubscriptionRef.changes(syncBackend.isConnected)
            }).pipe(Stream.unwrap),
          GetMetadata: (args) =>
            Effect.gen(function* () {
              const syncBackend = yield* getSyncBackend(args)
              return syncBackend.metadata
            }),
        })
      }),
    )

    const ServerLive = RpcServer.layer(DoRpcProxyRpcs).pipe(
      Layer.provide(handlersLayer),
      Layer.provide(FetchHttpClient.layer),
      Layer.provide(KeyValueStore.layerMemory),
    )

    setupDurableObjectWebSocketRpc({
      doSelf: this,
      rpcLayer: ServerLive,
      webSocketMode: 'hibernate',
    })
  }

  override async fetch(request: CfTypes.Request): Promise<CfTypes.Response> {
    const upgradeHeader = request.headers.get('Upgrade')
    if (upgradeHeader === undefined || upgradeHeader !== 'websocket') {
      return new Response('Durable Object expected Upgrade: websocket', { status: 426 })
    }

    const { 0: client, 1: server } = new WebSocketPair()

    // Accept the connection
    // We'd use this for non-hibernated WebSocket connections
    // server.accept()
    // server.addEventListener('message', (event) => {
    //   console.log('message from client', event.data)
    //   Queue.offer(incomingQueue, event.data).pipe(Effect.tapCauseLogPretty, Effect.runFork)
    // })

    // Hibernate the server
    this.ctx.acceptWebSocket(server)

    // await this.launchServer(server)

    return new Response(null, {
      status: 101,
      webSocket: client,
    })
  }

  getClientProbe(): { instanceId: string } {
    return { instanceId: this.instanceId }
  }

  async syncUpdateRpc(payload: Uint8Array<ArrayBuffer>) {
    await handleSyncUpdateRpc(this.ctx, payload)
  }
}

/**
 * A real LiveStore client running inside a DO: it boots a store that subscribes to the sync backend,
 * so a reverse-RPC update is applied to (and materialized in) its own store. On reconstruction the
 * store-less wake re-boots from the storeId carried in the reverse-RPC and catches up.
 */
export class StoreClientDo extends DurableObjectBase implements ClientDoWithRpcCallback {
  __DURABLE_OBJECT_BRAND = 'ClientDO' as never
  env: Env
  ctx: CfTypes.DurableObjectState
  /** Never persisted: a different id means this DO was evicted and rebuilt. */
  instanceId = crypto.randomUUID()
  /** The live `livePull` subscriber. Dropped on reconstruction; a reverse-RPC wake re-boots it. */
  #store: Store<typeof schema> | undefined

  constructor(state: CfTypes.DurableObjectState, env: Env) {
    super(state, env)
    this.ctx = state
    this.env = env
  }

  async bootStore(storeId: string): Promise<void> {
    await this.#boot(storeId)
  }

  /** Graceful client-done: an explicit store shutdown closes the sync backend scope (finalizers run). */
  async disposeStore(_storeId: string): Promise<void> {
    if (this.#store === undefined) return
    await this.#store.shutdownPromise()
    this.#store = undefined
  }

  /** Materialized todos read from the (possibly just-recovered) store. */
  async getStoreProbe(_storeId: string): Promise<{ instanceId: string; todoIds: string[] }> {
    const todoIds = this.#store === undefined ? [] : this.#store.query(tables.todos).map((todo) => todo.id)
    return { instanceId: this.instanceId, todoIds }
  }

  async syncUpdateRpc(payload: Uint8Array<ArrayBuffer>, storeId: string) {
    await this.#boot(storeId)
    await handleSyncUpdateRpc(this.ctx, payload)
  }

  async #boot(storeId: string): Promise<void> {
    if (this.#store !== undefined) return
    this.#store = await createStoreDoPromise({
      schema,
      storeId,
      clientId: 'store-client-do',
      sessionId: 'store-client-do-session',
      durableObject: { ctx: this.ctx, env: this.env, bindingName: 'STORE_CLIENT_DO' },
      getSyncBackendStub: () => this.env.SYNC_BACKEND_DO.get(this.env.SYNC_BACKEND_DO.idFromName(storeId)),
      livePull: true,
    })
  }
}

export default {
  fetch: async (request: CfTypes.Request, env: Env, ctx: CfTypes.ExecutionContext) => {
    const url = new URL(request.url)

    if (url.pathname.endsWith('/instance/sync') === true) {
      const storeId = url.searchParams.get('storeId')
      if (storeId === null) {
        return new Response('storeId required', { status: 400 })
      }
      const probe = await env.SYNC_BACKEND_DO.get(env.SYNC_BACKEND_DO.idFromName(storeId)).getHibernationProbe()
      return new Response(JSON.stringify(probe), { headers: { 'content-type': 'application/json' } })
    }

    if (url.pathname.endsWith('/instance/close-websockets') === true && request.method === 'POST') {
      const storeId = url.searchParams.get('storeId')
      if (storeId === null) return new Response('storeId required', { status: 400 })
      webSocketOutages.set(storeId, { until: Date.now() + 20_000, openAttempts: 0 })
      const count = await env.SYNC_BACKEND_DO.get(env.SYNC_BACKEND_DO.idFromName(storeId)).closeWebSockets()
      return new Response(JSON.stringify({ count }), { headers: { 'content-type': 'application/json' } })
    }

    if (url.pathname.endsWith('/instance/websocket-outage') === true) {
      const storeId = url.searchParams.get('storeId')
      if (storeId === null) return new Response('storeId required', { status: 400 })
      return new Response(JSON.stringify(webSocketOutages.get(storeId)), {
        headers: { 'content-type': 'application/json' },
      })
    }

    if (url.pathname.endsWith('/instance/push-probe') === true) {
      const storeId = url.searchParams.get('storeId')
      if (storeId === null) {
        return new Response('storeId required', { status: 400 })
      }
      const probe = await env.SYNC_BACKEND_DO.get(env.SYNC_BACKEND_DO.idFromName(storeId)).getPushProbe(storeId)
      return new Response(JSON.stringify(probe), { headers: { 'content-type': 'application/json' } })
    }

    if (url.pathname.endsWith('/instance/rpc-subs') === true) {
      const storeId = url.searchParams.get('storeId')
      if (storeId === null) {
        return new Response('storeId required', { status: 400 })
      }
      const count = await env.SYNC_BACKEND_DO.get(env.SYNC_BACKEND_DO.idFromName(storeId)).getRpcSubscriptionCount(
        storeId,
      )
      return new Response(JSON.stringify({ count }), { headers: { 'content-type': 'application/json' } })
    }

    if (url.pathname.endsWith('/instance/client') === true) {
      const probe = await env.TEST_CLIENT_DO.get(env.TEST_CLIENT_DO.idFromName('test-client-do')).getClientProbe()
      return new Response(JSON.stringify(probe), { headers: { 'content-type': 'application/json' } })
    }

    if (url.pathname.endsWith('/store/boot') === true) {
      const storeId = url.searchParams.get('storeId')
      if (storeId === null) {
        return new Response('storeId required', { status: 400 })
      }
      await env.STORE_CLIENT_DO.get(env.STORE_CLIENT_DO.idFromName(storeId)).bootStore(storeId)
      return new Response(null, { status: 204 })
    }

    if (url.pathname.endsWith('/store/dispose') === true) {
      const storeId = url.searchParams.get('storeId')
      if (storeId === null) {
        return new Response('storeId required', { status: 400 })
      }
      await env.STORE_CLIENT_DO.get(env.STORE_CLIENT_DO.idFromName(storeId)).disposeStore(storeId)
      return new Response(null, { status: 204 })
    }

    if (url.pathname.endsWith('/store/probe') === true) {
      const storeId = url.searchParams.get('storeId')
      if (storeId === null) {
        return new Response('storeId required', { status: 400 })
      }
      const probe = await env.STORE_CLIENT_DO.get(env.STORE_CLIENT_DO.idFromName(storeId)).getStoreProbe(storeId)
      return new Response(JSON.stringify(probe), { headers: { 'content-type': 'application/json' } })
    }

    if (url.pathname.startsWith('/snapshot-e2e/') === true) {
      return handleSnapshotE2eRequest({ request, url, env, ctx })
    }

    if (url.pathname.includes('/snapshot-test/') === true) {
      return handleSnapshotTestRequest({ request, url, env })
    }

    const snapshotRequest = matchSnapshotRequest(request)
    if (snapshotRequest !== undefined) {
      return handleSnapshotRequest({
        request,
        snapshotRequest,
        env,
        syncBackendBinding: 'SYNC_BACKEND_DO',
        bucketBinding: 'SNAPSHOT_BUCKET',
        validatePayload: (payload) => {
          if (typeof payload === 'object' && payload !== null && 'deny' in payload) {
            throw new Error('Snapshot access denied by test validatePayload')
          }
        },
      })
    }

    const searchParams = matchSyncRequest(request)
    if (searchParams !== undefined) {
      const outage = webSocketOutages.get(searchParams.storeId)
      if (outage !== undefined && request.headers.get('Upgrade')?.toLowerCase() === 'websocket') {
        outage.openAttempts++
        if (Date.now() < outage.until) return new Response('Test backend unavailable', { status: 503 })
      }
      return handleSyncRequest({
        request,
        searchParams,
        env,
        ctx,
        syncBackendBinding: 'SYNC_BACKEND_DO',
      })
    }

    if (url.pathname.endsWith('/do-rpc-ws-proxy') === true) {
      const doName = 'test-client-do' // TODO make configurable/dynamic

      const id = env.TEST_CLIENT_DO.idFromName(doName)
      const durableObject = env.TEST_CLIENT_DO.get(id)

      return durableObject.fetch(request)
    }

    return new Response('Invalid path', { status: 400 })
  },
}

type SyncBackendArgs = {
  clientId: string
  storeId: string
  payload: any
}

/**
 * Test-only endpoints driving the snapshot sink and the DO-RPC snapshot source inside workerd, where the R2 and Durable
 * Object bindings live. The sink normally runs in a checkpointer DO; a worker request exercises the same bindings.
 */
const handleSnapshotTestRequest = async ({
  request,
  url,
  env,
}: {
  request: CfTypes.Request
  url: URL
  env: Env
}): Promise<CfTypes.Response> => {
  const storeId = url.searchParams.get('storeId')
  if (storeId === null) return new Response('storeId required', { status: 400 })

  const getSyncBackendStub = () => env.SYNC_BACKEND_DO.get(env.SYNC_BACKEND_DO.idFromName(storeId))
  const json = (value: unknown, status = 200) =>
    new Response(JSON.stringify(value), { status, headers: { 'content-type': 'application/json' } })

  if (url.pathname.endsWith('/snapshot-test/context') === true) {
    return json(await getSyncBackendStub().getSnapshotContextProbe())
  }

  if (url.pathname.endsWith('/snapshot-test/mark-stale') === true) {
    return json({ rowsWritten: await getSyncBackendStub().markSnapshotManifestsStale() })
  }

  if (url.pathname.endsWith('/snapshot-test/put-manifest') === true) {
    return json(await getSyncBackendStub().putSnapshotManifest(await request.text()))
  }

  if (url.pathname.endsWith('/snapshot-test/put-orphan') === true) {
    // A blob under the store's prefix that no manifest points at (e.g. left behind by a failed publish).
    const objectKey = `snapshots/${encodeURIComponent(storeId)}/orphan/${crypto.randomUUID()}.sqlite`
    await env.SNAPSHOT_BUCKET.put(objectKey, new Uint8Array(16), { customMetadata: { storeId } })
    return json({ objectKey })
  }

  if (url.pathname.endsWith('/snapshot-test/blobs') === true) {
    const listed = await env.SNAPSHOT_BUCKET.list({ prefix: `snapshots/${encodeURIComponent(storeId)}/` })
    return json(listed.objects.map((object) => object.key).toSorted())
  }

  if (url.pathname.endsWith('/snapshot-test/publish') === true) {
    const { manifest, bytesBase64 } = jsonParse(await request.text()) as {
      manifest: Omit<Snapshot.SnapshotManifest, 'objectKey'>
      bytesBase64: string
    }
    const sink = makeR2SnapshotSink({
      bucket: env.SNAPSHOT_BUCKET,
      storeId,
      getSyncBackendStub,
      graceMs: Number(url.searchParams.get('graceMs') ?? 60_000),
    })
    const bytes = Uint8Array.from(atob(bytesBase64), (char) => char.charCodeAt(0))
    const result = await sink.publish({ manifest, bytes }).pipe(Effect.result, Effect.runPromise)
    return Result.isSuccess(result) === true
      ? json(result.success)
      : json({ reason: result.failure.reason, message: result.failure.message }, 409)
  }

  if (url.pathname.endsWith('/snapshot-test/do-rpc-source') === true) {
    const key = url.searchParams.get('key') ?? ''
    const result = await Effect.gen(function* () {
      const source = yield* makeDoRpcSnapshotSource({ getSyncBackendStub, bucket: env.SNAPSHOT_BUCKET })({
        storeId,
        clientId: 'snapshot-test',
        payload: undefined,
      })
      const manifest = yield* source.getManifest({ key })
      if (manifest._tag === 'None') return { manifest: null, sha256: null }
      const bytes = yield* source.download(manifest.value)
      return { manifest: manifest.value, sha256: Snapshot.sha256Hex(bytes) }
    }).pipe(Effect.scoped, Effect.provide(FetchHttpClient.layer), Effect.runPromise)
    return json(result)
  }

  return new Response('Invalid snapshot test path', { status: 400 })
}

/**
 * Routes under `/snapshot-e2e/` use the checkpointer-enabled {@link SnapshotSyncBackendDO}: sync traffic, the snapshot
 * routes, and test endpoints to drive the checkpointers. `/snapshot-e2e/measure/*` drives {@link MeasureCheckpointerDO}.
 */
const handleSnapshotE2eRequest = async ({
  request,
  url,
  env,
  ctx,
}: {
  request: CfTypes.Request
  url: URL
  env: Env
  ctx: CfTypes.ExecutionContext
}): Promise<CfTypes.Response> => {
  const snapshotRequest = matchSnapshotRequest(request)
  if (snapshotRequest !== undefined) {
    return handleSnapshotRequest({
      request,
      snapshotRequest,
      env,
      syncBackendBinding: 'SNAPSHOT_SYNC_BACKEND_DO',
      bucketBinding: 'SNAPSHOT_BUCKET',
    })
  }

  const searchParams = matchSyncRequest(request)
  if (searchParams !== undefined) {
    return handleSyncRequest({ request, searchParams, env, ctx, syncBackendBinding: 'SNAPSHOT_SYNC_BACKEND_DO' })
  }

  const storeId = url.searchParams.get('storeId')
  if (storeId === null) return new Response('storeId required', { status: 400 })
  const json = (value: unknown) =>
    new Response(JSON.stringify(value), { headers: { 'content-type': 'application/json' } })

  const namespace =
    url.pathname.includes('/measure-buffered/') === true
      ? env.MEASURE_BUFFERED_CHECKPOINTER_DO
      : url.pathname.includes('/measure/') === true
        ? env.MEASURE_CHECKPOINTER_DO
        : env.CHECKPOINTER_DO
  const checkpointer = getCheckpointerStub({ namespace, storeId, schema })

  if (url.pathname.endsWith('/checkpointer/ensure-running') === true) {
    await checkpointer.ensureRunning(storeId)
    return json(null)
  }
  if (url.pathname.endsWith('/checkpointer/status') === true) {
    return json(await checkpointer.status())
  }
  if (url.pathname.endsWith('/checkpointer/stats') === true) {
    return json(await checkpointer.stats(storeId))
  }
  if (url.pathname.endsWith('/checkpointer/publish-now') === true) {
    return json(await checkpointer.publishNow(storeId))
  }
  if (url.pathname.endsWith('/reset-backend') === true) {
    await env.SNAPSHOT_SYNC_BACKEND_DO.get(env.SNAPSHOT_SYNC_BACKEND_DO.idFromName(storeId))
      .resetBackendForTest()
      .catch(() => {
        // `abort()` rejects the in-flight call by design.
      })
    return json(null)
  }

  return new Response('Invalid snapshot e2e path', { status: 400 })
}
