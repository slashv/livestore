import { IsOfflineError, SyncBackend, UnknownError } from '@livestore/common'
import { type CfTypes, DoRpcReadError, layerProtocolDurableObject } from '@livestore/common-cf'
import { EventSequenceNumber } from '@livestore/common/schema'
import { splitArrayBySize } from '@livestore/common/sync'
import { shouldNeverHappen } from '@livestore/utils'
import {
  Duration,
  Effect,
  identity,
  Layer,
  Match,
  Option,
  Predicate,
  Queue,
  Random,
  ReadonlyArray as EffectArray,
  Ref,
  RpcClient,
  RpcSerialization,
  Schema,
  Stream,
  Struct,
  SubscriptionRef,
} from '@livestore/utils/effect'

import type { SyncBackendRpcInterface } from '../../cf-worker/shared.ts'
import { MAX_DO_RPC_REQUEST_BYTES, MAX_PUSH_EVENTS_PER_REQUEST } from '../../common/constants.ts'
import { SyncDoRpc } from '../../common/do-rpc-schema.ts'
import { SyncMessage } from '../../common/mod.ts'
import type { SyncMetadata } from '../../common/sync-message-types.ts'

type PushBatchItem = SyncMessage.PushRequest['batch'][number]

export interface SyncBackendRpcStub extends CfTypes.DurableObjectStub, SyncBackendRpcInterface {}

export interface DoRpcSyncOptions {
  /**
   * Returns a stub for the sync backend Durable Object. Called for every RPC call, because Cloudflare leaves a stub
   * broken after many exceptions, so a retry needs a fresh one.
   */
  getSyncBackendStub: () => SyncBackendRpcStub
  /**
   * State handle of the client DurableObject running this sync backend. Scopes live-pull routing to
   * this instance so it resets when the DO is reconstructed (see {@link handleSyncUpdateRpc}).
   */
  durableObjectState: CfTypes.DurableObjectState
  /** Information about this DurableObject instance so the Sync DO instance can call back to this instance */
  durableObjectContext: {
    /** See `wrangler.toml` for the binding name */
    bindingName: string
    /** `state.id.toString()` in the DO */
    durableObjectId: string
  }
}

/**
 * Creates a sync backend that uses Durable Object RPC to communicate with the sync backend.
 *
 * Used internally by `@livestore/adapter-cf` to connect to the sync backend.
 */
export const makeDoRpcSync =
  ({
    getSyncBackendStub,
    durableObjectState,
    durableObjectContext,
  }: DoRpcSyncOptions): SyncBackend.SyncBackendConstructor<SyncMetadata> =>
  ({ storeId, payload }) =>
    Effect.gen(function* () {
      const isConnected = yield* SubscriptionRef.make(true)

      const ProtocolLive = layerProtocolDurableObject({
        callRpc: (payload) => getSyncBackendStub().rpc(payload),
        callerContext: durableObjectContext,
      })

      const context = yield* Layer.build(ProtocolLive)

      const rpcClient = yield* RpcClient.make(SyncDoRpc).pipe(Effect.provide(context))

      const pullBackoff = yield* makePullBackoff

      // Nothing to do here
      const connect = Effect.void

      const backendIdHelper = yield* SyncBackend.makeBackendIdHelper

      const pull: SyncBackend.SyncBackend<SyncMetadata>['pull'] = (cursor, options) => {
        /**
         * Highest backend event delivered by this pull. Live updates are callbacks racing the catch-up stream: one may
         * repeat an event the catch-up already delivered (dropped here), and one may be lost before this pull's queue
         * is registered or after the buffer overflowed (detected as a gap; the pull then fails as offline, so the
         * leader re-pulls from its cursor instead of merging a non-contiguous batch).
         */
        let lastDeliveredSeqNum: number = Option.match(cursor, {
          onNone: () => EventSequenceNumber.Client.ROOT.global,
          onSome: (_) => _.eventSequenceNumber,
        })
        const trackDelivered = (res: SyncMessage.PullResponse) => {
          const last = res.batch.at(-1)
          if (last !== undefined && last.eventEncoded.seqNum > lastDeliveredSeqNum) {
            lastDeliveredSeqNum = last.eventEncoded.seqNum
          }
        }
        const sequenceLiveUpdate = (res: SyncMessage.PullResponse) =>
          Effect.suspend(() => {
            const batch = res.batch.filter((item) => item.eventEncoded.seqNum > lastDeliveredSeqNum)
            const first = batch[0]
            if (first === undefined) return Effect.succeed([])
            if (first.eventEncoded.parentSeqNum !== lastDeliveredSeqNum) {
              return Effect.fail(
                new IsOfflineError({
                  cause: new Error(
                    `DO-RPC live pull missed updates (expected parent ${lastDeliveredSeqNum}, got ${first.eventEncoded.parentSeqNum})`,
                  ),
                }),
              )
            }
            const sequenced = { ...res, batch }
            trackDelivered(sequenced)
            return Effect.succeed([sequenced])
          })

        return rpcClient['SyncDoRpc.Pull']({
          cursor: cursor.pipe(
            Option.map((a) => ({
              eventSequenceNumber: a.eventSequenceNumber,
              backendId: backendIdHelper.get().pipe(Option.getOrThrow),
            })),
          ),
          storeId,
          rpcContext: options?.live === true ? { callerContext: durableObjectContext } : undefined,
        }).pipe(
          // Catch-up must make progress. Live callbacks below may legitimately remain idle indefinitely.
          Stream.timeoutOrElse({
            duration: '60 seconds',
            orElse: () =>
              Stream.fail(new IsOfflineError({ cause: new Error('DO-RPC pull made no progress for 60 seconds') })),
          }),
          Stream.tap((res) => Effect.sync(() => trackDelivered(res))),
          options?.live === true
            ? Stream.concatWithLastElement((res) =>
                Effect.gen(function* () {
                  if (res._tag === 'None')
                    return shouldNeverHappen('There should at least be a no-more page info response')

                  const requestId = res.value.rpcRequestId
                  const routing = pullRoutingFor(durableObjectState)

                  const queue = yield* Effect.acquireRelease(Queue.unbounded<SyncMessage.PullResponse>(), (queue) =>
                    // Drop the routing entry on release so it can't outlive its (now shut-down) queue
                    Effect.sync(() => routing.delete(requestId)).pipe(Effect.andThen(Queue.shutdown(queue))),
                  )

                  // Callbacks that arrived while the catch-up was still streaming were buffered; claim them in the
                  // same synchronous step that registers the queue so none slip in between.
                  yield* Effect.sync(() => {
                    routing.set(requestId, queue)
                    return takePendingUpdates(durableObjectState, requestId)
                  }).pipe(Effect.flatMap((pending) => Queue.offerAll(queue, pending)))

                  // Graceful shutdown only (eviction runs no finalizers); matched on requestId so a late send can't drop a newer pull's row
                  yield* Effect.addFinalizer(() =>
                    rpcClient['SyncDoRpc.Unsubscribe']({
                      storeId,
                      durableObjectId: durableObjectContext.durableObjectId,
                      requestId,
                    }).pipe(Effect.timeout('5 seconds'), Effect.tapCauseLogPretty, Effect.ignore),
                  )

                  return Stream.fromQueue(queue).pipe(
                    Stream.mapEffect(sequenceLiveUpdate),
                    Stream.flatMap(Stream.fromIterable),
                  )
                }).pipe(Stream.unwrap),
              )
            : identity,
          Stream.catchReason('RpcClientError', 'RpcClientDefect', (reason) =>
            Stream.fromEffect(classifyTransportFailure(reason.cause)),
          ),
          Stream.tapError((error) => (error._tag === 'IsOfflineError' ? pullBackoff.wait : Effect.void)),
          Stream.tap((res) => (res.pageInfo._tag === 'NoMore' ? pullBackoff.reset : Effect.void)),
          Stream.tap((res) => backendIdHelper.lazySet(res.backendId)),
          Stream.map((res) => Struct.omit(res, ['backendId'])),
          Stream.mapError((cause) =>
            cause._tag === 'UnknownError' || cause._tag === 'BackendIdMismatchError' || cause._tag === 'IsOfflineError'
              ? cause
              : new UnknownError({ cause }),
          ),
          Stream.withSpan('rpc-sync-client:pull'),
        )
      }

      const push: SyncBackend.SyncBackend<{ createdAt: string }>['push'] = Effect.fn('rpc-sync-client:push')(
        function* (batch) {
          if (batch.length === 0) {
            return
          }

          const backendId = backendIdHelper.get()
          if (EffectArray.isReadonlyArrayNonEmpty(batch) === false) {
            return
          }

          const batchChunks = yield* splitArrayBySize({
            maxItems: MAX_PUSH_EVENTS_PER_REQUEST,
            maxBytes: MAX_DO_RPC_REQUEST_BYTES,
            encode: (items: ReadonlyArray<PushBatchItem>) => ({
              batch: items,
              storeId,
              backendId,
            }),
          })(batch).pipe(Effect.mapError((cause) => new UnknownError({ cause })))

          for (const batchChunk of batchChunks) {
            yield* rpcClient['SyncDoRpc.Push']({ batch: batchChunk, storeId, backendId })
          }
        },
        Effect.catchReason('RpcClientError', 'RpcClientDefect', (reason) => classifyTransportFailure(reason.cause)),
        Effect.mapError((cause) =>
          cause._tag === 'UnknownError' ||
          cause._tag === 'ServerAheadError' ||
          cause._tag === 'BackendIdMismatchError' ||
          cause._tag === 'IsOfflineError'
            ? cause
            : new UnknownError({ cause }),
        ),
      )

      const ping: SyncBackend.SyncBackend<{ createdAt: string }>['ping'] = rpcClient['SyncDoRpc.Ping']({
        storeId,
        payload,
      }).pipe(
        Effect.catchReason('RpcClientError', 'RpcClientDefect', (reason) => classifyTransportFailure(reason.cause)),
        Effect.mapError((cause) => (cause._tag === 'IsOfflineError' ? cause : new UnknownError({ cause }))),
        Effect.withSpan('rpc-sync-client:ping'),
      )

      return SyncBackend.of({
        connect,
        isConnected,
        pull,
        push,
        ping,
        metadata: {
          name: 'rpc-sync-client',
          description: 'Cloudflare Durable Object RPC Sync Client',
          protocol: 'rpc',
          storeId,
        },
        supports: {
          pullPageInfoKnown: true,
          pullLive: true,
        },
      })
    }).pipe(Effect.withSpan('rpc-sync-client:makeDoRpcSync'))

/**
 * Routes an update from the sync backend into this client's live pull.
 *
 * Only `ctx` and `payload` go here; `storeId` is for reloading your store on a rebuilt DO (see example).
 *
 * ```ts
 * import { DurableObject } from 'cloudflare:workers'
 * import { ClientDoWithRpcCallback } from '@livestore/common-cf'
 *
 * export class MyDurableObject extends DurableObject implements ClientDoWithRpcCallback {
 *   // ...
 *
 *   async syncUpdateRpc(payload: Uint8Array<ArrayBuffer>, storeId: string) {
 *     await this.getStore(storeId)
 *     return handleSyncUpdateRpc(this.ctx, payload)
 *   }
 * }
 * ```
 */
export const handleSyncUpdateRpc = (ctx: CfTypes.DurableObjectState, payload: Uint8Array<ArrayBuffer>) =>
  Effect.gen(function* () {
    const serialization = yield* RpcSerialization.RpcSerialization
    const parser = serialization.makeUnsafe()
    const [response] = parser.decode(payload)
    const decodedPayload = yield* Schema.decodeUnknownEffect(ResponseChunkEncoded)(response)
    const decoded = yield* Schema.decodeEffect(serialization.codecFor(Schema.NonEmptyArray(SyncMessage.PullResponse)))(
      decodedPayload.values,
    )

    const pullStreamQueue = pullRoutingFor(ctx).get(decodedPayload.requestId)

    if (pullStreamQueue === undefined) {
      // Either the pull's catch-up is still streaming (its queue registers when it ends and claims this buffer), or
      // the DO was rebuilt without its store (the buffer is never claimed; the rebooted store pulls from its cursor).
      bufferPendingUpdates(ctx, decodedPayload.requestId, decoded)
      yield* Effect.logDebug(`No pull stream queue found for ${decodedPayload.requestId}, buffered the update`)
    } else {
      // Case: DO was still alive, so the existing `pull` will pick up the new events
      yield* Queue.offerAll(pullStreamQueue, decoded)
    }
  }).pipe(
    Effect.provide(RpcSerialization.layerSchemaBinary()),
    Effect.withSpan('rpc-sync-client:rpcCallback'),
    Effect.tapCauseLogPretty,
    Effect.runPromise,
  )

const ResponseChunkEncoded = Schema.Struct({
  requestId: Schema.String,
  values: Schema.Uint8Array,
})

type EffectRpcRequestId = string // 0, 1, 2, ...
type PullRouting = Map<EffectRpcRequestId, Queue.Queue<SyncMessage.PullResponse>>

/**
 * Per-instance map from a live pull's request id to its response queue, keyed off the client DO's
 * `DurableObjectState` (not a module global) for two independent reasons:
 * - Per-instance: a reconstructed DO starts empty, so a reverse-RPC whose store is gone hits the
 *   `handleSyncUpdateRpc` drop branch (the recovery hook) instead of a stale queue that outlived it.
 * - Keyed by request id: a late chunk from a superseded pull generation finds no entry and is
 *   dropped here — never reaching `SyncState.merge`, which dies on an event at/below the upstream head.
 */
const pullRoutingByInstance = new WeakMap<CfTypes.DurableObjectState, PullRouting>()

const pullRoutingFor = (ctx: CfTypes.DurableObjectState): PullRouting => {
  const existing = pullRoutingByInstance.get(ctx)
  if (existing !== undefined) return existing
  const routing: PullRouting = new Map()
  pullRoutingByInstance.set(ctx, routing)
  return routing
}

/**
 * Live updates whose pull has not registered its queue yet, per client DO instance and request id. Bounded: once a
 * request's buffer is full, further updates are dropped and the pull's gap detection forces a re-pull.
 */
const pendingUpdatesByInstance = new WeakMap<
  CfTypes.DurableObjectState,
  Map<EffectRpcRequestId, SyncMessage.PullResponse[]>
>()

const MAX_PENDING_UPDATES_PER_REQUEST = 256

const bufferPendingUpdates = (
  ctx: CfTypes.DurableObjectState,
  requestId: EffectRpcRequestId,
  updates: ReadonlyArray<SyncMessage.PullResponse>,
) => {
  const byRequest = pendingUpdatesByInstance.get(ctx) ?? new Map<EffectRpcRequestId, SyncMessage.PullResponse[]>()
  pendingUpdatesByInstance.set(ctx, byRequest)
  const pending = byRequest.get(requestId) ?? []
  byRequest.set(requestId, pending)
  for (const update of updates) {
    if (pending.length >= MAX_PENDING_UPDATES_PER_REQUEST) return
    pending.push(update)
  }
}

const takePendingUpdates = (
  ctx: CfTypes.DurableObjectState,
  requestId: EffectRpcRequestId,
): ReadonlyArray<SyncMessage.PullResponse> => {
  const byRequest = pendingUpdatesByInstance.get(ctx)
  const pending = byRequest?.get(requestId) ?? []
  byRequest?.delete(requestId)
  return pending
}

/**
 * Calls require Cloudflare's retryable flag. Transferred streams can lose those flags, so unflagged read
 * failures are also offline (application failures arrive as encoded RPC exits). Explicit overload and
 * non-retryable remote errors stay terminal.
 * https://developers.cloudflare.com/durable-objects/best-practices/error-handling/
 */
const classifyTransportFailure = (failure: unknown) => {
  const cause = failure instanceof DoRpcReadError ? failure.cause : failure
  return Match.value({
    retryable:
      hasTrueFlag(cause, 'retryable') || (failure instanceof DoRpcReadError && hasTrueFlag(cause, 'remote') === false),
    overloaded: hasTrueFlag(cause, 'overloaded'),
  }).pipe(
    Match.when({ overloaded: true }, () => Effect.die(cause)),
    Match.when({ retryable: true }, () => Effect.fail(new IsOfflineError({ cause }))),
    Match.orElse(() => Effect.die(cause)),
  )
}

const hasTrueFlag = (cause: unknown, flag: 'retryable' | 'overloaded' | 'remote') =>
  Predicate.hasProperty(cause, flag) === true && cause[flag] === true

/**
 * This transport delays retryable failures before surfacing them, independently of the leader's connectivity gate.
 * The attempt count spans pull calls and resets only when a pull completes its catch-up, so a pull that delivers a
 * page and then fails does not reset it.
 */
const makePullBackoff = Effect.gen(function* () {
  const attempt = yield* Ref.make(0)
  return {
    wait: Effect.gen(function* () {
      const n = yield* Ref.getAndUpdate(attempt, (n) => n + 1)
      const jitter = yield* Random.nextBetween(0.8, 1.2)
      yield* Effect.sleep(Duration.millis(Math.min(1_000 * 2 ** n, 30_000) * jitter))
    }),
    reset: Ref.set(attempt, 0),
  }
})
