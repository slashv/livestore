import { casesHandled, TRACE_VERBOSE } from '@livestore/utils'
import {
  type HttpClient,
  type Latch,
  type Scope,
  type Tracer,
  Cause,
  Deferred,
  Duration,
  Effect,
  Exit,
  FiberHandle,
  FiberSet,
  Queue,
  ReadonlyArray,
  Ref,
  Schema,
  Stream,
  Subscribable,
  SubscriptionRef,
} from '@livestore/utils/effect'

import { type SqliteDb, UnknownError } from '../adapter-types.ts'
import { PullItem } from '../ClientSessionLeaderThreadProxy.ts'
import { IntentionalShutdownCause } from '../errors.ts'
import * as EventlogSqliteDb from '../EventlogSqliteDb.ts'
import type { LiveStoreSchema } from '../schema/mod.ts'
import { EventSequenceNumber, LiveStoreEvent } from '../schema/mod.ts'
import type { BackendIdMismatchError, IsOfflineError, ServerAheadError } from '../sync/errors.ts'
import type * as SyncBackend from '../sync/sync-backend.ts'
import * as SyncState from '../sync/syncstate.ts'
import * as Eventlog from './eventlog.ts'
import * as LeaderSyncCommitter from './LeaderSyncCommitter.ts'
import {
  LeaderAheadError,
  NonContiguousBatchError,
  NonMonotonicBatchError,
  type RejectedPushError,
  StaleRebaseGenerationError,
} from './RejectedPushError.ts'
import * as Shutdown from './shutdown-channel.ts'
import type { InitialBlockingSyncContext } from './types.ts'

export interface Options {
  readonly schema: LiveStoreSchema
  readonly runtime: Runtime
  readonly initialBlockingSyncContext: InitialBlockingSyncContext
  readonly initialSyncState: SyncState.SyncState
  readonly onError: 'shutdown' | 'ignore'
  readonly onBackendIdMismatch: 'reset' | 'shutdown' | 'ignore'
  readonly params: { readonly localPushBatchSize?: number; readonly backendPushBatchSize?: number }
  readonly livePull: boolean
  readonly testing: {
    readonly delays?: { readonly localPushProcessing?: Effect.Effect<void> }
    readonly hooks?: {
      readonly localPushAdmitted?: (events: ReadonlyArray<LiveStoreEvent.Client.Encoded>) => Effect.Effect<void>
    }
  }
}

export interface Runtime {
  readonly syncBackend: SyncBackend.SyncBackend | undefined
  readonly shutdownChannel: Shutdown.ShutdownChannel
  readonly devtoolsLatch: Latch.Latch | undefined
  readonly span: Tracer.Span | undefined
}

export interface LeaderSyncLoop {
  readonly boot: Effect.Effect<
    { initialLeaderHead: EventSequenceNumber.Client.Composite },
    never,
    Scope.Scope | HttpClient.HttpClient
  >
  readonly push: (batch: ReadonlyArray<LiveStoreEvent.Client.Encoded>) => Effect.Effect<void, RejectedPushError>
  readonly pull: (args: { cursor: EventSequenceNumber.Client.Composite }) => Stream.Stream<typeof PullItem.Type>
  readonly pullQueue: (args: {
    cursor: EventSequenceNumber.Client.Composite
  }) => Effect.Effect<Queue.Queue<typeof PullItem.Type>, never, Scope.Scope>
  readonly syncState: Subscribable.Subscribable<SyncState.SyncState>
}

/**
 * Owns leader-sync orchestration. The mailbox awaits each durable transition, so planning, committing, publication,
 * and acknowledgement remain one readable run-to-completion operation instead of a command/result protocol.
 */
export const make = Effect.fnUntraced(function* ({
  schema,
  runtime,
  initialBlockingSyncContext,
  initialSyncState,
  onError,
  onBackendIdMismatch,
  livePull,
  params,
  testing,
}: Options) {
  const dbEventlog = yield* EventlogSqliteDb.EventlogSqliteDb
  const syncCommitter = yield* LeaderSyncCommitter.LeaderSyncCommitter
  const { devtoolsLatch, shutdownChannel, span, syncBackend } = runtime

  const mailbox = yield* Queue.unbounded<Event>()
  const syncStateRef = yield* SubscriptionRef.make(initialSyncState)
  const connectedSessions = yield* makePullQueueSet
  const bootDeferred = yield* Deferred.make<EventSequenceNumber.Client.Composite>()
  const startedDeferred = yield* Deferred.make<void>()
  const stoppedDeferred = yield* Deferred.make<void>()
  const nextLocalRequestId = yield* Ref.make(1)
  const nextPullBatchId = yield* Ref.make(1)
  const localRequests = yield* Ref.make<LocalRequestRegistry>({ _tag: 'open', requests: new Map() })
  const pullBatches = yield* Ref.make(new Map<PullBatchId, Deferred.Deferred<void>>())
  const bootStarted = yield* Ref.make(false)
  const pushHandle = yield* FiberHandle.make<void, never>()
  const pullHandle = yield* FiberHandle.make<void, never>()
  const backgroundFibers = yield* FiberSet.make<void, never>()

  const config: Config = {
    backendEnabled: syncBackend !== undefined,
    livePull,
    localCommitBatchSize: params.localPushBatchSize ?? 10,
    backendPushBatchSize: params.backendPushBatchSize ?? 50,
    onError,
    onBackendIdMismatch,
    localWorkInitiallyBlocked: testing.delays?.localPushProcessing !== undefined,
  }
  const isClientOnlyEvent = (event: LiveStoreEvent.Client.Encoded) =>
    schema.eventsDefsMap.get(event.name)?.options.clientOnly ?? false
  let model = initialModel(config, initialSyncState, isClientOnlyEvent)

  const send = (event: Event) => Queue.offer(mailbox, event).pipe(Effect.asVoid)
  const fork = (effect: Effect.Effect<void>) => FiberSet.run(backgroundFibers, effect).pipe(Effect.asVoid)
  const allocateOperationId = () => {
    const operationId = model.nextOperationId
    model = { ...model, nextOperationId: operationId + 1 }
    return operationId
  }

  const stop = (args: {
    readonly lifecycle: 'stopping' | 'failed'
    readonly error?: unknown
    readonly notify: boolean
  }) =>
    Effect.gen(function* () {
      if ((yield* Deferred.isDone(stoppedDeferred)) === true) return false
      model = { ...model, lifecycle: args.lifecycle }
      if (args.notify === true && args.error !== undefined) {
        // A broken shutdown channel must not prevent resource cleanup or leave admitted pushes unresolved.
        yield* shutdownChannel
          .send(
            Schema.is(Shutdown.All)(args.error) === true
              ? args.error
              : UnknownError.make({ cause: args.error, note: 'Leader sync loop failed' }),
          )
          .pipe(Effect.exit)
      }
      yield* Effect.all([
        FiberHandle.clear(pushHandle),
        FiberHandle.clear(pullHandle),
        FiberSet.clear(backgroundFibers),
        interruptAllLocalRequests(localRequests),
        interruptAllPullBatches(pullBatches),
      ])
      yield* Deferred.succeed(stoppedDeferred, void 0)
      return false
    })

  const stopForSyncFailure = (cause: unknown) =>
    stop({ lifecycle: 'failed', error: cause, notify: config.onError === 'shutdown' })

  const startProviderPush = () =>
    Effect.gen(function* () {
      if (syncBackend === undefined || model.lifecycle !== 'running' || model.push._tag !== 'idle') return
      if (model.push.queued.length === 0) return
      const operationId = allocateOperationId()
      const batch = model.push.queued.slice(0, config.backendPushBatchSize)
      const queued = model.push.queued.slice(batch.length)
      model = {
        ...model,
        push: {
          _tag: 'in-flight',
          operationId,
          attempt: 0,
          batch,
          queued,
        },
      }
      yield* FiberHandle.run(
        pushHandle,
        runProviderPush({ operationId, batch }, syncBackend, devtoolsLatch, send),
      ).pipe(Effect.asVoid)
    })

  const startProviderPull = (attempt: number) =>
    Effect.gen(function* () {
      if (syncBackend === undefined || model.lifecycle !== 'running') return
      const pullId = allocateOperationId()
      model = { ...model, pull: { _tag: 'streaming', pullId, pagination: 'between-pages', attempt } }
      yield* FiberHandle.run(
        pullHandle,
        runProviderPull({
          pullId,
          cursor: model.syncState.upstreamHead,
          live: config.livePull,
          syncBackend,
          devtoolsLatch,
          dbEventlog,
          pullBatches,
          nextPullBatchId,
          initialBlockingSyncContext,
          send,
        }),
      ).pipe(Effect.asVoid)
    })

  const replacePushPlan = (replacement: ReadonlyArray<LiveStoreEvent.Client.Encoded>) =>
    Effect.gen(function* () {
      if (model.push._tag === 'disabled') return
      const inFlight = model.push._tag === 'in-flight'
      model = { ...model, push: { _tag: 'idle', queued: replacement } }
      if (inFlight === true) yield* FiberHandle.clear(pushHandle)
      yield* startProviderPush()
    })

  const publish = (args: {
    syncState: SyncState.SyncState
    payload: typeof SyncState.PayloadUpstream.Type
    materializerHashes: ReadonlyArray<LiveStoreEvent.Client.MaterializerHash>
  }) =>
    Effect.gen(function* () {
      model = { ...model, syncState: args.syncState }
      yield* SubscriptionRef.set(syncStateRef, args.syncState)
      yield* connectedSessions.offer({
        payload: args.payload,
        globalHead: args.syncState.upstreamHead,
        leaderHead: args.syncState.localHead,
        materializerHashes: args.materializerHashes,
      })
    })

  const processLocalBatch = (items: ReadonlyArray<LocalItem>) =>
    Effect.gen(function* () {
      const mergeExit = yield* SyncState.merge({
        syncState: model.syncState,
        payload: { _tag: 'local-push', newEvents: items.map((item) => item.event) },
        isClientOnlyEvent,
        isEqualEvent: LiveStoreEvent.Client.isEqualEncoded,
      }).pipe(Effect.exit)
      if (Exit.isFailure(mergeExit) === true) return yield* stopForSyncFailure(Cause.squash(mergeExit.cause))
      const merge = mergeExit.value
      if (merge._tag === 'reject') {
        const generation = items[0]?.event.seqNum.rebaseGeneration
        const queuedSameGeneration = model.localQueue.filter(
          (item) => item.event.seqNum.rebaseGeneration === generation,
        )
        const rejectedItems = [...items, ...queuedSameGeneration]
        const rejectedKeys = new Set(rejectedItems.map(localItemKey))
        const firstEvent = items[0]?.event
        model = {
          ...model,
          localQueue: model.localQueue.filter((item) => !rejectedKeys.has(localItemKey(item))),
          reservations: model.reservations.filter((item) => !rejectedKeys.has(localItemKey(item))),
        }
        if (firstEvent !== undefined) {
          const error = new LeaderAheadError({
            minimumExpectedNum: merge.expectedMinimumId,
            providedNum: firstEvent.seqNum,
            sessionId: firstEvent.sessionId,
          })
          yield* rejectLocalItems(
            localRequests,
            rejectedItems.map((item) => ({ item, error })),
          )
        }
        yield* send({ _tag: 'ContinueWork' })
        return true
      }
      if (merge._tag === 'rebase') return yield* stopForSyncFailure(new Error('Local push required rebase'))

      const events = merge.newSyncState.pending.slice(model.syncState.pending.length)
      if (events.length !== merge.newEvents.length) {
        return yield* stopForSyncFailure(new Error('Local push was not retained as pending'))
      }
      const commitExit = yield* syncCommitter.commitLocal({ events }).pipe(Effect.exit)
      if (Exit.isFailure(commitExit) === true) return yield* stopForSyncFailure(Cause.squash(commitExit.cause))
      const receipt = commitExit.value
      if (EventSequenceNumber.Client.isEqual(receipt.stateHead, merge.newSyncState.localHead) === false) {
        return yield* stopForSyncFailure({
          _tag: 'CommitReceiptMismatch',
          expectedStateHead: merge.newSyncState.localHead,
          receipt,
        })
      }

      const committedSyncState = replacePendingEvents(merge.newSyncState, receipt.committedEvents)
      const completedKeys = new Set(items.map(localItemKey))
      model = {
        ...model,
        reservations: model.reservations.filter((item) => !completedKeys.has(localItemKey(item))),
      }
      yield* publish({
        syncState: committedSyncState,
        payload: SyncState.PayloadUpstreamAdvance.make({ newEvents: receipt.committedEvents }),
        materializerHashes: receipt.materializerHashes,
      })
      model = enqueuePushEvents(
        model,
        receipt.committedEvents.filter((event) => !isClientOnlyEvent(event)),
      )
      yield* startProviderPush()
      yield* completeLocalItems(localRequests, items)
      yield* send({ _tag: 'ContinueWork' })
      return true
    })

  const processUpstreamBatch = (batch: UpstreamBatch) =>
    Effect.gen(function* () {
      const mergeExit = yield* SyncState.merge({
        syncState: model.syncState,
        payload: SyncState.PayloadUpstreamAdvance.make({ newEvents: batch.events }),
        isClientOnlyEvent,
        isEqualEvent: LiveStoreEvent.Client.isEqualEncoded,
        ignoreClientOnlyEvents: true,
      }).pipe(Effect.exit)
      if (Exit.isFailure(mergeExit) === true) return yield* stopForSyncFailure(Cause.squash(mergeExit.cause))
      const merge = mergeExit.value
      if (merge._tag === 'reject') return yield* stopForSyncFailure(new Error('Upstream batch rejected'))

      const rollbackEvents = merge._tag === 'rebase' ? merge.rollbackEvents : []
      const confirmedEvents = merge._tag === 'advance' ? merge.confirmedEvents : []
      const backendHead = batch.events.at(-1)?.seqNum
      if (backendHead === undefined) return yield* stopForSyncFailure(new Error('Upstream batch has no head'))
      const commitExit = yield* syncCommitter
        .commitUpstream({
          pulledEvents: batch.pulledEvents,
          events: merge.newEvents,
          rollbackEvents,
          confirmedEvents,
          backendHead,
        })
        .pipe(Effect.exit)
      if (Exit.isFailure(commitExit) === true) return yield* stopForSyncFailure(Cause.squash(commitExit.cause))
      const receipt = commitExit.value
      if (
        EventSequenceNumber.Client.isEqual(receipt.stateHead, merge.newSyncState.localHead) === false ||
        EventSequenceNumber.Client.isEqual(receipt.backendHead, backendHead) === false
      ) {
        return yield* stopForSyncFailure({
          _tag: 'CommitReceiptMismatch',
          expectedStateHead: merge.newSyncState.localHead,
          expectedBackendHead: backendHead,
          receipt,
        })
      }

      const committedSyncState = replacePendingEvents(merge.newSyncState, receipt.committedEvents)
      const payload =
        merge._tag === 'rebase'
          ? SyncState.PayloadUpstreamRebase.make({ rollbackEvents, newEvents: receipt.committedEvents })
          : SyncState.PayloadUpstreamAdvance.make({ newEvents: receipt.committedEvents })
      yield* publish({ syncState: committedSyncState, payload, materializerHashes: receipt.materializerHashes })
      yield* completePullBatch(pullBatches, batch.batchId)
      yield* replacePushPlan(committedSyncState.pending.filter((event) => !isClientOnlyEvent(event)))
      yield* send({ _tag: 'ContinueWork' })
      return true
    })

  const processNextWork = () =>
    Effect.gen(function* () {
      if (model.lifecycle !== 'running') return true
      const [upstream, ...remainingUpstream] = model.upstreamQueue
      if (upstream !== undefined) {
        model = { ...model, upstreamQueue: remainingUpstream }
        return yield* processUpstreamBatch(upstream)
      }
      if (model.pull._tag === 'streaming' && model.pull.pagination === 'more-expected') return true
      if (model.localWorkEnabled === false || model.localQueue.length === 0) return true

      const selected = model.localQueue.slice(0, config.localCommitBatchSize)
      const selectedKeys = new Set(selected.map(localItemKey))
      const remaining = model.localQueue.filter((item) => !selectedKeys.has(localItemKey(item)))
      const currentGeneration = model.syncState.localHead.rebaseGeneration
      const staleItems = selected.filter((item) => item.event.seqNum.rebaseGeneration < currentGeneration)
      const activeItems = selected.filter((item) => item.event.seqNum.rebaseGeneration >= currentGeneration)
      model = { ...model, localQueue: remaining }
      if (staleItems.length > 0) {
        const staleKeys = new Set(staleItems.map(localItemKey))
        model = {
          ...model,
          localQueue: [...activeItems, ...remaining],
          reservations: model.reservations.filter((item) => !staleKeys.has(localItemKey(item))),
        }
        yield* rejectLocalItems(
          localRequests,
          staleItems.map((item) => ({
            item,
            error: new StaleRebaseGenerationError({
              currentRebaseGeneration: currentGeneration,
              providedRebaseGeneration: item.event.seqNum.rebaseGeneration,
              sessionId: item.event.sessionId,
            }),
          })),
        )
        yield* send({ _tag: 'ContinueWork' })
        return true
      }
      return yield* processLocalBatch(activeItems)
    })

  const handleBackendMismatch = (error: BackendIdMismatchError, direction: 'pull' | 'push') =>
    Effect.gen(function* () {
      switch (config.onBackendIdMismatch) {
        case 'ignore':
          model = {
            ...model,
            ...(direction === 'pull'
              ? { pull: { _tag: 'completed' } as const }
              : { push: { _tag: 'disabled' } as const }),
          }
          if (direction === 'pull') yield* send({ _tag: 'ContinueWork' })
          return true
        case 'shutdown':
          return yield* stop({ lifecycle: 'failed', error, notify: true })
        case 'reset': {
          model = { ...model, lifecycle: 'stopping' }
          yield* Effect.all([
            FiberHandle.clear(pushHandle),
            FiberHandle.clear(pullHandle),
            FiberSet.clear(backgroundFibers),
            interruptAllLocalRequests(localRequests),
            interruptAllPullBatches(pullBatches),
          ])
          const resetExit = yield* syncCommitter.resetLocalDatabases.pipe(Effect.exit)
          const cause =
            Exit.isFailure(resetExit) === true
              ? Cause.squash(resetExit.cause)
              : IntentionalShutdownCause.make({ reason: 'backend-id-mismatch' })
          yield* shutdownChannel
            .send(
              Schema.is(Shutdown.All)(cause) === true
                ? cause
                : UnknownError.make({ cause, note: 'Leader sync database reset failed' }),
            )
            .pipe(Effect.orDie)
          model = { ...model, lifecycle: 'failed' }
          yield* Deferred.succeed(stoppedDeferred, void 0)
          return false
        }
        default:
          return casesHandled(config.onBackendIdMismatch)
      }
    })

  const handleEvent = (event: Event): Effect.Effect<boolean> =>
    Effect.gen(function* () {
      if (event._tag === 'ShutdownRequested') {
        return yield* stop({ lifecycle: 'stopping', notify: false })
      }
      if (model.lifecycle !== 'running') {
        if (event._tag === 'LocalPushRequested') yield* interruptLocalRequests(localRequests, [event.requestId])
        return true
      }

      switch (event._tag) {
        case 'LocalPushRequested': {
          const pushHead = model.reservations.at(-1)?.event.seqNum ?? model.syncState.localHead
          const validationError = validatePushBatch(event.events, pushHead, isClientOnlyEvent)
          const items = event.events.map((pushedEvent, index) => ({
            requestId: event.requestId,
            index,
            event: pushedEvent,
          }))
          if (validationError !== undefined) {
            yield* rejectLocalItems(
              localRequests,
              items.map((item) => ({ item, error: validationError })),
            )
            return true
          }
          model = {
            ...model,
            localQueue: [...model.localQueue, ...items],
            reservations: [...model.reservations, ...items],
          }
          yield* testing.hooks?.localPushAdmitted?.(event.events) ?? Effect.void
          yield* send({ _tag: 'ContinueWork' })
          return true
        }
        case 'LocalWorkEnabled':
          model = { ...model, localWorkEnabled: true }
          yield* send({ _tag: 'ContinueWork' })
          return true
        case 'ContinueWork':
          return yield* processNextWork()
        case 'UpstreamBatchReceived': {
          if (model.pull._tag !== 'streaming' || model.pull.pullId !== event.batch.pullId) {
            yield* completePullBatch(pullBatches, event.batch.batchId)
            return true
          }
          if (event.batch.events.length === 0) {
            if (event.batch.pageInfo._tag === 'NoMore') {
              model = { ...model, pull: { ...model.pull, pagination: 'between-pages' } }
            }
            yield* completePullBatch(pullBatches, event.batch.batchId)
            yield* send({ _tag: 'ContinueWork' })
            return true
          }
          model = {
            ...model,
            pull: {
              ...model.pull,
              pagination: event.batch.pageInfo._tag === 'NoMore' ? 'between-pages' : 'more-expected',
            },
            upstreamQueue: [...model.upstreamQueue, event.batch],
          }
          yield* send({ _tag: 'ContinueWork' })
          return true
        }
        case 'PullCompleted':
          if (model.pull._tag === 'streaming' && model.pull.pullId === event.pullId) {
            model = { ...model, pull: { _tag: 'completed' } }
            yield* send({ _tag: 'ContinueWork' })
          }
          return true
        case 'PullFailed':
          if (model.pull._tag !== 'streaming' || model.pull.pullId !== event.pullId) return true
          if (event.error._tag === 'IsOfflineError') {
            const retryId = allocateOperationId()
            const attempt = model.pull.attempt + 1
            model = { ...model, pull: { _tag: 'retry-wait', retryId, attempt } }
            yield* fork(
              Effect.sleep(Duration.millis(retryDelay(attempt))).pipe(
                Effect.andThen(send({ _tag: 'PullRetryElapsed', retryId })),
              ),
            )
            return true
          }
          if (event.error._tag === 'BackendIdMismatchError') return yield* handleBackendMismatch(event.error, 'pull')
          if (config.onError === 'shutdown')
            return yield* stop({ lifecycle: 'failed', error: event.error, notify: true })
          model = { ...model, pull: { _tag: 'completed' } }
          yield* send({ _tag: 'ContinueWork' })
          return true
        case 'PullRetryElapsed':
          if (model.pull._tag === 'retry-wait' && model.pull.retryId === event.retryId) {
            yield* startProviderPull(model.pull.attempt)
          }
          return true
        case 'PushSucceeded':
          if (model.push._tag === 'in-flight' && model.push.operationId === event.operationId) {
            model = {
              ...model,
              push: { _tag: 'idle', queued: model.push.queued },
            }
            yield* startProviderPush()
          }
          return true
        case 'PushFailed':
          if (model.push._tag !== 'in-flight' || model.push.operationId !== event.operationId) return true
          if (event.error._tag === 'ServerAheadError') {
            model = {
              ...model,
              push: {
                _tag: 'awaiting-pull',
                queued: [...model.push.batch, ...model.push.queued],
              },
            }
            return true
          }
          if (event.error._tag === 'BackendIdMismatchError') return yield* handleBackendMismatch(event.error, 'push')
          const retryId = allocateOperationId()
          const attempt = model.push.attempt + 1
          model = {
            ...model,
            push: {
              _tag: 'retry-wait',
              retryId,
              attempt,
              batch: model.push.batch,
              queued: model.push.queued,
            },
          }
          yield* fork(
            Effect.sleep(Duration.millis(retryDelay(attempt))).pipe(
              Effect.andThen(send({ _tag: 'PushRetryElapsed', retryId })),
            ),
          )
          return true
        case 'PushRetryElapsed':
          if (model.push._tag === 'retry-wait' && model.push.retryId === event.retryId) {
            const operationId = allocateOperationId()
            const push = model.push
            model = {
              ...model,
              push: {
                _tag: 'in-flight',
                operationId,
                attempt: push.attempt,
                batch: push.batch,
                queued: push.queued,
              },
            }
            if (syncBackend !== undefined) {
              yield* FiberHandle.run(
                pushHandle,
                runProviderPush({ operationId, batch: push.batch }, syncBackend, devtoolsLatch, send),
              ).pipe(Effect.asVoid)
            }
          }
          return true
        default:
          return casesHandled(event)
      }
    })

  const run = Effect.gen(function* () {
    let running = true
    while (running === true) {
      const event = yield* Queue.take(mailbox)
      const exit = yield* handleEvent(event).pipe(Effect.exit)
      if (Exit.isFailure(exit) === true) {
        running = yield* stop({ lifecycle: 'failed', error: Cause.squash(exit.cause), notify: true })
      } else {
        running = exit.value
      }
    }
    yield* Queue.shutdown(mailbox)
  })

  const push: LeaderSyncLoop['push'] = (events) =>
    Effect.gen(function* () {
      if (events.length === 0) return
      const requestId = yield* Ref.modify(nextLocalRequestId, (id) => [id, id + 1])
      const deferred = yield* Deferred.make<void, RejectedPushError>()
      const admitted = yield* Ref.modify(localRequests, (registry) => {
        if (registry._tag === 'closed') return [false, registry]
        return [
          true,
          {
            _tag: 'open' as const,
            requests: new Map(registry.requests).set(requestId, { deferred, remaining: events.length }),
          },
        ]
      })
      if (admitted === false) return yield* Effect.interrupt
      yield* Deferred.await(startedDeferred)
      yield* send({ _tag: 'LocalPushRequested', requestId, events })
      yield* Deferred.await(deferred)
    }).pipe(
      Effect.withSpan('@livestore/common:LeaderSyncProcessor:push', {
        attributes: { batchSize: events.length, batch: TRACE_VERBOSE === true ? events : undefined },
        links: span !== undefined ? [{ span, attributes: {} }] : undefined,
      }),
    )

  const boot: LeaderSyncLoop['boot'] = Effect.gen(function* () {
    const shouldStart = yield* Ref.modify(bootStarted, (started) => [started === false, true])
    if (shouldStart === false) return { initialLeaderHead: yield* Deferred.await(bootDeferred) }
    yield* run.pipe(Effect.forkScoped)
    model = { ...model, lifecycle: 'running' }
    yield* SubscriptionRef.set(syncStateRef, initialSyncState)
    yield* Deferred.succeed(startedDeferred, void 0)
    if (syncBackend !== undefined) {
      yield* startProviderPull(0)
      yield* startProviderPush()
    }
    if (testing.delays?.localPushProcessing !== undefined) {
      yield* fork(testing.delays.localPushProcessing.pipe(Effect.andThen(send({ _tag: 'LocalWorkEnabled' }))))
    }
    yield* Deferred.succeed(bootDeferred, initialSyncState.localHead)
    yield* Effect.addFinalizer(() =>
      send({ _tag: 'ShutdownRequested', reason: 'scope-closed' }).pipe(
        Effect.ignore,
        Effect.andThen(Deferred.await(stoppedDeferred)),
      ),
    )
    return { initialLeaderHead: initialSyncState.localHead }
  }).pipe(Effect.withSpanScoped('@livestore/common:LeaderSyncProcessor:boot'), Effect.uninterruptible)

  return {
    boot,
    push,
    pull: ({ cursor }) =>
      Effect.gen(function* () {
        const queue = yield* connectedSessions.makeQueue(cursor)
        return Stream.fromQueue(queue)
      }).pipe(Stream.unwrap),
    pullQueue: ({ cursor }) => connectedSessions.makeQueue(cursor),
    syncState: Subscribable.make({
      get: SubscriptionRef.get(syncStateRef),
      changes: SubscriptionRef.changes(syncStateRef),
    }),
  } satisfies LeaderSyncLoop
})

type OperationId = number
type LocalRequestId = number
type PullBatchId = number

interface Config {
  readonly backendEnabled: boolean
  readonly livePull: boolean
  readonly localCommitBatchSize: number
  readonly backendPushBatchSize: number
  readonly onError: 'shutdown' | 'ignore'
  readonly onBackendIdMismatch: 'reset' | 'shutdown' | 'ignore'
  readonly localWorkInitiallyBlocked: boolean
}

interface LocalItem {
  readonly requestId: LocalRequestId
  readonly index: number
  readonly event: LiveStoreEvent.Client.Encoded
}

interface UpstreamBatch {
  readonly pullId: OperationId
  readonly batchId: PullBatchId
  readonly events: ReadonlyArray<LiveStoreEvent.Client.Encoded>
  readonly pulledEvents: ReadonlyArray<LeaderSyncCommitter.PulledEvent>
  readonly pageInfo: SyncBackend.PullResPageInfo
}

type ProviderPushError = IsOfflineError | BackendIdMismatchError | UnknownError | ServerAheadError
type ProviderPullError = IsOfflineError | BackendIdMismatchError | UnknownError

type Event =
  | { readonly _tag: 'ContinueWork' }
  | { readonly _tag: 'LocalWorkEnabled' }
  | {
      readonly _tag: 'LocalPushRequested'
      readonly requestId: LocalRequestId
      readonly events: ReadonlyArray<LiveStoreEvent.Client.Encoded>
    }
  | { readonly _tag: 'UpstreamBatchReceived'; readonly batch: UpstreamBatch }
  | { readonly _tag: 'PullCompleted'; readonly pullId: OperationId }
  | { readonly _tag: 'PullFailed'; readonly pullId: OperationId; readonly error: ProviderPullError }
  | { readonly _tag: 'PullRetryElapsed'; readonly retryId: OperationId }
  | { readonly _tag: 'PushSucceeded'; readonly operationId: OperationId }
  | { readonly _tag: 'PushFailed'; readonly operationId: OperationId; readonly error: ProviderPushError }
  | { readonly _tag: 'PushRetryElapsed'; readonly retryId: OperationId }
  | { readonly _tag: 'ShutdownRequested'; readonly reason: string }

type PullState =
  | { readonly _tag: 'disabled' }
  | {
      readonly _tag: 'streaming'
      readonly pullId: OperationId
      readonly pagination: 'between-pages' | 'more-expected'
      readonly attempt: number
    }
  | { readonly _tag: 'retry-wait'; readonly retryId: OperationId; readonly attempt: number }
  | { readonly _tag: 'completed' }

type PushState =
  | { readonly _tag: 'disabled' }
  | { readonly _tag: 'idle'; readonly queued: EventBatch }
  | {
      readonly _tag: 'in-flight'
      readonly operationId: OperationId
      readonly attempt: number
      readonly batch: EventBatch
      readonly queued: EventBatch
    }
  | {
      readonly _tag: 'retry-wait'
      readonly retryId: OperationId
      readonly attempt: number
      readonly batch: EventBatch
      readonly queued: EventBatch
    }
  | { readonly _tag: 'awaiting-pull'; readonly queued: EventBatch }

type EventBatch = ReadonlyArray<LiveStoreEvent.Client.Encoded>

interface Model {
  readonly lifecycle: 'starting' | 'running' | 'stopping' | 'failed'
  readonly syncState: SyncState.SyncState
  readonly pull: PullState
  readonly push: PushState
  readonly localQueue: ReadonlyArray<LocalItem>
  readonly reservations: ReadonlyArray<LocalItem>
  readonly upstreamQueue: ReadonlyArray<UpstreamBatch>
  readonly localWorkEnabled: boolean
  readonly nextOperationId: OperationId
}

interface LocalRequest {
  readonly deferred: Deferred.Deferred<void, RejectedPushError>
  readonly remaining: number
}

type LocalRequestRegistry =
  | { readonly _tag: 'open'; readonly requests: Map<LocalRequestId, LocalRequest> }
  | { readonly _tag: 'closed' }

const initialModel = (
  config: Config,
  initialSyncState: SyncState.SyncState,
  isClientOnlyEvent: (event: LiveStoreEvent.Client.Encoded) => boolean,
): Model => ({
  lifecycle: 'starting',
  syncState: initialSyncState,
  pull: config.backendEnabled === true ? { _tag: 'completed' } : { _tag: 'disabled' },
  push:
    config.backendEnabled === true
      ? { _tag: 'idle', queued: initialSyncState.pending.filter((event) => !isClientOnlyEvent(event)) }
      : { _tag: 'disabled' },
  localQueue: [],
  reservations: [],
  upstreamQueue: [],
  localWorkEnabled: config.localWorkInitiallyBlocked === false,
  nextOperationId: 1,
})

const enqueuePushEvents = (model: Model, events: EventBatch): Model => {
  if (model.push._tag === 'disabled' || events.length === 0) return model
  return { ...model, push: { ...model.push, queued: [...model.push.queued, ...events] } }
}

const runProviderPush = (
  operation: { readonly operationId: OperationId; readonly batch: EventBatch },
  syncBackend: SyncBackend.SyncBackend,
  devtoolsLatch: Latch.Latch | undefined,
  send: (event: Event) => Effect.Effect<void>,
) =>
  Effect.gen(function* () {
    yield* SubscriptionRef.waitUntil(syncBackend.isConnected, (connected) => connected === true)
    if (devtoolsLatch !== undefined) yield* devtoolsLatch.await
    yield* syncBackend.push(operation.batch.map(LiveStoreEvent.Client.toGlobal))
  }).pipe(
    Effect.matchEffect({
      onFailure: (error) => send({ _tag: 'PushFailed', operationId: operation.operationId, error }),
      onSuccess: () => send({ _tag: 'PushSucceeded', operationId: operation.operationId }),
    }),
    Effect.catchCause((cause) =>
      send({
        _tag: 'PushFailed',
        operationId: operation.operationId,
        error: UnknownError.make({ cause, note: 'Sync backend push defected' }),
      }),
    ),
    Effect.interruptible,
  )

const runProviderPull = ({
  pullId,
  cursor,
  live,
  syncBackend,
  devtoolsLatch,
  dbEventlog,
  pullBatches,
  nextPullBatchId,
  initialBlockingSyncContext,
  send,
}: {
  pullId: OperationId
  cursor: EventSequenceNumber.Client.Composite
  live: boolean
  syncBackend: SyncBackend.SyncBackend
  devtoolsLatch: Latch.Latch | undefined
  dbEventlog: SqliteDb
  pullBatches: Ref.Ref<Map<PullBatchId, Deferred.Deferred<void>>>
  nextPullBatchId: Ref.Ref<number>
  initialBlockingSyncContext: InitialBlockingSyncContext
  send: (event: Event) => Effect.Effect<void>
}) =>
  Effect.gen(function* () {
    const cursorInfo = yield* Eventlog.getSyncBackendCursorInfoForDb(dbEventlog, { remoteHead: cursor.global })
    yield* syncBackend.pull(cursorInfo, { live }).pipe(
      Stream.runForEach(({ batch, pageInfo }) =>
        Effect.gen(function* () {
          yield* SubscriptionRef.waitUntil(syncBackend.isConnected, (connected) => connected === true)
          if (devtoolsLatch !== undefined) yield* devtoolsLatch.await
          const batchId = yield* Ref.modify(nextPullBatchId, (id) => [id, id + 1])
          const completion = yield* Deferred.make<void>()
          yield* Ref.update(pullBatches, (batches) => new Map(batches).set(batchId, completion))
          const pulledEvents = batch.map((item) => ({
            event: LiveStoreEvent.Client.fromGlobal(item.eventEncoded),
            syncMetadata: item.metadata,
          }))
          yield* send({
            _tag: 'UpstreamBatchReceived',
            batch: { pullId, batchId, events: pulledEvents.map(({ event }) => event), pulledEvents, pageInfo },
          })
          yield* Deferred.await(completion)
          yield* initialBlockingSyncContext.update({ processed: batch.length, pageInfo })
          yield* Effect.yieldNow
        }),
      ),
    )
  }).pipe(
    Effect.matchEffect({
      onFailure: (error) => send({ _tag: 'PullFailed', pullId, error }),
      onSuccess: () => Effect.void,
    }),
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause) === true
        ? Effect.failCause(cause)
        : send({ _tag: 'PullFailed', pullId, error: UnknownError.make({ cause, note: 'Sync backend pull defected' }) }),
    ),
    Effect.ensuring(send({ _tag: 'PullCompleted', pullId })),
    Effect.interruptible,
  )

const validatePushBatch = (
  batch: EventBatch,
  pushHead: EventSequenceNumber.Client.Composite,
  isClientOnlyEvent: (event: LiveStoreEvent.Client.Encoded) => boolean,
): RejectedPushError | undefined => {
  for (let i = 1; i < batch.length; i++) {
    if (EventSequenceNumber.Client.isGreaterThanOrEqual(batch[i - 1]!.seqNum, batch[i]!.seqNum) === true) {
      return new NonMonotonicBatchError({
        precedingSeqNum: batch[i - 1]!.seqNum,
        violatingSeqNum: batch[i]!.seqNum,
        violationIndex: i,
        sessionId: batch[i]!.sessionId,
      })
    }
  }
  const first = batch[0]
  if (first === undefined) return undefined
  if (EventSequenceNumber.Client.isGreaterThanOrEqual(pushHead, first.seqNum) === true) {
    return new LeaderAheadError({ minimumExpectedNum: pushHead, providedNum: first.seqNum, sessionId: first.sessionId })
  }
  if (first.seqNum.rebaseGeneration < pushHead.rebaseGeneration) {
    return new StaleRebaseGenerationError({
      currentRebaseGeneration: pushHead.rebaseGeneration,
      providedRebaseGeneration: first.seqNum.rebaseGeneration,
      sessionId: first.sessionId,
    })
  }
  let precedingSeqNum = pushHead
  for (let i = 0; i < batch.length; i++) {
    const item = batch[i]!
    const expectedPair = EventSequenceNumber.Client.nextPair({
      seqNum: precedingSeqNum,
      isClientOnly: isClientOnlyEvent(item),
      rebaseGeneration: item.seqNum.rebaseGeneration,
    })
    if (
      EventSequenceNumber.Client.isEqual(item.seqNum, expectedPair.seqNum) === false ||
      isSameSequencePosition(item.parentSeqNum, expectedPair.parentSeqNum) === false
    ) {
      return new NonContiguousBatchError({
        expectedSeqNum: expectedPair.seqNum,
        providedSeqNum: item.seqNum,
        expectedParentSeqNum: expectedPair.parentSeqNum,
        providedParentSeqNum: item.parentSeqNum,
        violationIndex: i,
        sessionId: item.sessionId,
      })
    }
    precedingSeqNum = item.seqNum
  }
  return undefined
}

const replacePendingEvents = (syncState: SyncState.SyncState, committedEvents: EventBatch) =>
  new SyncState.SyncState({
    ...syncState,
    pending: syncState.pending.map(
      (pendingEvent) =>
        committedEvents.find((committedEvent) =>
          EventSequenceNumber.Client.isEqual(committedEvent.seqNum, pendingEvent.seqNum),
        ) ?? pendingEvent,
    ),
  })

const completeLocalItems = (requestsRef: Ref.Ref<LocalRequestRegistry>, items: ReadonlyArray<LocalItem>) =>
  Effect.gen(function* () {
    const counts = countRequestItems(items)
    const completions: Deferred.Deferred<void, RejectedPushError>[] = []
    yield* Ref.update(requestsRef, (registry) => {
      if (registry._tag === 'closed') return registry
      const next = new Map(registry.requests)
      for (const [requestId, count] of counts) {
        const request = next.get(requestId)
        if (request === undefined) continue
        const remaining = request.remaining - count
        if (remaining === 0) {
          next.delete(requestId)
          completions.push(request.deferred)
        } else {
          next.set(requestId, { ...request, remaining })
        }
      }
      return { _tag: 'open' as const, requests: next }
    })
    yield* Effect.forEach(completions, (deferred) => Deferred.succeed(deferred, void 0), { discard: true })
  })

const rejectLocalItems = (
  requestsRef: Ref.Ref<LocalRequestRegistry>,
  items: ReadonlyArray<{ readonly item: LocalItem; readonly error: RejectedPushError }>,
) =>
  Effect.gen(function* () {
    const failures: Array<{ deferred: Deferred.Deferred<void, RejectedPushError>; error: RejectedPushError }> = []
    yield* Ref.update(requestsRef, (registry) => {
      if (registry._tag === 'closed') return registry
      const next = new Map(registry.requests)
      for (const { item, error } of items) {
        const request = next.get(item.requestId)
        if (request === undefined) continue
        next.delete(item.requestId)
        failures.push({ deferred: request.deferred, error })
      }
      return { _tag: 'open' as const, requests: next }
    })
    yield* Effect.forEach(failures, ({ deferred, error }) => Deferred.fail(deferred, error), { discard: true })
  })

const interruptLocalRequests = (
  requestsRef: Ref.Ref<LocalRequestRegistry>,
  requestIds: ReadonlyArray<LocalRequestId>,
) =>
  Effect.gen(function* () {
    const deferreds: Deferred.Deferred<void, RejectedPushError>[] = []
    yield* Ref.update(requestsRef, (registry) => {
      if (registry._tag === 'closed') return registry
      const next = new Map(registry.requests)
      for (const requestId of new Set(requestIds)) {
        const request = next.get(requestId)
        if (request !== undefined) deferreds.push(request.deferred)
        next.delete(requestId)
      }
      return { _tag: 'open' as const, requests: next }
    })
    yield* Effect.forEach(deferreds, Deferred.interrupt, { discard: true })
  })

const interruptAllLocalRequests = (requestsRef: Ref.Ref<LocalRequestRegistry>) =>
  Effect.gen(function* () {
    const registry = yield* Ref.getAndSet(requestsRef, { _tag: 'closed' })
    if (registry._tag === 'closed') return
    yield* Effect.forEach(registry.requests.values(), ({ deferred }) => Deferred.interrupt(deferred), { discard: true })
  })

const interruptAllPullBatches = (batchesRef: Ref.Ref<Map<PullBatchId, Deferred.Deferred<void>>>) =>
  Effect.gen(function* () {
    const batches = yield* Ref.getAndSet(batchesRef, new Map())
    yield* Effect.forEach(batches.values(), Deferred.interrupt, { discard: true })
  })

const completePullBatch = (batchesRef: Ref.Ref<Map<PullBatchId, Deferred.Deferred<void>>>, batchId: PullBatchId) =>
  Effect.gen(function* () {
    let completion: Deferred.Deferred<void> | undefined
    yield* Ref.update(batchesRef, (current) => {
      const next = new Map(current)
      completion = next.get(batchId)
      next.delete(batchId)
      return next
    })
    if (completion !== undefined) yield* Deferred.succeed(completion, void 0)
  })

const countRequestItems = (items: ReadonlyArray<LocalItem>) => {
  const counts = new Map<LocalRequestId, number>()
  for (const item of items) counts.set(item.requestId, (counts.get(item.requestId) ?? 0) + 1)
  return counts
}

interface PullQueueSet {
  makeQueue: (
    cursor: EventSequenceNumber.Client.Composite,
  ) => Effect.Effect<Queue.Queue<typeof PullItem.Type>, never, Scope.Scope>
  offer: (item: {
    payload: typeof SyncState.PayloadUpstream.Type
    globalHead: EventSequenceNumber.Client.Composite
    leaderHead: EventSequenceNumber.Client.Composite
    materializerHashes: ReadonlyArray<LiveStoreEvent.Client.MaterializerHash>
  }) => Effect.Effect<void>
}

const makePullQueueSet = Effect.gen(function* () {
  const set = new Set<Queue.Queue<typeof PullItem.Type>>()
  const cachedPullItems = new Map<string, (typeof PullItem.Type)[]>()
  yield* Effect.addFinalizer(() =>
    Effect.gen(function* () {
      for (const queue of set) yield* Queue.shutdown(queue)
      set.clear()
    }),
  )
  const makeQueue: PullQueueSet['makeQueue'] = (cursor) =>
    Effect.gen(function* () {
      const queue = yield* Effect.acquireRelease(Queue.unbounded<typeof PullItem.Type>(), Queue.shutdown)
      yield* Effect.addFinalizer(() => Effect.sync(() => set.delete(queue)))
      const pullItems = Array.from(cachedPullItems.entries())
        .flatMap(([seqNum, items]) =>
          items.map((item) => ({ item, seqNum: EventSequenceNumber.Client.fromString(seqNum) })),
        )
        .filter(({ seqNum }) => EventSequenceNumber.Client.isGreaterThan(seqNum, cursor))
        .toSorted((left, right) => EventSequenceNumber.Client.compare(left.seqNum, right.seqNum))
        .map(({ item }) =>
          item.payload._tag === 'upstream-advance'
            ? PullItem.make({
                globalHead: item.globalHead,
                materializerHashes: item.materializerHashes.filter(({ eventNum }) =>
                  EventSequenceNumber.Client.isGreaterThan(eventNum, cursor),
                ),
                payload: {
                  _tag: 'upstream-advance',
                  newEvents: ReadonlyArray.dropWhile(item.payload.newEvents, (event) =>
                    EventSequenceNumber.Client.isGreaterThanOrEqual(cursor, event.seqNum),
                  ),
                },
              })
            : item,
        )
      yield* Queue.offerAll(queue, pullItems)
      set.add(queue)
      return queue
    })
  const offer: PullQueueSet['offer'] = (item) =>
    Effect.gen(function* () {
      const key = EventSequenceNumber.Client.toString(item.leaderHead)
      const pullItem = PullItem.make({
        payload: item.payload,
        globalHead: item.globalHead,
        materializerHashes: item.materializerHashes,
      })
      const cached = cachedPullItems.get(key)
      if (cached === undefined) cachedPullItems.set(key, [pullItem])
      else cached.push(pullItem)
      for (const queue of set) yield* Queue.offer(queue, pullItem)
    })
  return { makeQueue, offer }
})

const localItemKey = (item: LocalItem) => `${item.requestId}:${item.index}`
const retryDelay = (attempt: number) => Math.min(30_000, 1_000 * 2 ** Math.max(0, attempt - 1))
const isSameSequencePosition = (
  left: EventSequenceNumber.Client.Composite,
  right: EventSequenceNumber.Client.Composite,
) => left.global === right.global && left.client === right.client
