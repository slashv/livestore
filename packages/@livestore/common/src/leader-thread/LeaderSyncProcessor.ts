import { TRACE_VERBOSE } from '@livestore/utils'
import {
  type HttpClient,
  type Latch,
  type Scope,
  type Tracer,
  Cause,
  Context,
  Deferred,
  Effect,
  Layer,
  Option,
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
import type * as SyncBackend from '../sync/sync-backend.ts'
import * as SyncState from '../sync/syncstate.ts'
import * as Eventlog from './eventlog.ts'
import * as LeaderSyncCommitter from './LeaderSyncCommitter.ts'
import * as LeaderSyncMachine from './LeaderSyncMachine.ts'
import {
  LeaderAheadError,
  NonContiguousBatchError,
  NonMonotonicBatchError,
  type RejectedPushError,
  StaleRebaseGenerationError,
} from './RejectedPushError.ts'
import * as Shutdown from './shutdown-channel.ts'
import type { InitialBlockingSyncContext } from './types.ts'

export const TypeId = '~@livestore/common/LeaderSyncProcessor' as const
export type TypeId = typeof TypeId

/**
 * Public boundary for the Effect Machine leader-sync coordinator.
 *
 * The root machine serializes admissions and durable work. Provider pull and push are state-owned child machines, so
 * their retries and cancellation semantics are visible without nesting their Cartesian product into the coordinator.
 * `LeaderSyncCommitter` remains the deep durability seam.
 */
export class LeaderSyncProcessor extends Context.Service<LeaderSyncProcessor, Service>()(
  '@livestore/common/LeaderSyncProcessor',
) {}

export interface Service {
  readonly [TypeId]: TypeId
  readonly boot: Effect.Effect<
    { initialLeaderHead: EventSequenceNumber.Client.Composite },
    never,
    Scope.Scope | HttpClient.HttpClient
  >
  /** Resolves only after durable commit, publication, and backend propagation scheduling. */
  readonly push: (batch: ReadonlyArray<LiveStoreEvent.Client.Encoded>) => Effect.Effect<void, RejectedPushError>
  readonly pull: (args: { cursor: EventSequenceNumber.Client.Composite }) => Stream.Stream<typeof PullItem.Type>
  readonly pullQueue: (args: {
    cursor: EventSequenceNumber.Client.Composite
  }) => Effect.Effect<Queue.Queue<typeof PullItem.Type>, never, Scope.Scope>
  readonly syncState: Subscribable.Subscribable<SyncState.SyncState>
}

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

  const syncStateRef = yield* SubscriptionRef.make(initialSyncState)
  const connectedSessions = yield* makePullQueueSet
  const bootDeferred = yield* Deferred.make<EventSequenceNumber.Client.Composite>()
  const machineRuntime = yield* Deferred.make<LeaderSyncMachine.Runtime>()
  const nextLocalRequestId = yield* Ref.make(1)
  const nextPullBatchId = yield* Ref.make(1)
  const localRequests = yield* Ref.make<LocalRequestRegistry>({ _tag: 'open', requests: new Map() })
  const pullBatches = yield* Ref.make(new Map<LeaderSyncMachine.PullBatchId, Deferred.Deferred<void>>())
  const bootStarted = yield* Ref.make(false)
  const stopStarted = yield* Ref.make(false)

  const isClientOnlyEvent = (event: LiveStoreEvent.Client.Encoded) =>
    schema.eventsDefsMap.get(event.name)?.options.clientOnly ?? false

  const publish = (args: {
    syncState: SyncState.SyncState
    payload: typeof SyncState.PayloadUpstream.Type
    materializerHashes: ReadonlyArray<LiveStoreEvent.Client.MaterializerHash>
  }) =>
    Effect.gen(function* () {
      yield* SubscriptionRef.set(syncStateRef, args.syncState)
      yield* connectedSessions.offer({
        payload: args.payload,
        globalHead: args.syncState.upstreamHead,
        leaderHead: args.syncState.localHead,
        materializerHashes: args.materializerHashes,
      })
    })

  const processLocal: LeaderSyncMachine.Dependencies['processLocal'] = (syncState, items) =>
    Effect.gen(function* () {
      const currentGeneration = syncState.localHead.rebaseGeneration
      const staleItems = items.filter((item) => item.event.seqNum.rebaseGeneration < currentGeneration)
      if (staleItems.length > 0) {
        const first = staleItems[0]!
        return {
          _tag: 'rejected' as const,
          items: staleItems,
          error: new StaleRebaseGenerationError({
            currentRebaseGeneration: currentGeneration,
            providedRebaseGeneration: first.event.seqNum.rebaseGeneration,
            sessionId: first.event.sessionId,
          }),
        }
      }

      const merge = yield* SyncState.merge({
        syncState,
        payload: { _tag: 'local-push', newEvents: items.map((item) => item.event) },
        isClientOnlyEvent,
        isEqualEvent: LiveStoreEvent.Client.isEqualEncoded,
      })
      if (merge._tag === 'reject') {
        const first = items[0]
        if (first === undefined) return yield* Effect.fail(new Error('Rejected an empty local batch'))
        return {
          _tag: 'rejected' as const,
          items,
          error: new LeaderAheadError({
            minimumExpectedNum: merge.expectedMinimumId,
            providedNum: first.event.seqNum,
            sessionId: first.event.sessionId,
          }),
        }
      }
      if (merge._tag === 'rebase') return yield* Effect.fail(new Error('Local push required rebase'))

      const events = merge.newSyncState.pending.slice(syncState.pending.length)
      if (events.length !== merge.newEvents.length) {
        return yield* Effect.fail(new Error('Local push was not retained as pending'))
      }
      const receipt = yield* syncCommitter.commitLocal({ events })
      if (EventSequenceNumber.Client.isEqual(receipt.stateHead, merge.newSyncState.localHead) === false) {
        return yield* Effect.fail({
          _tag: 'CommitReceiptMismatch',
          expectedStateHead: merge.newSyncState.localHead,
          receipt,
        })
      }

      const committedSyncState = replacePendingEvents(merge.newSyncState, receipt.committedEvents)
      yield* publish({
        syncState: committedSyncState,
        payload: SyncState.PayloadUpstreamAdvance.make({ newEvents: receipt.committedEvents }),
        materializerHashes: receipt.materializerHashes,
      })
      return {
        _tag: 'committed' as const,
        syncState: committedSyncState,
        items,
        pushEvents: receipt.committedEvents.filter((event) => !isClientOnlyEvent(event)),
      }
    })

  const processUpstream: LeaderSyncMachine.Dependencies['processUpstream'] = (syncState, batch) =>
    Effect.gen(function* () {
      if (batch.events.length === 0) {
        yield* completePullBatch(pullBatches, batch.batchId)
        return { syncState, batch, pushPlan: undefined }
      }
      const merge = yield* SyncState.merge({
        syncState,
        payload: SyncState.PayloadUpstreamAdvance.make({ newEvents: batch.events }),
        isClientOnlyEvent,
        isEqualEvent: LiveStoreEvent.Client.isEqualEncoded,
        ignoreClientOnlyEvents: true,
      })
      if (merge._tag === 'reject') return yield* Effect.fail(new Error('Upstream batch rejected'))

      const rollbackEvents = merge._tag === 'rebase' ? merge.rollbackEvents : []
      const confirmedEvents = merge._tag === 'advance' ? merge.confirmedEvents : []
      const backendHead = batch.events.at(-1)?.seqNum
      if (backendHead === undefined) return yield* Effect.fail(new Error('Upstream batch has no head'))
      const receipt = yield* syncCommitter.commitUpstream({
        pulledEvents: batch.pulledEvents,
        events: merge.newEvents,
        rollbackEvents,
        confirmedEvents,
        backendHead,
      })
      if (
        EventSequenceNumber.Client.isEqual(receipt.stateHead, merge.newSyncState.localHead) === false ||
        EventSequenceNumber.Client.isEqual(receipt.backendHead, backendHead) === false
      ) {
        return yield* Effect.fail({
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
      return {
        syncState: committedSyncState,
        batch,
        pushPlan: committedSyncState.pending.filter((event) => !isClientOnlyEvent(event)),
      }
    })

  const settleLocal: LeaderSyncMachine.Dependencies['settleLocal'] = (result) =>
    result._tag === 'committed'
      ? completeLocalItems(localRequests, result.items)
      : rejectLocalItems(
          localRequests,
          result.items.map((item) => ({ item, error: result.error })),
        )

  const stop: LeaderSyncMachine.Dependencies['stop'] = (request) =>
    Effect.gen(function* () {
      const shouldStop = yield* Ref.modify(stopStarted, (started) => [started === false, true])
      if (shouldStop === false) return

      if (request._tag === 'reset') {
        const resetExit = yield* syncCommitter.resetLocalDatabases.pipe(Effect.exit)
        const cause =
          resetExit._tag === 'Failure'
            ? Cause.squash(resetExit.cause)
            : IntentionalShutdownCause.make({ reason: 'backend-id-mismatch' })
        yield* shutdownChannel
          .send(
            Schema.is(Shutdown.All)(cause) === true
              ? cause
              : UnknownError.make({ cause, note: 'Leader sync database reset failed' }),
          )
          .pipe(Effect.exit)
      } else if (request._tag === 'failure' && request.notify === true) {
        yield* shutdownChannel
          .send(
            Schema.is(Shutdown.All)(request.error) === true
              ? request.error
              : UnknownError.make({ cause: request.error, note: 'Leader sync machine failed' }),
          )
          .pipe(Effect.exit)
      }

      yield* Effect.all([interruptAllLocalRequests(localRequests), interruptAllPullBatches(pullBatches)])
    })

  const machineDependencies: LeaderSyncMachine.Dependencies = {
    initialSyncState,
    config: {
      backendEnabled: syncBackend !== undefined,
      localCommitBatchSize: params.localPushBatchSize ?? 10,
      backendPushBatchSize: params.backendPushBatchSize ?? 50,
      localWorkInitiallyBlocked: testing.delays?.localPushProcessing !== undefined,
      onError,
      onBackendIdMismatch,
    },
    isClientOnlyEvent,
    validatePushBatch: (batch, pushHead) => validatePushBatch(batch, pushHead, isClientOnlyEvent),
    processLocal,
    processUpstream,
    settleLocal,
    stop,
    providerPush: (batch) =>
      syncBackend === undefined
        ? Effect.die(new Error('Provider push started without a sync backend'))
        : providerPush(syncBackend, devtoolsLatch, batch),
    providerPull: (cursor, parent) =>
      syncBackend === undefined
        ? Effect.die(new Error('Provider pull started without a sync backend'))
        : providerPull({
            cursor,
            live: livePull,
            syncBackend,
            devtoolsLatch,
            dbEventlog,
            pullBatches,
            nextPullBatchId,
            initialBlockingSyncContext,
            parent,
          }),
  }

  const push: Service['push'] = (events) =>
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
      const machine = yield* Deferred.await(machineRuntime)
      yield* machine.send(LeaderSyncMachine.LeaderSyncEvents.PushRequested({ requestId, events }))
      yield* Deferred.await(deferred)
    }).pipe(
      Effect.withSpan('@livestore/common:LeaderSyncProcessor:push', {
        attributes: { batchSize: events.length, batch: TRACE_VERBOSE === true ? events : undefined },
        links: span !== undefined ? [{ span, attributes: {} }] : undefined,
      }),
    )

  const boot: Service['boot'] = Effect.gen(function* () {
    const shouldStart = yield* Ref.modify(bootStarted, (started) => [started === false, true])
    if (shouldStart === false) return { initialLeaderHead: yield* Deferred.await(bootDeferred) }

    const machine = yield* LeaderSyncMachine.start(machineDependencies, (emission) =>
      emission._tag === 'LocalPushAdmitted'
        ? (testing.hooks?.localPushAdmitted?.(emission.events) ?? Effect.void)
        : Effect.void,
    )
    yield* Deferred.succeed(machineRuntime, machine)
    yield* machine.send(LeaderSyncMachine.LeaderSyncEvents.Boot())
    if (testing.delays?.localPushProcessing !== undefined) {
      yield* testing.delays.localPushProcessing.pipe(
        Effect.andThen(machine.send(LeaderSyncMachine.LeaderSyncEvents.LocalWorkEnabled())),
        Effect.forkScoped,
      )
    }
    yield* Deferred.succeed(bootDeferred, initialSyncState.localHead)
    yield* Effect.addFinalizer(() =>
      machine
        .send(LeaderSyncMachine.LeaderSyncEvents.ShutdownRequested())
        .pipe(Effect.andThen(machine.join), Effect.ignore),
    )
    return { initialLeaderHead: initialSyncState.localHead }
  }).pipe(Effect.withSpanScoped('@livestore/common:LeaderSyncProcessor:boot'), Effect.uninterruptible)

  return LeaderSyncProcessor.of({
    [TypeId]: TypeId,
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
  })
})

export const layer = (options: Options) => Layer.effect(LeaderSyncProcessor, make(options))

interface LocalRequest {
  readonly deferred: Deferred.Deferred<void, RejectedPushError>
  readonly remaining: number
}

type LocalRequestRegistry =
  | { readonly _tag: 'open'; readonly requests: Map<LeaderSyncMachine.LocalRequestId, LocalRequest> }
  | { readonly _tag: 'closed' }

const providerPush = (
  syncBackend: SyncBackend.SyncBackend,
  devtoolsLatch: Latch.Latch | undefined,
  batch: LeaderSyncMachine.EventBatch,
): Effect.Effect<void, LeaderSyncMachine.ProviderPushError> =>
  Effect.gen(function* () {
    yield* SubscriptionRef.waitUntil(syncBackend.isConnected, (connected) => connected === true)
    if (devtoolsLatch !== undefined) yield* devtoolsLatch.await
    yield* syncBackend.push(batch.map(LiveStoreEvent.Client.toGlobal))
  }).pipe(mapDefects('Sync backend push defected'), Effect.interruptible)

const providerPull = ({
  cursor,
  live,
  syncBackend,
  devtoolsLatch,
  dbEventlog,
  pullBatches,
  nextPullBatchId,
  initialBlockingSyncContext,
  parent,
}: {
  cursor: EventSequenceNumber.Client.Composite
  live: boolean
  syncBackend: SyncBackend.SyncBackend
  devtoolsLatch: Latch.Latch | undefined
  dbEventlog: SqliteDb
  pullBatches: Ref.Ref<Map<LeaderSyncMachine.PullBatchId, Deferred.Deferred<void>>>
  nextPullBatchId: Ref.Ref<number>
  initialBlockingSyncContext: InitialBlockingSyncContext
  parent: MachineTarget
}): Effect.Effect<void, LeaderSyncMachine.ProviderPullError> =>
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
          yield* parent
            .send(
              LeaderSyncMachine.LeaderSyncEvents.UpstreamBatchReceived({
                batch: { batchId, events: pulledEvents.map(({ event }) => event), pulledEvents, pageInfo },
              }),
            )
            .pipe(Effect.orDie)
          yield* Deferred.await(completion)
          yield* initialBlockingSyncContext.update({ processed: batch.length, pageInfo })
          yield* Effect.yieldNow
        }),
      ),
    )
  }).pipe(
    Effect.catchCause((cause) => (Cause.hasInterruptsOnly(cause) === true ? Effect.void : Effect.failCause(cause))),
    mapDefects('Sync backend pull defected'),
    Effect.interruptible,
  )

type MachineTarget = Parameters<LeaderSyncMachine.Dependencies['providerPull']>[1]

const mapDefects =
  (note: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | UnknownError, R> =>
    Effect.catchCause(effect, (cause): Effect.Effect<never, E | UnknownError> => {
      const error = Cause.findErrorOption(cause)
      if (Option.isSome(error) === true) return Effect.fail(error.value)
      return Effect.fail(UnknownError.make({ cause, note }))
    })

const validatePushBatch = (
  batch: LeaderSyncMachine.EventBatch,
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

const replacePendingEvents = (
  syncState: SyncState.SyncState,
  committedEvents: LeaderSyncMachine.EventBatch,
): SyncState.SyncState =>
  new SyncState.SyncState({
    ...syncState,
    pending: syncState.pending.map(
      (pendingEvent) =>
        committedEvents.find((committedEvent) =>
          EventSequenceNumber.Client.isEqual(committedEvent.seqNum, pendingEvent.seqNum),
        ) ?? pendingEvent,
    ),
  })

const completeLocalItems = (
  requestsRef: Ref.Ref<LocalRequestRegistry>,
  items: ReadonlyArray<LeaderSyncMachine.LocalItem>,
) =>
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
    yield* Effect.forEach(completions, (deferred) => Deferred.succeed(deferred, undefined), { discard: true })
  })

const rejectLocalItems = (
  requestsRef: Ref.Ref<LocalRequestRegistry>,
  items: ReadonlyArray<{ readonly item: LeaderSyncMachine.LocalItem; readonly error: RejectedPushError }>,
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

const interruptAllLocalRequests = (requestsRef: Ref.Ref<LocalRequestRegistry>) =>
  Effect.gen(function* () {
    const registry = yield* Ref.getAndSet(requestsRef, { _tag: 'closed' })
    if (registry._tag === 'closed') return
    yield* Effect.forEach(registry.requests.values(), ({ deferred }) => Deferred.interrupt(deferred), { discard: true })
  })

const interruptAllPullBatches = (batchesRef: Ref.Ref<Map<LeaderSyncMachine.PullBatchId, Deferred.Deferred<void>>>) =>
  Effect.gen(function* () {
    const batches = yield* Ref.getAndSet(batchesRef, new Map())
    yield* Effect.forEach(batches.values(), Deferred.interrupt, { discard: true })
  })

const completePullBatch = (
  batchesRef: Ref.Ref<Map<LeaderSyncMachine.PullBatchId, Deferred.Deferred<void>>>,
  batchId: LeaderSyncMachine.PullBatchId,
) =>
  Effect.gen(function* () {
    let completion: Deferred.Deferred<void> | undefined
    yield* Ref.update(batchesRef, (current) => {
      const next = new Map(current)
      completion = next.get(batchId)
      next.delete(batchId)
      return next
    })
    if (completion !== undefined) yield* Deferred.succeed(completion, undefined)
  })

const countRequestItems = (items: ReadonlyArray<LeaderSyncMachine.LocalItem>) => {
  const counts = new Map<LeaderSyncMachine.LocalRequestId, number>()
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

const isSameSequencePosition = (
  left: EventSequenceNumber.Client.Composite,
  right: EventSequenceNumber.Client.Composite,
) => left.global === right.global && left.client === right.client
