import { Machine } from '@typeonce/effect-machine'

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
  Queue,
  ReadonlyArray,
  Ref,
  Schema,
  Stream,
  Subscribable,
  SubscriptionRef,
} from '@livestore/utils/effect'

import { UnknownError } from '../adapter-types.ts'
import { PullItem } from '../ClientSessionLeaderThreadProxy.ts'
import { IntentionalShutdownCause } from '../errors.ts'
import * as EventlogSqliteDb from '../EventlogSqliteDb.ts'
import type { LiveStoreSchema } from '../schema/mod.ts'
import { EventSequenceNumber, LiveStoreEvent } from '../schema/mod.ts'
import type { BackendIdMismatchError } from '../sync/errors.ts'
import type * as SyncBackend from '../sync/sync-backend.ts'
import * as SyncState from '../sync/syncstate.ts'
import * as LeaderSyncCommitter from './LeaderSyncCommitter.ts'
import * as ProviderPull from './LeaderSyncProviderPull.ts'
import * as ProviderPush from './LeaderSyncProviderPush.ts'
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
 * Public interface and implementation module for leader sync.
 *
 * The private root machine below owns admission order, commit order, and the canonical sync state. Provider push and
 * pull are deep child modules because each owns an independent retrying lifecycle. `LeaderSyncCommitter` remains the
 * durability interface.
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

  // This is a public read model. Scheduling and commits read only the sync state owned by the root machine.
  const syncStateView = yield* SubscriptionRef.make(initialSyncState)
  const connectedSessions = yield* makePullQueueSet
  const bootDeferred = yield* Deferred.make<EventSequenceNumber.Client.Composite>()
  const machineRuntime = yield* Deferred.make<CoordinatorRuntime>()
  const nextLocalRequestId = yield* Ref.make(1)
  const localRequests = yield* Ref.make<LocalRequestRegistry>({ _tag: 'open', requests: new Map() })
  const bootStarted = yield* Ref.make(false)
  const stopStarted = yield* Ref.make(false)

  const providerPull = yield* ProviderPull.make({
    syncBackend,
    devtoolsLatch,
    dbEventlog,
    live: livePull,
    initialBlockingSyncContext,
  })
  const ProviderPushChild = ProviderPush.make({
    syncBackend,
    devtoolsLatch,
    batchSize: params.backendPushBatchSize ?? 50,
  })
  const ProviderPullChild = providerPull.child

  const localCommitBatchSize = params.localPushBatchSize ?? 10
  const localWorkInitiallyBlocked = testing.delays?.localPushProcessing !== undefined
  const isClientOnlyEvent = (event: LiveStoreEvent.Client.Encoded) =>
    schema.eventsDefsMap.get(event.name)?.options.clientOnly ?? false

  const publish = (publication: Publication) =>
    Effect.gen(function* () {
      yield* SubscriptionRef.set(syncStateView, publication.syncState)
      yield* connectedSessions.offer({
        payload: publication.payload,
        globalHead: publication.syncState.upstreamHead,
        leaderHead: publication.syncState.localHead,
        materializerHashes: publication.materializerHashes,
      })
    })

  const commitLocal = (syncState: SyncState.SyncState, items: ReadonlyArray<LocalItem>) =>
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
      return {
        _tag: 'committed' as const,
        syncState: committedSyncState,
        items,
        pushEvents: receipt.committedEvents.filter((event) => !isClientOnlyEvent(event)),
        publication: {
          syncState: committedSyncState,
          payload: SyncState.PayloadUpstreamAdvance.make({ newEvents: receipt.committedEvents }),
          materializerHashes: receipt.materializerHashes,
        },
      }
    })

  const commitUpstream = (syncState: SyncState.SyncState, batch: ProviderPull.UpstreamBatch) =>
    Effect.gen(function* () {
      if (batch.events.length === 0) {
        return { syncState, batch, pushPlan: undefined, publication: undefined }
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
      return {
        syncState: committedSyncState,
        batch,
        pushPlan: committedSyncState.pending.filter((event) => !isClientOnlyEvent(event)),
        publication: { syncState: committedSyncState, payload, materializerHashes: receipt.materializerHashes },
      }
    })

  const completeLocal = (result: LocalWorkResult, appendProviderPush: (events: EventBatch) => Effect.Effect<void>) =>
    result._tag === 'rejected'
      ? rejectLocalItems(
          localRequests,
          result.items.map((item) => ({ item, error: result.error })),
        )
      : Effect.gen(function* () {
          yield* publish(result.publication)
          if (result.pushEvents.length > 0) {
            yield* appendProviderPush(result.pushEvents)
          }
          yield* completeLocalItems(localRequests, result.items)
        })

  const completeUpstream = (
    result: UpstreamWorkResult,
    replaceProviderPushPlan: (events: EventBatch) => Effect.Effect<void>,
  ) =>
    Effect.gen(function* () {
      if (result.publication !== undefined) yield* publish(result.publication)
      if (result.pushPlan !== undefined) {
        yield* replaceProviderPushPlan(result.pushPlan)
      }
      yield* providerPull.complete(result.batch.batchId)
    })

  const stop = (request: TerminationRequest) =>
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

      yield* Effect.all([interruptAllLocalRequests(localRequests), providerPull.interruptAll])
    })

  const initialRunningData = (): RunningData => ({
    syncState: initialSyncState,
    admissions: [],
    localQueue: [],
    reservations: [],
    upstreamQueue: [],
    localWorkEnabled: localWorkInitiallyBlocked === false,
    pullPagination: 'between-pages',
    termination: undefined,
  })

  const makeMachine = () =>
    Machine.make({
      id: 'LeaderSyncProcessor',
      states: LeaderSyncStates.states,
      events: LeaderSyncEvents,
      internalEvents: InternalEvents,
      initial: (to) => to.Starting(),
    }).handle({
      Starting: {
        on: {
          Boot: (to) =>
            to.full
              .Running()
              .resolve(({ target }) => target.from(initialRunningData(), (running) => running.Idle.from())),
          ShutdownRequested: (to) =>
            to.full.Stopping().resolve(({ target }) => target.from({ request: { _tag: 'shutdown' } })),
        },
      },
      Running: {
        invoke: (from) => [
          from
            .child(ProviderPushChild, {
              input: ({ state }) => ({
                queued: state.syncState.pending.filter((event) => !isClientOnlyEvent(event)),
              }),
            })
            .onFailure((to) =>
              to.none.resolve(({ error }, enqueue) => {
                enqueue.raise(InternalEvents.ChildFailed({ error }))
              }),
            ),
          from
            .child(ProviderPullChild, { input: ({ state }) => ({ cursor: state.syncState.upstreamHead }) })
            .onFailure((to) =>
              to.none.resolve(({ error }, enqueue) => {
                enqueue.raise(InternalEvents.ChildFailed({ error }))
              }),
            ),
        ],
        on: {
          PushRequested: (to) =>
            to.local.update(({ current, event, owner }) =>
              owner.from({ ...current, admissions: [...current.admissions, event] }),
            ),
          LocalWorkEnabled: (to) =>
            to.local.update(({ current, owner }) => owner.from({ ...current, localWorkEnabled: true })),
          UpstreamBatchReceived: (to) =>
            to.local.update(({ current, event, owner }) => {
              const { batch } = event
              return owner.from({
                ...current,
                pullPagination: batch.pageInfo._tag === 'NoMore' ? 'between-pages' : 'more-expected',
                upstreamQueue: [...current.upstreamQueue, batch],
              })
            }),
          PullCompleted: (to) =>
            to.local.update(({ current, owner }) => owner.from({ ...current, pullPagination: 'between-pages' })),
          ProviderPullFailed: (to) =>
            to.local.update(({ current, event, owner }) =>
              owner.from({
                ...current,
                pullPagination: 'between-pages',
                termination: terminationFor({ onError, onBackendIdMismatch }, event.error),
              }),
            ),
          ProviderPushFailed: (to) =>
            to.local.update(({ current, event, owner }, enqueue) => {
              if (event.error._tag === 'ServerAheadError') {
                const pulledThroughRequiredHead =
                  current.syncState.upstreamHead.global >= event.error.minimumExpectedNum - 1
                if (pulledThroughRequiredHead === true) {
                  enqueue.sendTo(
                    ProviderPushChild,
                    ProviderPush.Events.ReplacePlan({
                      events: current.syncState.pending.filter((item) => !isClientOnlyEvent(item)),
                    }),
                  )
                }
                return owner.from(current)
              }
              const termination = terminationFor({ onError, onBackendIdMismatch }, event.error)
              if (termination === undefined) enqueue.sendTo(ProviderPushChild, ProviderPush.Events.Disable())
              return owner.from({ ...current, termination })
            }),
          ChildFailed: (to) =>
            to.local.update(({ current, event, owner }) =>
              owner.from({ ...current, termination: { _tag: 'failure', error: event.error, notify: true } }),
            ),
          ShutdownRequested: (to) =>
            to.local.update(({ current, owner }) => owner.from({ ...current, termination: { _tag: 'shutdown' } })),
        },
        states: {
          Idle: {
            always: (to) =>
              to
                .branches({
                  stop: { target: to.full.Stopping() },
                  admission: { target: to.local.Admitting() },
                  upstream: { target: to.local.CommittingUpstream() },
                  local: { target: to.local.CommittingLocal() },
                })
                .resolve(
                  ({ ancestors, decline, select }) => {
                    const running = ancestors.Running
                    if (running.termination !== undefined) return select.stop.from({ request: running.termination })
                    const admission = running.admissions[0]
                    if (admission !== undefined) return select.admission.from({ admission })
                    const upstream = running.upstreamQueue[0]
                    if (upstream !== undefined) return select.upstream.from({ batch: upstream })
                    if (
                      running.pullPagination !== 'more-expected' &&
                      running.localWorkEnabled === true &&
                      running.localQueue.length > 0
                    ) {
                      return select.local.from({ items: running.localQueue.slice(0, localCommitBatchSize) })
                    }
                    return decline()
                  },
                  { declinable: true },
                ),
          },
          Admitting: {
            invoke: (from) =>
              from
                .effect('admit-local-push', ({ ancestors, state }) => {
                  const pushHead =
                    ancestors.Running.reservations.at(-1)?.event.seqNum ?? ancestors.Running.syncState.localHead
                  const error = validatePushBatch(state.admission.events, pushHead, isClientOnlyEvent)
                  const items = state.admission.events.map((event, index) => ({
                    requestId: state.admission.requestId,
                    index,
                    event,
                  }))
                  const result: AdmissionResult = { admission: state.admission, items, error }
                  return error === undefined
                    ? (testing.hooks?.localPushAdmitted?.(state.admission.events) ?? Effect.void).pipe(
                        Effect.as(result),
                      )
                    : rejectLocalItems(
                        localRequests,
                        items.map((item) => ({ item, error })),
                      ).pipe(Effect.as(result))
                })
                .onDone((to) =>
                  to.local
                    .Idle()
                    .updating(to.branch.Running)
                    .resolve(({ ancestors, output, owner, target }) => {
                      const current = ancestors.Running
                      const admissions = current.admissions.filter(
                        (admission) => admission.requestId !== output.admission.requestId,
                      )
                      return target.from().update(
                        owner.from(
                          output.error === undefined
                            ? {
                                ...current,
                                admissions,
                                localQueue: [...current.localQueue, ...output.items],
                                reservations: [...current.reservations, ...output.items],
                              }
                            : { ...current, admissions },
                        ),
                      )
                    }),
                ),
          },
          CommittingLocal: {
            invoke: (from) =>
              from
                .effect('commit-local', ({ ancestors, state }) => commitLocal(ancestors.Running.syncState, state.items))
                .onDone((to) =>
                  to.local
                    .CompletingLocal()
                    .updating(to.branch.Running)
                    .resolve(({ ancestors, output, owner, target }) => {
                      const current = ancestors.Running
                      if (output._tag === 'rejected') {
                        const generation = output.items[0]?.event.seqNum.rebaseGeneration
                        const rejectedItems = current.localQueue.filter(
                          (item) => item.event.seqNum.rebaseGeneration === generation,
                        )
                        const rejectedKeys = new Set(rejectedItems.map(localItemKey))
                        return target.from({ result: { ...output, items: rejectedItems } }).update(
                          owner.from({
                            ...current,
                            localQueue: current.localQueue.filter((item) => !rejectedKeys.has(localItemKey(item))),
                            reservations: current.reservations.filter((item) => !rejectedKeys.has(localItemKey(item))),
                          }),
                        )
                      }
                      const completedKeys = new Set(output.items.map(localItemKey))
                      return target.from({ result: output }).update(
                        owner.from({
                          ...current,
                          syncState: output.syncState,
                          localQueue: current.localQueue.filter((item) => !completedKeys.has(localItemKey(item))),
                          reservations: current.reservations.filter((item) => !completedKeys.has(localItemKey(item))),
                        }),
                      )
                    }),
                )
                .onFailure((to) =>
                  to.local
                    .Idle()
                    .updating(to.branch.Running)
                    .resolve(({ ancestors, error, owner, target }) =>
                      target.from().update(
                        owner.from({
                          ...ancestors.Running,
                          termination: { _tag: 'failure', error, notify: onError === 'shutdown' },
                        }),
                      ),
                    ),
                ),
          },
          CompletingLocal: {
            invoke: (from) =>
              from
                .effect('complete-local', ({ children, state }) =>
                  completeLocal(state.result, (events) =>
                    children.sendTo(ProviderPushChild, ProviderPush.Events.Append({ events })).pipe(Effect.orDie),
                  ),
                )
                .onDone((to) => to.local.Idle()),
          },
          CommittingUpstream: {
            invoke: (from) =>
              from
                .effect('commit-upstream', ({ ancestors, state }) =>
                  commitUpstream(ancestors.Running.syncState, state.batch),
                )
                .onDone((to) =>
                  to.local
                    .CompletingUpstream()
                    .updating(to.branch.Running)
                    .resolve(({ ancestors, output, owner, target }) =>
                      target.from({ result: output }).update(
                        owner.from({
                          ...ancestors.Running,
                          syncState: output.syncState,
                          upstreamQueue: ancestors.Running.upstreamQueue.filter(
                            (batch) => batch.batchId !== output.batch.batchId,
                          ),
                        }),
                      ),
                    ),
                )
                .onFailure((to) =>
                  to.local
                    .Idle()
                    .updating(to.branch.Running)
                    .resolve(({ ancestors, error, owner, target }) =>
                      target.from().update(
                        owner.from({
                          ...ancestors.Running,
                          termination: { _tag: 'failure', error, notify: onError === 'shutdown' },
                        }),
                      ),
                    ),
                ),
          },
          CompletingUpstream: {
            invoke: (from) =>
              from
                .effect('complete-upstream', ({ children, state }) =>
                  completeUpstream(state.result, (events) =>
                    children.sendTo(ProviderPushChild, ProviderPush.Events.ReplacePlan({ events })).pipe(Effect.orDie),
                  ),
                )
                .onDone((to) => to.local.Idle()),
          },
        },
      },
      Stopping: {
        invoke: (from) => from.effect('stop', ({ state }) => stop(state.request)).onDone((to) => to.full.Stopped()),
      },
      Stopped: {},
    })

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
      yield* machine.send(LeaderSyncEvents.PushRequested({ requestId, events }))
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

    const ref = yield* Machine.start(makeMachine()).pipe(Effect.orDie)
    const stopped = yield* Deferred.make<void>()
    yield* ref.join.pipe(
      Effect.catchCause((cause) => stop({ _tag: 'failure', error: cause, notify: true })),
      Effect.ensuring(Deferred.succeed(stopped, undefined)),
      Effect.forkScoped,
    )
    const machine: CoordinatorRuntime = {
      send: (event) => ref.send(event).pipe(Effect.ignore),
      join: Deferred.await(stopped),
    }
    yield* Deferred.succeed(machineRuntime, machine)
    yield* machine.send(LeaderSyncEvents.Boot())
    if (testing.delays?.localPushProcessing !== undefined) {
      yield* testing.delays.localPushProcessing.pipe(
        Effect.andThen(machine.send(LeaderSyncEvents.LocalWorkEnabled())),
        Effect.forkScoped,
      )
    }
    yield* Deferred.succeed(bootDeferred, initialSyncState.localHead)
    yield* Effect.addFinalizer(() =>
      machine.send(LeaderSyncEvents.ShutdownRequested()).pipe(Effect.andThen(machine.join), Effect.ignore),
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
      get: SubscriptionRef.get(syncStateView),
      changes: SubscriptionRef.changes(syncStateView),
    }),
  })
})

export const layer = (options: Options) => Layer.effect(LeaderSyncProcessor, make(options))

type LocalRequestId = number
type EventBatch = ReadonlyArray<LiveStoreEvent.Client.Encoded>

interface LocalItem {
  readonly requestId: LocalRequestId
  readonly index: number
  readonly event: LiveStoreEvent.Client.Encoded
}

interface LocalAdmission {
  readonly requestId: LocalRequestId
  readonly events: EventBatch
}

type TerminationRequest =
  | { readonly _tag: 'shutdown' }
  | { readonly _tag: 'failure'; readonly error: unknown; readonly notify: boolean }
  | { readonly _tag: 'reset'; readonly error: BackendIdMismatchError }

interface RunningData {
  readonly syncState: SyncState.SyncState
  readonly admissions: ReadonlyArray<LocalAdmission>
  readonly localQueue: ReadonlyArray<LocalItem>
  readonly reservations: ReadonlyArray<LocalItem>
  readonly upstreamQueue: ReadonlyArray<ProviderPull.UpstreamBatch>
  readonly localWorkEnabled: boolean
  readonly pullPagination: 'between-pages' | 'more-expected'
  readonly termination: TerminationRequest | undefined
}

interface Publication {
  readonly syncState: SyncState.SyncState
  readonly payload: typeof SyncState.PayloadUpstream.Type
  readonly materializerHashes: ReadonlyArray<LiveStoreEvent.Client.MaterializerHash>
}

type LocalWorkResult =
  | {
      readonly _tag: 'committed'
      readonly syncState: SyncState.SyncState
      readonly items: ReadonlyArray<LocalItem>
      readonly pushEvents: EventBatch
      readonly publication: Publication
    }
  | {
      readonly _tag: 'rejected'
      readonly error: RejectedPushError
      readonly items: ReadonlyArray<LocalItem>
    }

interface UpstreamWorkResult {
  readonly syncState: SyncState.SyncState
  readonly batch: ProviderPull.UpstreamBatch
  readonly pushPlan: EventBatch | undefined
  readonly publication: Publication | undefined
}

interface AdmissionResult {
  readonly admission: LocalAdmission
  readonly items: ReadonlyArray<LocalItem>
  readonly error: RejectedPushError | undefined
}

interface CoordinatorRuntime {
  readonly send: (event: LeaderSyncEventInput) => Effect.Effect<void>
  readonly join: Effect.Effect<void>
}

type LeaderSyncEventInput = Machine.Machine.EventInputOf<Machine.Machine.EventProtocolSchemas<typeof LeaderSyncEvents>>

interface LocalRequest {
  readonly deferred: Deferred.Deferred<void, RejectedPushError>
  readonly remaining: number
}

type LocalRequestRegistry =
  | { readonly _tag: 'open'; readonly requests: Map<LocalRequestId, LocalRequest> }
  | { readonly _tag: 'closed' }

const opaque = <A>(identifier: string) =>
  Schema.declare<A>((value): value is A => value !== undefined, { identifier, expected: identifier })

const LocalItemSchema = opaque<LocalItem>('LeaderSyncProcessor.LocalItem')
const LocalAdmissionSchema = opaque<LocalAdmission>('LeaderSyncProcessor.LocalAdmission')
const LocalWorkResultSchema = opaque<LocalWorkResult>('LeaderSyncProcessor.LocalWorkResult')
const UpstreamWorkResultSchema = opaque<UpstreamWorkResult>('LeaderSyncProcessor.UpstreamWorkResult')
const UpstreamBatchSchema = opaque<ProviderPull.UpstreamBatch>('LeaderSyncProcessor.UpstreamBatch')
const TerminationRequestSchema = opaque<TerminationRequest>('LeaderSyncProcessor.TerminationRequest')

const State = Schema.TaggedUnion({
  Running: {
    syncState: SyncState.SyncState,
    admissions: Schema.Array(LocalAdmissionSchema),
    localQueue: Schema.Array(LocalItemSchema),
    reservations: Schema.Array(LocalItemSchema),
    upstreamQueue: Schema.Array(UpstreamBatchSchema),
    localWorkEnabled: Schema.Boolean,
    pullPagination: Schema.Literals(['between-pages', 'more-expected']),
    termination: Schema.UndefinedOr(TerminationRequestSchema),
  },
  Admitting: { admission: LocalAdmissionSchema },
  CommittingLocal: { items: Schema.Array(LocalItemSchema) },
  CompletingLocal: { result: LocalWorkResultSchema },
  CommittingUpstream: { batch: UpstreamBatchSchema },
  CompletingUpstream: { result: UpstreamWorkResultSchema },
  Stopping: { request: TerminationRequestSchema },
})

const LeaderSyncStates = Machine.states({
  Starting: {},
  Running: {
    schema: State.cases.Running,
    initial: 'Idle',
    states: {
      Idle: {},
      Admitting: State.cases.Admitting,
      CommittingLocal: State.cases.CommittingLocal,
      CompletingLocal: State.cases.CompletingLocal,
      CommittingUpstream: State.cases.CommittingUpstream,
      CompletingUpstream: State.cases.CompletingUpstream,
    },
  },
  Stopping: State.cases.Stopping,
  Stopped: { type: 'final' },
})

const LeaderSyncEvents = Machine.events(
  Schema.TaggedUnion({
    Boot: {},
    PushRequested: { requestId: Schema.Number, events: Schema.Array(LiveStoreEvent.Client.Encoded) },
    LocalWorkEnabled: {},
    ShutdownRequested: {},
  }),
  ProviderPush.ParentEvents,
  ProviderPull.ParentEvents,
)

const InternalEvents = Machine.internalEvents(
  Schema.TaggedUnion({
    ChildFailed: { error: Schema.Defect() },
  }),
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

const replacePendingEvents = (syncState: SyncState.SyncState, committedEvents: EventBatch): SyncState.SyncState =>
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
    yield* Effect.forEach(completions, (deferred) => Deferred.succeed(deferred, undefined), { discard: true })
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

const interruptAllLocalRequests = (requestsRef: Ref.Ref<LocalRequestRegistry>) =>
  Effect.gen(function* () {
    const registry = yield* Ref.getAndSet(requestsRef, { _tag: 'closed' })
    if (registry._tag === 'closed') return
    yield* Effect.forEach(registry.requests.values(), ({ deferred }) => Deferred.interrupt(deferred), { discard: true })
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

const terminationFor = (
  config: { readonly onError: 'shutdown' | 'ignore'; readonly onBackendIdMismatch: 'reset' | 'shutdown' | 'ignore' },
  error: ProviderPull.ProviderPullError | ProviderPush.ProviderPushError,
): TerminationRequest | undefined => {
  if (error._tag === 'BackendIdMismatchError') {
    switch (config.onBackendIdMismatch) {
      case 'ignore':
        return undefined
      case 'shutdown':
        return { _tag: 'failure', error, notify: true }
      case 'reset':
        return { _tag: 'reset', error }
    }
  }
  return config.onError === 'shutdown' ? { _tag: 'failure', error, notify: true } : undefined
}

const localItemKey = (item: LocalItem) => `${item.requestId}:${item.index}`

const isSameSequencePosition = (
  left: EventSequenceNumber.Client.Composite,
  right: EventSequenceNumber.Client.Composite,
) => left.global === right.global && left.client === right.client
