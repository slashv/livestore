import { Machine } from '@typeonce/effect-machine'

import { Deferred, Duration, Effect, Schema, type Scope, Stream } from '@livestore/utils/effect'

import type { UnknownError } from '../adapter-types.ts'
import { EventSequenceNumber, LiveStoreEvent } from '../schema/mod.ts'
import type { BackendIdMismatchError, IsOfflineError, ServerAheadError } from '../sync/errors.ts'
import type * as SyncBackend from '../sync/sync-backend.ts'
import * as SyncState from '../sync/syncstate.ts'
import type * as LeaderSyncCommitter from './LeaderSyncCommitter.ts'
import type { RejectedPushError } from './RejectedPushError.ts'

export type OperationId = number
export type LocalRequestId = number
export type PullBatchId = number
export type EventBatch = ReadonlyArray<LiveStoreEvent.Client.Encoded>

export interface LocalItem {
  readonly requestId: LocalRequestId
  readonly index: number
  readonly event: LiveStoreEvent.Client.Encoded
}

export interface LocalAdmission {
  readonly requestId: LocalRequestId
  readonly events: EventBatch
}

export interface UpstreamBatch {
  readonly batchId: PullBatchId
  readonly events: EventBatch
  readonly pulledEvents: ReadonlyArray<LeaderSyncCommitter.PulledEvent>
  readonly pageInfo: SyncBackend.PullResPageInfo
}

export type ProviderPushError = IsOfflineError | BackendIdMismatchError | UnknownError | ServerAheadError
export type ProviderPullError = IsOfflineError | BackendIdMismatchError | UnknownError

export type TerminationRequest =
  | { readonly _tag: 'shutdown' }
  | { readonly _tag: 'failure'; readonly error: unknown; readonly notify: boolean }
  | { readonly _tag: 'reset'; readonly error: BackendIdMismatchError }

export interface RunningData {
  readonly syncState: SyncState.SyncState
  readonly admissions: ReadonlyArray<LocalAdmission>
  readonly localQueue: ReadonlyArray<LocalItem>
  readonly reservations: ReadonlyArray<LocalItem>
  readonly upstreamQueue: ReadonlyArray<UpstreamBatch>
  readonly localWorkEnabled: boolean
  readonly pullPagination: 'between-pages' | 'more-expected'
  readonly termination: TerminationRequest | undefined
}

export type LocalWorkResult =
  | {
      readonly _tag: 'committed'
      readonly syncState: SyncState.SyncState
      readonly items: ReadonlyArray<LocalItem>
      readonly pushEvents: EventBatch
    }
  | {
      readonly _tag: 'rejected'
      readonly error: RejectedPushError
      readonly items: ReadonlyArray<LocalItem>
    }

export interface UpstreamWorkResult {
  readonly syncState: SyncState.SyncState
  readonly batch: UpstreamBatch
  readonly pushPlan: EventBatch | undefined
}

interface AdmissionResult {
  readonly admission: LocalAdmission
  readonly items: ReadonlyArray<LocalItem>
  readonly error: RejectedPushError | undefined
}

export interface Config {
  readonly backendEnabled: boolean
  readonly localCommitBatchSize: number
  readonly backendPushBatchSize: number
  readonly localWorkInitiallyBlocked: boolean
  readonly onError: 'shutdown' | 'ignore'
  readonly onBackendIdMismatch: 'reset' | 'shutdown' | 'ignore'
}

export interface Dependencies {
  readonly initialSyncState: SyncState.SyncState
  readonly config: Config
  readonly isClientOnlyEvent: (event: LiveStoreEvent.Client.Encoded) => boolean
  readonly validatePushBatch: (
    batch: EventBatch,
    pushHead: EventSequenceNumber.Client.Composite,
  ) => RejectedPushError | undefined
  readonly processLocal: (
    syncState: SyncState.SyncState,
    items: ReadonlyArray<LocalItem>,
  ) => Effect.Effect<LocalWorkResult, unknown>
  readonly processUpstream: (
    syncState: SyncState.SyncState,
    batch: UpstreamBatch,
  ) => Effect.Effect<UpstreamWorkResult, unknown>
  readonly settleLocal: (result: LocalWorkResult) => Effect.Effect<void>
  readonly stop: (request: TerminationRequest) => Effect.Effect<void>
  readonly providerPush: (batch: EventBatch) => Effect.Effect<void, ProviderPushError>
  readonly providerPull: (
    cursor: EventSequenceNumber.Client.Composite,
    parent: Machine.MachineTarget<LeaderSyncEventInput>,
  ) => Effect.Effect<void, ProviderPullError>
}

const opaque = <A>(identifier: string) =>
  Schema.declare<A>((value): value is A => value !== undefined, { identifier, expected: identifier })

const LocalItemSchema = opaque<LocalItem>('LeaderSyncMachine.LocalItem')
const LocalAdmissionSchema = opaque<LocalAdmission>('LeaderSyncMachine.LocalAdmission')
const UpstreamBatchSchema = opaque<UpstreamBatch>('LeaderSyncMachine.UpstreamBatch')
const TerminationRequestSchema = opaque<TerminationRequest>('LeaderSyncMachine.TerminationRequest')
const ProviderPushErrorSchema = opaque<ProviderPushError>('LeaderSyncMachine.ProviderPushError')
const ProviderPullErrorSchema = opaque<ProviderPullError>('LeaderSyncMachine.ProviderPullError')
const LocalWorkResultSchema = opaque<LocalWorkResult>('LeaderSyncMachine.LocalWorkResult')
const UpstreamWorkResultSchema = opaque<UpstreamWorkResult>('LeaderSyncMachine.UpstreamWorkResult')
const AdmissionResultSchema = opaque<AdmissionResult>('LeaderSyncMachine.AdmissionResult')

const RootState = Schema.TaggedUnion({
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
  ApplyingAdmission: { admission: LocalAdmissionSchema },
  CommittingLocal: { items: Schema.Array(LocalItemSchema) },
  CommittingUpstream: { batch: UpstreamBatchSchema },
  Stopping: { request: TerminationRequestSchema },
})

export const LeaderSyncStates = Machine.states({
  Starting: {},
  Running: {
    schema: RootState.cases.Running,
    initial: 'Ready',
    states: {
      Ready: {},
      Admitting: {},
      ApplyingAdmission: RootState.cases.ApplyingAdmission,
      SelectingUpstream: {},
      SelectingLocal: {},
      Waiting: {},
      CommittingLocal: RootState.cases.CommittingLocal,
      CommittingUpstream: RootState.cases.CommittingUpstream,
    },
  },
  Stopping: RootState.cases.Stopping,
  Stopped: { type: 'final' },
})

export const LeaderSyncEvents = Machine.events(
  Schema.TaggedUnion({
    Boot: {},
    PushRequested: { requestId: Schema.Number, events: Schema.Array(LiveStoreEvent.Client.Encoded) },
    LocalWorkEnabled: {},
    UpstreamBatchReceived: { batch: UpstreamBatchSchema },
    PullCompleted: {},
    ProviderPullFailed: { error: ProviderPullErrorSchema },
    ProviderPushFailed: { error: ProviderPushErrorSchema },
    ShutdownRequested: {},
  }),
)

const InternalEvents = Machine.internalEvents(
  Schema.TaggedUnion({
    ChildFailed: { error: Schema.Defect() },
  }),
)

export const LeaderSyncEmissions = Machine.emittedEvents(
  Schema.TaggedUnion({
    LocalPushAdmitted: { events: Schema.Array(LiveStoreEvent.Client.Encoded) },
    LocalItemsCompleted: { items: Schema.Array(LocalItemSchema) },
    LocalItemsRejected: {
      rejections: Schema.Array(
        opaque<{ readonly item: LocalItem; readonly error: RejectedPushError }>('LeaderSyncMachine.LocalItemRejection'),
      ),
    },
    PullBatchReleased: { batchId: Schema.Number },
  }),
)

const ProviderPushState = Schema.TaggedUnion({
  Active: { queued: Schema.Array(LiveStoreEvent.Client.Encoded) },
  InFlight: {
    batch: Schema.Array(LiveStoreEvent.Client.Encoded),
    attempt: Schema.Number,
  },
  RetryWaiting: {
    batch: Schema.Array(LiveStoreEvent.Client.Encoded),
    attempt: Schema.Number,
  },
  HandlingFailure: {
    batch: Schema.Array(LiveStoreEvent.Client.Encoded),
    attempt: Schema.Number,
    error: ProviderPushErrorSchema,
  },
  RestoringAwaitingPull: { batch: Schema.Array(LiveStoreEvent.Client.Encoded) },
  AwaitingPull: {},
  Failed: { error: ProviderPushErrorSchema },
})

const ProviderPushStates = Machine.states({
  Disabled: {},
  Active: {
    schema: ProviderPushState.cases.Active,
    initial: 'Idle',
    states: {
      Idle: {},
      InFlight: ProviderPushState.cases.InFlight,
      RetryWaiting: ProviderPushState.cases.RetryWaiting,
      HandlingFailure: ProviderPushState.cases.HandlingFailure,
      RestoringAwaitingPull: ProviderPushState.cases.RestoringAwaitingPull,
      AwaitingPull: ProviderPushState.cases.AwaitingPull,
      Failed: ProviderPushState.cases.Failed,
    },
  },
})

const ProviderPushEvents = Machine.events(
  Schema.TaggedUnion({
    Append: { events: Schema.Array(LiveStoreEvent.Client.Encoded) },
    ReplacePlan: { events: Schema.Array(LiveStoreEvent.Client.Encoded) },
    Disable: {},
  }),
)

const ProviderPushInput = Schema.Struct({
  enabled: Schema.Boolean,
  queued: Schema.Array(LiveStoreEvent.Client.Encoded),
})

const makeProviderPushMachine = (dependencies: Dependencies) =>
  Machine.make({
    id: 'LeaderProviderPush',
    states: ProviderPushStates.states,
    events: ProviderPushEvents,
    parent: Machine.parent(LeaderSyncEvents),
    input: ProviderPushInput,
    initial: (to) =>
      dependencies.config.backendEnabled === true
        ? to.Active.initial.resolve(({ input, target }) =>
            target.from({ queued: input.queued }, (active) => active.Idle.from()),
          )
        : to.Disabled(),
  }).handle({
    Disabled: {},
    Active: {
      on: {
        Append: (to) =>
          to.local.update(({ current, event, owner }) => owner.from({ queued: [...current.queued, ...event.events] })),
        ReplacePlan: (to) =>
          to.full
            .Active()
            .resolve(({ event, target }) => target.from({ queued: event.events }, (active) => active.Idle.from())),
        Disable: (to) => to.full.Disabled(),
      },
      states: {
        Idle: {
          always: (to) =>
            to.local
              .InFlight()
              .updating(to.branch.Active)
              .resolve(
                ({ containingState, decline, owner, target }) => {
                  if (containingState.queued.length === 0) return decline()
                  const batch = containingState.queued.slice(0, dependencies.config.backendPushBatchSize)
                  return target
                    .from({ batch, attempt: 0 })
                    .update(owner.from({ queued: containingState.queued.slice(batch.length) }))
                },
                { declinable: true },
              ),
        },
        InFlight: {
          invoke: (from) =>
            from
              .effect('provider-push', ({ parent, state }) =>
                dependencies
                  .providerPush(state.batch)
                  .pipe(
                    Effect.tapError((error) =>
                      error._tag === 'BackendIdMismatchError'
                        ? parent.send(LeaderSyncEvents.ProviderPushFailed({ error })).pipe(Effect.ignore)
                        : Effect.void,
                    ),
                  ),
              )
              .onDone((to) => to.local.Idle())
              .onFailure((to) =>
                to.local
                  .HandlingFailure()
                  .resolve(({ error, state, target }) =>
                    target.from({ batch: state.batch, attempt: state.attempt, error }),
                  ),
              ),
        },
        RetryWaiting: {
          invoke: (from) =>
            from
              .timer('provider-push-retry', ({ state }) => Duration.millis(retryDelay(state.attempt)))
              .onDone((to) =>
                to.local
                  .InFlight()
                  .resolve(({ state, target }) => target.from({ batch: state.batch, attempt: state.attempt })),
              ),
        },
        HandlingFailure: {
          always: (to) =>
            to
              .branches({
                awaitingPull: { target: to.local.RestoringAwaitingPull() },
                failed: { target: to.local.Failed() },
                retry: { target: to.local.RetryWaiting() },
              })
              .resolve(({ select, state }) => {
                if (state.error._tag === 'ServerAheadError') {
                  return select.awaitingPull.from({ batch: state.batch })
                }
                if (state.error._tag === 'BackendIdMismatchError') {
                  return select.failed.from({ error: state.error })
                }
                return select.retry.from({ batch: state.batch, attempt: state.attempt + 1 })
              }),
        },
        RestoringAwaitingPull: {
          always: (to) =>
            to.local
              .AwaitingPull()
              .updating(to.branch.Active)
              .resolve(({ containingState, owner, state, target }) =>
                target.from().update(owner.from({ queued: [...state.batch, ...containingState.queued] })),
              ),
        },
        AwaitingPull: {},
        Failed: {},
      },
    },
  })

const ProviderPullState = Schema.TaggedUnion({
  Streaming: { cursor: EventSequenceNumber.Client.Composite, attempt: Schema.Number },
  RetryWaiting: { cursor: EventSequenceNumber.Client.Composite, attempt: Schema.Number },
  Failed: { error: ProviderPullErrorSchema },
})

const ProviderPullStates = Machine.states({
  Disabled: {},
  Streaming: ProviderPullState.cases.Streaming,
  RetryWaiting: ProviderPullState.cases.RetryWaiting,
  Completed: {},
  Failed: ProviderPullState.cases.Failed,
})

const ProviderPullEvents = Machine.events(Schema.TaggedUnion({ Disable: {} }))
const ProviderPullInput = Schema.Struct({
  enabled: Schema.Boolean,
  cursor: EventSequenceNumber.Client.Composite,
})

const makeProviderPullMachine = (dependencies: Dependencies) =>
  Machine.make({
    id: 'LeaderProviderPull',
    states: ProviderPullStates.states,
    events: ProviderPullEvents,
    parent: Machine.parent(LeaderSyncEvents),
    input: ProviderPullInput,
    initial: (to) =>
      dependencies.config.backendEnabled === true
        ? to.Streaming().resolve(({ input, target }) => target.from({ cursor: input.cursor, attempt: 0 }))
        : to.Disabled(),
  }).handle({
    Disabled: {},
    Streaming: {
      invoke: (from) =>
        from
          .effect('provider-pull', ({ parent, state }) =>
            dependencies.providerPull(state.cursor, parent).pipe(
              Effect.tapError((error) =>
                error._tag === 'IsOfflineError'
                  ? Effect.void
                  : parent.send(LeaderSyncEvents.ProviderPullFailed({ error })).pipe(Effect.ignore),
              ),
              Effect.ensuring(parent.send(LeaderSyncEvents.PullCompleted()).pipe(Effect.ignore)),
            ),
          )
          .onDone((to) => to.full.Completed())
          .onFailure((to) =>
            to
              .branches({
                retry: { target: to.full.RetryWaiting() },
                failed: { target: to.full.Failed() },
              })
              .resolve(({ error, select, state }) => {
                if (error._tag === 'IsOfflineError') {
                  return select.retry.from({ cursor: state.cursor, attempt: state.attempt + 1 })
                }
                return select.failed.from({ error })
              }),
          ),
      on: { Disable: (to) => to.full.Disabled() },
    },
    RetryWaiting: {
      invoke: (from) =>
        from
          .timer('provider-pull-retry', ({ state }) => Duration.millis(retryDelay(state.attempt)))
          .onDone((to) =>
            to.full
              .Streaming()
              .resolve(({ state, target }) => target.from({ cursor: state.cursor, attempt: state.attempt })),
          ),
      on: { Disable: (to) => to.full.Disabled() },
    },
    Completed: {},
    Failed: { on: { Disable: (to) => to.full.Disabled() } },
  })

/**
 * Creates the leader coordinator and its two state-owned provider machines.
 *
 * Durable local/upstream work stays in one compound branch, while provider push and pull are independent child
 * statecharts. This avoids a deeply nested Cartesian state tree without hiding either provider lifecycle in flags.
 */
const makeLeaderSyncMachine = (dependencies: Dependencies) => {
  const ProviderPush = Machine.child('provider-push', makeProviderPushMachine(dependencies))
  const ProviderPull = Machine.child('provider-pull', makeProviderPullMachine(dependencies))
  const initialRunningData = (): RunningData => ({
    syncState: dependencies.initialSyncState,
    admissions: [],
    localQueue: [],
    reservations: [],
    upstreamQueue: [],
    localWorkEnabled: dependencies.config.localWorkInitiallyBlocked === false,
    pullPagination: 'between-pages',
    termination: undefined,
  })

  return Machine.make({
    id: 'LeaderSyncProcessor',
    states: LeaderSyncStates.states,
    events: LeaderSyncEvents,
    internalEvents: InternalEvents,
    emittedEvents: LeaderSyncEmissions,
    initial: (to) => to.Starting(),
  }).handle({
    Starting: {
      on: {
        Boot: (to) =>
          to.full
            .Running()
            .resolve(({ target }) => target.from(initialRunningData(), (running) => running.Ready.from())),
        ShutdownRequested: (to) =>
          to.full.Stopping().resolve(({ target }) => target.from({ request: { _tag: 'shutdown' } })),
      },
    },
    Running: {
      invoke: (from) => [
        from
          .child(ProviderPush, {
            input: ({ state }) => ({
              enabled: dependencies.config.backendEnabled,
              queued: state.syncState.pending.filter((event) => !dependencies.isClientOnlyEvent(event)),
            }),
          })
          .onFailure((to) =>
            to.none.resolve(({ error }, enqueue) => {
              enqueue.raise(InternalEvents.ChildFailed({ error }))
            }),
          ),
        from
          .child(ProviderPull, {
            input: ({ state }) => ({
              enabled: dependencies.config.backendEnabled,
              cursor: state.syncState.upstreamHead,
            }),
          })
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
            const pullPagination = batch.pageInfo._tag === 'NoMore' ? 'between-pages' : 'more-expected'
            return owner.from({
              ...current,
              pullPagination,
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
              termination: terminationFor(dependencies.config, event.error, 'pull'),
            }),
          ),
        ProviderPushFailed: (to) =>
          to.local.update(({ current, event, owner }, enqueue) => {
            const termination = terminationFor(dependencies.config, event.error, 'push')
            if (termination === undefined) enqueue.sendTo(ProviderPush, ProviderPushEvents.Disable())
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
        Ready: {
          always: (to) => to.local.Admitting(),
        },
        Admitting: {
          always: (to) =>
            to
              .branches({
                stop: { target: to.full.Stopping() },
                admission: { target: to.local.ApplyingAdmission() },
                next: { target: to.local.SelectingUpstream() },
              })
              .resolve(({ ancestors, select }) => {
                const running = ancestors.Running
                if (running.termination !== undefined) return select.stop.from({ request: running.termination })
                const admission = running.admissions[0]
                return admission === undefined ? select.next.from() : select.admission.from({ admission })
              }),
        },
        ApplyingAdmission: {
          invoke: (from) =>
            from
              .effect('admit-local-push', ({ ancestors, state }) => {
                const pushHead =
                  ancestors.Running.reservations.at(-1)?.event.seqNum ?? ancestors.Running.syncState.localHead
                const error = dependencies.validatePushBatch(state.admission.events, pushHead)
                const items = state.admission.events.map((event, index) => ({
                  requestId: state.admission.requestId,
                  index,
                  event,
                }))
                const result: AdmissionResult = { admission: state.admission, items, error }
                return error === undefined
                  ? Effect.succeed(result)
                  : dependencies.settleLocal({ _tag: 'rejected', error, items }).pipe(Effect.as(result))
              })
              .onDone((to) =>
                to.local
                  .Ready()
                  .updating(to.branch.Running)
                  .resolve(({ ancestors, output, owner, target }, enqueue) => {
                    const current = ancestors.Running
                    const admissions = current.admissions.filter(
                      (admission) => admission.requestId !== output.admission.requestId,
                    )
                    if (output.error === undefined) {
                      enqueue.emit(LeaderSyncEmissions.LocalPushAdmitted({ events: output.admission.events }))
                    }
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
        SelectingUpstream: {
          always: (to) =>
            to
              .branches({
                commit: { target: to.local.CommittingUpstream() },
                next: { target: to.local.SelectingLocal() },
              })
              .resolve(({ ancestors, select }) => {
                const batch = ancestors.Running.upstreamQueue[0]
                return batch === undefined ? select.next.from() : select.commit.from({ batch })
              }),
        },
        SelectingLocal: {
          always: (to) =>
            to
              .branches({
                commit: { target: to.local.CommittingLocal() },
                wait: { target: to.local.Waiting() },
              })
              .resolve(({ ancestors, select }) => {
                const running = ancestors.Running
                if (
                  running.pullPagination === 'more-expected' ||
                  running.localWorkEnabled === false ||
                  running.localQueue.length === 0
                ) {
                  return select.wait.from()
                }
                return select.commit.from({
                  items: running.localQueue.slice(0, dependencies.config.localCommitBatchSize),
                })
              }),
        },
        Waiting: {
          always: (to) =>
            to.local.Ready().resolve(
              ({ ancestors, decline, target }) => {
                const running = ancestors.Running
                const hasWork =
                  running.termination !== undefined ||
                  running.admissions.length > 0 ||
                  running.upstreamQueue.length > 0 ||
                  (running.pullPagination !== 'more-expected' &&
                    running.localWorkEnabled === true &&
                    running.localQueue.length > 0)
                return hasWork === true ? target.from() : decline()
              },
              { declinable: true },
            ),
        },
        CommittingLocal: {
          invoke: (from) =>
            from
              .effect('commit-local', ({ ancestors, children, state }) =>
                Effect.gen(function* () {
                  const result = yield* dependencies.processLocal(ancestors.Running.syncState, state.items)
                  if (result._tag === 'committed' && result.pushEvents.length > 0) {
                    yield* children.sendTo(ProviderPush, ProviderPushEvents.Append({ events: result.pushEvents }))
                  }
                  yield* dependencies.settleLocal(result)
                  return result
                }),
              )
              .onDone((to) =>
                to.local
                  .Ready()
                  .updating(to.branch.Running)
                  .resolve(({ ancestors, output, owner, target }, enqueue) => {
                    const current = ancestors.Running
                    if (output._tag === 'rejected') {
                      const generation = output.items[0]?.event.seqNum.rebaseGeneration
                      const queuedSameGeneration = current.localQueue.filter(
                        (item) => item.event.seqNum.rebaseGeneration === generation,
                      )
                      const rejectedItems = [...output.items, ...queuedSameGeneration]
                      const rejectedKeys = new Set(rejectedItems.map(localItemKey))
                      enqueue.emit(
                        LeaderSyncEmissions.LocalItemsRejected({
                          rejections: rejectedItems.map((item) => ({ item, error: output.error })),
                        }),
                      )
                      return target.from().update(
                        owner.from({
                          ...current,
                          localQueue: current.localQueue.filter((item) => !rejectedKeys.has(localItemKey(item))),
                          reservations: current.reservations.filter((item) => !rejectedKeys.has(localItemKey(item))),
                        }),
                      )
                    }
                    const completedKeys = new Set(output.items.map(localItemKey))
                    enqueue.emit(LeaderSyncEmissions.LocalItemsCompleted({ items: output.items }))
                    return target.from().update(
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
                  .Ready()
                  .updating(to.branch.Running)
                  .resolve(({ ancestors, error, owner, target }) =>
                    target.from().update(
                      owner.from({
                        ...ancestors.Running,
                        termination: { _tag: 'failure', error, notify: dependencies.config.onError === 'shutdown' },
                      }),
                    ),
                  ),
              ),
        },
        CommittingUpstream: {
          invoke: (from) =>
            from
              .effect('commit-upstream', ({ ancestors, state }) =>
                dependencies.processUpstream(ancestors.Running.syncState, state.batch),
              )
              .onDone((to) =>
                to.local
                  .Ready()
                  .updating(to.branch.Running)
                  .resolve(({ ancestors, output, owner, target }, enqueue) => {
                    if (output.pushPlan !== undefined) {
                      enqueue.sendTo(ProviderPush, ProviderPushEvents.ReplacePlan({ events: output.pushPlan }))
                    }
                    return target.from().update(
                      owner.from({
                        ...ancestors.Running,
                        syncState: output.syncState,
                        upstreamQueue: ancestors.Running.upstreamQueue.filter(
                          (batch) => batch.batchId !== output.batch.batchId,
                        ),
                      }),
                    )
                  }),
              )
              .onFailure((to) =>
                to.local
                  .Ready()
                  .updating(to.branch.Running)
                  .resolve(({ ancestors, error, owner, target }) =>
                    target.from().update(
                      owner.from({
                        ...ancestors.Running,
                        termination: { _tag: 'failure', error, notify: dependencies.config.onError === 'shutdown' },
                      }),
                    ),
                  ),
              ),
        },
      },
    },
    Stopping: {
      invoke: (from) =>
        from.effect('stop', ({ state }) => dependencies.stop(state.request)).onDone((to) => to.full.Stopped()),
    },
    Stopped: {},
  })
}

export type LeaderSyncEventInput =
  | ReturnType<typeof LeaderSyncEvents.Boot>
  | ReturnType<typeof LeaderSyncEvents.PushRequested>
  | ReturnType<typeof LeaderSyncEvents.LocalWorkEnabled>
  | ReturnType<typeof LeaderSyncEvents.UpstreamBatchReceived>
  | ReturnType<typeof LeaderSyncEvents.PullCompleted>
  | ReturnType<typeof LeaderSyncEvents.ProviderPullFailed>
  | ReturnType<typeof LeaderSyncEvents.ProviderPushFailed>
  | ReturnType<typeof LeaderSyncEvents.ShutdownRequested>
export type LeaderSyncEmission = Machine.EventOf<typeof LeaderSyncEmissions>

export interface Runtime {
  readonly send: (event: LeaderSyncEventInput) => Effect.Effect<void>
  readonly join: Effect.Effect<void>
}

/** Starts only after the hot emission stream has an active consumer. */
export const start = (
  dependencies: Dependencies,
  onEmission: (emission: LeaderSyncEmission) => Effect.Effect<void>,
): Effect.Effect<Runtime, never, Scope.Scope> =>
  Effect.gen(function* () {
    const prepared = yield* Machine.prepare(makeLeaderSyncMachine(dependencies)).pipe(Effect.orDie)
    yield* prepared.emissions.pipe(Stream.runForEach(onEmission), Effect.forkScoped({ startImmediately: true }))
    const ref = yield* prepared.start.pipe(Effect.orDie)
    const stopped = yield* Deferred.make<void>()
    yield* ref.join.pipe(
      Effect.catchCause((cause) => dependencies.stop({ _tag: 'failure', error: cause, notify: true })),
      Effect.ensuring(Deferred.succeed(stopped, undefined)),
      Effect.forkScoped,
    )
    return {
      // A scoped owner can finalize after the machine has already completed its own shutdown transition.
      send: (event) => ref.send(event).pipe(Effect.ignore),
      join: Deferred.await(stopped),
    }
  })

const terminationFor = (
  config: Config,
  error: ProviderPullError | ProviderPushError,
  _direction: 'pull' | 'push',
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
const retryDelay = (attempt: number) => Math.min(30_000, 1_000 * 2 ** Math.max(0, attempt - 1))
