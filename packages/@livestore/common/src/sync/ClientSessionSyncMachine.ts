import { Machine } from '@typeonce/effect-machine'

import { Cause, Effect, Exit, Option, Schema } from '@livestore/utils/effect'

import type { PullItem } from '../ClientSessionLeaderThreadProxy.ts'
import type { MaterializeError } from '../errors.ts'
import type { RejectedPushError } from '../leader-thread/RejectedPushError.ts'
import type * as MaterializationJournal from '../MaterializationJournal.ts'
import { LiveStoreEvent } from '../schema/mod.ts'

export type PullRequestId = number
export type SuspensionId = number
export type EventBatch = ReadonlyArray<LiveStoreEvent.Client.Encoded>
export type ProcessorError = MaterializeError | MaterializationJournal.MaterializationJournalError

export interface PullRequest {
  readonly requestId: PullRequestId
  readonly item: typeof PullItem.Type
}

export interface Rejection {
  readonly error: RejectedPushError
  readonly events: EventBatch
}

export interface PullApplyResult {
  /** Replaces the complete push plan after a rebase or recovered rejection. */
  readonly pushPlan: EventBatch | undefined
}

export interface PushFailure {
  readonly _tag: 'PushFailure'
  readonly cause: Cause.Cause<never>
}

export interface PullFailure {
  readonly _tag: 'PullFailure'
  readonly cause: Cause.Cause<never>
}

interface ApplyPullFailure {
  readonly _tag: 'ApplyPullFailure'
  readonly cause: Cause.Cause<ProcessorError>
}

export interface Suspension {
  readonly suspensionId: SuspensionId
  readonly await: Effect.Effect<void>
}

export interface Dependencies {
  readonly currentPending: () => EventBatch
  readonly leaderPushBatchSize: number
  readonly pushLeader: (batch: EventBatch) => Effect.Effect<void, RejectedPushError | PushFailure>
  readonly runPull: (parent: Machine.MachineTarget<ClientSessionEventInput>) => Effect.Effect<void, PullFailure>
  readonly applyPull: (
    request: PullRequest,
    rejection: Rejection | undefined,
    suspendPush: Effect.Effect<void>,
  ) => Effect.Effect<PullApplyResult, ProcessorError>
  readonly registerSuspension: Effect.Effect<Suspension>
  readonly completeSuspension: (suspensionId: SuspensionId) => Effect.Effect<void>
  readonly completePull: (requestId: PullRequestId) => Effect.Effect<void>
  readonly observeRejection: Effect.Effect<void>
  readonly signalDrainStarted: Effect.Effect<void>
  readonly notifyFailure: (cause: Cause.Cause<ProcessorError>) => Effect.Effect<void>
  readonly finishShutdown: (exit: Exit.Exit<void, never>) => Effect.Effect<void>
  readonly runtimeFailed: (cause: Cause.Cause<unknown>) => Effect.Effect<void>
}

const opaque = <A>(identifier: string) =>
  Schema.declare<A>((value): value is A => value !== undefined, { identifier, expected: identifier })

const PullRequestSchema = opaque<PullRequest>('ClientSessionSyncMachine.PullRequest')
const RejectionSchema = opaque<Rejection>('ClientSessionSyncMachine.Rejection')
const RejectedPushErrorSchema = opaque<RejectedPushError>('ClientSessionSyncMachine.RejectedPushError')
const PushFailureSchema = opaque<PushFailure>('ClientSessionSyncMachine.PushFailure')
const PullFailureSchema = opaque<PullFailure>('ClientSessionSyncMachine.PullFailure')
const CauseSchema = opaque<Cause.Cause<ProcessorError>>('ClientSessionSyncMachine.Cause')
const ShutdownExitSchema = opaque<Exit.Exit<void, never>>('ClientSessionSyncMachine.ShutdownExit')

const RootState = Schema.TaggedUnion({
  Running: { rejection: Schema.UndefinedOr(RejectionSchema), drainRequested: Schema.Boolean },
  ApplyingPull: { request: PullRequestSchema },
  Failed: { cause: CauseSchema },
  Stopping: { exit: ShutdownExitSchema },
})

export const ClientSessionSyncStates = Machine.states({
  Starting: {},
  Running: {
    schema: RootState.cases.Running,
    initial: 'Active',
    states: {
      Active: {},
      ApplyingPull: RootState.cases.ApplyingPull,
      Draining: {},
    },
  },
  Failed: RootState.cases.Failed,
  Stopping: RootState.cases.Stopping,
  Stopped: { type: 'final' },
})

export const ClientSessionSyncEvents = Machine.events(
  Schema.TaggedUnion({
    Boot: {},
    LocalPushAdmitted: { events: Schema.Array(LiveStoreEvent.Client.Encoded) },
    PullItemReceived: { request: PullRequestSchema },
    PullFailed: { failure: PullFailureSchema },
    PushRejected: { error: RejectedPushErrorSchema, events: Schema.Array(LiveStoreEvent.Client.Encoded) },
    PushFailed: { failure: PushFailureSchema },
    PushDrained: {},
    PushDrainFailed: { error: RejectedPushErrorSchema },
    ShutdownRequested: { mode: Schema.Literals(['drain', 'immediate']) },
  }),
)

const PushState = Schema.TaggedUnion({
  Active: {
    queued: Schema.Array(LiveStoreEvent.Client.Encoded),
    activeBatch: Schema.Array(LiveStoreEvent.Client.Encoded),
    bootstrapFence: Schema.Array(LiveStoreEvent.Client.Encoded),
    draining: Schema.Boolean,
  },
  Dequeuing: { batch: Schema.Array(LiveStoreEvent.Client.Encoded) },
  InFlight: { batch: Schema.Array(LiveStoreEvent.Client.Encoded) },
  AwaitingReconciliation: {
    error: RejectedPushErrorSchema,
    rejectedEvents: Schema.Array(LiveStoreEvent.Client.Encoded),
  },
  Suspending: { suspensionId: Schema.Number },
  ReportingDrainFailure: { error: RejectedPushErrorSchema },
  Failed: { failure: PushFailureSchema },
})

const PushStates = Machine.states({
  Active: {
    schema: PushState.cases.Active,
    initial: 'Idle',
    states: {
      Idle: {},
      Dequeuing: PushState.cases.Dequeuing,
      InFlight: PushState.cases.InFlight,
      AwaitingReconciliation: PushState.cases.AwaitingReconciliation,
      Suspending: PushState.cases.Suspending,
      Suspended: {},
      ReportingDrained: {},
      ReportingDrainFailure: PushState.cases.ReportingDrainFailure,
      Failed: PushState.cases.Failed,
    },
  },
})

const PushEvents = Machine.events(
  Schema.TaggedUnion({
    Append: { events: Schema.Array(LiveStoreEvent.Client.Encoded) },
    ReplacePlan: { events: Schema.Array(LiveStoreEvent.Client.Encoded) },
    Suspend: { suspensionId: Schema.Number },
    BeginDrain: {},
  }),
)

const PushInput = Schema.Struct({ queued: Schema.Array(LiveStoreEvent.Client.Encoded) })

const makePushMachine = (dependencies: Dependencies) =>
  Machine.make({
    id: 'ClientSessionLeaderPush',
    states: PushStates.states,
    events: PushEvents,
    parent: Machine.parent(ClientSessionSyncEvents),
    input: PushInput,
    initial: (to) =>
      to.Active.initial.resolve(({ input, target }) =>
        target.from(
          { queued: input.queued, activeBatch: [], bootstrapFence: input.queued, draining: false },
          (active) => active.Idle.from(),
        ),
      ),
  }).handle({
    Active: {
      on: {
        Append: (to) =>
          to.local.update(({ current, event, owner }) => {
            return owner.from({
              ...current,
              queued: enqueueUnique(
                [...current.bootstrapFence, ...current.activeBatch, ...current.queued],
                current.queued,
                event.events,
              ),
              bootstrapFence: current.bootstrapFence.filter(
                (fencedEvent) =>
                  event.events.some((incoming) => LiveStoreEvent.Client.isEqualEncoded(fencedEvent, incoming)) ===
                  false,
              ),
            })
          }),
        ReplacePlan: (to) =>
          to.full
            .Active()
            .resolve(({ event, target }) =>
              target.from(
                { queued: event.events, activeBatch: [], bootstrapFence: event.events, draining: false },
                (active) => active.Idle.from(),
              ),
            ),
        Suspend: (to) =>
          to.local.Suspending().resolve(({ event, target }) => target.from({ suspensionId: event.suspensionId })),
        BeginDrain: (to) => to.local.update(({ current, owner }) => owner.from({ ...current, draining: true })),
      },
      states: {
        Idle: {
          always: (to) =>
            to
              .branches({
                push: { target: to.local.Dequeuing() },
                drained: { target: to.local.ReportingDrained() },
              })
              .resolve(
                ({ containingState, decline, select }) => {
                  if (containingState.queued.length > 0) {
                    const batch = containingState.queued.slice(0, dependencies.leaderPushBatchSize)
                    return select.push.from({ batch })
                  }
                  return containingState.draining === true ? select.drained.from() : decline()
                },
                { declinable: true },
              ),
        },
        Dequeuing: {
          always: (to) =>
            to.local
              .InFlight()
              .updating(to.branch.Active)
              .resolve(({ containingState, owner, state, target }) =>
                target.from({ batch: state.batch }).update(
                  owner.from({
                    ...containingState,
                    activeBatch: state.batch,
                    queued: containingState.queued.slice(state.batch.length),
                  }),
                ),
              ),
        },
        InFlight: {
          invoke: (from) =>
            from
              .effect('leader-push', ({ parent, state }) =>
                dependencies
                  .pushLeader(state.batch)
                  .pipe(
                    Effect.tapError((error) =>
                      error._tag === 'PushFailure'
                        ? parent.send(ClientSessionSyncEvents.PushFailed({ failure: error })).pipe(Effect.ignore)
                        : dependencies.observeRejection.pipe(
                            Effect.andThen(
                              parent
                                .send(ClientSessionSyncEvents.PushRejected({ error, events: state.batch }))
                                .pipe(Effect.ignore),
                            ),
                          ),
                    ),
                  ),
              )
              .onDone((to) =>
                to.local
                  .Idle()
                  .updating(to.branch.Active)
                  .resolve(({ containingState, owner, target }) =>
                    target.from().update(owner.from({ ...containingState, activeBatch: [] })),
                  ),
              )
              .onFailure((to) =>
                to
                  .branches({
                    rejected: { target: to.local.AwaitingReconciliation() },
                    failed: { target: to.local.Failed() },
                  })
                  .resolve(({ error, select, state }) =>
                    error._tag === 'PushFailure'
                      ? select.failed.from({ failure: error })
                      : select.rejected.from({ error, rejectedEvents: state.batch }),
                  ),
              ),
        },
        AwaitingReconciliation: {
          always: (to) =>
            to.local
              .ReportingDrainFailure()
              .resolve(
                ({ ancestors, decline, state, target }) =>
                  ancestors.Active.draining === true ? target.from({ error: state.error }) : decline(),
                { declinable: true },
              ),
        },
        Suspending: {
          invoke: (from) =>
            from
              .effect('confirm-push-suspension', ({ state }) => dependencies.completeSuspension(state.suspensionId))
              .onDone((to) => to.local.Suspended()),
        },
        Suspended: {},
        ReportingDrained: {
          invoke: (from) =>
            from
              .effect('report-push-drained', ({ parent }) =>
                parent.send(ClientSessionSyncEvents.PushDrained()).pipe(Effect.ignore),
              )
              .onDone((to) => to.none),
        },
        ReportingDrainFailure: {
          invoke: (from) =>
            from
              .effect('report-push-drain-failure', ({ parent, state }) =>
                parent.send(ClientSessionSyncEvents.PushDrainFailed({ error: state.error })).pipe(Effect.ignore),
              )
              .onDone((to) => to.none),
        },
        Failed: {},
      },
    },
  })

const PullState = Schema.TaggedUnion({ Failed: { failure: PullFailureSchema } })

const PullStates = Machine.states({
  Streaming: {},
  Disabled: {},
  Failed: PullState.cases.Failed,
})

const PullEvents = Machine.events(Schema.TaggedUnion({ Disable: {} }))

const makePullMachine = (dependencies: Dependencies) =>
  Machine.make({
    id: 'ClientSessionLeaderPull',
    states: PullStates.states,
    events: PullEvents,
    parent: Machine.parent(ClientSessionSyncEvents),
    initial: (to) => to.Streaming(),
  }).handle({
    Streaming: {
      invoke: (from) =>
        from
          .effect('leader-pull', ({ parent }) =>
            dependencies
              .runPull(parent)
              .pipe(
                Effect.tapError((failure) =>
                  parent.send(ClientSessionSyncEvents.PullFailed({ failure })).pipe(Effect.ignore),
                ),
              ),
          )
          .onDone((to) => to.full.Disabled())
          .onFailure((to) => to.full.Failed().resolve(({ error, target }) => target.from({ failure: error }))),
      on: { Disable: (to) => to.full.Disabled() },
    },
    Disabled: {},
    Failed: { on: { Disable: (to) => to.full.Disabled() } },
  })

/**
 * Session coordinator with independently owned push and pull relationships.
 *
 * Local SQLite admission deliberately remains outside this machine. Everything asynchronous after admission is
 */
const makeClientSessionSyncMachine = (dependencies: Dependencies) => {
  const Push = Machine.child('leader-push', makePushMachine(dependencies))
  const Pull = Machine.child('leader-pull', makePullMachine(dependencies))

  return Machine.make({
    id: 'ClientSessionSyncProcessor',
    states: ClientSessionSyncStates.states,
    events: ClientSessionSyncEvents,
    initial: (to) => to.Starting(),
  }).handle({
    Starting: {
      on: {
        Boot: (to) =>
          to.full
            .Running()
            .resolve(({ target }) =>
              target.from({ rejection: undefined, drainRequested: false }, (running) => running.Active.from()),
            ),
        ShutdownRequested: (to) => to.full.Stopping().resolve(({ target }) => target.from({ exit: Exit.void })),
      },
    },
    Running: {
      invoke: (from) => [
        from
          .child(Push, { input: () => ({ queued: dependencies.currentPending() }) })
          .onFailure((to) => to.full.Failed().resolve(({ error, target }) => target.from({ cause: Cause.die(error) }))),
        from
          .child(Pull)
          .onFailure((to) => to.full.Failed().resolve(({ error, target }) => target.from({ cause: Cause.die(error) }))),
      ],
      on: {
        LocalPushAdmitted: (to) =>
          to.none.resolve(({ event }, enqueue) => {
            enqueue.sendTo(Push, PushEvents.Append({ events: event.events }))
          }),
        PushRejected: (to) =>
          to.local.update(({ current, event, owner }, enqueue) => {
            const pending = dependencies.currentPending()
            if (isRejectedBatchRecovered(event.events, pending) === true) {
              enqueue.sendTo(Push, PushEvents.ReplacePlan({ events: pending }))
              return owner.from({ ...current, rejection: undefined })
            }
            return owner.from({ ...current, rejection: { error: event.error, events: event.events } })
          }),
        PushFailed: (to) =>
          to.full.Failed().resolve(({ event, target }) => target.from({ cause: event.failure.cause })),
        PullFailed: (to) =>
          to.full.Failed().resolve(({ event, target }) => target.from({ cause: event.failure.cause })),
        ShutdownRequested: (to) =>
          to
            .branches({
              immediate: { target: to.full.Stopping() },
              drain: { target: to.local.Draining() },
            })
            .resolve(({ event, select }, enqueue) => {
              if (event.mode === 'immediate') return select.immediate.from({ exit: Exit.void })
              enqueue.sendTo(Pull, PullEvents.Disable())
              enqueue.sendTo(Push, PushEvents.BeginDrain())
              return select.drain.from()
            }),
      },
      states: {
        Active: {
          on: {
            PullItemReceived: (to) =>
              to.local.ApplyingPull().resolve(({ event, target }) => target.from({ request: event.request })),
          },
        },
        ApplyingPull: {
          invoke: (from) =>
            from
              .effect('apply-pull', ({ ancestors, children, state }) => {
                const suspendPush = Effect.gen(function* () {
                  const suspension = yield* dependencies.registerSuspension
                  yield* children
                    .sendTo(Push, PushEvents.Suspend({ suspensionId: suspension.suspensionId }))
                    .pipe(Effect.orDie)
                  yield* suspension.await
                })
                return dependencies.applyPull(state.request, ancestors.Running.rejection, suspendPush).pipe(
                  Effect.catchCause((cause) => Effect.fail<ApplyPullFailure>({ _tag: 'ApplyPullFailure', cause })),
                  Effect.ensuring(dependencies.completePull(state.request.requestId)),
                )
              })
              .onDone((to) =>
                to.local
                  .Active()
                  .updating(to.branch.Running)
                  .resolve(({ ancestors, output, owner, target }, enqueue) => {
                    if (output.pushPlan !== undefined) {
                      enqueue.sendTo(Push, PushEvents.ReplacePlan({ events: output.pushPlan }))
                    }
                    if (ancestors.Running.drainRequested === true) {
                      enqueue.raise(ClientSessionSyncEvents.ShutdownRequested({ mode: 'drain' }))
                    }
                    return target.from().update(
                      owner.from({
                        ...ancestors.Running,
                        rejection: output.pushPlan === undefined ? ancestors.Running.rejection : undefined,
                        drainRequested: false,
                      }),
                    )
                  }),
              )
              .onFailure((to) => to.full.Failed().resolve(({ error, target }) => target.from({ cause: error.cause }))),
          on: {
            ShutdownRequested: (to) =>
              to.branch.Running.update(
                ({ current, decline, event, owner }) =>
                  event.mode === 'drain' ? owner.from({ ...current, drainRequested: true }) : decline(),
                { declinable: true },
              ),
          },
        },
        Draining: {
          invoke: (from) =>
            from.effect('signal-drain-started', () => dependencies.signalDrainStarted).onDone((to) => to.none),
          on: {
            PushDrained: (to) => to.full.Stopping().resolve(({ target }) => target.from({ exit: Exit.void })),
            PushDrainFailed: (to) =>
              to.full.Stopping().resolve(({ event, target }) => target.from({ exit: Exit.die(event.error) })),
            PushFailed: (to) =>
              to.full
                .Stopping()
                .resolve(({ event, target }) => target.from({ exit: Exit.failCause(event.failure.cause) })),
            PullFailed: (to) => to.none,
          },
        },
      },
    },
    Failed: {
      invoke: (from) =>
        from
          .effect('notify-session-failure', ({ state }) => dependencies.notifyFailure(state.cause))
          .onDone((to) => to.none),
      on: {
        ShutdownRequested: (to) => to.full.Stopping().resolve(({ target }) => target.from({ exit: Exit.void })),
      },
    },
    Stopping: {
      invoke: (from) =>
        from
          .effect('finish-shutdown', ({ state }) => dependencies.finishShutdown(state.exit))
          .onDone((to) => to.full.Stopped()),
    },
    Stopped: {},
  })
}

export type ClientSessionEventInput =
  | ReturnType<typeof ClientSessionSyncEvents.Boot>
  | ReturnType<typeof ClientSessionSyncEvents.LocalPushAdmitted>
  | ReturnType<typeof ClientSessionSyncEvents.PullItemReceived>
  | ReturnType<typeof ClientSessionSyncEvents.PullFailed>
  | ReturnType<typeof ClientSessionSyncEvents.PushRejected>
  | ReturnType<typeof ClientSessionSyncEvents.PushFailed>
  | ReturnType<typeof ClientSessionSyncEvents.PushDrained>
  | ReturnType<typeof ClientSessionSyncEvents.PushDrainFailed>
  | ReturnType<typeof ClientSessionSyncEvents.ShutdownRequested>

export interface Runtime {
  readonly send: (event: ClientSessionEventInput) => Effect.Effect<void>
  readonly stop: Effect.Effect<void>
}

export const start = (dependencies: Dependencies): Effect.Effect<Runtime> =>
  Effect.gen(function* () {
    const ref = yield* Machine.start(makeClientSessionSyncMachine(dependencies)).pipe(Effect.orDie)
    yield* ref.join.pipe(
      Effect.catchCause((cause) => {
        const error = Cause.findErrorOption(cause)
        return Option.isSome(error) === true && error.value._tag === 'StoppedError'
          ? Effect.void
          : dependencies.runtimeFailed(cause)
      }),
      Effect.forkDetach,
    )
    return {
      send: (event) => ref.send(event).pipe(Effect.ignore),
      stop: ref.stop,
    }
  })

const enqueueUnique = (scheduled: EventBatch, queued: EventBatch, events: EventBatch): EventBatch => {
  const additions = events.filter(
    (event) =>
      scheduled.some((scheduledEvent) => LiveStoreEvent.Client.isEqualEncoded(scheduledEvent, event)) === false,
  )
  return [...queued, ...additions]
}

const isRejectedBatchRecovered = (rejectedEvents: EventBatch, pendingEvents: EventBatch): boolean =>
  rejectedEvents.every(
    (rejectedEvent) =>
      pendingEvents.some((pendingEvent) => LiveStoreEvent.Client.isEqualEncoded(pendingEvent, rejectedEvent)) === false,
  )
