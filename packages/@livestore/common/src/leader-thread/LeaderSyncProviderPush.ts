import { Machine } from '@typeonce/effect-machine'

import { type Latch, Cause, Duration, Effect, Option, Schema, SubscriptionRef } from '@livestore/utils/effect'

import { UnknownError } from '../adapter-types.ts'
import { LiveStoreEvent } from '../schema/mod.ts'
import type { BackendIdMismatchError, IsOfflineError, ServerAheadError } from '../sync/errors.ts'
import type * as SyncBackend from '../sync/sync-backend.ts'

export type EventBatch = ReadonlyArray<LiveStoreEvent.Client.Encoded>
export type ProviderPushError = IsOfflineError | BackendIdMismatchError | UnknownError | ServerAheadError

export interface Options {
  readonly syncBackend: SyncBackend.SyncBackend | undefined
  readonly devtoolsLatch: Latch.Latch | undefined
  readonly batchSize: number
}

export { Events, make, ParentEvents }

const opaque = <A>(identifier: string) =>
  Schema.declare<A>((value): value is A => value !== undefined, { identifier, expected: identifier })

const ProviderPushErrorSchema = opaque<ProviderPushError>('LeaderSyncProviderPush.ProviderPushError')

/** Events sent by the provider-push child to its leader coordinator. */
const ParentEvents = Machine.events(
  Schema.TaggedUnion({
    ProviderPushFailed: { error: ProviderPushErrorSchema },
  }),
)

/** Commands accepted by the provider-push child. */
const Events = Machine.events(
  Schema.TaggedUnion({
    Append: { events: Schema.Array(LiveStoreEvent.Client.Encoded) },
    ReplacePlan: { events: Schema.Array(LiveStoreEvent.Client.Encoded) },
    Disable: {},
  }),
)

const State = Schema.TaggedUnion({
  Active: { queued: Schema.Array(LiveStoreEvent.Client.Encoded) },
  Pushing: {
    batch: Schema.Array(LiveStoreEvent.Client.Encoded),
    attempt: Schema.Number,
  },
  BackingOff: {
    batch: Schema.Array(LiveStoreEvent.Client.Encoded),
    attempt: Schema.Number,
  },
  ClassifyingFailure: {
    batch: Schema.Array(LiveStoreEvent.Client.Encoded),
    attempt: Schema.Number,
    error: ProviderPushErrorSchema,
  },
  RestoringPlan: { batch: Schema.Array(LiveStoreEvent.Client.Encoded) },
  Failed: { error: ProviderPushErrorSchema },
})

const States = Machine.states({
  Disabled: {},
  Active: {
    schema: State.cases.Active,
    initial: 'Idle',
    states: {
      Idle: {},
      Pushing: State.cases.Pushing,
      BackingOff: State.cases.BackingOff,
      ClassifyingFailure: State.cases.ClassifyingFailure,
      RestoringPlan: State.cases.RestoringPlan,
      AwaitingPull: {},
      Failed: State.cases.Failed,
    },
  },
})

const Input = Schema.Struct({ queued: Schema.Array(LiveStoreEvent.Client.Encoded) })

type EventSchemas = Machine.Machine.EventProtocolSchemas<typeof Events>
type ParentEventSchemas = Machine.Machine.ParentEventSchemas<
  'required',
  Machine.Machine.EventProtocolSchemas<typeof ParentEvents>
>
/** Names Effect Machine's otherwise-private invocation requirement so declaration emit stays stable. */
type MachineRuntimeRequirement = Effect.Services<ReturnType<typeof Machine.sendTo>>
type ProviderPushMachine = Machine.Machine<
  typeof States.states,
  EventSchemas,
  typeof Input,
  never,
  Machine.ChildAlreadyExistsError,
  MachineRuntimeRequirement,
  never,
  never,
  never,
  never,
  readonly [],
  never,
  EventSchemas,
  ParentEventSchemas
>

/**
 * Owns the complete provider-push lifecycle: connectivity gating, batching, retry backoff, server-ahead recovery,
 * backend mismatch failure, and cancellation when the leader leaves its running state.
 */
const make = ({
  syncBackend,
  devtoolsLatch,
  batchSize,
}: Options): Machine.ChildMachine<'provider-push', ProviderPushMachine> => {
  const push = (batch: EventBatch): Effect.Effect<void, ProviderPushError> =>
    syncBackend === undefined
      ? Effect.die(new Error('Provider push started without a sync backend'))
      : Effect.gen(function* () {
          yield* SubscriptionRef.waitUntil(syncBackend.isConnected, (connected) => connected === true)
          if (devtoolsLatch !== undefined) yield* devtoolsLatch.await
          yield* syncBackend.push(batch.map(LiveStoreEvent.Client.toGlobal))
        }).pipe(mapDefects('Sync backend push defected'), Effect.interruptible)

  const machine: ProviderPushMachine = Machine.make({
    id: 'LeaderProviderPush',
    states: States.states,
    events: Events,
    parent: Machine.parent(ParentEvents),
    input: Input,
    initial: (to) =>
      syncBackend === undefined
        ? to.Disabled()
        : to.Active.initial.resolve(({ input, target }) =>
            target.from({ queued: input.queued }, (active) => active.Idle.from()),
          ),
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
              .Pushing()
              .updating(to.branch.Active)
              .resolve(
                ({ containingState, decline, owner, target }) => {
                  if (containingState.queued.length === 0) return decline()
                  const batch = containingState.queued.slice(0, batchSize)
                  return target
                    .from({ batch, attempt: 0 })
                    .update(owner.from({ queued: containingState.queued.slice(batch.length) }))
                },
                { declinable: true },
              ),
        },
        Pushing: {
          invoke: (from) =>
            from
              .effect('provider-push', ({ parent, state }) =>
                push(state.batch).pipe(
                  Effect.tapError((error) =>
                    error._tag === 'BackendIdMismatchError' || error._tag === 'ServerAheadError'
                      ? parent.send(ParentEvents.ProviderPushFailed({ error })).pipe(Effect.ignore)
                      : Effect.void,
                  ),
                ),
              )
              .onDone((to) => to.local.Idle())
              .onFailure((to) =>
                to.local
                  .ClassifyingFailure()
                  .resolve(({ error, state, target }) =>
                    target.from({ batch: state.batch, attempt: state.attempt, error }),
                  ),
              ),
        },
        BackingOff: {
          invoke: (from) =>
            from
              .timer('provider-push-retry', ({ state }) => Duration.millis(retryDelay(state.attempt)))
              .onDone((to) =>
                to.local
                  .Pushing()
                  .resolve(({ state, target }) => target.from({ batch: state.batch, attempt: state.attempt })),
              ),
        },
        ClassifyingFailure: {
          always: (to) =>
            to
              .branches({
                serverAhead: { target: to.local.RestoringPlan() },
                failed: { target: to.local.Failed() },
                retry: { target: to.local.BackingOff() },
              })
              .resolve(({ select, state }) => {
                if (state.error._tag === 'ServerAheadError') return select.serverAhead.from({ batch: state.batch })
                if (state.error._tag === 'BackendIdMismatchError') return select.failed.from({ error: state.error })
                return select.retry.from({ batch: state.batch, attempt: state.attempt + 1 })
              }),
        },
        RestoringPlan: {
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

  return Machine.child('provider-push', machine)
}

const mapDefects =
  (note: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | UnknownError, R> =>
    Effect.catchCause(effect, (cause): Effect.Effect<never, E | UnknownError> => {
      const error = Cause.findErrorOption(cause)
      if (Option.isSome(error) === true) return Effect.fail(error.value)
      return Effect.fail(UnknownError.make({ cause, note }))
    })

const retryDelay = (attempt: number) => Math.min(30_000, 1_000 * 2 ** Math.max(0, attempt - 1))
