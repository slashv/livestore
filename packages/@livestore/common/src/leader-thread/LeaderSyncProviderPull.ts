import { Machine } from '@typeonce/effect-machine'

import {
  type Latch,
  Cause,
  Deferred,
  Duration,
  Effect,
  Option,
  Ref,
  Schema,
  Stream,
  SubscriptionRef,
} from '@livestore/utils/effect'

import { type SqliteDb, UnknownError } from '../adapter-types.ts'
import { EventSequenceNumber, LiveStoreEvent } from '../schema/mod.ts'
import type { BackendIdMismatchError, IsOfflineError } from '../sync/errors.ts'
import type * as SyncBackend from '../sync/sync-backend.ts'
import * as Eventlog from './eventlog.ts'
import type * as LeaderSyncCommitter from './LeaderSyncCommitter.ts'
import type { InitialBlockingSyncContext } from './types.ts'

export type PullBatchId = number
export type ProviderPullError = IsOfflineError | BackendIdMismatchError | UnknownError

export interface UpstreamBatch {
  readonly batchId: PullBatchId
  readonly events: ReadonlyArray<LiveStoreEvent.Client.Encoded>
  readonly pulledEvents: ReadonlyArray<LeaderSyncCommitter.PulledEvent>
  readonly pageInfo: SyncBackend.PullResPageInfo
}

export interface Options {
  readonly syncBackend: SyncBackend.SyncBackend | undefined
  readonly devtoolsLatch: Latch.Latch | undefined
  readonly dbEventlog: SqliteDb
  readonly live: boolean
  readonly initialBlockingSyncContext: InitialBlockingSyncContext
}

export interface ProviderPull {
  readonly child: Machine.ChildMachine<'provider-pull', ProviderPullMachine>
  readonly complete: (batchId: PullBatchId) => Effect.Effect<void>
  readonly interruptAll: Effect.Effect<void>
}

export { Events, make, ParentEvents }

const opaque = <A>(identifier: string) =>
  Schema.declare<A>((value): value is A => value !== undefined, { identifier, expected: identifier })

const ProviderPullErrorSchema = opaque<ProviderPullError>('LeaderSyncProviderPull.ProviderPullError')
const UpstreamBatchSchema = opaque<UpstreamBatch>('LeaderSyncProviderPull.UpstreamBatch')

/** Events sent by the provider-pull child to its leader coordinator. */
const ParentEvents = Machine.events(
  Schema.TaggedUnion({
    UpstreamBatchReceived: { batch: UpstreamBatchSchema },
    PullCompleted: {},
    ProviderPullFailed: { error: ProviderPullErrorSchema },
  }),
)

/** Commands accepted by the provider-pull child. */
const Events = Machine.events(Schema.TaggedUnion({ Disable: {} }))

const State = Schema.TaggedUnion({
  Streaming: { cursor: EventSequenceNumber.Client.Composite, attempt: Schema.Number },
  BackingOff: { cursor: EventSequenceNumber.Client.Composite, attempt: Schema.Number },
  Failed: { error: ProviderPullErrorSchema },
})

const States = Machine.states({
  Disabled: {},
  Streaming: State.cases.Streaming,
  BackingOff: State.cases.BackingOff,
  Completed: {},
  Failed: State.cases.Failed,
})

const Input = Schema.Struct({ cursor: EventSequenceNumber.Client.Composite })

type EventSchemas = Machine.Machine.EventProtocolSchemas<typeof Events>
type ParentEventSchemas = Machine.Machine.ParentEventSchemas<
  'required',
  Machine.Machine.EventProtocolSchemas<typeof ParentEvents>
>
/** Names Effect Machine's otherwise-private invocation requirement so declaration emit stays stable. */
type MachineRuntimeRequirement = Effect.Services<ReturnType<typeof Machine.sendTo>>
type ProviderPullMachine = Machine.Machine<
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
 * Owns the complete provider-pull lifecycle, including the stream, retry backoff, page backpressure, cursor lookup,
 * progress reporting, and cancellation of outstanding pages.
 */
const make: (options: Options) => Effect.Effect<ProviderPull> = Effect.fnUntraced(function* ({
  syncBackend,
  devtoolsLatch,
  dbEventlog,
  live,
  initialBlockingSyncContext,
}: Options) {
  const nextBatchId = yield* Ref.make(1)
  const batches = yield* Ref.make(new Map<PullBatchId, Deferred.Deferred<void>>())

  const pull = (
    cursor: EventSequenceNumber.Client.Composite,
    parent: Machine.MachineTarget<
      Machine.Machine.EventInputOf<Machine.Machine.EventProtocolSchemas<typeof ParentEvents>>
    >,
  ): Effect.Effect<void, ProviderPullError> =>
    syncBackend === undefined
      ? Effect.die(new Error('Provider pull started without a sync backend'))
      : Effect.gen(function* () {
          const cursorInfo = yield* Eventlog.getSyncBackendCursorInfoForDb(dbEventlog, { remoteHead: cursor.global })
          yield* syncBackend.pull(cursorInfo, { live }).pipe(
            Stream.runForEach(({ batch, pageInfo }) =>
              Effect.gen(function* () {
                yield* SubscriptionRef.waitUntil(syncBackend.isConnected, (connected) => connected === true)
                if (devtoolsLatch !== undefined) yield* devtoolsLatch.await
                const batchId = yield* Ref.modify(nextBatchId, (id) => [id, id + 1])
                const completion = yield* Deferred.make<void>()
                yield* Ref.update(batches, (current) => new Map(current).set(batchId, completion))
                const pulledEvents = batch.map((item) => ({
                  event: LiveStoreEvent.Client.fromGlobal(item.eventEncoded),
                  syncMetadata: item.metadata,
                }))
                yield* parent
                  .send(
                    ParentEvents.UpstreamBatchReceived({
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
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause) === true ? Effect.void : Effect.failCause(cause),
          ),
          mapDefects('Sync backend pull defected'),
          Effect.interruptible,
        )

  const machine: ProviderPullMachine = Machine.make({
    id: 'LeaderProviderPull',
    states: States.states,
    events: Events,
    parent: Machine.parent(ParentEvents),
    input: Input,
    initial: (to) =>
      syncBackend === undefined
        ? to.Disabled()
        : to.Streaming().resolve(({ input, target }) => target.from({ cursor: input.cursor, attempt: 0 })),
  }).handle({
    Disabled: {},
    Streaming: {
      invoke: (from) =>
        from
          .effect('provider-pull', ({ parent, state }) =>
            pull(state.cursor, parent).pipe(
              Effect.tapError((error) =>
                error._tag === 'IsOfflineError'
                  ? Effect.void
                  : parent.send(ParentEvents.ProviderPullFailed({ error })).pipe(Effect.ignore),
              ),
              Effect.ensuring(parent.send(ParentEvents.PullCompleted()).pipe(Effect.ignore)),
            ),
          )
          .onDone((to) => to.full.Completed())
          .onFailure((to) =>
            to
              .branches({
                retry: { target: to.full.BackingOff() },
                failed: { target: to.full.Failed() },
              })
              .resolve(({ error, select, state }) =>
                error._tag === 'IsOfflineError'
                  ? select.retry.from({ cursor: state.cursor, attempt: state.attempt + 1 })
                  : select.failed.from({ error }),
              ),
          ),
      on: { Disable: (to) => to.full.Disabled() },
    },
    BackingOff: {
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

  const complete = (batchId: PullBatchId) =>
    Effect.gen(function* () {
      let completion: Deferred.Deferred<void> | undefined
      yield* Ref.update(batches, (current) => {
        const next = new Map(current)
        completion = next.get(batchId)
        next.delete(batchId)
        return next
      })
      if (completion !== undefined) yield* Deferred.succeed(completion, undefined)
    })

  const interruptAll = Effect.gen(function* () {
    const current = yield* Ref.getAndSet(batches, new Map())
    yield* Effect.forEach(current.values(), Deferred.interrupt, { discard: true })
  })

  return { child: Machine.child('provider-pull', machine), complete, interruptAll }
})

const mapDefects =
  (note: string) =>
  <A, E, R>(effect: Effect.Effect<A, E, R>): Effect.Effect<A, E | UnknownError, R> =>
    Effect.catchCause(effect, (cause): Effect.Effect<never, E | UnknownError> => {
      const error = Cause.findErrorOption(cause)
      if (Option.isSome(error) === true) return Effect.fail(error.value)
      return Effect.fail(UnknownError.make({ cause, note }))
    })

const retryDelay = (attempt: number) => Math.min(30_000, 1_000 * 2 ** Math.max(0, attempt - 1))
