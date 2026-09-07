/// <reference lib="dom" />
import { casesHandled, LS_DEV, TRACE_VERBOSE } from '@livestore/utils'
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Filter,
  FiberHandle,
  Option,
  Queue,
  References,
  Schema,
  type Scope,
  Stream,
  Subscribable,
} from '@livestore/utils/effect'

import type { ClientSession } from '../adapter-types.ts'
import type { PullItem } from '../ClientSessionLeaderThreadProxy.ts'
import { type MaterializeError, UnknownError } from '../errors.ts'
import type { RejectedPushError } from '../leader-thread/RejectedPushError.ts'
import * as MaterializationJournal from '../MaterializationJournal.ts'
import * as EventSequenceNumber from '../schema/EventSequenceNumber/mod.ts'
import * as LiveStoreEvent from '../schema/LiveStoreEvent/mod.ts'
import { type LiveStoreSchema, SystemTables } from '../schema/mod.ts'
import { resolveSessionIdSymbolInEventArgs } from '../session-id-symbol.ts'
import * as SqliteDbHelper from '../sqlite-db-helper.ts'
import * as StateHead from '../StateHead.ts'
import * as StateSqliteDb from '../StateSqliteDb.ts'
import * as SyncState from './syncstate.ts'

export interface ClientSessionSyncProcessor {
  boot: Effect.Effect<void, never, Scope.Scope>
  shutdown: (exit: Exit.Exit<unknown, unknown>) => Effect.Effect<void>
  encodeEvents: (
    events: ReadonlyArray<LiveStoreEvent.Input.Decoded>,
  ) => Effect.Effect<ReadonlyArray<LiveStoreEvent.Client.Encoded>>
  push: (events: ReadonlyArray<LiveStoreEvent.Client.Encoded>) => Effect.Effect<void>
  materializeEvents: (
    events: ReadonlyArray<LiveStoreEvent.Client.Encoded>,
  ) => Effect.Effect<
    { writeTables: Set<string> },
    MaterializeError | MaterializationJournal.MaterializationJournalError
  >
  /** Only used for debugging and observability. */
  syncState: Subscribable.Subscribable<SyncState.SyncState>
  debug: {
    awaitDrainStarted: Effect.Effect<void>
    awaitRejection: Effect.Effect<void>
    print: () => void
    debugInfo: () => { rebaseCount: number; advanceCount: number }
  }
}

export type RebaseBarrierPoint =
  | 'before_leader_push_fiber_interrupt'
  | 'before_queue_reconcile'
  | 'before_leader_push_fiber_run'

const jsonStringify = Schema.encodeSync(Schema.UnknownFromJsonString)

const PULL_CHUNK_SIZE = 32

/**
 * Coordinates optimistic session commits with the leader.
 *
 * Local commits stay synchronous so UI reads see them immediately. Pulls, propagation results, rejection recovery,
 * and shutdown pass through one mailbox. Pulls yield between complete SQLite/model transitions, never inside one.
 * Local commits during those pauses see a coherent prefix and are included in the next step's rebase.
 * Unlike the leader, this processor also refreshes reactive tables and has no downstream sessions.
 */
export const makeClientSessionSyncProcessor = Effect.fn('makeClientSessionSyncProcessor')(function* ({
  schema,
  clientSession,
  materializeEvent,
  refreshTables,
  params,
  confirmUnsavedChanges,
}: {
  schema: LiveStoreSchema
  clientSession: ClientSession
  /** Must use synchronous SQLite/materializer effects, as required by the Store.commit runSync path too. */
  materializeEvent: (
    eventEncoded: LiveStoreEvent.Client.Encoded,
    options: { materializerHashLeader: Option.Option<number> },
  ) => Effect.Effect<
    {
      writeTables: Set<string>
      materializerHash: Option.Option<number>
    },
    MaterializeError | MaterializationJournal.MaterializationJournalError
  >
  refreshTables: (tables: Set<string>) => void
  params: {
    leaderPushBatchSize: number
    /** Test-only deterministic pauses inside rebase. */
    rebaseBarriers?: Partial<Record<RebaseBarrierPoint, Effect.Effect<void>>>
  }
  /** Registers the web adapter's unsaved-changes warning. */
  confirmUnsavedChanges: boolean
}) {
  const materializationJournal = yield* MaterializationJournal.MaterializationJournal
  const stateHead = yield* StateHead.StateHead
  const dbState = yield* StateSqliteDb.StateSqliteDb
  const eventSchema = LiveStoreEvent.Client.makeSchemaMemo(schema)

  const rebaseBarrier = (point: RebaseBarrierPoint): Effect.Effect<void> =>
    params.rebaseBarriers?.[point] ?? Effect.void

  const leaderHead = clientSession.leaderThread.initialState.leaderHead
  const syncStateRef = {
    current: new SyncState.SyncState({ localHead: leaderHead, upstreamHead: leaderHead, pending: [] }),
  }

  const syncStateUpdateQueue = yield* Queue.unbounded<SyncState.SyncState>()
  const isClientOnlyEvent = (eventEncoded: LiveStoreEvent.Client.Encoded) =>
    schema.eventsDefsMap.get(eventEncoded.name)?.options.clientOnly ?? false

  // The mailbox owns asynchronous transitions; local admission remains synchronous.
  const mailbox = yield* Queue.unbounded<Event>()
  const shutdownDone = yield* Deferred.make<void>()
  const drainStartedSignal = yield* Deferred.make<void>()
  const rejectionObserved = yield* Deferred.make<void>()
  let shutdownStarted = false
  let model: Model = {
    lifecycle: 'starting',
    push: { _tag: 'idle', queued: [] },
    nextOperationId: 1,
    terminalCause: undefined,
  }
  let leaderPushingFiberHandle: FiberHandle.FiberHandle<void, never> | undefined
  let pullingFiberHandle: FiberHandle.FiberHandle<void, never> | undefined
  const send = (event: Event) => Queue.offer(mailbox, event).pipe(Effect.asVoid)

  const finishShutdown = (exit: Exit.Exit<void, never>) =>
    Effect.gen(function* () {
      if (model.lifecycle === 'stopped') return false
      if (pullingFiberHandle !== undefined) yield* FiberHandle.clear(pullingFiberHandle)
      if (leaderPushingFiberHandle !== undefined) yield* FiberHandle.clear(leaderPushingFiberHandle)
      model = { ...model, lifecycle: 'stopped' }
      yield* Deferred.done(shutdownDone, exit)
      return false
    })

  const failProcessor = (cause: Cause.Cause<ProcessorError>) =>
    Effect.gen(function* () {
      const terminalCause = Cause.die(Cause.squash(cause))
      if (model.lifecycle === 'stopping') return yield* finishShutdown(Exit.failCause(terminalCause))
      if (model.lifecycle === 'stopped') return false
      if (model.lifecycle === 'failed') return true

      model = { ...model, lifecycle: 'failed', terminalCause: model.terminalCause ?? terminalCause }
      if (pullingFiberHandle !== undefined) yield* FiberHandle.clear(pullingFiberHandle)
      if (leaderPushingFiberHandle !== undefined) yield* FiberHandle.clear(leaderPushingFiberHandle)

      // ClientSession owns the wider store lifecycle. Notify it outside this mailbox so its shutdown can call back
      // into this processor without deadlocking the event loop that must receive `ShutdownRequested`.
      yield* clientSession.shutdown(Exit.failCause(cause)).pipe(Effect.forkDetach, Effect.asVoid)
      return true
    })

  const runLeaderPush = (operationId: OperationId, batch: EventBatch) =>
    clientSession.leaderThread.events.push(batch).pipe(
      Effect.matchEffect({
        onFailure: (error) => send({ _tag: 'LeaderPushRejected', operationId, error }),
        onSuccess: () => send({ _tag: 'LeaderPushSucceeded', operationId }),
      }),
      Effect.catchCause((cause) =>
        Cause.hasInterruptsOnly(cause) === true ? Effect.void : send({ _tag: 'LeaderPushFailed', operationId, cause }),
      ),
      Effect.interruptible,
    )

  const startLeaderPush = () =>
    Effect.gen(function* () {
      if (model.push._tag !== 'idle' || model.push.queued.length === 0) return
      if (leaderPushingFiberHandle === undefined) {
        return yield* Effect.die(new Error('Client session leader-push runtime has not started'))
      }

      const operationId = model.nextOperationId
      const batch = model.push.queued.slice(0, params.leaderPushBatchSize)
      const queued = model.push.queued.slice(batch.length)
      model = {
        ...model,
        nextOperationId: operationId + 1,
        push: { _tag: 'in-flight', operationId, batch, queued },
      }
      yield* FiberHandle.run(leaderPushingFiberHandle, runLeaderPush(operationId, batch)).pipe(Effect.asVoid)
    })

  const mergePull = (payload: typeof SyncState.PayloadUpstream.Type) =>
    SyncState.merge({
      syncState: syncStateRef.current,
      payload,
      isClientOnlyEvent,
      isEqualEvent: LiveStoreEvent.Client.isEqualEncoded,
    }).pipe(
      Effect.filterOrElse(
        (result) => result._tag !== 'reject',
        () => Effect.die(new Error('Unexpected reject in client-session-sync-processor')),
      ),
    )

  const handlePullItem = ({ payload, globalHead, materializerHashes }: typeof PullItem.Type) =>
    Effect.gen(function* () {
      // Validate the complete payload before committing any prefix. This is not a plan: local commits can still
      // arrive during cancellation, so each step must merge again from the then-current state.
      yield* mergePull(payload)
      // An explicit rebase replaces already-observed upstream history. Its first step must reach the old
      // upstream head before yielding, rather than expose a backwards cursor or violate merge's head invariant.
      const minimumEnd =
        payload._tag === 'upstream-rebase'
          ? payload.newEvents.findIndex((event) =>
              EventSequenceNumber.Client.isGreaterThanOrEqual(event.seqNum, syncStateRef.current.upstreamHead),
            ) + 1
          : 0
      const rejectionAtPullStart = model.push._tag === 'awaiting-reconciliation' ? model.push : undefined
      let pushCancelled = false
      let offset = 0
      do {
        const newEvents = payload.newEvents.slice(offset, Math.max(offset + PULL_CHUNK_SIZE, minimumEnd))
        const last = offset + newEvents.length === payload.newEvents.length
        const chunk = offset === 0 ? { ...payload, newEvents } : { _tag: 'upstream-advance' as const, newEvents }
        const applied = yield* Effect.gen(function* () {
          const result = yield* mergePull(chunk)
          if (result._tag === 'rebase' && pushCancelled === false) return false

          const writeTables = new Set<string>()
          yield* Effect.gen(function* () {
            if (result._tag === 'rebase') {
              yield* materializationJournal.rollback(result.rollbackEvents.map((event) => event.seqNum))
              // Rollback can touch tables absent from the replacement events. The journal doesn't return table
              // names, so invalidate conservatively rather than leave cached queries on the removed history.
              if (result.rollbackEvents.length > 0) {
                for (const table of schema.state.sqlite.tables.keys()) {
                  if (SystemTables.isStateSystemTable(table) === false) writeTables.add(table)
                }
              }
            }
            for (const event of result.newEvents) {
              const materialized = yield* materializeEvent(event, {
                // Replayed pending events can temporarily share a key with a later chunk's leader event.
                // Only actual incoming events carry the leader's materializer hash.
                materializerHashLeader:
                  chunk.newEvents.some((incoming) => LiveStoreEvent.Client.isEqualEncoded(incoming, event)) === true
                    ? (materializerHashes.find(({ eventNum }) =>
                        EventSequenceNumber.Client.isEqual(eventNum, event.seqNum),
                      )?.hash ?? Option.none())
                    : Option.none(),
              })
              for (const table of materialized.writeTables) writeTables.add(table)
            }
            yield* stateHead.set(result.newSyncState.localHead)
            // The leader only confirms globalHead after the whole payload. Earlier steps still need their journal.
            if (last === true) yield* materializationJournal.discardUpTo(globalHead)
          }).pipe(
            SqliteDbHelper.withSavepoint(dbState),
            Effect.mapError((cause) => (cause._tag === 'SqliteError' ? new UnknownError({ cause }) : cause)),
          )

          syncStateRef.current = result.newSyncState
          yield* Queue.offer(syncStateUpdateQueue, syncStateRef.current)
          // Subscribers may synchronously commit here: both SQLite and the model already describe the same state.
          if (writeTables.size > 0) refreshTables(writeTables)
          return true
        }).pipe(
          // These are synchronous SQLite/materializer effects, just like Store.commit. A savepoint alone is not
          // exclusion: automatic fiber yields would let another commit use this same connection mid-transaction.
          Effect.provideService(References.PreventSchedulerYield, true),
          Effect.uninterruptible,
        )

        if (applied === false) {
          yield* rebaseBarrier('before_leader_push_fiber_interrupt')
          if (leaderPushingFiberHandle !== undefined) yield* FiberHandle.clear(leaderPushingFiberHandle)
          yield* rebaseBarrier('before_queue_reconcile')
          pushCancelled = true
          continue
        }
        offset += newEvents.length
        if (last === true) break
        // Yield only with a complete prefix plus all current optimistic events materialized. Pending replay can
        // still be expensive; this bounds incoming work per step, not wall-clock time or pending backlog size.
        yield* Effect.yieldNow
      } while (offset <= payload.newEvents.length)

      if (pushCancelled === true) debugInfo.rebaseCount++
      else debugInfo.advanceCount++
      const recoveredRejection =
        rejectionAtPullStart !== undefined &&
        isRejectedBatchRecovered(rejectionAtPullStart.rejectedEvents, syncStateRef.current.pending)
      if (pushCancelled === true || recoveredRejection === true) {
        if (pushCancelled === true) yield* rebaseBarrier('before_leader_push_fiber_run')
        model = { ...model, push: { _tag: 'idle', queued: syncStateRef.current.pending } }
        yield* startLeaderPush()
      }
    })

  const handleShutdownRequested = (exit: Exit.Exit<unknown, unknown>) =>
    Effect.gen(function* () {
      model = { ...model, lifecycle: 'stopping' }
      if (pullingFiberHandle !== undefined) yield* FiberHandle.clear(pullingFiberHandle)

      if (Exit.isFailure(exit) === true) return yield* finishShutdown(Exit.void)

      yield* Deferred.succeed(drainStartedSignal, undefined)
      if (model.terminalCause !== undefined) return yield* finishShutdown(Exit.failCause(model.terminalCause))
      if (model.push._tag === 'awaiting-reconciliation') {
        return yield* finishShutdown(Exit.die(model.push.error))
      }
      if (model.push._tag === 'idle') {
        if (model.push.queued.length === 0) return yield* finishShutdown(Exit.void)
        yield* startLeaderPush()
      }
      return true
    })

  const handleEvent = (event: Event) =>
    Effect.gen(function* () {
      if (event._tag === 'ShutdownRequested') return yield* handleShutdownRequested(event.exit)

      if (event._tag === 'PullItemReceived') {
        return yield* (model.lifecycle === 'running' ? handlePullItem(event.item) : Effect.void).pipe(
          Effect.ensuring(Deferred.succeed(event.completed, undefined)),
          Effect.as(true),
        )
      }

      if (event._tag === 'PullFailed') return yield* failProcessor(event.cause)
      if (model.lifecycle !== 'running' && model.lifecycle !== 'stopping') return true

      switch (event._tag) {
        case 'LocalPushAdmitted': {
          if (model.lifecycle !== 'running' || model.push._tag === 'awaiting-reconciliation') return true
          // A pull may have rebased/confirmed these events while their admission waited in the mailbox.
          const currentEvents = syncStateRef.current.pending.filter((pending) =>
            event.events.some((admitted) => LiveStoreEvent.Client.isEqualEncoded(pending, admitted)),
          )
          model = { ...model, push: enqueueUnique(model.push, currentEvents) }
          yield* startLeaderPush()
          return true
        }
        case 'LeaderPushSucceeded': {
          if (model.push._tag !== 'in-flight' || model.push.operationId !== event.operationId) return true
          const queued = model.push.queued
          model = { ...model, push: { _tag: 'idle', queued } }
          if (model.lifecycle === 'stopping' && queued.length === 0) {
            return yield* finishShutdown(Exit.void)
          }
          yield* startLeaderPush()
          return true
        }
        case 'LeaderPushRejected': {
          if (model.push._tag !== 'in-flight' || model.push.operationId !== event.operationId) return true
          debugInfo.rejectCount++
          yield* Deferred.succeed(rejectionObserved, undefined)

          if (isRejectedBatchRecovered(model.push.batch, syncStateRef.current.pending) === true) {
            const queued = syncStateRef.current.pending
            model = { ...model, push: { _tag: 'idle', queued } }
            if (model.lifecycle === 'stopping' && queued.length === 0) {
              return yield* finishShutdown(Exit.void)
            }
            yield* startLeaderPush()
            return true
          }

          const rejectedEvents = model.push.batch
          model = {
            ...model,
            push: { _tag: 'awaiting-reconciliation', error: event.error, rejectedEvents },
          }
          return model.lifecycle === 'stopping' ? yield* finishShutdown(Exit.die(event.error)) : true
        }
        case 'LeaderPushFailed': {
          if (model.push._tag !== 'in-flight' || model.push.operationId !== event.operationId) return true
          return yield* failProcessor(event.cause)
        }
        default:
          return casesHandled(event)
      }
    })

  const run = Effect.gen(function* () {
    let running = true
    while (running === true) {
      const event = yield* Queue.take(mailbox)
      const exit = yield* handleEvent(event).pipe(Effect.exit)
      running = Exit.isFailure(exit) === true ? yield* failProcessor(exit.cause) : exit.value
    }
    yield* Queue.shutdown(mailbox)
  })

  const runLeaderPull = Stream.suspend(() =>
    clientSession.leaderThread.events.pull({ cursor: syncStateRef.current.upstreamHead }),
  ).pipe(
    Stream.tap(() => (clientSession.devtools.enabled === true ? clientSession.devtools.pullLatch.await : Effect.void)),
    Stream.tap((item) =>
      Effect.gen(function* () {
        const completed = yield* Deferred.make<void>()
        yield* send({ _tag: 'PullItemReceived', item, completed })
        yield* Deferred.await(completed)
      }),
    ),
    Stream.runDrain,
    Effect.forever,
    Effect.interruptible,
    Effect.catchCause((cause) =>
      Cause.hasInterruptsOnly(cause) === true ? Effect.void : send({ _tag: 'PullFailed', cause }),
    ),
    Effect.withSpan('client-session-sync-processor:pull'),
    Effect.tapCauseLogPretty,
  )

  const boot: ClientSessionSyncProcessor['boot'] = Effect.gen(function* () {
    if (
      confirmUnsavedChanges === true &&
      typeof window !== 'undefined' &&
      typeof window.addEventListener === 'function'
    ) {
      const onBeforeUnload = (event: BeforeUnloadEvent) => {
        if (syncStateRef.current.pending.length > 0) event.preventDefault()
      }
      yield* Effect.acquireRelease(
        Effect.sync(() => window.addEventListener('beforeunload', onBeforeUnload)),
        () => Effect.sync(() => window.removeEventListener('beforeunload', onBeforeUnload)),
      )
    }

    leaderPushingFiberHandle = yield* FiberHandle.make<void, never>()
    pullingFiberHandle = yield* FiberHandle.make<void, never>()
    model = { ...model, lifecycle: 'running' }
    yield* run.pipe(Effect.forkScoped)
    yield* FiberHandle.run(pullingFiberHandle, runLeaderPull)
  }).pipe(Effect.withSpan('client-session-sync-processor:boot'))

  const shutdown: ClientSessionSyncProcessor['shutdown'] = (exit) =>
    Effect.suspend(() => {
      if (shutdownStarted === true) return Deferred.await(shutdownDone)
      shutdownStarted = true
      return send({ _tag: 'ShutdownRequested', exit }).pipe(Effect.andThen(Deferred.await(shutdownDone)))
    })

  const encodeEvents: ClientSessionSyncProcessor['encodeEvents'] = Effect.fn(
    'client-session-sync-processor:encode-events',
  )(function* (events) {
    // Store materializes before calling push. Reject here too, before a failed/stopping processor can change SQLite.
    yield* checkLocalAdmission
    let baseEventSequenceNumber = syncStateRef.current.localHead
    return yield* Effect.forEach(events, ({ name, args }) =>
      Effect.gen(function* () {
        const eventDef = yield* Effect.fromNullishOr(schema.eventsDefsMap.get(name)).pipe(Effect.orDieDebugger)
        const nextNumPair = EventSequenceNumber.Client.nextPair({
          seqNum: baseEventSequenceNumber,
          isClientOnly: eventDef.options.clientOnly,
          rebaseGeneration: baseEventSequenceNumber.rebaseGeneration,
        })
        baseEventSequenceNumber = nextNumPair.seqNum
        // Encoding known-valid domain data: an encode failure is an invariant violation (a defect),
        // so `Effect.orDie` is the correct modeling — it keeps the typed error channel narrow.
        const encoded = yield* Schema.encodeUnknownEffect(eventSchema)({
          name,
          // Client-document events expose SessionIdSymbol as an input placeholder, but encoded events are persisted
          // and replayed by concrete id. Resolve during schema encoding so commit never mutates the caller's event.
          args: resolveSessionIdSymbolInEventArgs(args, clientSession.sessionId),
          ...nextNumPair,
          clientId: clientSession.clientId,
          sessionId: clientSession.sessionId,
        }).pipe(Effect.orDie)
        return LiveStoreEvent.Client.Encoded.make(encoded)
      }),
    )
  })

  const materializeEvents: ClientSessionSyncProcessor['materializeEvents'] = Effect.fn(
    'client-session-sync-processor:materialize-events',
  )(function* (events) {
    const writeTables = new Set<string>()
    for (const event of events) {
      const { writeTables: newWriteTables } = yield* materializeEvent(event, {
        materializerHashLeader: Option.none(),
      })
      for (const table of newWriteTables) {
        writeTables.add(table)
      }
    }
    return { writeTables }
  })

  const push: ClientSessionSyncProcessor['push'] = Effect.fn('client-session-sync-processor:push')(
    function* (encodedEvents) {
      yield* checkLocalAdmission

      const mergeResult = yield* SyncState.merge({
        syncState: syncStateRef.current,
        payload: { _tag: 'local-push', newEvents: encodedEvents },
        isClientOnlyEvent,
        isEqualEvent: LiveStoreEvent.Client.isEqualEncoded,
      }).pipe(
        Effect.filterMapOrElse(Filter.tagged<typeof SyncState.MergeResult.Type>()('advance'), () =>
          Effect.die(new Error('Expected advance from local-push merge')),
        ),
      )

      yield* Effect.annotateCurrentSpan({
        batchSize: encodedEvents.length,
        mergeResultTag: mergeResult._tag,
        eventCounts: encodedEvents.reduce<Record<string, number>>((acc, event) => {
          acc[event.name] = (acc[event.name] ?? 0) + 1
          return acc
        }, {}),
        ...(TRACE_VERBOSE === true ? { mergeResult: jsonStringify(mergeResult) } : {}),
      })

      syncStateRef.current = mergeResult.newSyncState
      yield* Queue.offer(syncStateUpdateQueue, mergeResult.newSyncState)
      yield* send({ _tag: 'LocalPushAdmitted', events: mergeResult.newEvents })
    },
  )

  const checkLocalAdmission = Effect.suspend(() =>
    shutdownStarted === true || model.lifecycle === 'failed' || model.lifecycle === 'stopped'
      ? Effect.die(new Error('Cannot push events after the client session sync processor starts shutting down'))
      : Effect.void,
  )

  const debugInfo = { rebaseCount: 0, advanceCount: 0, rejectCount: 0 }

  return {
    boot,
    shutdown,
    encodeEvents,
    materializeEvents,
    push,
    syncState: Subscribable.make({
      get: Effect.sync(() => syncStateRef.current),
      changes: Stream.fromQueue(syncStateUpdateQueue),
    }),
    debug: {
      awaitDrainStarted: Deferred.await(drainStartedSignal),
      awaitRejection: Deferred.await(rejectionObserved),
      print: () => console.log('ClientSessionSyncProcessor', { debugInfo, syncState: syncStateRef.current, model }),
      debugInfo: () => debugInfo,
    },
  } satisfies ClientSessionSyncProcessor
})

type OperationId = number
type ProcessorError = MaterializeError | MaterializationJournal.MaterializationJournalError | UnknownError
type EventBatch = ReadonlyArray<LiveStoreEvent.Client.Encoded>

type Event =
  | { readonly _tag: 'LocalPushAdmitted'; readonly events: EventBatch }
  | {
      readonly _tag: 'PullItemReceived'
      readonly item: typeof PullItem.Type
      readonly completed: Deferred.Deferred<void>
    }
  | { readonly _tag: 'PullFailed'; readonly cause: Cause.Cause<never> }
  | { readonly _tag: 'LeaderPushSucceeded'; readonly operationId: OperationId }
  | { readonly _tag: 'LeaderPushRejected'; readonly operationId: OperationId; readonly error: RejectedPushError }
  | { readonly _tag: 'LeaderPushFailed'; readonly operationId: OperationId; readonly cause: Cause.Cause<never> }
  | { readonly _tag: 'ShutdownRequested'; readonly exit: Exit.Exit<unknown, unknown> }

type LeaderPushState =
  | { readonly _tag: 'idle'; readonly queued: EventBatch }
  | {
      readonly _tag: 'in-flight'
      readonly operationId: OperationId
      readonly batch: EventBatch
      readonly queued: EventBatch
    }
  | { readonly _tag: 'awaiting-reconciliation'; readonly error: RejectedPushError; readonly rejectedEvents: EventBatch }

interface Model {
  readonly lifecycle: 'starting' | 'running' | 'stopping' | 'failed' | 'stopped'
  readonly push: LeaderPushState
  readonly nextOperationId: OperationId
  readonly terminalCause: Cause.Cause<never> | undefined
}

const enqueueUnique = (
  push: Exclude<LeaderPushState, { readonly _tag: 'awaiting-reconciliation' }>,
  events: EventBatch,
): LeaderPushState => {
  const scheduled = push._tag === 'in-flight' ? [...push.batch, ...push.queued] : push.queued
  const additions = events.filter(
    (event) =>
      scheduled.some((scheduledEvent) => LiveStoreEvent.Client.isEqualEncoded(scheduledEvent, event)) === false,
  )
  return { ...push, queued: [...push.queued, ...additions] }
}

const isRejectedBatchRecovered = (rejectedEvents: EventBatch, pendingEvents: EventBatch): boolean =>
  rejectedEvents.every(
    (rejectedEvent) =>
      pendingEvents.some((pendingEvent) => LiveStoreEvent.Client.isEqualEncoded(pendingEvent, rejectedEvent)) === false,
  )
