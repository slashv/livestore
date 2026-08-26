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
  Schema,
  type Scope,
  Stream,
  Subscribable,
} from '@livestore/utils/effect'

import type { ClientSession } from '../adapter-types.ts'
import type { PullItem } from '../ClientSessionLeaderThreadProxy.ts'
import type { MaterializeError } from '../errors.ts'
import type { RejectedPushError } from '../leader-thread/RejectedPushError.ts'
import * as MaterializationJournal from '../MaterializationJournal.ts'
import * as EventSequenceNumber from '../schema/EventSequenceNumber/mod.ts'
import * as LiveStoreEvent from '../schema/LiveStoreEvent/mod.ts'
import type { LiveStoreSchema } from '../schema/mod.ts'
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

/**
 * Coordinates optimistic session commits with the leader.
 *
 * Local commits stay synchronous so UI reads see them immediately. Pulls, propagation results, rejection recovery,
 * and shutdown pass through one mailbox. Conflicting pulls rebase pending events without blocking new local commits.
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

  const handlePullItem = ({ payload, globalHead, materializerHashes }: typeof PullItem.Type) =>
    Effect.gen(function* () {
      const rejectionAtPullStart = model.push._tag === 'awaiting-reconciliation' ? model.push : undefined
      const mergeResult = yield* SyncState.merge({
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

      // Local admission can run during an asynchronous rebase. Install the merged base before the first async step so
      // a synchronous commit appends to the new pending suffix rather than to the state we are replacing.
      syncStateRef.current = mergeResult.newSyncState

      const recoveredRejection =
        rejectionAtPullStart !== undefined &&
        model.push === rejectionAtPullStart &&
        isRejectedBatchRecovered(rejectionAtPullStart.rejectedEvents, syncStateRef.current.pending) === true
      let resumeLeaderPush = false

      if (mergeResult._tag === 'rebase') {
        yield* Effect.spanEvent('merge:pull:rebase', {
          payloadTag: payload._tag,
          ...(TRACE_VERBOSE === true ? { payload: jsonStringify(payload) } : {}),
          newEventsCount: mergeResult.newEvents.length,
          rollbackCount: mergeResult.rollbackEvents.length,
          ...(TRACE_VERBOSE === true ? { res: jsonStringify(mergeResult) } : {}),
        })
        debugInfo.rebaseCount++

        yield* rebaseBarrier('before_leader_push_fiber_interrupt')
        if (leaderPushingFiberHandle !== undefined) yield* FiberHandle.clear(leaderPushingFiberHandle)

        if (LS_DEV === true) {
          yield* Effect.logDebug(
            'merge:pull:rebase: rollback',
            mergeResult.rollbackEvents.length,
            ...mergeResult.rollbackEvents.slice(0, 10).map(LiveStoreEvent.Client.toJSON),
          )
        }

        if (mergeResult.rollbackEvents.length > 0) {
          const headAfterRollback = mergeResult.rollbackEvents[0]!.parentSeqNum
          yield* Effect.gen(function* () {
            yield* materializationJournal.rollback(mergeResult.rollbackEvents.map((event) => event.seqNum))
            yield* stateHead.set(headAfterRollback)
          }).pipe(
            SqliteDbHelper.withSavepoint(dbState),
            Effect.mapError((cause) =>
              MaterializationJournal.isMaterializationJournalError(cause) === true
                ? cause
                : new MaterializationJournal.MaterializationJournalError({ method: 'rollback', cause }),
            ),
          )
        }

        yield* rebaseBarrier('before_queue_reconcile')

        // Re-read the live suffix because synchronous commits may have landed during rollback. Their mailbox events
        // are still queued, so admission is de-duplicated when those events are handled later.
        model = { ...model, push: { _tag: 'idle', queued: syncStateRef.current.pending } }

        resumeLeaderPush = true
      } else {
        yield* Effect.spanEvent('merge:pull:advance', {
          payloadTag: payload._tag,
          ...(TRACE_VERBOSE === true ? { payload: jsonStringify(payload) } : {}),
          newEventsCount: mergeResult.newEvents.length,
          ...(TRACE_VERBOSE === true ? { res: jsonStringify(mergeResult) } : {}),
        })
        debugInfo.advanceCount++

        if (recoveredRejection === true) {
          model = { ...model, push: { _tag: 'idle', queued: syncStateRef.current.pending } }
          resumeLeaderPush = true
        }
      }

      if (mergeResult.newEvents.length > 0) {
        const writeTables = new Set<string>()
        for (const event of mergeResult.newEvents) {
          const { writeTables: newWriteTables } = yield* materializeEvent(event, {
            materializerHashLeader:
              materializerHashes.find(({ eventNum }) => EventSequenceNumber.Client.isEqual(eventNum, event.seqNum))
                ?.hash ?? Option.none(),
          })
          for (const table of newWriteTables) writeTables.add(table)
        }
        refreshTables(writeTables)
      }

      yield* materializationJournal.discardUpTo(globalHead)

      // A synchronous local commit may have extended pending while this pull item was materialized. Publish the live
      // state rather than the earlier merge snapshot so observers never see that admitted suffix disappear.
      yield* Queue.offer(syncStateUpdateQueue, syncStateRef.current)
      if (resumeLeaderPush === true) {
        if (mergeResult._tag === 'rebase') yield* rebaseBarrier('before_leader_push_fiber_run')
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
          model = { ...model, push: enqueueUnique(model.push, event.events) }
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
      if (shutdownStarted === true || model.lifecycle === 'failed' || model.lifecycle === 'stopped') {
        return yield* Effect.die(
          new Error('Cannot push events after the client session sync processor starts shutting down'),
        )
      }

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
type ProcessorError = MaterializeError | MaterializationJournal.MaterializationJournalError
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
