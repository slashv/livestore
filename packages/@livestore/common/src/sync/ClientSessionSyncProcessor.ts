/// <reference lib="dom" />
import { LS_DEV, TRACE_VERBOSE } from '@livestore/utils'
import {
  Cause,
  Deferred,
  Effect,
  Exit,
  Filter,
  Option,
  Queue,
  Ref,
  Schema,
  type Scope,
  Stream,
  Subscribable,
} from '@livestore/utils/effect'

import type { ClientSession } from '../adapter-types.ts'
import type { MaterializeError } from '../errors.ts'
import * as MaterializationJournal from '../MaterializationJournal.ts'
import * as EventSequenceNumber from '../schema/EventSequenceNumber/mod.ts'
import * as LiveStoreEvent from '../schema/LiveStoreEvent/mod.ts'
import type { LiveStoreSchema } from '../schema/mod.ts'
import { resolveSessionIdSymbolInEventArgs } from '../session-id-symbol.ts'
import * as SqliteDbHelper from '../sqlite-db-helper.ts'
import * as StateHead from '../StateHead.ts'
import * as StateSqliteDb from '../StateSqliteDb.ts'
import * as ClientSessionSyncMachine from './ClientSessionSyncMachine.ts'
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

const jsonStringify = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))

/**
 * Coordinates synchronous optimistic commits with an Effect Machine runtime.
 *
 * The synchronous seam is intentional: local SQLite commits must update `syncStateRef` before returning to UI code.
 * The machine owns every asynchronous relationship after admission: leader push batching, pull application, rejection
 * reconciliation, rebase cancellation, failure notification, and graceful draining.
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

  const leaderHead = clientSession.leaderThread.initialState.leaderHead
  const syncStateRef = {
    current: new SyncState.SyncState({ localHead: leaderHead, upstreamHead: leaderHead, pending: [] }),
  }
  const syncStateUpdateQueue = yield* Queue.unbounded<SyncState.SyncState>()
  const shutdownDone = yield* Deferred.make<void>()
  const drainStartedSignal = yield* Deferred.make<void>()
  const rejectionObserved = yield* Deferred.make<void>()
  const pullRequests = yield* Ref.make(new Map<ClientSessionSyncMachine.PullRequestId, Deferred.Deferred<void>>())
  const suspensions = yield* Ref.make(new Map<ClientSessionSyncMachine.SuspensionId, Deferred.Deferred<void>>())
  const nextPullRequestId = yield* Ref.make(1)
  const nextSuspensionId = yield* Ref.make(1)
  const debugInfo = { rebaseCount: 0, advanceCount: 0, rejectCount: 0 }
  let shutdownStarted = false
  let shutdownFinished = false
  let processorFailed = false

  const rebaseBarrier = (point: RebaseBarrierPoint): Effect.Effect<void> =>
    params.rebaseBarriers?.[point] ?? Effect.void

  const isClientOnlyEvent = (eventEncoded: LiveStoreEvent.Client.Encoded) =>
    schema.eventsDefsMap.get(eventEncoded.name)?.options.clientOnly ?? false

  const completePull = (requestId: ClientSessionSyncMachine.PullRequestId) =>
    takeCompletion(pullRequests, requestId).pipe(
      Effect.flatMap((completion) =>
        completion === undefined ? Effect.void : Deferred.succeed(completion, undefined),
      ),
    )

  const completeSuspension = (suspensionId: ClientSessionSyncMachine.SuspensionId) =>
    takeCompletion(suspensions, suspensionId).pipe(
      Effect.flatMap((completion) =>
        completion === undefined ? Effect.void : Deferred.succeed(completion, undefined),
      ),
    )

  const finishShutdown = (exit: Exit.Exit<void, never>) =>
    Effect.gen(function* () {
      if (shutdownFinished === true) return
      shutdownFinished = true
      yield* interruptCompletions(pullRequests)
      yield* interruptCompletions(suspensions)
      yield* Deferred.done(shutdownDone, exit)
    })

  const notifyFailure = (cause: Cause.Cause<ClientSessionSyncMachine.ProcessorError>) =>
    Effect.gen(function* () {
      processorFailed = true
      // ClientSession owns the wider store lifecycle. Detaching avoids waiting for its shutdown to call back here.
      yield* clientSession.shutdown(Exit.failCause(cause)).pipe(Effect.forkDetach, Effect.asVoid)
    })

  const applyPull: ClientSessionSyncMachine.Dependencies['applyPull'] = (request, rejectionAtPullStart, suspendPush) =>
    Effect.gen(function* () {
      const { payload, globalHead, materializerHashes } = request.item
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

      // Install the merged base before any asynchronous rollback work. A synchronous local commit can then append to
      // this live pending suffix while the machine is in ApplyingPull.
      syncStateRef.current = mergeResult.newSyncState
      const recoveredRejection =
        rejectionAtPullStart !== undefined &&
        isRejectedBatchRecovered(rejectionAtPullStart.events, syncStateRef.current.pending) === true
      let pushPlan: ClientSessionSyncMachine.EventBatch | undefined

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
        yield* suspendPush

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
        // Re-read the live suffix: synchronous commits may have landed while rollback was suspended.
        pushPlan = syncStateRef.current.pending
      } else {
        yield* Effect.spanEvent('merge:pull:advance', {
          payloadTag: payload._tag,
          ...(TRACE_VERBOSE === true ? { payload: jsonStringify(payload) } : {}),
          newEventsCount: mergeResult.newEvents.length,
          ...(TRACE_VERBOSE === true ? { res: jsonStringify(mergeResult) } : {}),
        })
        debugInfo.advanceCount++
        if (recoveredRejection === true) pushPlan = syncStateRef.current.pending
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
      yield* Queue.offer(syncStateUpdateQueue, syncStateRef.current)
      if (mergeResult._tag === 'rebase') yield* rebaseBarrier('before_leader_push_fiber_run')
      return { pushPlan }
    })

  const machineDependencies: ClientSessionSyncMachine.Dependencies = {
    currentPending: () => syncStateRef.current.pending,
    leaderPushBatchSize: params.leaderPushBatchSize,
    pushLeader: (batch) =>
      clientSession.leaderThread.events
        .push(batch)
        .pipe(
          Effect.catchDefect((defect) =>
            Effect.fail<ClientSessionSyncMachine.PushFailure>({ _tag: 'PushFailure', cause: Cause.die(defect) }),
          ),
        ),
    runPull: (parent) =>
      Stream.suspend(() => clientSession.leaderThread.events.pull({ cursor: syncStateRef.current.upstreamHead })).pipe(
        Stream.tap(() =>
          clientSession.devtools.enabled === true ? clientSession.devtools.pullLatch.await : Effect.void,
        ),
        Stream.runForEach((item) =>
          Effect.gen(function* () {
            const requestId = yield* Ref.modify(nextPullRequestId, (id) => [id, id + 1])
            const completion = yield* Deferred.make<void>()
            yield* Ref.update(pullRequests, (current) => new Map(current).set(requestId, completion))
            yield* parent
              .send(ClientSessionSyncMachine.ClientSessionSyncEvents.PullItemReceived({ request: { requestId, item } }))
              .pipe(Effect.orDie)
            yield* Deferred.await(completion)
          }),
        ),
        Effect.forever,
        Effect.interruptible,
        Effect.catchCause((cause) =>
          Cause.hasInterruptsOnly(cause) === true
            ? Effect.void
            : Effect.fail<ClientSessionSyncMachine.PullFailure>({ _tag: 'PullFailure', cause }),
        ),
        Effect.withSpan('client-session-sync-processor:pull'),
        Effect.tapCauseLogPretty,
      ),
    applyPull,
    registerSuspension: Effect.gen(function* () {
      const suspensionId = yield* Ref.modify(nextSuspensionId, (id) => [id, id + 1])
      const completion = yield* Deferred.make<void>()
      yield* Ref.update(suspensions, (current) => new Map(current).set(suspensionId, completion))
      return { suspensionId, await: Deferred.await(completion) }
    }),
    completeSuspension,
    completePull,
    observeRejection: Effect.sync(() => debugInfo.rejectCount++).pipe(
      Effect.andThen(Deferred.succeed(rejectionObserved, undefined)),
    ),
    signalDrainStarted: Deferred.succeed(drainStartedSignal, undefined),
    notifyFailure,
    finishShutdown,
    runtimeFailed: (cause) =>
      notifyFailure(Cause.die(Cause.squash(cause))).pipe(
        Effect.andThen(finishShutdown(Exit.die(Cause.squash(cause)))),
        Effect.ignore,
      ),
  }

  const machine = yield* ClientSessionSyncMachine.start(machineDependencies)

  const shutdown: ClientSessionSyncProcessor['shutdown'] = (exit) =>
    Effect.suspend(() => {
      if (shutdownStarted === true) return Deferred.await(shutdownDone)
      shutdownStarted = true
      const mode = Exit.isFailure(exit) === true ? 'immediate' : 'drain'
      return machine
        .send(ClientSessionSyncMachine.ClientSessionSyncEvents.ShutdownRequested({ mode }))
        .pipe(Effect.andThen(Deferred.await(shutdownDone)))
    })

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

    yield* machine.send(ClientSessionSyncMachine.ClientSessionSyncEvents.Boot())
    yield* Effect.addFinalizer(() => shutdown(Exit.void).pipe(Effect.ignore, Effect.andThen(machine.stop)))
  }).pipe(Effect.withSpan('client-session-sync-processor:boot'))

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
        const encoded = yield* Schema.encodeUnknownEffect(eventSchema)({
          name,
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
      for (const table of newWriteTables) writeTables.add(table)
    }
    return { writeTables }
  })

  const push: ClientSessionSyncProcessor['push'] = Effect.fn('client-session-sync-processor:push')(
    function* (encodedEvents) {
      if (shutdownStarted === true || processorFailed === true || shutdownFinished === true) {
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
      yield* machine.send(
        ClientSessionSyncMachine.ClientSessionSyncEvents.LocalPushAdmitted({ events: mergeResult.newEvents }),
      )
    },
  )

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
      print: () =>
        console.log('ClientSessionSyncProcessor', { debugInfo, syncState: syncStateRef.current, processorFailed }),
      debugInfo: () => debugInfo,
    },
  } satisfies ClientSessionSyncProcessor
})

const takeCompletion = <Id>(
  completionsRef: Ref.Ref<Map<Id, Deferred.Deferred<void>>>,
  id: Id,
): Effect.Effect<Deferred.Deferred<void> | undefined> =>
  Ref.modify(completionsRef, (current) => {
    const next = new Map(current)
    const completion = next.get(id)
    next.delete(id)
    return [completion, next]
  })

const interruptCompletions = <Id>(completionsRef: Ref.Ref<Map<Id, Deferred.Deferred<void>>>) =>
  Effect.gen(function* () {
    const completions = yield* Ref.getAndSet(completionsRef, new Map())
    yield* Effect.forEach(completions.values(), Deferred.interrupt, { discard: true })
  })

const isRejectedBatchRecovered = (
  rejectedEvents: ClientSessionSyncMachine.EventBatch,
  pendingEvents: ClientSessionSyncMachine.EventBatch,
): boolean =>
  rejectedEvents.every(
    (rejectedEvent) =>
      pendingEvents.some((pendingEvent) => LiveStoreEvent.Client.isEqualEncoded(pendingEvent, rejectedEvent)) === false,
  )
