/// <reference lib="dom" />
import { casesHandled, TRACE_VERBOSE } from '@livestore/utils'
import {
  Cause,
  Deferred,
  Effect,
  Exit,
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
  commit: (
    events: ReadonlyArray<LiveStoreEvent.Input.Decoded>,
  ) => Effect.Effect<{ writeTables: Set<string> }, ProcessorError>
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

/**
 * One synchronous owner for local commits, pulled state and propagation decisions.
 *
 * Store.commit calls dispatch directly. Network results enter the same handler. Every transition finishes its
 * SQLite/model work before returning; it never waits on another fiber. Store retains its local subscriber refresh.
 *
 * The command runner does the waiting. Its reconciliation loop calls dispatch for one complete step, refreshes
 * subscribers, then yields. Loop locals describe traversal, not an alternative copy of session state. This keeps
 * local commits responsive between steps without a second state-changing path or a continuation-event framework.
 *
 * Read transition for the workflow map, then commitLocalEvents or applyPullStep for state changes. Persistence
 * details live in applyPullToSqlite; reconcile shows where cancellation, callbacks and yielding can interleave.
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
  /** Must remain synchronous, including the SQLite/journal/head services, just like Store.commit. */
  materializeEvent: (
    eventEncoded: LiveStoreEvent.Client.Encoded,
    options: { materializerHashLeader: Option.Option<number> },
  ) => Effect.Effect<
    { writeTables: Set<string>; materializerHash: Option.Option<number> },
    MaterializeError | MaterializationJournal.MaterializationJournalError
  >
  refreshTables: (tables: Set<string>) => void
  params: {
    leaderPushBatchSize: number
    /** Test-only pauses in the async runner, never inside dispatch. */
    rebaseBarriers?: Partial<Record<RebaseBarrierPoint, Effect.Effect<void>>>
  }
  confirmUnsavedChanges: boolean
}) {
  const materializationJournal = yield* MaterializationJournal.MaterializationJournal
  const stateHead = yield* StateHead.StateHead
  const dbState = yield* StateSqliteDb.StateSqliteDb
  const eventSchema = LiveStoreEvent.Client.makeSchemaMemo(schema)
  const commands = yield* Queue.unbounded<Command>()
  const syncStateUpdateQueue = yield* Queue.unbounded<SyncState.SyncState>()
  const shutdownDone = yield* Deferred.make<void>()
  const drainStartedSignal = yield* Deferred.make<void>()
  const rejectionObserved = yield* Deferred.make<void>()
  const leaderHead = clientSession.leaderThread.initialState.leaderHead
  let model: Model = {
    lifecycle: 'starting',
    syncState: new SyncState.SyncState({ localHead: leaderHead, upstreamHead: leaderHead, pending: [] }),
    push: { _tag: 'idle', queued: [] },
    nextOperationId: 1,
    reconciliation: undefined,
    shutdownExit: undefined,
    terminalCause: undefined,
  }
  const debugInfo = { rebaseCount: 0, advanceCount: 0, rejectCount: 0 }

  /** All model changes run through these workflows under dispatch's synchronous ownership guard. */
  const transition = (event: Event, deferNotification: DeferNotification): Effect.Effect<StepResult, ProcessorError> =>
    Effect.gen(function* () {
      switch (event._tag) {
        case 'Started':
          if (model.lifecycle === 'starting') model = { ...model, lifecycle: 'running' }
          break
        case 'Commit':
          return yield* commitLocalEvents(event.events, deferNotification)
        case 'PullReceived':
          yield* acceptPull(event, deferNotification)
          break
        case 'PullStep':
          return yield* applyPullStep(event, deferNotification)
        case 'PullFinished':
          yield* finishPull(event, deferNotification)
          break
        case 'PushSucceeded':
        case 'PushRejected':
        case 'PushFailed':
          yield* completePush(event, deferNotification)
          break
        case 'Failed':
          yield* failSession(event.cause, deferNotification)
          break
        case 'ShutdownRequested':
          yield* requestShutdown(event.exit, deferNotification)
          break
        case 'DrainStarted':
          yield* startDrain(event.exit, deferNotification)
          break
        case 'Stopped':
          model = { ...model, lifecycle: 'stopped', reconciliation: undefined }
          yield* deferNotification(Deferred.done(shutdownDone, event.exit))
          break
        default:
          casesHandled(event)
      }
      return { _tag: 'applied', writeTables: new Set<string>() }
    })

  let dispatching = false
  const dispatch = (event: Event) =>
    Effect.suspend(() => {
      if (dispatching === true) return Effect.die(new Error('Reentrant session transition from materialization'))
      dispatching = true
      // Completing a Deferred or offering a Queue can resume another Effect fiber inline. Stage those notifications
      // until the owner is released, just as we do for subscriber callbacks. Reentrant callers then see finished state.
      const notifications: Array<Effect.Effect<unknown>> = []
      return transition(event, (effect) =>
        Effect.sync(() => {
          notifications.push(effect)
        }),
      ).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            dispatching = false
          }),
        ),
        Effect.tap(() => Effect.forEach(notifications, (effect) => effect, { discard: true })),
      )
    }).pipe(
      // Exclusion relies on synchronous storage/materializers, not merely a savepoint or an uninterruptible fiber.
      Effect.provideService(References.PreventSchedulerYield, true),
      Effect.uninterruptible,
    )

  // Synchronous owner workflows. Waiting and subscriber callbacks belong to the runner below.

  const commitLocalEvents = (
    events: ReadonlyArray<LiveStoreEvent.Input.Decoded>,
    deferNotification: DeferNotification,
  ): Effect.Effect<StepResult, ProcessorError> =>
    Effect.gen(function* () {
      yield* checkLocalAdmission
      const encoded = yield* encodeEvents(events)
      const result = yield* merge({ _tag: 'local-push', newEvents: encoded })
      if (result._tag !== 'advance') return yield* Effect.die(new Error('Expected advance from local-push merge'))
      yield* Effect.annotateCurrentSpan({
        batchSize: encoded.length,
        mergeResultTag: result._tag,
        eventCounts: encoded.reduce<Record<string, number>>((counts, item) => {
          counts[item.name] = (counts[item.name] ?? 0) + 1
          return counts
        }, {}),
        ...(TRACE_VERBOSE === true ? { mergeResult: Schema.encodeSync(Schema.UnknownFromJsonString)(result) } : {}),
      })
      const { writeTables } = yield* materializeEvents(encoded).pipe(
        SqliteDbHelper.withSavepoint(dbState),
        Effect.mapError((cause) => (cause._tag === 'SqliteError' ? new UnknownError({ cause }) : cause)),
      )
      model = {
        ...model,
        syncState: result.newSyncState,
        push:
          model.push._tag === 'awaiting-reconciliation'
            ? model.push
            : { ...model.push, queued: [...model.push.queued, ...result.newEvents] },
      }
      yield* deferNotification(Queue.offer(syncStateUpdateQueue, model.syncState))
      yield* reserveNextPush(deferNotification)
      return { _tag: 'applied', writeTables }
    })

  const acceptPull = (
    event: Extract<Event, { _tag: 'PullReceived' }>,
    deferNotification: DeferNotification,
  ): Effect.Effect<void, ProcessorError> =>
    Effect.gen(function* () {
      if (model.lifecycle !== 'running' || model.shutdownExit !== undefined) {
        yield* deferNotification(Deferred.succeed(event.completed, undefined))
        return
      }
      // Validate the whole payload before applying a prefix; never retain this as a materialization plan.
      const result = yield* merge(event.item.payload)
      if (result._tag === 'reject') return yield* Effect.die(new Error('Unexpected rejected pull'))
      if (model.reconciliation !== undefined) return yield* Effect.die(new Error('Pull backpressure was bypassed'))
      const id = model.nextOperationId
      const minimumEnd =
        event.item.payload._tag === 'upstream-rebase'
          ? event.item.payload.newEvents.findIndex((item) =>
              EventSequenceNumber.Client.isGreaterThanOrEqual(item.seqNum, model.syncState.upstreamHead),
            ) + 1
          : 0
      model = {
        ...model,
        nextOperationId: id + 1,
        reconciliation: { id },
      }
      yield* deferNotification(
        Queue.offer(commands, { _tag: 'Reconcile', id, item: event.item, minimumEnd, completed: event.completed }),
      )
    })

  const applyPullStep = (
    event: Extract<Event, { _tag: 'PullStep' }>,
    deferNotification: DeferNotification,
  ): Effect.Effect<StepResult, ProcessorError> =>
    Effect.gen(function* () {
      if (model.lifecycle !== 'running' || model.reconciliation?.id !== event.id) return { _tag: 'obsolete' }
      const result = yield* merge(event.payload)
      if (result._tag === 'reject') return yield* Effect.die(new Error('Unexpected rejected pull'))
      if (result._tag === 'rebase' && event.pushCancelled === false) return { _tag: 'cancel-push' }
      const { writeTables } = yield* applyPullToSqlite(result, event)
      model = {
        ...model,
        syncState: result.newSyncState,
        // Invalidate the old operation before any queued completion can run. No replacement starts mid-pull.
        push: result._tag === 'rebase' ? { _tag: 'idle', queued: result.newSyncState.pending } : model.push,
      }
      yield* deferNotification(Queue.offer(syncStateUpdateQueue, model.syncState))
      return { _tag: 'applied', writeTables }
    })

  const finishPull = (
    event: Extract<Event, { _tag: 'PullFinished' }>,
    deferNotification: DeferNotification,
  ): Effect.Effect<void, ProcessorError> =>
    Effect.gen(function* () {
      if (model.lifecycle !== 'running' || model.reconciliation?.id !== event.id) return
      // A push can finish between steps now. Judge rejection recovery against its current state, not a
      // snapshot taken when the pull started (the later prefixes may have confirmed the rejected batch).
      const recovered =
        model.push._tag === 'awaiting-reconciliation' &&
        isRejectedBatchRecovered(model.push.rejectedEvents, model.syncState.pending)
      model = {
        ...model,
        reconciliation: undefined,
        push:
          event.rebased === true || recovered === true ? { _tag: 'idle', queued: model.syncState.pending } : model.push,
      }
      if (event.rebased === true) debugInfo.rebaseCount++
      else debugInfo.advanceCount++
      yield* reserveNextPush(deferNotification)
    })

  const completePush = (
    event: Extract<Event, { _tag: 'PushSucceeded' | 'PushRejected' | 'PushFailed' }>,
    deferNotification: DeferNotification,
  ): Effect.Effect<void, ProcessorError> =>
    Effect.gen(function* () {
      if (model.lifecycle !== 'running' && model.lifecycle !== 'stopping') return
      if (model.push._tag !== 'in-flight' || model.push.operationId !== event.operationId) return
      if (event._tag === 'PushFailed') {
        yield* failSession(event.cause, deferNotification)
        return
      }
      if (event._tag === 'PushSucceeded') {
        model = { ...model, push: { _tag: 'idle', queued: model.push.queued } }
      } else {
        debugInfo.rejectCount++
        yield* deferNotification(Deferred.succeed(rejectionObserved, undefined))
        model = {
          ...model,
          push:
            isRejectedBatchRecovered(model.push.batch, model.syncState.pending) === true
              ? { _tag: 'idle', queued: model.syncState.pending }
              : { _tag: 'awaiting-reconciliation', rejectedEvents: model.push.batch, error: event.error },
        }
        if (model.lifecycle === 'stopping' && model.push._tag === 'awaiting-reconciliation') {
          yield* deferNotification(Queue.offer(commands, { _tag: 'FinishShutdown', exit: Exit.die(model.push.error) }))
        }
      }
      yield* reserveNextPush(deferNotification)
    })

  /** Reserve the operation before its command becomes visible to the asynchronous runner. */
  const reserveNextPush = (deferNotification: DeferNotification): Effect.Effect<void, ProcessorError> =>
    Effect.gen(function* () {
      if (model.reconciliation !== undefined || (model.lifecycle !== 'running' && model.lifecycle !== 'stopping'))
        return
      if (model.push._tag !== 'idle') return
      if (model.push.queued.length === 0) {
        if (model.lifecycle === 'stopping')
          yield* deferNotification(Queue.offer(commands, { _tag: 'FinishShutdown', exit: Exit.void }))
        return
      }
      const operationId = model.nextOperationId
      const batch = model.push.queued.slice(0, params.leaderPushBatchSize)
      model = {
        ...model,
        nextOperationId: operationId + 1,
        push: { _tag: 'in-flight', operationId, batch, queued: model.push.queued.slice(batch.length) },
      }
      yield* deferNotification(Queue.offer(commands, { _tag: 'Push', operationId, batch }))
    })

  const requestShutdown = (
    exit: Exit.Exit<unknown, unknown>,
    deferNotification: DeferNotification,
  ): Effect.Effect<void, ProcessorError> =>
    Effect.gen(function* () {
      if (model.shutdownExit !== undefined || model.lifecycle === 'stopped') return
      // Close admission now, but finish the already-accepted pull before starting the graceful drain.
      model = { ...model, shutdownExit: exit }
      yield* deferNotification(Queue.offer(commands, { _tag: 'BeginShutdown', exit }))
    })

  const startDrain = (
    exit: Exit.Exit<unknown, unknown>,
    deferNotification: DeferNotification,
  ): Effect.Effect<void, ProcessorError> =>
    Effect.gen(function* () {
      if (model.lifecycle === 'stopped') return
      model = { ...model, lifecycle: 'stopping' }
      if (Exit.isFailure(exit) === true) {
        yield* deferNotification(Queue.offer(commands, { _tag: 'FinishShutdown', exit: Exit.void }))
        return
      }
      yield* deferNotification(Deferred.succeed(drainStartedSignal, undefined))
      if (model.terminalCause !== undefined) {
        yield* deferNotification(
          Queue.offer(commands, { _tag: 'FinishShutdown', exit: Exit.failCause(model.terminalCause) }),
        )
      } else if (model.push._tag === 'awaiting-reconciliation') {
        yield* deferNotification(Queue.offer(commands, { _tag: 'FinishShutdown', exit: Exit.die(model.push.error) }))
      } else yield* reserveNextPush(deferNotification)
    })

  const failSession = (
    cause: Cause.Cause<ProcessorError>,
    deferNotification: DeferNotification,
  ): Effect.Effect<void, ProcessorError> =>
    Effect.gen(function* () {
      if (model.lifecycle === 'failed' || model.lifecycle === 'stopped') return
      const terminalCause = Cause.die(Cause.squash(cause))
      const stopping = model.lifecycle === 'stopping'
      model = { ...model, lifecycle: 'failed', reconciliation: undefined, terminalCause }
      yield* deferNotification(
        Queue.offer(
          commands,
          stopping === true
            ? { _tag: 'FinishShutdown', exit: Exit.failCause(terminalCause) }
            : { _tag: 'NotifyFailure', cause },
        ),
      )
    })

  // Synchronous admission, encoding and persistence. These helpers never install or publish the session model.

  const isClientOnlyEvent = (event: LiveStoreEvent.Client.Encoded) =>
    schema.eventsDefsMap.get(event.name)?.options.clientOnly ?? false
  const merge = (payload: typeof SyncState.Payload.Type) =>
    SyncState.merge({
      syncState: model.syncState,
      payload,
      isClientOnlyEvent,
      isEqualEvent: LiveStoreEvent.Client.isEqualEncoded,
    })
  const checkLocalAdmission = Effect.suspend(() =>
    model.shutdownExit !== undefined || model.lifecycle !== 'running'
      ? Effect.die(new Error('Cannot push events after the client session sync processor starts shutting down'))
      : Effect.void,
  )

  const encodeEvents = Effect.fn('client-session-sync-processor:encode-events')(function* (
    events: ReadonlyArray<LiveStoreEvent.Input.Decoded>,
  ) {
    let baseEventSequenceNumber = model.syncState.localHead
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

  const materializeEvents = Effect.fn('client-session-sync-processor:materialize-events')(function* (
    events: EventBatch,
  ) {
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

  /** Complete the SQLite step before the owner installs its matching model or publishes notifications. */
  const applyPullToSqlite = (
    result: SyncState.MergeResultAdvance | SyncState.MergeResultRebase,
    event: Extract<Event, { _tag: 'PullStep' }>,
  ) =>
    Effect.gen(function* () {
      const writeTables = new Set<string>()
      if (result._tag === 'rebase') {
        yield* materializationJournal.rollback(result.rollbackEvents.map((item) => item.seqNum))
        // Rollback may touch tables absent from replacement events. Refresh all user tables in that case.
        if (result.rollbackEvents.length > 0) {
          for (const table of schema.state.sqlite.tables.keys()) {
            if (SystemTables.isStateSystemTable(table) === false) writeTables.add(table)
          }
        }
      }
      for (const item of result.newEvents) {
        const materialized = yield* materializeEvent(item, {
          // A replayed pending event can temporarily share a sequence number with a later incoming event.
          materializerHashLeader:
            event.payload.newEvents.some((incoming) => LiveStoreEvent.Client.isEqualEncoded(incoming, item)) === true
              ? (event.materializerHashes.find(({ eventNum }) =>
                  EventSequenceNumber.Client.isEqual(eventNum, item.seqNum),
                )?.hash ?? Option.none())
              : Option.none(),
        })
        for (const table of materialized.writeTables) writeTables.add(table)
      }
      yield* stateHead.set(result.newSyncState.localHead)
      if (event.last === true) yield* materializationJournal.discardUpTo(event.globalHead)
      return { writeTables }
    }).pipe(
      SqliteDbHelper.withSavepoint(dbState),
      Effect.mapError((cause) => (cause._tag === 'SqliteError' ? new UnknownError({ cause }) : cause)),
    )

  // Asynchronous runner. It reads current state and enters the owner through dispatch.

  const rebaseBarrier = (point: RebaseBarrierPoint) => params.rebaseBarriers?.[point] ?? Effect.void
  const reportFailure = (cause: Cause.Cause<ProcessorError>) => dispatch({ _tag: 'Failed', cause }).pipe(Effect.orDie)
  const report = (event: Event) => dispatch(event).pipe(Effect.catchCause(reportFailure), Effect.asVoid)

  const reconcile = (command: Extract<Command, { _tag: 'Reconcile' }>, pushHandle: RunnerHandles['push']) =>
    Effect.gen(function* () {
      let offset = 0
      let pushCancelled = false
      do {
        const newEvents = command.item.payload.newEvents.slice(
          offset,
          Math.max(offset + PULL_CHUNK_SIZE, command.minimumEnd),
        )
        const last = offset + newEvents.length === command.item.payload.newEvents.length
        const result = yield* dispatch({
          _tag: 'PullStep',
          id: command.id,
          pushCancelled,
          last,
          payload: offset === 0 ? { ...command.item.payload, newEvents } : { _tag: 'upstream-advance', newEvents },
          materializerHashes: command.item.materializerHashes,
          globalHead: command.item.globalHead,
        })
        if (result._tag === 'obsolete') return
        if (result._tag === 'cancel-push') {
          yield* rebaseBarrier('before_leader_push_fiber_interrupt')
          yield* FiberHandle.clear(pushHandle)
          yield* rebaseBarrier('before_queue_reconcile')
          pushCancelled = true
          continue
        }
        // The owner is released before callbacks: subscribers can commit against the completed prefix.
        if (result.writeTables.size > 0) refreshTables(result.writeTables)
        offset += newEvents.length
        if (last === true) break
        yield* Effect.yieldNow
      } while (offset <= command.item.payload.newEvents.length)
      if (pushCancelled === true) yield* rebaseBarrier('before_leader_push_fiber_run')
      yield* dispatch({ _tag: 'PullFinished', id: command.id, rebased: pushCancelled })
    }).pipe(Effect.ensuring(Deferred.succeed(command.completed, undefined)))

  const startLeaderPush = (command: Extract<Command, { _tag: 'Push' }>, pushHandle: RunnerHandles['push']) =>
    Effect.gen(function* () {
      // A command can wait behind reconciliation and become obsolete before its fiber even starts.
      if (
        model.push._tag !== 'in-flight' ||
        model.push.operationId !== command.operationId ||
        (model.lifecycle !== 'running' && model.lifecycle !== 'stopping')
      )
        return
      yield* FiberHandle.run(
        pushHandle,
        Effect.suspend(() => clientSession.leaderThread.events.push(command.batch)).pipe(
          Effect.matchEffect({
            onFailure: (error) => report({ _tag: 'PushRejected', operationId: command.operationId, error }),
            onSuccess: () => report({ _tag: 'PushSucceeded', operationId: command.operationId }),
          }),
          Effect.catchCause((cause) =>
            Cause.hasInterruptsOnly(cause) === true
              ? Effect.void
              : report({ _tag: 'PushFailed', operationId: command.operationId, cause }),
          ),
          Effect.interruptible,
        ),
      )
    })

  const runCommand = (command: Command, handles: RunnerHandles): Effect.Effect<void, ProcessorError> =>
    Effect.gen(function* () {
      switch (command._tag) {
        case 'Push':
          yield* startLeaderPush(command, handles.push)
          break
        case 'Reconcile':
          yield* reconcile(command, handles.push)
          break
        case 'BeginShutdown':
          yield* FiberHandle.clear(handles.pull)
          yield* dispatch({ _tag: 'DrainStarted', exit: command.exit })
          break
        case 'FinishShutdown':
          yield* FiberHandle.clear(handles.pull)
          yield* FiberHandle.clear(handles.push)
          yield* dispatch({ _tag: 'Stopped', exit: command.exit })
          break
        case 'NotifyFailure':
          yield* FiberHandle.clear(handles.pull)
          yield* FiberHandle.clear(handles.push)
          // Store shutdown may call back into this processor. Supervise it without blocking the runner it needs.
          yield* FiberHandle.run(handles.shutdown, clientSession.shutdown(Exit.failCause(command.cause)))
          break
        default:
          casesHandled(command)
      }
    })

  const runCommands = (handles: RunnerHandles) =>
    Effect.gen(function* () {
      while (model.lifecycle !== 'stopped') {
        yield* runCommand(yield* Queue.take(commands), handles).pipe(Effect.catchCause(reportFailure))
      }
      yield* Queue.shutdown(commands)
    })

  const pull = Stream.suspend(() =>
    clientSession.leaderThread.events.pull({ cursor: model.syncState.upstreamHead }),
  ).pipe(
    Stream.tap(() => (clientSession.devtools.enabled === true ? clientSession.devtools.pullLatch.await : Effect.void)),
    Stream.tap((item) =>
      Effect.gen(function* () {
        const completed = yield* Deferred.make<void>()
        yield* report({ _tag: 'PullReceived', item, completed })
        yield* Deferred.await(completed)
      }),
    ),
    Stream.runDrain,
    Effect.forever,
    Effect.interruptible,
    Effect.catchCause((cause) => (Cause.hasInterruptsOnly(cause) === true ? Effect.void : reportFailure(cause))),
    Effect.withSpan('client-session-sync-processor:pull'),
    Effect.tapCauseLogPretty,
  )

  const installBeforeUnloadWarning = Effect.gen(function* () {
    if (
      confirmUnsavedChanges === true &&
      typeof window !== 'undefined' &&
      typeof window.addEventListener === 'function'
    ) {
      const onBeforeUnload = (event: BeforeUnloadEvent) => {
        if (model.syncState.pending.length > 0) event.preventDefault()
      }
      yield* Effect.acquireRelease(
        Effect.sync(() => window.addEventListener('beforeunload', onBeforeUnload)),
        () => Effect.sync(() => window.removeEventListener('beforeunload', onBeforeUnload)),
      )
    }
  })

  const boot: ClientSessionSyncProcessor['boot'] = Effect.gen(function* () {
    const handles: RunnerHandles = {
      push: yield* FiberHandle.make<void, never>(),
      pull: yield* FiberHandle.make<void, never>(),
      shutdown: yield* FiberHandle.make<void, never>(),
    }
    yield* installBeforeUnloadWarning
    yield* dispatch({ _tag: 'Started' }).pipe(Effect.orDie)
    yield* runCommands(handles).pipe(Effect.forkScoped)
    yield* FiberHandle.run(handles.pull, pull.pipe(Effect.asVoid))
  }).pipe(Effect.withSpan('client-session-sync-processor:boot'))

  return {
    boot,
    shutdown: (exit) =>
      dispatch({ _tag: 'ShutdownRequested', exit }).pipe(Effect.orDie, Effect.andThen(Deferred.await(shutdownDone))),
    commit: Effect.fn('client-session-sync-processor:commit')((events: ReadonlyArray<LiveStoreEvent.Input.Decoded>) =>
      dispatch({ _tag: 'Commit', events }).pipe(
        Effect.flatMap((result) =>
          result._tag === 'applied'
            ? Effect.succeed({ writeTables: result.writeTables })
            : Effect.die(new Error('Local commit must complete synchronously')),
        ),
      ),
    ),
    syncState: Subscribable.make({
      get: Effect.sync(() => model.syncState),
      changes: Stream.fromQueue(syncStateUpdateQueue),
    }),
    debug: {
      awaitDrainStarted: Deferred.await(drainStartedSignal),
      awaitRejection: Deferred.await(rejectionObserved),
      print: () => console.log('ClientSessionSyncProcessor', { debugInfo, model }),
      debugInfo: () => debugInfo,
    },
  } satisfies ClientSessionSyncProcessor
})

/** Delivery is staged until dispatch releases the owner; a failed transition drops its notifications. */
type DeferNotification = (effect: Effect.Effect<unknown>) => Effect.Effect<void>
interface RunnerHandles {
  readonly push: FiberHandle.FiberHandle<void, never>
  readonly pull: FiberHandle.FiberHandle<void, never>
  readonly shutdown: FiberHandle.FiberHandle<void, never>
}
type ProcessorError = MaterializeError | MaterializationJournal.MaterializationJournalError | UnknownError
type EventBatch = ReadonlyArray<LiveStoreEvent.Client.Encoded>
type Pull = typeof PullItem.Type
type Event =
  | { readonly _tag: 'Started' }
  | { readonly _tag: 'Commit'; readonly events: ReadonlyArray<LiveStoreEvent.Input.Decoded> }
  | { readonly _tag: 'PullReceived'; readonly item: Pull; readonly completed: Deferred.Deferred<void> }
  | {
      readonly _tag: 'PullStep'
      readonly id: number
      readonly payload: Pull['payload']
      readonly pushCancelled: boolean
      readonly last: boolean
      readonly globalHead: Pull['globalHead']
      readonly materializerHashes: Pull['materializerHashes']
    }
  | { readonly _tag: 'PullFinished'; readonly id: number; readonly rebased: boolean }
  | { readonly _tag: 'PushSucceeded'; readonly operationId: number }
  | { readonly _tag: 'PushRejected'; readonly operationId: number; readonly error: RejectedPushError }
  | { readonly _tag: 'PushFailed'; readonly operationId: number; readonly cause: Cause.Cause<never> }
  | { readonly _tag: 'Failed'; readonly cause: Cause.Cause<ProcessorError> }
  | { readonly _tag: 'ShutdownRequested' | 'DrainStarted'; readonly exit: Exit.Exit<unknown, unknown> }
  | { readonly _tag: 'Stopped'; readonly exit: Exit.Exit<void> }
type Command =
  | { readonly _tag: 'Push'; readonly operationId: number; readonly batch: EventBatch }
  | {
      readonly _tag: 'Reconcile'
      readonly id: number
      readonly item: Pull
      readonly minimumEnd: number
      readonly completed: Deferred.Deferred<void>
    }
  | { readonly _tag: 'BeginShutdown'; readonly exit: Exit.Exit<unknown, unknown> }
  | { readonly _tag: 'FinishShutdown'; readonly exit: Exit.Exit<void> }
  | { readonly _tag: 'NotifyFailure'; readonly cause: Cause.Cause<ProcessorError> }
type StepResult =
  | { readonly _tag: 'applied'; readonly writeTables: Set<string> }
  | { readonly _tag: 'cancel-push' }
  | { readonly _tag: 'obsolete' }
type LeaderPushState =
  | { readonly _tag: 'idle'; readonly queued: EventBatch }
  | {
      readonly _tag: 'in-flight'
      readonly operationId: number
      readonly batch: EventBatch
      readonly queued: EventBatch
    }
  | { readonly _tag: 'awaiting-reconciliation'; readonly error: RejectedPushError; readonly rejectedEvents: EventBatch }
interface Model {
  readonly lifecycle: 'starting' | 'running' | 'stopping' | 'failed' | 'stopped'
  readonly syncState: SyncState.SyncState
  readonly push: LeaderPushState
  readonly nextOperationId: number
  readonly reconciliation: { readonly id: number } | undefined
  readonly shutdownExit: Exit.Exit<unknown, unknown> | undefined
  readonly terminalCause: Cause.Cause<never> | undefined
}
const PULL_CHUNK_SIZE = 32
const isRejectedBatchRecovered = (rejectedEvents: EventBatch, pendingEvents: EventBatch): boolean =>
  rejectedEvents.every(
    (rejected) => pendingEvents.some((pending) => LiveStoreEvent.Client.isEqualEncoded(pending, rejected)) === false,
  )
