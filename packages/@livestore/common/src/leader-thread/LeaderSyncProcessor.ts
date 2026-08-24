import { casesHandled, TRACE_VERBOSE } from '@livestore/utils'
import {
  type HttpClient,
  type Latch,
  type Scope,
  type Tracer,
  Context,
  Deferred,
  Duration,
  Effect,
  FiberHandle,
  FiberSet,
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
import type { UnknownEventError } from '../errors.ts'
import * as EventlogSqliteDb from '../EventlogSqliteDb.ts'
import { makeMaterializerHash } from '../materializer-helper.ts'
import type { LiveStoreSchema } from '../schema/mod.ts'
import { EventSequenceNumber, LiveStoreEvent, resolveEventDef } from '../schema/mod.ts'
import { EVENTLOG_META_TABLE, SYNC_STATUS_TABLE } from '../schema/state/sqlite/system-tables/eventlog-tables.ts'
import * as StateSqliteDb from '../StateSqliteDb.ts'
import type { SyncBackend } from '../sync/sync.ts'
import * as SyncState from '../sync/syncstate.ts'
import { sql } from '../util.ts'
import * as Eventlog from './eventlog.ts'
import * as LeaderSyncCommitter from './LeaderSyncCommitter.ts'
import * as Machine from './LeaderSyncMachine.ts'
import * as MachineRuntime from './LeaderSyncMachineRuntime.ts'
import { isRejectedPushError, type RejectedPushError } from './RejectedPushError.ts'
import * as Shutdown from './shutdown-channel.ts'
import type { InitialBlockingSyncContext } from './types.ts'

export const TypeId = '~@livestore/common/LeaderSyncProcessor' as const
export type TypeId = typeof TypeId

/** Effect adapters and public API for the pure `LeaderSyncMachine` orchestration kernel. */
export class LeaderSyncProcessor extends Context.Service<LeaderSyncProcessor, Service>()(
  '@livestore/common/LeaderSyncProcessor',
) {}

export interface Service {
  readonly [TypeId]: TypeId
  readonly pull: (args: { cursor: EventSequenceNumber.Client.Composite }) => Stream.Stream<typeof PullItem.Type>
  readonly pullQueue: (args: {
    cursor: EventSequenceNumber.Client.Composite
  }) => Effect.Effect<Queue.Queue<typeof PullItem.Type>, never, Scope.Scope>
  /** Resolves only after durable commit, publication, and backend propagation scheduling. */
  readonly push: (batch: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>) => Effect.Effect<void, RejectedPushError>
  readonly pushPartial: (args: {
    event: LiveStoreEvent.Input.Encoded
    clientId: string
    sessionId: string
  }) => Effect.Effect<void, UnknownEventError>
  readonly boot: Effect.Effect<
    { initialLeaderHead: EventSequenceNumber.Client.Composite },
    never,
    Scope.Scope | HttpClient.HttpClient
  >
  readonly syncState: Subscribable.Subscribable<SyncState.SyncState>
}

interface Options {
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
      readonly localPushAdmitted?: (events: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>) => Effect.Effect<void>
    }
  }
}

interface Runtime {
  readonly syncBackend: SyncBackend.SyncBackend | undefined
  readonly shutdownChannel: Shutdown.ShutdownChannel
  readonly devtoolsLatch: Latch.Latch | undefined
  readonly span: Tracer.Span | undefined
}

interface LocalRequest {
  readonly deferred: Deferred.Deferred<void, RejectedPushError>
  readonly remaining: number
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
  const dbState = yield* StateSqliteDb.StateSqliteDb
  const dbEventlog = yield* EventlogSqliteDb.EventlogSqliteDb
  const syncCommitter = yield* LeaderSyncCommitter.LeaderSyncCommitter
  const { devtoolsLatch, shutdownChannel, span, syncBackend } = runtime
  const syncStateRef = yield* SubscriptionRef.make(initialSyncState)
  const connectedSessions = yield* makePullQueueSet
  const machineDeferred = yield* Deferred.make<MachineRuntime.Runtime>()
  const bootDeferred = yield* Deferred.make<EventSequenceNumber.Client.Composite>()
  const stoppedDeferred = yield* Deferred.make<void>()
  const nextLocalRequestId = yield* Ref.make(1)
  const nextPullBatchId = yield* Ref.make(1)
  const localRequests = yield* Ref.make(new Map<Machine.LocalRequestId, LocalRequest>())
  const pullBatches = yield* Ref.make(new Map<Machine.PullBatchId, Deferred.Deferred<void>>())
  const bootStarted = yield* Ref.make(false)

  const isClientOnlyEvent = (event: LiveStoreEvent.Client.EncodedWithMeta) =>
    schema.eventsDefsMap.get(event.name)?.options.clientOnly ?? false

  const push: Service['push'] = (events) =>
    Effect.gen(function* () {
      if (events.length === 0) return
      const requestId = yield* Ref.modify(nextLocalRequestId, (id) => [id, id + 1])
      const deferred = yield* Deferred.make<void, RejectedPushError>()
      yield* Ref.update(localRequests, (requests) =>
        new Map(requests).set(requestId, { deferred, remaining: events.length }),
      )
      const machine = yield* Deferred.await(machineDeferred)
      yield* machine.send({ _tag: 'LocalPushRequested', requestId, events })
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
    const pushHandle = yield* FiberHandle.make<void, never>()
    const pullHandle = yield* FiberHandle.make<void, never>()
    const commandFibers = yield* FiberSet.make<void, never>()
    const machine = yield* MachineRuntime.make({
      initialState: Machine.initial(
        {
          backendEnabled: syncBackend !== undefined,
          livePull,
          localCommitBatchSize: params.localPushBatchSize ?? 10,
          backendPushBatchSize: params.backendPushBatchSize ?? 50,
          onError,
          onBackendIdMismatch,
          localWorkInitiallyBlocked: testing.delays?.localPushProcessing !== undefined,
        },
        initialSyncState,
      ),
      transition: Machine.makeTransition({ isClientOnlyEvent }),
      execute: makeCommandExecutor({
        schema,
        syncBackend,
        devtoolsLatch,
        dbState,
        dbEventlog,
        syncCommitter,
        syncStateRef,
        connectedSessions,
        localRequests,
        pullBatches,
        nextPullBatchId,
        bootDeferred,
        stoppedDeferred,
        pushHandle,
        pullHandle,
        commandFibers,
        shutdownChannel,
        initialBlockingSyncContext,
        testing,
        isClientOnlyEvent,
      }),
    })
    yield* Deferred.succeed(machineDeferred, machine)
    yield* machine.run.pipe(Effect.forkScoped)
    yield* machine.send({ _tag: 'Start' })
    yield* Effect.addFinalizer(() =>
      machine
        .send({ _tag: 'ShutdownRequested', reason: 'scope-closed' })
        .pipe(Effect.ignore, Effect.andThen(Deferred.await(stoppedDeferred))),
    )
    return { initialLeaderHead: yield* Deferred.await(bootDeferred) }
  }).pipe(Effect.withSpanScoped('@livestore/common:LeaderSyncProcessor:boot'))

  return LeaderSyncProcessor.of({
    [TypeId]: TypeId,
    boot,
    push,
    pushPartial: ({ event: { name, args }, clientId, sessionId }) =>
      Effect.gen(function* () {
        const syncState = yield* SubscriptionRef.get(syncStateRef)
        const resolution = yield* resolveEventDef(schema, {
          operation: '@livestore/common:LeaderSyncProcessor:pushPartial',
          event: { name, args, clientId, sessionId, seqNum: syncState.localHead },
        })
        if (resolution._tag === 'unknown') return
        const event = new LiveStoreEvent.Client.EncodedWithMeta({
          name,
          args,
          clientId,
          sessionId,
          ...EventSequenceNumber.Client.nextPair({
            seqNum: syncState.localHead,
            isClientOnly: resolution.eventDef.options.clientOnly,
          }),
        })
        yield* push([event])
      }).pipe(Effect.catchIf(isRejectedPushError, Effect.die)),
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

const makeCommandExecutor = ({
  schema,
  syncBackend,
  devtoolsLatch,
  dbState,
  dbEventlog,
  syncCommitter,
  syncStateRef,
  connectedSessions,
  localRequests,
  pullBatches,
  nextPullBatchId,
  bootDeferred,
  stoppedDeferred,
  pushHandle,
  pullHandle,
  commandFibers,
  shutdownChannel,
  initialBlockingSyncContext,
  testing,
  isClientOnlyEvent,
}: {
  schema: LiveStoreSchema
  syncBackend: SyncBackend.SyncBackend | undefined
  devtoolsLatch: Latch.Latch | undefined
  dbState: SqliteDb
  dbEventlog: SqliteDb
  syncCommitter: LeaderSyncCommitter.Service
  syncStateRef: SubscriptionRef.SubscriptionRef<SyncState.SyncState>
  connectedSessions: PullQueueSet
  localRequests: Ref.Ref<Map<Machine.LocalRequestId, LocalRequest>>
  pullBatches: Ref.Ref<Map<Machine.PullBatchId, Deferred.Deferred<void>>>
  nextPullBatchId: Ref.Ref<number>
  bootDeferred: Deferred.Deferred<EventSequenceNumber.Client.Composite>
  stoppedDeferred: Deferred.Deferred<void>
  pushHandle: FiberHandle.FiberHandle<void, never>
  pullHandle: FiberHandle.FiberHandle<void, never>
  commandFibers: FiberSet.FiberSet<void, never>
  shutdownChannel: Shutdown.ShutdownChannel
  initialBlockingSyncContext: InitialBlockingSyncContext
  testing: Options['testing']
  isClientOnlyEvent: (event: LiveStoreEvent.Client.EncodedWithMeta) => boolean
}) => {
  const fork = (effect: Effect.Effect<void>) => FiberSet.run(commandFibers, effect).pipe(Effect.asVoid)

  const execute = (
    command: Machine.Command,
    send: MachineRuntime.Runtime['send'],
  ): Effect.Effect<void, never, Scope.Scope> => {
    switch (command._tag) {
      case 'CompleteBoot':
        return Deferred.succeed(bootDeferred, command.initialLeaderHead).pipe(Effect.asVoid)
      case 'AwaitLocalWorkGate':
        return fork(
          (testing.delays?.localPushProcessing ?? Effect.void).pipe(Effect.andThen(send({ _tag: 'LocalWorkEnabled' }))),
        )
      case 'SetObservableSyncState':
        return SubscriptionRef.set(syncStateRef, command.syncState)
      case 'NotifyLocalPushAdmitted':
        return testing.hooks?.localPushAdmitted?.(command.events) ?? Effect.void
      case 'PlanLocal':
        return planLocal(command, send, isClientOnlyEvent)
      case 'CommitLocal':
        return fork(
          syncCommitter.commitLocal({ events: command.plan.events }).pipe(
            Effect.matchEffect({
              onFailure: (error) => send({ _tag: 'LocalCommitFailed', operationId: command.operationId, error }),
              onSuccess: (receipt) => send({ _tag: 'LocalCommitSucceeded', operationId: command.operationId, receipt }),
            }),
            Effect.catchCause((cause) =>
              send({ _tag: 'LocalCommitDefected', operationId: command.operationId, cause }),
            ),
          ),
        )
      case 'PlanUpstream':
        return fork(planUpstream(command, send, isClientOnlyEvent))
      case 'CommitUpstream':
        return fork(
          syncCommitter
            .commitUpstream({
              pulledEvents: command.plan.batch.events,
              events: command.plan.events,
              rollbackEvents: command.plan.rollbackEvents,
              confirmedEvents: command.plan.confirmedEvents,
              backendHead: command.plan.batch.events.at(-1)!.seqNum,
            })
            .pipe(
              Effect.matchEffect({
                onFailure: (error) => send({ _tag: 'UpstreamCommitFailed', operationId: command.operationId, error }),
                onSuccess: (receipt) =>
                  send({ _tag: 'UpstreamCommitSucceeded', operationId: command.operationId, receipt }),
              }),
              Effect.catchCause((cause) =>
                send({ _tag: 'UpstreamCommitDefected', operationId: command.operationId, cause }),
              ),
            ),
        )
      case 'PublishSessions':
        return connectedSessions.offer(command)
      case 'CompleteLocalItems':
        return completeLocalItems(localRequests, command.items)
      case 'RejectLocalItems':
        return rejectLocalItems(localRequests, command.items)
      case 'InterruptLocalRequests':
        return interruptLocalRequests(localRequests, command.requestIds)
      case 'StartProviderPull':
        return syncBackend === undefined
          ? Effect.void
          : FiberHandle.run(
              pullHandle,
              runProviderPull({
                command,
                syncBackend,
                devtoolsLatch,
                schema,
                dbState,
                dbEventlog,
                pullBatches,
                nextPullBatchId,
                initialBlockingSyncContext,
                send,
              }),
            ).pipe(Effect.asVoid)
      case 'CompletePullBatch':
        return completePullBatch(pullBatches, command.batch.batchId)
      case 'SchedulePullRetry':
        return fork(
          Effect.sleep(Duration.millis(command.delayMs)).pipe(
            Effect.andThen(send({ _tag: 'PullRetryElapsed', retryId: command.retryId })),
          ),
        )
      case 'StartProviderPush':
        return syncBackend === undefined
          ? Effect.void
          : FiberHandle.run(pushHandle, runProviderPush(command, syncBackend, devtoolsLatch, send)).pipe(Effect.asVoid)
      case 'CancelProviderPush':
        return FiberHandle.clear(pushHandle).pipe(
          Effect.andThen(send({ _tag: 'PushCancelled', operationId: command.operationId })),
        )
      case 'SchedulePushRetry':
        return fork(
          Effect.sleep(Duration.millis(command.delayMs)).pipe(
            Effect.andThen(send({ _tag: 'PushRetryElapsed', retryId: command.retryId })),
          ),
        )
      case 'ResetDatabases':
        return fork(
          clearLocalDatabases({ dbEventlog, dbState }).pipe(
            Effect.matchEffect({
              onFailure: (cause) => send({ _tag: 'BackendResetFailed', operationId: command.operationId, cause }),
              onSuccess: () => send({ _tag: 'BackendResetSucceeded', operationId: command.operationId }),
            }),
          ),
        )
      case 'SendShutdown':
        return shutdownChannel
          .send(
            Schema.is(Shutdown.All)(command.error) === true
              ? command.error
              : UnknownError.make({ cause: command.error, note: 'Leader sync machine failed' }),
          )
          .pipe(Effect.orDie)
      case 'CancelProviderOperations':
        return Effect.all([FiberHandle.clear(pushHandle), FiberHandle.clear(pullHandle)]).pipe(Effect.asVoid)
      case 'StopRuntime':
        return Effect.all([
          FiberHandle.clear(pushHandle),
          FiberHandle.clear(pullHandle),
          FiberSet.clear(commandFibers),
          interruptAllLocalRequests(localRequests),
          interruptAllPullBatches(pullBatches),
          Deferred.succeed(stoppedDeferred, void 0),
        ]).pipe(Effect.asVoid)
      default:
        return casesHandled(command)
    }
  }
  return execute
}

const planLocal = (
  command: Extract<Machine.Command, { _tag: 'PlanLocal' }>,
  send: MachineRuntime.Runtime['send'],
  isClientOnlyEvent: (event: LiveStoreEvent.Client.EncodedWithMeta) => boolean,
) =>
  Effect.gen(function* () {
    const merge = yield* SyncState.merge({
      syncState: command.syncState,
      payload: { _tag: 'local-push', newEvents: command.items.map((item) => item.event) },
      isClientOnlyEvent,
      isEqualEvent: LiveStoreEvent.Client.isEqualEncoded,
    })
    if (merge._tag === 'reject') {
      return yield* send({
        _tag: 'LocalPlanRejected',
        operationId: command.operationId,
        minimumExpectedNum: merge.expectedMinimumId,
      })
    }
    if (merge._tag === 'rebase') {
      return yield* send({
        _tag: 'LocalPlanningFailed',
        operationId: command.operationId,
        cause: new Error('Local push required rebase'),
      })
    }
    const events = merge.newSyncState.pending.slice(command.syncState.pending.length)
    if (events.length !== merge.newEvents.length) {
      return yield* send({
        _tag: 'LocalPlanningFailed',
        operationId: command.operationId,
        cause: new Error('Local push was not retained as pending'),
      })
    }
    yield* send({
      _tag: 'LocalPlanned',
      operationId: command.operationId,
      plan: { items: command.items, proposedSyncState: merge.newSyncState, events },
    })
  }).pipe(Effect.catchCause((cause) => send({ _tag: 'LocalPlanningFailed', operationId: command.operationId, cause })))

const planUpstream = (
  command: Extract<Machine.Command, { _tag: 'PlanUpstream' }>,
  send: MachineRuntime.Runtime['send'],
  isClientOnlyEvent: (event: LiveStoreEvent.Client.EncodedWithMeta) => boolean,
) =>
  SyncState.merge({
    syncState: command.syncState,
    payload: SyncState.PayloadUpstreamAdvance.make({ newEvents: command.batch.events }),
    isClientOnlyEvent,
    isEqualEvent: LiveStoreEvent.Client.isEqualEncoded,
    ignoreClientOnlyEvents: true,
  }).pipe(
    Effect.flatMap((merge) =>
      merge._tag === 'reject'
        ? send({
            _tag: 'UpstreamPlanningFailed',
            operationId: command.operationId,
            cause: new Error('Upstream batch rejected'),
          })
        : send({
            _tag: 'UpstreamPlanned',
            operationId: command.operationId,
            plan: {
              batch: command.batch,
              proposedSyncState: merge.newSyncState,
              mergeTag: merge._tag,
              events: merge.newEvents,
              rollbackEvents: merge._tag === 'rebase' ? merge.rollbackEvents : [],
              confirmedEvents: merge._tag === 'advance' ? merge.confirmedEvents : [],
            },
          }),
    ),
    Effect.catchCause((cause) => send({ _tag: 'UpstreamPlanningFailed', operationId: command.operationId, cause })),
  )

const runProviderPush = (
  command: Extract<Machine.Command, { _tag: 'StartProviderPush' }>,
  syncBackend: SyncBackend.SyncBackend,
  devtoolsLatch: Latch.Latch | undefined,
  send: MachineRuntime.Runtime['send'],
) =>
  Effect.gen(function* () {
    yield* SubscriptionRef.waitUntil(syncBackend.isConnected, (connected) => connected === true)
    if (devtoolsLatch !== undefined) yield* devtoolsLatch.await
    yield* syncBackend.push(command.batch.map((event) => event.toGlobal()))
  }).pipe(
    Effect.matchEffect({
      onFailure: (error) => send({ _tag: 'PushFailed', operationId: command.operationId, error }),
      onSuccess: () => send({ _tag: 'PushSucceeded', operationId: command.operationId }),
    }),
    Effect.interruptible,
  )

const runProviderPull = ({
  command,
  syncBackend,
  devtoolsLatch,
  schema,
  dbState,
  dbEventlog,
  pullBatches,
  nextPullBatchId,
  initialBlockingSyncContext,
  send,
}: {
  command: Extract<Machine.Command, { _tag: 'StartProviderPull' }>
  syncBackend: SyncBackend.SyncBackend
  devtoolsLatch: Latch.Latch | undefined
  schema: LiveStoreSchema
  dbState: SqliteDb
  dbEventlog: SqliteDb
  pullBatches: Ref.Ref<Map<Machine.PullBatchId, Deferred.Deferred<void>>>
  nextPullBatchId: Ref.Ref<number>
  initialBlockingSyncContext: InitialBlockingSyncContext
  send: MachineRuntime.Runtime['send']
}) =>
  Effect.gen(function* () {
    const cursorInfo = yield* Eventlog.getSyncBackendCursorInfoForDb(dbEventlog, { remoteHead: command.cursor.global })
    const hashMaterializer = makeMaterializerHash({ schema, dbState })
    yield* syncBackend.pull(cursorInfo, { live: command.live }).pipe(
      Stream.runForEach(({ batch, pageInfo }) =>
        Effect.gen(function* () {
          yield* SubscriptionRef.waitUntil(syncBackend.isConnected, (connected) => connected === true)
          if (devtoolsLatch !== undefined) yield* devtoolsLatch.await
          const batchId = yield* Ref.modify(nextPullBatchId, (id) => [id, id + 1])
          const completion = yield* Deferred.make<void>()
          yield* Ref.update(pullBatches, (batches) => new Map(batches).set(batchId, completion))
          const events = batch.map((item) =>
            LiveStoreEvent.Client.EncodedWithMeta.fromGlobal(item.eventEncoded, {
              syncMetadata: item.metadata,
              materializerHashLeader: hashMaterializer(LiveStoreEvent.Global.toClientEncoded(item.eventEncoded)),
              materializerHashSession: Option.none(),
            }),
          )
          yield* send({ _tag: 'UpstreamBatchReceived', batch: { pullId: command.pullId, batchId, events, pageInfo } })
          yield* Deferred.await(completion)
          yield* initialBlockingSyncContext.update({ processed: batch.length, pageInfo })
          // A completed page is a run-to-completion boundary. Yielding here lets already-admitted local work and
          // provider completions enter the mailbox before a hot live stream produces its next page.
          yield* Effect.yieldNow
        }),
      ),
    )
  }).pipe(
    Effect.matchEffect({
      onFailure: (error) => send({ _tag: 'PullFailed', pullId: command.pullId, error }),
      onSuccess: () => Effect.void,
    }),
    // Stream interruption is not a typed provider failure. Completion still releases pagination priority; a typed
    // failure event, when present, is processed first and can replace this with retry or terminal handling.
    Effect.ensuring(send({ _tag: 'PullCompleted', pullId: command.pullId })),
    Effect.interruptible,
  )

const completeLocalItems = (
  requestsRef: Ref.Ref<Map<Machine.LocalRequestId, LocalRequest>>,
  items: ReadonlyArray<Machine.LocalItem>,
) =>
  Effect.gen(function* () {
    const counts = countRequestItems(items)
    const completions: Deferred.Deferred<void, RejectedPushError>[] = []
    yield* Ref.update(requestsRef, (current) => {
      const next = new Map(current)
      for (const [requestId, count] of counts) {
        const request = next.get(requestId)
        if (request === undefined) continue
        const remaining = request.remaining - count
        if (remaining <= 0) {
          next.delete(requestId)
          completions.push(request.deferred)
        } else next.set(requestId, { ...request, remaining })
      }
      return next
    })
    yield* Effect.forEach(completions, (deferred) => Deferred.succeed(deferred, void 0), { discard: true })
  })

const rejectLocalItems = (
  requestsRef: Ref.Ref<Map<Machine.LocalRequestId, LocalRequest>>,
  items: ReadonlyArray<{ readonly item: Machine.LocalItem; readonly error: RejectedPushError }>,
) =>
  Effect.gen(function* () {
    const failures: Array<{ deferred: Deferred.Deferred<void, RejectedPushError>; error: RejectedPushError }> = []
    yield* Ref.update(requestsRef, (current) => {
      const next = new Map(current)
      for (const { item, error } of items) {
        const request = next.get(item.requestId)
        if (request === undefined) continue
        next.delete(item.requestId)
        failures.push({ deferred: request.deferred, error })
      }
      return next
    })
    yield* Effect.forEach(failures, ({ deferred, error }) => Deferred.fail(deferred, error), { discard: true })
  })

const interruptLocalRequests = (
  requestsRef: Ref.Ref<Map<Machine.LocalRequestId, LocalRequest>>,
  requestIds: ReadonlyArray<Machine.LocalRequestId>,
) =>
  Effect.gen(function* () {
    const deferreds: Deferred.Deferred<void, RejectedPushError>[] = []
    yield* Ref.update(requestsRef, (current) => {
      const next = new Map(current)
      for (const requestId of new Set(requestIds)) {
        const request = next.get(requestId)
        if (request !== undefined) deferreds.push(request.deferred)
        next.delete(requestId)
      }
      return next
    })
    yield* Effect.forEach(deferreds, Deferred.interrupt, { discard: true })
  })

const interruptAllLocalRequests = (requestsRef: Ref.Ref<Map<Machine.LocalRequestId, LocalRequest>>) =>
  Effect.gen(function* () {
    const requests = yield* Ref.getAndSet(requestsRef, new Map())
    yield* Effect.forEach(requests.values(), ({ deferred }) => Deferred.interrupt(deferred), { discard: true })
  })

const interruptAllPullBatches = (batchesRef: Ref.Ref<Map<Machine.PullBatchId, Deferred.Deferred<void>>>) =>
  Effect.gen(function* () {
    const batches = yield* Ref.getAndSet(batchesRef, new Map())
    yield* Effect.forEach(batches.values(), Deferred.interrupt, { discard: true })
  })

const completePullBatch = (
  batchesRef: Ref.Ref<Map<Machine.PullBatchId, Deferred.Deferred<void>>>,
  batchId: Machine.PullBatchId,
) =>
  Effect.gen(function* () {
    let completion: Deferred.Deferred<void> | undefined
    yield* Ref.update(batchesRef, (current) => {
      const next = new Map(current)
      completion = next.get(batchId)
      next.delete(batchId)
      return next
    })
    if (completion !== undefined) yield* Deferred.succeed(completion, void 0)
  })

const countRequestItems = (items: ReadonlyArray<Machine.LocalItem>) => {
  const counts = new Map<Machine.LocalRequestId, number>()
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
      const pullItem = PullItem.make({ payload: item.payload, globalHead: item.globalHead })
      const cached = cachedPullItems.get(key)
      if (cached === undefined) cachedPullItems.set(key, [pullItem])
      else cached.push(pullItem)
      for (const queue of set) yield* Queue.offer(queue, pullItem)
    })
  return { makeQueue, offer }
})

const clearLocalDatabases = ({ dbEventlog, dbState }: { dbEventlog: SqliteDb; dbState: SqliteDb }) =>
  Effect.try({
    try: () => {
      dbEventlog.execute(sql`DELETE FROM ${EVENTLOG_META_TABLE}`)
      dbEventlog.execute(sql`DELETE FROM ${SYNC_STATUS_TABLE}`)
      const tables = dbState.select<{ name: string }>(
        sql`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`,
      )
      for (const { name } of tables) dbState.execute(`DROP TABLE IF EXISTS "${name}"`)
    },
    catch: (cause) => UnknownError.make({ cause, note: 'Failed to reset local databases after backend mismatch' }),
  })
