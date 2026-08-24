import { casesHandled } from '@livestore/utils'

import { IntentionalShutdownCause, type UnknownError } from '../errors.ts'
import type { MaterializeError } from '../errors.ts'
import type * as MaterializationJournal from '../MaterializationJournal.ts'
import type { LiveStoreEvent } from '../schema/mod.ts'
import { EventSequenceNumber } from '../schema/mod.ts'
import type { BackendIdMismatchError, IsOfflineError, ServerAheadError } from '../sync/errors.ts'
import type * as SyncBackend from '../sync/sync-backend.ts'
import * as SyncState from '../sync/syncstate.ts'
import type * as LeaderSyncCommitter from './LeaderSyncCommitter.ts'
import {
  LeaderAheadError,
  NonContiguousBatchError,
  NonMonotonicBatchError,
  type RejectedPushError,
  StaleRebaseGenerationError,
} from './RejectedPushError.ts'

export type OperationId = number
export type LocalRequestId = number
export type PullBatchId = number

export interface Config {
  readonly backendEnabled: boolean
  readonly livePull: boolean
  readonly localCommitBatchSize: number
  readonly backendPushBatchSize: number
  readonly onError: 'shutdown' | 'ignore'
  readonly onBackendIdMismatch: 'reset' | 'shutdown' | 'ignore'
  readonly localWorkInitiallyBlocked: boolean
}

export interface LocalItem {
  readonly requestId: LocalRequestId
  readonly index: number
  readonly event: LiveStoreEvent.Client.EncodedWithMeta
}

export interface UpstreamBatch {
  readonly pullId: OperationId
  readonly batchId: PullBatchId
  readonly events: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
  readonly pageInfo: SyncBackend.PullResPageInfo
}

export interface LocalCommitPlan {
  readonly items: ReadonlyArray<LocalItem>
  readonly proposedSyncState: SyncState.SyncState
  readonly events: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
}

export interface UpstreamCommitPlan {
  readonly batch: UpstreamBatch
  readonly proposedSyncState: SyncState.SyncState
  readonly mergeTag: 'advance' | 'rebase'
  readonly events: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
  readonly rollbackEvents: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
  readonly confirmedEvents: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
}

export type WorkState =
  | { readonly _tag: 'idle' }
  | { readonly _tag: 'planning-local'; readonly operationId: OperationId; readonly items: ReadonlyArray<LocalItem> }
  | { readonly _tag: 'committing-local'; readonly operationId: OperationId; readonly plan: LocalCommitPlan }
  | { readonly _tag: 'planning-upstream'; readonly operationId: OperationId; readonly batch: UpstreamBatch }
  | { readonly _tag: 'committing-upstream'; readonly operationId: OperationId; readonly plan: UpstreamCommitPlan }

export type PullState =
  | { readonly _tag: 'disabled' }
  | {
      readonly _tag: 'streaming'
      readonly pullId: OperationId
      readonly pagination: 'between-pages' | 'more-expected'
      readonly attempt: number
    }
  | { readonly _tag: 'retry-wait'; readonly retryId: OperationId; readonly attempt: number }
  | { readonly _tag: 'completed' }

export type PushState =
  | { readonly _tag: 'disabled' }
  | {
      readonly _tag: 'idle'
      readonly generation: number
      readonly queued: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
    }
  | {
      readonly _tag: 'in-flight'
      readonly operationId: OperationId
      readonly generation: number
      readonly attempt: number
      readonly batch: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
      readonly queued: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
    }
  | {
      readonly _tag: 'retry-wait'
      readonly retryId: OperationId
      readonly generation: number
      readonly attempt: number
      readonly batch: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
      readonly queued: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
    }
  | {
      readonly _tag: 'awaiting-pull'
      readonly generation: number
      readonly queued: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
    }
  | {
      readonly _tag: 'cancelling'
      readonly operationId: OperationId
      readonly generation: number
      readonly replacement: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
    }

export interface RunningState {
  readonly _tag: 'running'
  readonly config: Config
  readonly syncState: SyncState.SyncState
  readonly work: WorkState
  readonly pull: PullState
  readonly push: PushState
  readonly localQueue: ReadonlyArray<LocalItem>
  readonly reservations: ReadonlyArray<LocalItem>
  readonly upstreamQueue: ReadonlyArray<UpstreamBatch>
  readonly localWorkEnabled: boolean
  readonly nextOperationId: OperationId
}

export type State =
  | {
      readonly _tag: 'starting'
      readonly config: Config
      readonly initialSyncState: SyncState.SyncState
      readonly nextOperationId: OperationId
    }
  | RunningState
  | {
      readonly _tag: 'resetting'
      readonly syncState: SyncState.SyncState
      readonly operationId: OperationId
      readonly backendMismatch: BackendIdMismatchError
    }
  | {
      readonly _tag: 'stopping'
      readonly syncState: SyncState.SyncState
      readonly reason: string
    }
  | {
      readonly _tag: 'failed'
      readonly syncState: SyncState.SyncState
      readonly failure: Failure
      readonly shutdownSent: boolean
    }

export interface Failure {
  readonly _tag: string
  readonly cause: unknown
}

export type ProviderPushError = IsOfflineError | BackendIdMismatchError | UnknownError | ServerAheadError
export type ProviderPullError = IsOfflineError | BackendIdMismatchError | UnknownError
export type CommitError = MaterializeError | MaterializationJournal.MaterializationJournalError

export type Event =
  | { readonly _tag: 'Start' }
  | { readonly _tag: 'LocalWorkEnabled' }
  | {
      readonly _tag: 'LocalPushRequested'
      readonly requestId: LocalRequestId
      readonly events: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
    }
  | { readonly _tag: 'LocalPlanned'; readonly operationId: OperationId; readonly plan: LocalCommitPlan }
  | {
      readonly _tag: 'LocalPlanRejected'
      readonly operationId: OperationId
      readonly minimumExpectedNum: EventSequenceNumber.Client.Composite
    }
  | { readonly _tag: 'LocalPlanningFailed'; readonly operationId: OperationId; readonly cause: unknown }
  | {
      readonly _tag: 'LocalCommitSucceeded'
      readonly operationId: OperationId
      readonly receipt: LeaderSyncCommitter.LocalCommitReceipt
    }
  | { readonly _tag: 'LocalCommitFailed'; readonly operationId: OperationId; readonly error: CommitError }
  | { readonly _tag: 'UpstreamBatchReceived'; readonly batch: UpstreamBatch }
  | { readonly _tag: 'UpstreamPlanned'; readonly operationId: OperationId; readonly plan: UpstreamCommitPlan }
  | { readonly _tag: 'UpstreamPlanningFailed'; readonly operationId: OperationId; readonly cause: unknown }
  | {
      readonly _tag: 'UpstreamCommitSucceeded'
      readonly operationId: OperationId
      readonly receipt: LeaderSyncCommitter.UpstreamCommitReceipt
    }
  | { readonly _tag: 'UpstreamCommitFailed'; readonly operationId: OperationId; readonly error: CommitError }
  | { readonly _tag: 'PullCompleted'; readonly pullId: OperationId }
  | { readonly _tag: 'PullFailed'; readonly pullId: OperationId; readonly error: ProviderPullError }
  | { readonly _tag: 'PullRetryElapsed'; readonly retryId: OperationId }
  | { readonly _tag: 'PushSucceeded'; readonly operationId: OperationId }
  | { readonly _tag: 'PushFailed'; readonly operationId: OperationId; readonly error: ProviderPushError }
  | { readonly _tag: 'PushRetryElapsed'; readonly retryId: OperationId }
  | { readonly _tag: 'PushCancelled'; readonly operationId: OperationId }
  | { readonly _tag: 'BackendResetSucceeded'; readonly operationId: OperationId }
  | { readonly _tag: 'BackendResetFailed'; readonly operationId: OperationId; readonly cause: unknown }
  | { readonly _tag: 'ShutdownRequested'; readonly reason: string }

export type Command =
  | { readonly _tag: 'CompleteBoot'; readonly initialLeaderHead: EventSequenceNumber.Client.Composite }
  | { readonly _tag: 'AwaitLocalWorkGate' }
  | { readonly _tag: 'SetObservableSyncState'; readonly syncState: SyncState.SyncState }
  | { readonly _tag: 'NotifyLocalPushAdmitted'; readonly events: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta> }
  | {
      readonly _tag: 'PlanLocal'
      readonly operationId: OperationId
      readonly syncState: SyncState.SyncState
      readonly items: ReadonlyArray<LocalItem>
    }
  | { readonly _tag: 'CommitLocal'; readonly operationId: OperationId; readonly plan: LocalCommitPlan }
  | {
      readonly _tag: 'PlanUpstream'
      readonly operationId: OperationId
      readonly syncState: SyncState.SyncState
      readonly batch: UpstreamBatch
    }
  | { readonly _tag: 'CommitUpstream'; readonly operationId: OperationId; readonly plan: UpstreamCommitPlan }
  | {
      readonly _tag: 'PublishSessions'
      readonly payload: typeof SyncState.PayloadUpstream.Type
      readonly globalHead: EventSequenceNumber.Client.Composite
      readonly leaderHead: EventSequenceNumber.Client.Composite
    }
  | { readonly _tag: 'CompleteLocalItems'; readonly items: ReadonlyArray<LocalItem> }
  | {
      readonly _tag: 'RejectLocalItems'
      readonly items: ReadonlyArray<{ readonly item: LocalItem; readonly error: RejectedPushError }>
    }
  | { readonly _tag: 'InterruptLocalRequests'; readonly requestIds: ReadonlyArray<LocalRequestId> }
  | {
      readonly _tag: 'StartProviderPull'
      readonly pullId: OperationId
      readonly cursor: EventSequenceNumber.Client.Composite
      readonly live: boolean
    }
  | { readonly _tag: 'CompletePullBatch'; readonly batch: UpstreamBatch }
  | { readonly _tag: 'SchedulePullRetry'; readonly retryId: OperationId; readonly delayMs: number }
  | {
      readonly _tag: 'StartProviderPush'
      readonly operationId: OperationId
      readonly batch: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
    }
  | { readonly _tag: 'CancelProviderPush'; readonly operationId: OperationId }
  | { readonly _tag: 'SchedulePushRetry'; readonly retryId: OperationId; readonly delayMs: number }
  | { readonly _tag: 'ResetDatabases'; readonly operationId: OperationId; readonly error: BackendIdMismatchError }
  | { readonly _tag: 'SendShutdown'; readonly error: unknown }
  | { readonly _tag: 'StopRuntime'; readonly reason: string }

export interface TransitionResult {
  readonly state: State
  readonly commands: ReadonlyArray<Command>
}

export const initial = (config: Config, initialSyncState: SyncState.SyncState): State => ({
  _tag: 'starting',
  config,
  initialSyncState,
  nextOperationId: 1,
})

export const makeTransition = ({
  isClientOnlyEvent,
}: {
  isClientOnlyEvent: (event: LiveStoreEvent.Client.EncodedWithMeta) => boolean
}) => {
  const transition = (state: State, event: Event): TransitionResult => {
    switch (state._tag) {
      case 'starting':
        return transitionStarting(state, event, isClientOnlyEvent)
      case 'running':
        return transitionRunning(state, event, isClientOnlyEvent)
      case 'resetting':
        return transitionResetting(state, event)
      case 'stopping':
        return transitionTerminal(state, event)
      case 'failed':
        return transitionTerminal(state, event)
      default:
        return casesHandled(state)
    }
  }
  return transition
}

const transitionStarting = (
  state: Extract<State, { _tag: 'starting' }>,
  event: Event,
  isClientOnlyEvent: (event: LiveStoreEvent.Client.EncodedWithMeta) => boolean,
): TransitionResult => {
  if (event._tag === 'LocalPushRequested') {
    return { state, commands: [{ _tag: 'InterruptLocalRequests', requestIds: [event.requestId] }] }
  }
  if (event._tag === 'ShutdownRequested') {
    return {
      state: { _tag: 'stopping', syncState: state.initialSyncState, reason: event.reason },
      commands: [{ _tag: 'StopRuntime', reason: event.reason }],
    }
  }
  if (event._tag !== 'Start') return { state, commands: [] }

  const pullId = state.nextOperationId
  const initialPending = state.initialSyncState.pending.filter((event) => !isClientOnlyEvent(event))
  const running: RunningState = {
    _tag: 'running',
    config: state.config,
    syncState: state.initialSyncState,
    work: { _tag: 'idle' },
    pull:
      state.config.backendEnabled === true
        ? { _tag: 'streaming', pullId, pagination: 'between-pages', attempt: 0 }
        : { _tag: 'disabled' },
    push:
      state.config.backendEnabled === true
        ? { _tag: 'idle', generation: 0, queued: initialPending }
        : { _tag: 'disabled' },
    localQueue: [],
    reservations: [],
    upstreamQueue: [],
    localWorkEnabled: state.config.localWorkInitiallyBlocked === false,
    nextOperationId: state.config.backendEnabled === true ? pullId + 1 : pullId,
  }

  const commands: Command[] = [
    { _tag: 'SetObservableSyncState', syncState: state.initialSyncState },
    { _tag: 'CompleteBoot', initialLeaderHead: state.initialSyncState.localHead },
  ]
  if (running.pull._tag === 'streaming') {
    commands.push({
      _tag: 'StartProviderPull',
      pullId: running.pull.pullId,
      cursor: state.initialSyncState.upstreamHead,
      live: state.config.livePull,
    })
  }
  if (state.config.localWorkInitiallyBlocked === true) commands.push({ _tag: 'AwaitLocalWorkGate' })
  return schedulePush(running, commands)
}

const transitionRunning = (
  state: RunningState,
  event: Event,
  isClientOnlyEvent: (event: LiveStoreEvent.Client.EncodedWithMeta) => boolean,
): TransitionResult => {
  if (event._tag === 'ShutdownRequested') {
    const requestIds = [...new Set(state.reservations.map((item) => item.requestId))]
    return {
      state: { _tag: 'stopping', syncState: state.syncState, reason: event.reason },
      commands: [
        { _tag: 'InterruptLocalRequests', requestIds },
        { _tag: 'StopRuntime', reason: event.reason },
      ],
    }
  }

  switch (event._tag) {
    case 'LocalPushRequested':
      return onLocalPushRequested(state, event, isClientOnlyEvent)
    case 'LocalWorkEnabled':
      return scheduleNextWork({ ...state, localWorkEnabled: true })
    case 'LocalPlanned':
      return onLocalPlanned(state, event)
    case 'LocalPlanRejected':
      return onLocalPlanRejected(state, event)
    case 'LocalPlanningFailed':
      return failOperation(state, event.operationId, { _tag: 'LocalPlanningFailed', cause: event.cause })
    case 'LocalCommitSucceeded':
      return onLocalCommitSucceeded(state, event, isClientOnlyEvent)
    case 'LocalCommitFailed':
      return failOperation(state, event.operationId, { _tag: event.error._tag, cause: event.error })
    case 'UpstreamBatchReceived':
      return onUpstreamBatchReceived(state, event)
    case 'UpstreamPlanned':
      return onUpstreamPlanned(state, event)
    case 'UpstreamPlanningFailed':
      return failOperation(state, event.operationId, { _tag: 'UpstreamPlanningFailed', cause: event.cause })
    case 'UpstreamCommitSucceeded':
      return onUpstreamCommitSucceeded(state, event, isClientOnlyEvent)
    case 'UpstreamCommitFailed':
      return failOperation(state, event.operationId, { _tag: event.error._tag, cause: event.error })
    case 'PullCompleted':
      return onPullCompleted(state, event)
    case 'PullFailed':
      return onPullFailed(state, event)
    case 'PullRetryElapsed':
      return onPullRetryElapsed(state, event)
    case 'PushSucceeded':
      return onPushSucceeded(state, event)
    case 'PushFailed':
      return onPushFailed(state, event)
    case 'PushRetryElapsed':
      return onPushRetryElapsed(state, event)
    case 'PushCancelled':
      return onPushCancelled(state, event)
    case 'BackendResetSucceeded':
    case 'BackendResetFailed':
      return { state, commands: [] }
    case 'Start':
      return { state, commands: [] }
    default:
      return casesHandled(event)
  }
}

const onLocalPushRequested = (
  state: RunningState,
  event: Extract<Event, { _tag: 'LocalPushRequested' }>,
  isClientOnlyEvent: (event: LiveStoreEvent.Client.EncodedWithMeta) => boolean,
): TransitionResult => {
  const pushHead = state.reservations.at(-1)?.event.seqNum ?? state.syncState.localHead
  const validationError = validatePushBatch(event.events, pushHead, isClientOnlyEvent)
  const items = event.events.map((pushedEvent, index) => ({ requestId: event.requestId, index, event: pushedEvent }))
  if (validationError !== undefined) {
    return {
      state,
      commands: [{ _tag: 'RejectLocalItems', items: items.map((item) => ({ item, error: validationError })) }],
    }
  }
  return scheduleNextWork(
    {
      ...state,
      localQueue: [...state.localQueue, ...items],
      reservations: [...state.reservations, ...items],
    },
    [{ _tag: 'NotifyLocalPushAdmitted', events: event.events }],
  )
}

const onLocalPlanned = (state: RunningState, event: Extract<Event, { _tag: 'LocalPlanned' }>): TransitionResult => {
  if (state.work._tag !== 'planning-local' || state.work.operationId !== event.operationId)
    return { state, commands: [] }
  return {
    state: { ...state, work: { _tag: 'committing-local', operationId: event.operationId, plan: event.plan } },
    commands: [{ _tag: 'CommitLocal', operationId: event.operationId, plan: event.plan }],
  }
}

const onLocalPlanRejected = (
  state: RunningState,
  event: Extract<Event, { _tag: 'LocalPlanRejected' }>,
): TransitionResult => {
  if (state.work._tag !== 'planning-local' || state.work.operationId !== event.operationId)
    return { state, commands: [] }
  const activeItems = state.work.items
  const generation = activeItems[0]?.event.seqNum.rebaseGeneration
  const queuedSameGeneration = state.localQueue.filter((item) => item.event.seqNum.rebaseGeneration === generation)
  const rejectedItems = [...activeItems, ...queuedSameGeneration]
  const rejectedKeys = new Set(rejectedItems.map(localItemKey))
  const firstEvent = activeItems[0]?.event
  if (firstEvent === undefined) return scheduleNextWork({ ...state, work: { _tag: 'idle' } })
  const error = new LeaderAheadError({
    minimumExpectedNum: event.minimumExpectedNum,
    providedNum: firstEvent.seqNum,
    sessionId: firstEvent.sessionId,
  })
  return scheduleNextWork(
    {
      ...state,
      work: { _tag: 'idle' },
      localQueue: state.localQueue.filter((item) => !rejectedKeys.has(localItemKey(item))),
      reservations: state.reservations.filter((item) => !rejectedKeys.has(localItemKey(item))),
    },
    [{ _tag: 'RejectLocalItems', items: rejectedItems.map((item) => ({ item, error })) }],
  )
}

const onLocalCommitSucceeded = (
  state: RunningState,
  event: Extract<Event, { _tag: 'LocalCommitSucceeded' }>,
  isClientOnlyEvent: (event: LiveStoreEvent.Client.EncodedWithMeta) => boolean,
): TransitionResult => {
  if (state.work._tag !== 'committing-local' || state.work.operationId !== event.operationId)
    return { state, commands: [] }
  const committedSyncState = replacePendingEvents(state.work.plan.proposedSyncState, event.receipt.committedEvents)
  const completedKeys = new Set(state.work.plan.items.map(localItemKey))
  let next: RunningState = {
    ...state,
    syncState: committedSyncState,
    work: { _tag: 'idle' },
    reservations: state.reservations.filter((item) => !completedKeys.has(localItemKey(item))),
  }
  const commands: Command[] = [
    { _tag: 'SetObservableSyncState', syncState: committedSyncState },
    {
      _tag: 'PublishSessions',
      payload: SyncState.PayloadUpstreamAdvance.make({ newEvents: event.receipt.committedEvents }),
      globalHead: committedSyncState.upstreamHead,
      leaderHead: committedSyncState.localHead,
    },
  ]
  next = enqueuePushEvents(
    next,
    event.receipt.committedEvents.filter((item) => !isClientOnlyEvent(item)),
  )
  const pushScheduled = schedulePush(next, commands)
  return scheduleNextWork(pushScheduled.state, [
    ...pushScheduled.commands,
    { _tag: 'CompleteLocalItems', items: state.work.plan.items },
  ])
}

const onUpstreamBatchReceived = (
  state: RunningState,
  event: Extract<Event, { _tag: 'UpstreamBatchReceived' }>,
): TransitionResult => {
  if (state.pull._tag !== 'streaming' || state.pull.pullId !== event.batch.pullId) {
    return { state, commands: [{ _tag: 'CompletePullBatch', batch: event.batch }] }
  }
  const pull: PullState = {
    ...state.pull,
    pagination: event.batch.pageInfo._tag === 'NoMore' ? 'between-pages' : 'more-expected',
  }
  if (event.batch.events.length === 0) {
    return scheduleNextWork({ ...state, pull }, [{ _tag: 'CompletePullBatch', batch: event.batch }])
  }
  return scheduleNextWork({ ...state, pull, upstreamQueue: [...state.upstreamQueue, event.batch] })
}

const onUpstreamPlanned = (
  state: RunningState,
  event: Extract<Event, { _tag: 'UpstreamPlanned' }>,
): TransitionResult => {
  if (state.work._tag !== 'planning-upstream' || state.work.operationId !== event.operationId)
    return { state, commands: [] }
  return {
    state: { ...state, work: { _tag: 'committing-upstream', operationId: event.operationId, plan: event.plan } },
    commands: [{ _tag: 'CommitUpstream', operationId: event.operationId, plan: event.plan }],
  }
}

const onUpstreamCommitSucceeded = (
  state: RunningState,
  event: Extract<Event, { _tag: 'UpstreamCommitSucceeded' }>,
  isClientOnlyEvent: (event: LiveStoreEvent.Client.EncodedWithMeta) => boolean,
): TransitionResult => {
  if (state.work._tag !== 'committing-upstream' || state.work.operationId !== event.operationId)
    return { state, commands: [] }
  const plan = state.work.plan
  const committedSyncState = replacePendingEvents(plan.proposedSyncState, event.receipt.committedEvents)
  const payload =
    plan.mergeTag === 'rebase'
      ? SyncState.PayloadUpstreamRebase.make({
          rollbackEvents: plan.rollbackEvents,
          newEvents: event.receipt.committedEvents,
        })
      : SyncState.PayloadUpstreamAdvance.make({ newEvents: event.receipt.committedEvents })
  const commands: Command[] = [
    { _tag: 'SetObservableSyncState', syncState: committedSyncState },
    {
      _tag: 'PublishSessions',
      payload,
      globalHead: committedSyncState.upstreamHead,
      leaderHead: committedSyncState.localHead,
    },
    { _tag: 'CompletePullBatch', batch: plan.batch },
  ]
  const pendingForPush = committedSyncState.pending.filter((item) => !isClientOnlyEvent(item))
  let next: RunningState = {
    ...state,
    syncState: committedSyncState,
    work: { _tag: 'idle' },
  }
  const replacement = replacePushPlan(next, pendingForPush, commands)
  next = replacement.state
  return scheduleNextWork(next, replacement.commands)
}

const onPullCompleted = (state: RunningState, event: Extract<Event, { _tag: 'PullCompleted' }>): TransitionResult => {
  if (state.pull._tag !== 'streaming' || state.pull.pullId !== event.pullId) return { state, commands: [] }
  return scheduleNextWork({ ...state, pull: { _tag: 'completed' } })
}

const onPullFailed = (state: RunningState, event: Extract<Event, { _tag: 'PullFailed' }>): TransitionResult => {
  if (state.pull._tag !== 'streaming' || state.pull.pullId !== event.pullId) return { state, commands: [] }
  if (event.error._tag === 'IsOfflineError') {
    const retryId = state.nextOperationId
    return scheduleNextWork(
      {
        ...state,
        pull: { _tag: 'retry-wait', retryId, attempt: state.pull.attempt + 1 },
        nextOperationId: retryId + 1,
      },
      [{ _tag: 'SchedulePullRetry', retryId, delayMs: 0 }],
    )
  }
  if (event.error._tag === 'BackendIdMismatchError') return handleBackendIdMismatch(state, event.error)
  return state.config.onError === 'shutdown'
    ? failMachine(state, { _tag: event.error._tag, cause: event.error }, true)
    : scheduleNextWork({ ...state, pull: { _tag: 'completed' } })
}

const onPullRetryElapsed = (
  state: RunningState,
  event: Extract<Event, { _tag: 'PullRetryElapsed' }>,
): TransitionResult => {
  if (state.pull._tag !== 'retry-wait' || state.pull.retryId !== event.retryId) return { state, commands: [] }
  const pullId = state.nextOperationId
  return {
    state: {
      ...state,
      pull: { _tag: 'streaming', pullId, pagination: 'between-pages', attempt: state.pull.attempt },
      nextOperationId: pullId + 1,
    },
    commands: [
      { _tag: 'StartProviderPull', pullId, cursor: state.syncState.upstreamHead, live: state.config.livePull },
    ],
  }
}

const onPushSucceeded = (state: RunningState, event: Extract<Event, { _tag: 'PushSucceeded' }>): TransitionResult => {
  if (state.push._tag !== 'in-flight' || state.push.operationId !== event.operationId) return { state, commands: [] }
  return schedulePush({
    ...state,
    push: { _tag: 'idle', generation: state.push.generation, queued: state.push.queued },
  })
}

const onPushFailed = (state: RunningState, event: Extract<Event, { _tag: 'PushFailed' }>): TransitionResult => {
  if (state.push._tag !== 'in-flight' || state.push.operationId !== event.operationId) return { state, commands: [] }
  if (event.error._tag === 'ServerAheadError') {
    return {
      state: {
        ...state,
        push: {
          _tag: 'awaiting-pull',
          generation: state.push.generation,
          queued: [...state.push.batch, ...state.push.queued],
        },
      },
      commands: [],
    }
  }
  if (event.error._tag === 'BackendIdMismatchError') return handleBackendIdMismatch(state, event.error)
  const retryId = state.nextOperationId
  const attempt = state.push.attempt + 1
  return {
    state: {
      ...state,
      push: {
        _tag: 'retry-wait',
        retryId,
        generation: state.push.generation,
        attempt,
        batch: state.push.batch,
        queued: state.push.queued,
      },
      nextOperationId: retryId + 1,
    },
    commands: [{ _tag: 'SchedulePushRetry', retryId, delayMs: pushRetryDelay(attempt) }],
  }
}

const onPushRetryElapsed = (
  state: RunningState,
  event: Extract<Event, { _tag: 'PushRetryElapsed' }>,
): TransitionResult => {
  if (state.push._tag !== 'retry-wait' || state.push.retryId !== event.retryId) return { state, commands: [] }
  const operationId = state.nextOperationId
  return {
    state: {
      ...state,
      push: {
        _tag: 'in-flight',
        operationId,
        generation: state.push.generation,
        attempt: state.push.attempt,
        batch: state.push.batch,
        queued: state.push.queued,
      },
      nextOperationId: operationId + 1,
    },
    commands: [{ _tag: 'StartProviderPush', operationId, batch: state.push.batch }],
  }
}

const onPushCancelled = (state: RunningState, event: Extract<Event, { _tag: 'PushCancelled' }>): TransitionResult => {
  if (state.push._tag !== 'cancelling' || state.push.operationId !== event.operationId) return { state, commands: [] }
  return schedulePush({
    ...state,
    push: { _tag: 'idle', generation: state.push.generation, queued: state.push.replacement },
  })
}

const handleBackendIdMismatch = (state: RunningState, error: BackendIdMismatchError): TransitionResult => {
  switch (state.config.onBackendIdMismatch) {
    case 'ignore':
      return { state: { ...state, pull: { _tag: 'completed' }, push: { _tag: 'disabled' } }, commands: [] }
    case 'shutdown':
      return failMachine(state, { _tag: error._tag, cause: error }, true)
    case 'reset': {
      const operationId = state.nextOperationId
      return {
        state: { _tag: 'resetting', syncState: state.syncState, operationId, backendMismatch: error },
        commands: [{ _tag: 'ResetDatabases', operationId, error }],
      }
    }
    default:
      return casesHandled(state.config.onBackendIdMismatch)
  }
}

const transitionResetting = (state: Extract<State, { _tag: 'resetting' }>, event: Event): TransitionResult => {
  if (event._tag === 'LocalPushRequested') {
    return { state, commands: [{ _tag: 'InterruptLocalRequests', requestIds: [event.requestId] }] }
  }
  if (event._tag === 'BackendResetSucceeded' && event.operationId === state.operationId) {
    const cause = IntentionalShutdownCause.make({ reason: 'backend-id-mismatch' })
    return {
      state: { _tag: 'failed', syncState: state.syncState, failure: { _tag: cause._tag, cause }, shutdownSent: true },
      commands: [
        { _tag: 'SendShutdown', error: cause },
        { _tag: 'StopRuntime', reason: cause._tag },
      ],
    }
  }
  if (event._tag === 'BackendResetFailed' && event.operationId === state.operationId) {
    return {
      state: {
        _tag: 'failed',
        syncState: state.syncState,
        failure: { _tag: 'BackendResetFailed', cause: event.cause },
        shutdownSent: true,
      },
      commands: [
        { _tag: 'SendShutdown', error: event.cause },
        { _tag: 'StopRuntime', reason: 'BackendResetFailed' },
      ],
    }
  }
  if (event._tag === 'ShutdownRequested') {
    return {
      state: { _tag: 'stopping', syncState: state.syncState, reason: event.reason },
      commands: [{ _tag: 'StopRuntime', reason: event.reason }],
    }
  }
  return { state, commands: [] }
}

const failOperation = (state: RunningState, operationId: OperationId, failure: Failure): TransitionResult => {
  if (isActiveWorkOperation(state.work, operationId) === false) return { state, commands: [] }
  return failMachine(state, failure, state.config.onError === 'shutdown')
}

const failMachine = (state: RunningState, failure: Failure, sendShutdown: boolean): TransitionResult => {
  const requestIds = [...new Set(state.reservations.map((item) => item.requestId))]
  return {
    state: { _tag: 'failed', syncState: state.syncState, failure, shutdownSent: sendShutdown },
    commands: [
      { _tag: 'InterruptLocalRequests', requestIds },
      ...(sendShutdown === true ? [{ _tag: 'SendShutdown', error: failure.cause } as const] : []),
      { _tag: 'StopRuntime', reason: failure._tag },
    ],
  }
}

const transitionTerminal = (state: Extract<State, { _tag: 'stopping' | 'failed' }>, event: Event): TransitionResult =>
  event._tag === 'LocalPushRequested'
    ? { state, commands: [{ _tag: 'InterruptLocalRequests', requestIds: [event.requestId] }] }
    : { state, commands: [] }

type RunningTransitionResult = { readonly state: RunningState; readonly commands: ReadonlyArray<Command> }

const scheduleNextWork = (state: RunningState, commands: ReadonlyArray<Command> = []): RunningTransitionResult => {
  if (state.work._tag !== 'idle') return { state, commands }
  if (state.upstreamQueue.length > 0) {
    const operationId = state.nextOperationId
    const [batch, ...upstreamQueue] = state.upstreamQueue
    return {
      state: {
        ...state,
        work: { _tag: 'planning-upstream', operationId, batch: batch! },
        upstreamQueue,
        nextOperationId: operationId + 1,
      },
      commands: [...commands, { _tag: 'PlanUpstream', operationId, syncState: state.syncState, batch: batch! }],
    }
  }
  if (state.pull._tag === 'streaming' && state.pull.pagination === 'more-expected') return { state, commands }
  if (state.localWorkEnabled === false) return { state, commands }
  if (state.localQueue.length === 0) return { state, commands }

  const items = state.localQueue.slice(0, state.config.localCommitBatchSize)
  const selectedKeys = new Set(items.map(localItemKey))
  const remainingQueue = state.localQueue.filter((item) => !selectedKeys.has(localItemKey(item)))
  const currentGeneration = state.syncState.localHead.rebaseGeneration
  const staleItems = items.filter((item) => item.event.seqNum.rebaseGeneration < currentGeneration)
  const activeItems = items.filter((item) => item.event.seqNum.rebaseGeneration >= currentGeneration)
  if (staleItems.length > 0) {
    const staleKeys = new Set(staleItems.map(localItemKey))
    const staleCommands: Command[] = [
      ...commands,
      {
        _tag: 'RejectLocalItems',
        items: staleItems.map((item) => ({
          item,
          error: new StaleRebaseGenerationError({
            currentRebaseGeneration: currentGeneration,
            providedRebaseGeneration: item.event.seqNum.rebaseGeneration,
            sessionId: item.event.sessionId,
          }),
        })),
      },
    ]
    const withoutStale: RunningState = {
      ...state,
      localQueue: [...activeItems, ...remainingQueue],
      reservations: state.reservations.filter((item) => !staleKeys.has(localItemKey(item))),
    }
    return scheduleNextWork(withoutStale, staleCommands)
  }
  const operationId = state.nextOperationId
  return {
    state: {
      ...state,
      work: { _tag: 'planning-local', operationId, items: activeItems },
      localQueue: remainingQueue,
      nextOperationId: operationId + 1,
    },
    commands: [...commands, { _tag: 'PlanLocal', operationId, syncState: state.syncState, items: activeItems }],
  }
}

const schedulePush = (state: RunningState, commands: ReadonlyArray<Command> = []): RunningTransitionResult => {
  if (state.push._tag !== 'idle' || state.push.queued.length === 0) return { state, commands }
  const operationId = state.nextOperationId
  const batch = state.push.queued.slice(0, state.config.backendPushBatchSize)
  const queued = state.push.queued.slice(batch.length)
  return {
    state: {
      ...state,
      push: {
        _tag: 'in-flight',
        operationId,
        generation: state.push.generation,
        attempt: 0,
        batch,
        queued,
      },
      nextOperationId: operationId + 1,
    },
    commands: [...commands, { _tag: 'StartProviderPush', operationId, batch }],
  }
}

const enqueuePushEvents = (
  state: RunningState,
  events: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>,
): RunningState => {
  if (state.push._tag === 'disabled' || events.length === 0) return state
  switch (state.push._tag) {
    case 'idle':
      return { ...state, push: { ...state.push, queued: [...state.push.queued, ...events] } }
    case 'in-flight':
    case 'retry-wait':
    case 'awaiting-pull':
      return { ...state, push: { ...state.push, queued: [...state.push.queued, ...events] } }
    case 'cancelling':
      return { ...state, push: { ...state.push, replacement: [...state.push.replacement, ...events] } }
    default:
      return casesHandled(state.push)
  }
}

const replacePushPlan = (
  state: RunningState,
  replacement: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>,
  commands: ReadonlyArray<Command>,
): { state: RunningState; commands: ReadonlyArray<Command> } => {
  if (state.push._tag === 'disabled') return { state, commands }
  const generation = state.push.generation + 1
  if (state.push._tag === 'in-flight') {
    return {
      state: {
        ...state,
        push: { _tag: 'cancelling', operationId: state.push.operationId, generation, replacement },
      },
      commands: [...commands, { _tag: 'CancelProviderPush', operationId: state.push.operationId }],
    }
  }
  const idlePush: PushState = { _tag: 'idle', generation, queued: replacement }
  const next: RunningState = { ...state, push: idlePush }
  const scheduled = schedulePush(next, commands)
  return { state: scheduled.state, commands: scheduled.commands }
}

const validatePushBatch = (
  batch: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>,
  pushHead: EventSequenceNumber.Client.Composite,
  isClientOnlyEvent: (event: LiveStoreEvent.Client.EncodedWithMeta) => boolean,
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

const replacePendingEvents = (
  syncState: SyncState.SyncState,
  committedEvents: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>,
) =>
  new SyncState.SyncState({
    ...syncState,
    pending: syncState.pending.map(
      (pendingEvent) =>
        committedEvents.find((committedEvent) =>
          EventSequenceNumber.Client.isEqual(committedEvent.seqNum, pendingEvent.seqNum),
        ) ?? pendingEvent,
    ),
  })

const isSameSequencePosition = (
  left: EventSequenceNumber.Client.Composite,
  right: EventSequenceNumber.Client.Composite,
) => left.global === right.global && left.client === right.client

const isActiveWorkOperation = (work: WorkState, operationId: OperationId) =>
  work._tag !== 'idle' && work.operationId === operationId

const localItemKey = (item: LocalItem) => `${item.requestId}:${item.index}`

const pushRetryDelay = (attempt: number) => Math.min(30_000, 1_000 * 2 ** Math.max(0, attempt - 1))
