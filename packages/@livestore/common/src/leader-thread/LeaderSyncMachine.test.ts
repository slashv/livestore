import { describe, expect, it } from 'vitest'

import { Option } from '@livestore/utils/effect'

import { EventSequenceNumber, LiveStoreEvent } from '../schema/mod.ts'
import { BackendIdMismatchError, IsOfflineError } from '../sync/errors.ts'
import * as SyncBackend from '../sync/sync-backend.ts'
import * as SyncState from '../sync/syncstate.ts'
import * as Machine from './LeaderSyncMachine.ts'

const transition = Machine.makeTransition({ isClientOnlyEvent: () => false })

describe('LeaderSyncMachine', () => {
  it('starts the lifecycle and exposes explicit pull and push operations', () => {
    const result = transition(Machine.initial(config({ backendEnabled: true }), emptySyncState), { _tag: 'Start' })

    expect(result.state._tag).toBe('running')
    expect(result.commands.map((command) => command._tag)).toEqual([
      'SetObservableSyncState',
      'CompleteBoot',
      'StartProviderPull',
    ])
    const running = expectRunning(result.state)
    expect(running.pull._tag).toBe('streaming')
    expect(running.push._tag).toBe('idle')
  })

  it('emits no publication or acknowledgement before durable local success', () => {
    const event = makeEvent(1)
    const started = transition(Machine.initial(config(), emptySyncState), { _tag: 'Start' }).state
    const admitted = transition(started, { _tag: 'LocalPushRequested', requestId: 1, events: [event] })
    expect(admitted.commands.map((command) => command._tag)).toEqual(['NotifyLocalPushAdmitted', 'PlanLocal'])
    const planning = expectRunning(admitted.state)
    expect(planning.work._tag).toBe('planning-local')
    if (planning.work._tag !== 'planning-local') throw new Error('Expected planning-local')

    const plan: Machine.LocalCommitPlan = {
      items: planning.work.items,
      proposedSyncState: new SyncState.SyncState({ pending: [event], upstreamHead: root, localHead: event.seqNum }),
      events: [event],
    }
    const planned = transition(admitted.state, {
      _tag: 'LocalPlanned',
      operationId: planning.work.operationId,
      plan,
    })
    expect(planned.commands.map((command) => command._tag)).toEqual(['CommitLocal'])
    const committing = expectRunning(planned.state)
    expect(committing.work._tag).toBe('committing-local')
    if (committing.work._tag !== 'committing-local') throw new Error('Expected committing-local')

    const committed = transition(planned.state, {
      _tag: 'LocalCommitSucceeded',
      operationId: committing.work.operationId,
      receipt: { _tag: 'local-commit', committedEvents: [event], stateHead: event.seqNum },
    })
    expect(committed.commands.map((command) => command._tag)).toEqual([
      'SetObservableSyncState',
      'PublishSessions',
      'CompleteLocalItems',
    ])
  })

  it('ignores duplicated and stale operation completions', () => {
    const started = transition(Machine.initial(config(), emptySyncState), { _tag: 'Start' }).state
    const stale = transition(started, {
      _tag: 'PushSucceeded',
      operationId: 999,
    })
    expect(stale.state).toBe(started)
    expect(stale.commands).toEqual([])
  })

  it('holds local work behind a multi-page upstream pagination fence', () => {
    const event = makeEvent(1)
    const started = transition(Machine.initial(config({ backendEnabled: true }), emptySyncState), {
      _tag: 'Start',
    }).state
    const running = expectRunning(started)
    expect(running.pull._tag).toBe('streaming')
    if (running.pull._tag !== 'streaming') throw new Error('Expected streaming pull')
    const received = transition(started, {
      _tag: 'UpstreamBatchReceived',
      batch: {
        pullId: running.pull.pullId,
        batchId: 1,
        events: [event],
        pageInfo: SyncBackend.pageInfoMoreKnown(1),
      },
    })
    const admitted = transition(received.state, { _tag: 'LocalPushRequested', requestId: 1, events: [event] })
    expect(admitted.commands.some((command) => command._tag === 'PlanLocal')).toBe(false)
    expect(expectRunning(admitted.state).localQueue).toHaveLength(1)
  })

  it('correlates backend reset completion and rejects late identities', () => {
    const started = transition(Machine.initial(config(), emptySyncState), { _tag: 'Start' }).state
    expectRunning(started)
    const resetState: Machine.State = {
      _tag: 'resetting',
      syncState: emptySyncState,
      operationId: 7,
      backendMismatch: new BackendIdMismatchError({ expected: 'before', received: 'after' }),
    }
    expect(transition(resetState, { _tag: 'BackendResetSucceeded', operationId: 6 }).state).toBe(resetState)
    expect(transition(resetState, { _tag: 'BackendResetSucceeded', operationId: 7 }).state._tag).toBe('failed')
  })

  it('quiesces an active durable commit and publishes its result before shutdown', () => {
    const { committing, event } = makeCommittingLocalState()
    const quiescing = transition(committing, { _tag: 'ShutdownRequested', reason: 'test-stop' })
    expect(quiescing.state._tag).toBe('quiescing')
    expect(quiescing.commands.map((command) => command._tag)).toEqual([
      'CancelProviderOperations',
      'InterruptLocalRequests',
    ])
    const completed = transition(quiescing.state, {
      _tag: 'LocalCommitSucceeded',
      operationId: committing.work.operationId,
      receipt: { _tag: 'local-commit', committedEvents: [event], stateHead: event.seqNum },
    })
    expect(completed.state._tag).toBe('stopping')
    expect(completed.commands.map((command) => command._tag)).toEqual([
      'SetObservableSyncState',
      'PublishSessions',
      'CompleteLocalItems',
      'StopRuntime',
    ])
  })

  it('drains an active durable commit before starting backend reset', () => {
    const { committing, event } = makeCommittingLocalState()
    const mismatch = new BackendIdMismatchError({ expected: 'before', received: 'after' })
    const withProviderPush: Machine.RunningState = {
      ...committing,
      config: { ...committing.config, onBackendIdMismatch: 'reset' },
      push: { _tag: 'in-flight', operationId: 42, generation: 0, attempt: 0, batch: [event], queued: [] },
    }
    const quiescing = transition(withProviderPush, { _tag: 'PushFailed', operationId: 42, error: mismatch })
    expect(quiescing.state._tag).toBe('quiescing')
    expect(quiescing.commands.some((command) => command._tag === 'ResetDatabases')).toBe(false)

    const completed = transition(quiescing.state, {
      _tag: 'LocalCommitSucceeded',
      operationId: committing.work.operationId,
      receipt: { _tag: 'local-commit', committedEvents: [event], stateHead: event.seqNum },
    })
    expect(completed.state._tag).toBe('resetting')
    expect(completed.commands.map((command) => command._tag)).toEqual([
      'SetObservableSyncState',
      'PublishSessions',
      'CompleteLocalItems',
      'ResetDatabases',
    ])
  })

  it('turns commit defects and receipt mismatches into terminal failures without publication', () => {
    const { committing, event } = makeCommittingLocalState()
    const defect = transition(committing, {
      _tag: 'LocalCommitDefected',
      operationId: committing.work.operationId,
      cause: new Error('materializer defect'),
    })
    expect(defect.state._tag).toBe('failed')
    expect(defect.commands.some((command) => command._tag === 'PublishSessions')).toBe(false)

    const mismatch = transition(committing, {
      _tag: 'LocalCommitSucceeded',
      operationId: committing.work.operationId,
      receipt: { _tag: 'local-commit', committedEvents: [event], stateHead: root },
    })
    expect(mismatch.state._tag).toBe('failed')
    expect(mismatch.commands.some((command) => command._tag === 'CompleteLocalItems')).toBe(false)
  })

  it('backs off pull retries and keeps the unaffected direction alive when mismatch is ignored', () => {
    const started = expectRunning(
      transition(Machine.initial(config({ backendEnabled: true, onBackendIdMismatch: 'ignore' }), emptySyncState), {
        _tag: 'Start',
      }).state,
    )
    if (started.pull._tag !== 'streaming') throw new Error('Expected streaming pull')
    const offline = transition(started, {
      _tag: 'PullFailed',
      pullId: started.pull.pullId,
      error: new IsOfflineError({ cause: new Error('offline') }),
    })
    expect(offline.commands).toEqual([{ _tag: 'SchedulePullRetry', retryId: 2, delayMs: 1000 }])

    const mismatch = new BackendIdMismatchError({ expected: 'before', received: 'after' })
    const pullIgnored = expectRunning(
      transition(started, { _tag: 'PullFailed', pullId: started.pull.pullId, error: mismatch }).state,
    )
    expect(pullIgnored.pull._tag).toBe('completed')
    expect(pullIgnored.push._tag).not.toBe('disabled')

    const pushing: Machine.RunningState = {
      ...started,
      push: { _tag: 'in-flight', operationId: 9, generation: 0, attempt: 0, batch: [makeEvent(1)], queued: [] },
    }
    const pushIgnored = expectRunning(
      transition(pushing, { _tag: 'PushFailed', operationId: 9, error: mismatch }).state,
    )
    expect(pushIgnored.push._tag).toBe('disabled')
    expect(pushIgnored.pull._tag).toBe('streaming')
  })
})

const root = EventSequenceNumber.Client.ROOT
const emptySyncState = new SyncState.SyncState({ pending: [], upstreamHead: root, localHead: root })

const config = (overrides: Partial<Machine.Config> = {}): Machine.Config => ({
  backendEnabled: false,
  livePull: true,
  localCommitBatchSize: 10,
  backendPushBatchSize: 50,
  onError: 'shutdown',
  onBackendIdMismatch: 'shutdown',
  localWorkInitiallyBlocked: false,
  ...overrides,
})

const makeEvent = (global: number) =>
  new LiveStoreEvent.Client.EncodedWithMeta({
    name: 'test',
    args: {},
    seqNum: EventSequenceNumber.Client.Composite.make({ global, client: 0 }),
    parentSeqNum: root,
    clientId: 'client',
    sessionId: 'session',
    meta: {
      syncMetadata: Option.none(),
      materializerHashLeader: Option.none(),
      materializerHashSession: Option.none(),
    },
  })

const expectRunning = (state: Machine.State): Machine.RunningState => {
  expect(state._tag).toBe('running')
  if (state._tag !== 'running') throw new Error(`Expected running state, got ${state._tag}`)
  return state
}

const makeCommittingLocalState = (): {
  committing: Machine.RunningState & { readonly work: Extract<Machine.WorkState, { _tag: 'committing-local' }> }
  event: LiveStoreEvent.Client.EncodedWithMeta
} => {
  const event = makeEvent(1)
  const started = expectRunning(transition(Machine.initial(config(), emptySyncState), { _tag: 'Start' }).state)
  const admitted = expectRunning(
    transition(started, { _tag: 'LocalPushRequested', requestId: 1, events: [event] }).state,
  )
  if (admitted.work._tag !== 'planning-local') throw new Error('Expected planning-local')
  const proposedSyncState = new SyncState.SyncState({ pending: [event], upstreamHead: root, localHead: event.seqNum })
  const planned = expectRunning(
    transition(admitted, {
      _tag: 'LocalPlanned',
      operationId: admitted.work.operationId,
      plan: { items: admitted.work.items, proposedSyncState, events: [event] },
    }).state,
  )
  if (planned.work._tag !== 'committing-local') throw new Error('Expected committing-local')
  return { committing: { ...planned, work: planned.work }, event }
}
