import { describe, expect, it } from 'vitest'

import { Option } from '@livestore/utils/effect'

import { EventSequenceNumber, LiveStoreEvent } from '../schema/mod.ts'
import { BackendIdMismatchError } from '../sync/errors.ts'
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
    if (result.state._tag !== 'running') return
    expect(result.state.pull._tag).toBe('streaming')
    expect(result.state.push._tag).toBe('idle')
  })

  it('emits no publication or acknowledgement before durable local success', () => {
    const event = makeEvent(1)
    const started = transition(Machine.initial(config(), emptySyncState), { _tag: 'Start' }).state
    const admitted = transition(started, { _tag: 'LocalPushRequested', requestId: 1, events: [event] })
    expect(admitted.commands.map((command) => command._tag)).toEqual(['NotifyLocalPushAdmitted', 'PlanLocal'])
    if (admitted.state._tag !== 'running' || admitted.state.work._tag !== 'planning-local') return

    const plan: Machine.LocalCommitPlan = {
      items: admitted.state.work.items,
      proposedSyncState: new SyncState.SyncState({ pending: [event], upstreamHead: root, localHead: event.seqNum }),
      events: [event],
    }
    const planned = transition(admitted.state, {
      _tag: 'LocalPlanned',
      operationId: admitted.state.work.operationId,
      plan,
    })
    expect(planned.commands.map((command) => command._tag)).toEqual(['CommitLocal'])
    if (planned.state._tag !== 'running' || planned.state.work._tag !== 'committing-local') return

    const committed = transition(planned.state, {
      _tag: 'LocalCommitSucceeded',
      operationId: planned.state.work.operationId,
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
    if (started._tag !== 'running' || started.pull._tag !== 'streaming') return
    const received = transition(started, {
      _tag: 'UpstreamBatchReceived',
      batch: {
        pullId: started.pull.pullId,
        batchId: 1,
        events: [],
        pageInfo: SyncBackend.pageInfoMoreKnown(1),
      },
    })
    const admitted = transition(received.state, { _tag: 'LocalPushRequested', requestId: 1, events: [event] })
    expect(admitted.commands.some((command) => command._tag === 'PlanLocal')).toBe(false)
    if (admitted.state._tag !== 'running') return
    expect(admitted.state.localQueue).toHaveLength(1)
  })

  it('correlates backend reset completion and rejects late identities', () => {
    const started = transition(Machine.initial(config(), emptySyncState), { _tag: 'Start' }).state
    if (started._tag !== 'running') return
    const resetState: Machine.State = {
      _tag: 'resetting',
      syncState: emptySyncState,
      operationId: 7,
      backendMismatch: new BackendIdMismatchError({ expected: 'before', received: 'after' }),
    }
    expect(transition(resetState, { _tag: 'BackendResetSucceeded', operationId: 6 }).state).toBe(resetState)
    expect(transition(resetState, { _tag: 'BackendResetSucceeded', operationId: 7 }).state._tag).toBe('failed')
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
