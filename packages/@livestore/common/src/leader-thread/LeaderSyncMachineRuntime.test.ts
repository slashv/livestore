import { expect } from 'vitest'

import { Vitest } from '@livestore/utils-dev/node-vitest'
import { Effect, Fiber } from '@livestore/utils/effect'

import { EventSequenceNumber } from '../schema/mod.ts'
import * as SyncState from '../sync/syncstate.ts'
import * as Machine from './LeaderSyncMachine.ts'
import * as MachineRuntime from './LeaderSyncMachineRuntime.ts'

Vitest.describe('LeaderSyncMachineRuntime', () => {
  Vitest.live('serializes events and terminates after StopRuntime', (test) =>
    Effect.gen(function* () {
      const executed: Machine.Command['_tag'][] = []
      const syncState = new SyncState.SyncState({
        pending: [],
        upstreamHead: EventSequenceNumber.Client.ROOT,
        localHead: EventSequenceNumber.Client.ROOT,
      })
      const runtime = yield* MachineRuntime.make({
        initialState: Machine.initial(
          {
            backendEnabled: false,
            livePull: false,
            localCommitBatchSize: 10,
            backendPushBatchSize: 50,
            onError: 'shutdown',
            onBackendIdMismatch: 'shutdown',
            localWorkInitiallyBlocked: false,
          },
          syncState,
        ),
        transition: Machine.makeTransition({ isClientOnlyEvent: () => false }),
        execute: (command) => Effect.sync(() => executed.push(command._tag)).pipe(Effect.asVoid),
      })
      const fiber = yield* runtime.run.pipe(Effect.forkScoped)

      yield* runtime.send({ _tag: 'Start' })
      yield* runtime.send({ _tag: 'ShutdownRequested', reason: 'test-complete' })
      yield* Fiber.join(fiber)

      expect(executed).toEqual([
        'SetObservableSyncState',
        'CompleteBoot',
        'CancelProviderOperations',
        'InterruptLocalRequests',
        'StopRuntime',
      ])
      expect((yield* runtime.state.get)._tag).toBe('stopping')
    }).pipe(Vitest.withTestCtx(test)),
  )

  Vitest.live('converts command defects into a terminal machine event', (test) =>
    Effect.gen(function* () {
      const syncState = new SyncState.SyncState({
        pending: [],
        upstreamHead: EventSequenceNumber.Client.ROOT,
        localHead: EventSequenceNumber.Client.ROOT,
      })
      const runtime = yield* MachineRuntime.make({
        initialState: Machine.initial(
          {
            backendEnabled: false,
            livePull: false,
            localCommitBatchSize: 10,
            backendPushBatchSize: 50,
            onError: 'shutdown',
            onBackendIdMismatch: 'shutdown',
            localWorkInitiallyBlocked: false,
          },
          syncState,
        ),
        transition: Machine.makeTransition({ isClientOnlyEvent: () => false }),
        execute: (command) =>
          command._tag === 'CompleteBoot' ? Effect.die(new Error('boot command defect')) : Effect.void,
      })
      const fiber = yield* runtime.run.pipe(Effect.forkScoped)
      yield* runtime.send({ _tag: 'Start' })
      yield* Fiber.join(fiber)

      expect((yield* runtime.state.get)._tag).toBe('failed')
    }).pipe(Vitest.withTestCtx(test)),
  )

  Vitest.live('stops a command tail after the first defect', (test) =>
    Effect.gen(function* () {
      const syncState = new SyncState.SyncState({
        pending: [],
        upstreamHead: EventSequenceNumber.Client.ROOT,
        localHead: EventSequenceNumber.Client.ROOT,
      })
      const executed: Machine.Command['_tag'][] = []
      const runtime = yield* MachineRuntime.make({
        initialState: Machine.initial(
          {
            backendEnabled: false,
            livePull: false,
            localCommitBatchSize: 10,
            backendPushBatchSize: 50,
            onError: 'shutdown',
            onBackendIdMismatch: 'shutdown',
            localWorkInitiallyBlocked: false,
          },
          syncState,
        ),
        transition: (state, event) => {
          if (event._tag === 'Start') {
            return {
              state,
              commands: [
                {
                  _tag: 'PublishSessions',
                  payload: SyncState.PayloadUpstreamAdvance.make({ newEvents: [] }),
                  globalHead: EventSequenceNumber.Client.ROOT,
                  leaderHead: EventSequenceNumber.Client.ROOT,
                },
                { _tag: 'CompleteLocalItems', items: [] },
              ],
            }
          }
          if (event._tag === 'CommandDefected') {
            return {
              state: { _tag: 'stopping', syncState, reason: 'command-defect' },
              commands: [{ _tag: 'StopRuntime', reason: 'command-defect' }],
            }
          }
          return { state, commands: [] }
        },
        execute: (command) =>
          Effect.sync(() => executed.push(command._tag)).pipe(
            Effect.andThen(command._tag === 'PublishSessions' ? Effect.die(new Error('publish defect')) : Effect.void),
          ),
      })
      const fiber = yield* runtime.run.pipe(Effect.forkScoped)
      yield* runtime.send({ _tag: 'Start' })
      yield* Fiber.join(fiber)

      expect(executed).toEqual(['PublishSessions', 'StopRuntime'])
      expect((yield* runtime.state.get)._tag).toBe('stopping')
    }).pipe(Vitest.withTestCtx(test)),
  )
})
