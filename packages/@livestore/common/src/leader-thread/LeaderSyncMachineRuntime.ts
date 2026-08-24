import { type Scope, Effect, Queue, Subscribable, SubscriptionRef } from '@livestore/utils/effect'

import type * as Machine from './LeaderSyncMachine.ts'

export interface Runtime {
  readonly send: (event: Machine.Event) => Effect.Effect<void>
  readonly run: Effect.Effect<void, never, Scope.Scope>
  readonly state: Subscribable.Subscribable<Machine.State>
}

/**
 * Creates the single-owner run-to-completion loop. Command executors may run scoped asynchronous work, but every
 * completion re-enters through `send`; they never mutate machine state directly.
 */
export const make = ({
  initialState,
  transition,
  execute,
}: {
  initialState: Machine.State
  transition: (state: Machine.State, event: Machine.Event) => Machine.TransitionResult
  execute: (command: Machine.Command, send: Runtime['send']) => Effect.Effect<void, never, Scope.Scope>
}): Effect.Effect<Runtime> =>
  Effect.gen(function* () {
    const mailbox = yield* Queue.unbounded<Machine.Event>()
    const stateRef = yield* SubscriptionRef.make(initialState)
    const send: Runtime['send'] = (event) => Queue.offer(mailbox, event)

    const run: Runtime['run'] = Effect.gen(function* () {
      let running = true
      while (running === true) {
        const event = yield* Queue.take(mailbox)
        const current = yield* SubscriptionRef.get(stateRef)
        const result = transition(current, event)
        yield* SubscriptionRef.set(stateRef, result.state)
        for (const command of result.commands) {
          yield* execute(command, send).pipe(
            Effect.catchCause((cause) => send({ _tag: 'CommandDefected', commandTag: command._tag, cause })),
          )
          if (command._tag === 'StopRuntime') {
            running = false
            break
          }
        }
      }
      yield* Queue.shutdown(mailbox)
    })

    return {
      send,
      run,
      state: Subscribable.make({ get: SubscriptionRef.get(stateRef), changes: SubscriptionRef.changes(stateRef) }),
    }
  })
