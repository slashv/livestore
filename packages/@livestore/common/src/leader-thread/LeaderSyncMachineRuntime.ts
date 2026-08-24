import { type Scope, Effect, Exit, Option, Queue, Subscribable, SubscriptionRef } from '@livestore/utils/effect'

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
      let nextEvent = Option.none<Machine.Event>()
      while (running === true) {
        const event = Option.isSome(nextEvent) === true ? nextEvent.value : yield* Queue.take(mailbox)
        nextEvent = Option.none()
        const current = yield* SubscriptionRef.get(stateRef)
        const result = transition(current, event)
        yield* SubscriptionRef.set(stateRef, result.state)
        for (const command of result.commands) {
          const exit = yield* execute(command, send).pipe(Effect.exit)
          if (Exit.isFailure(exit) === true) {
            if (command._tag === 'StopRuntime') running = false
            else {
              nextEvent = Option.some({ _tag: 'CommandDefected', commandTag: command._tag, cause: exit.cause })
            }
            break
          }
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
