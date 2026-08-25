import type { Queue, Stream } from '@livestore/utils/effect'
import { type HttpClient, type Scope, Context, Effect, Layer, type Subscribable } from '@livestore/utils/effect'

import type { PullItem } from '../ClientSessionLeaderThreadProxy.ts'
import type { UnknownEventError } from '../errors.ts'
import { EventSequenceNumber, LiveStoreEvent, resolveEventDef } from '../schema/mod.ts'
import type * as SyncState from '../sync/syncstate.ts'
import * as LeaderSyncLoop from './LeaderSyncLoop.ts'
import { isRejectedPushError, type RejectedPushError } from './RejectedPushError.ts'

export const TypeId = '~@livestore/common/LeaderSyncProcessor' as const
export type TypeId = typeof TypeId

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

export const make = Effect.fnUntraced(function* (options: LeaderSyncLoop.Options) {
  const loop = yield* LeaderSyncLoop.make(options)
  const pushPartial: Service['pushPartial'] = ({ event: { name, args }, clientId, sessionId }) =>
    Effect.gen(function* () {
      const syncState = yield* loop.syncState.get
      const resolution = yield* resolveEventDef(options.schema, {
        operation: '@livestore/common:LeaderSyncProcessor:pushPartial',
        event: { name, args, clientId, sessionId, seqNum: syncState.localHead },
      })
      if (resolution._tag === 'unknown') return
      yield* loop.push([
        new LiveStoreEvent.Client.EncodedWithMeta({
          name,
          args,
          clientId,
          sessionId,
          ...EventSequenceNumber.Client.nextPair({
            seqNum: syncState.localHead,
            isClientOnly: resolution.eventDef.options.clientOnly,
          }),
        }),
      ])
    }).pipe(Effect.catchIf(isRejectedPushError, Effect.die))

  return LeaderSyncProcessor.of({
    [TypeId]: TypeId,
    boot: loop.boot,
    push: loop.push,
    pushPartial,
    pull: loop.pull,
    pullQueue: loop.pullQueue,
    syncState: loop.syncState,
  })
})

export const layer = (options: LeaderSyncLoop.Options) => Layer.effect(LeaderSyncProcessor, make(options))
