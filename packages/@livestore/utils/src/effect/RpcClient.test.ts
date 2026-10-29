import * as Vitest from '@effect/vitest'
import { Cause, Effect, Exit, Layer, Schedule, Schema, Stream } from 'effect'
import { Rpc, RpcClientError, RpcGroup, RpcSerialization } from 'effect/rpc'
import { Socket } from 'effect/socket'

import { layerProtocolSocketWithIsConnected, make as makeRpcClient } from './RpcClient.ts'
import * as SubscriptionRef from './SubscriptionRef.ts'

class TestRpcs extends RpcGroup.make(Rpc.make('Live', { success: Schema.Finite, stream: true })) {}

Vitest.describe('layerProtocolSocketWithIsConnected', () => {
  // A connection that stops answering pings is dropped and reopened. Streams running on it must fail so their callers
  // re-subscribe on the new connection: the server forgot them with the old socket and will never send to them again.
  Vitest.live(
    'fails in-flight streams when a ping times out',
    Effect.fn(function* () {
      const isConnected = yield* SubscriptionRef.make(false)
      const protocol = layerProtocolSocketWithIsConnected({
        url: 'ws://test',
        isConnected,
        retryTransientErrors: Schedule.spaced('10 millis'),
        pingSchedule: Schedule.spaced('20 millis'),
      }).pipe(Layer.provide(Layer.succeed(Socket.Socket)(silentSocket)), Layer.provide(RpcSerialization.layerJson))
      const ctx = yield* Layer.build(protocol)
      const client = yield* makeRpcClient(TestRpcs).pipe(Effect.provide(ctx))

      const exit = yield* client.Live().pipe(Stream.runDrain, Effect.timeout('2 seconds'), Effect.exit)

      Vitest.expect(Exit.isFailure(exit)).toBe(true)
      if (Exit.isFailure(exit) === true) {
        const error = Cause.squash(exit.cause)
        Vitest.expect(error).toBeInstanceOf(RpcClientError.RpcClientError)
        Vitest.expect(error).toMatchObject({ reason: { _tag: 'SocketReadError' } })
      }
    }),
  )
})

/** A socket that opens and accepts writes but never delivers a frame, like a server that stopped responding. */
const silentSocket = Socket.make({
  reader: Effect.succeed({
    pull: Effect.never,
    upgrade: () => Effect.void,
  }),
  writer: Effect.succeed({
    write: () => Effect.void,
    writeAll: () => Effect.void,
  }),
})
