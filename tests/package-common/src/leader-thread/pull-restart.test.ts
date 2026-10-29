import { expect } from 'vitest'

import { makeMockSyncBackend, type SyncBackend } from '@livestore/common'
import { Vitest } from '@livestore/utils-dev/node-vitest'
import { Effect, Stream } from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import {
  bootLeader,
  encodeArgs,
  makeClientDbs,
  makeSqlite,
  makeWriterEvents,
  nextLocalEvent,
  waitForUpstreamHead,
} from '../snapshot/harness.ts'

/**
 * The leader must never stop following the backend silently. Pushes rejected as behind the backend wait for a pull,
 * so a pull that is not running (non-live mode) or a live pull whose stream ended must be restarted.
 */
Vitest.describe('leader pull restarts', () => {
  Vitest.live(
    'a push rejected as behind the backend triggers a pull when live pull is off',
    (test) =>
      Effect.gen(function* () {
        const makeSqliteDb = yield* makeSqlite
        const backend = yield* makeMockSyncBackend({ startConnected: true })
        const leader = yield* bootLeader({
          makeSqliteDb,
          dbs: yield* makeClientDbs(makeSqliteDb),
          clientId: 'agent',
          syncOptions: { backend: () => backend.makeSyncBackend, livePull: false },
        })
        // The initial (non-live) pull has completed; another writer moves the backend on.
        yield* Effect.sleep(50)
        yield* backend.advance(...makeWriterEvents({ count: 3 }))

        const event = yield* nextLocalEvent(
          leader,
          encodeArgs('todoCreated', { id: 'mine', text: 'x', author: 'agent' }),
        )
        yield* leader.ctx.syncProcessor.push([event])

        // The rejection must lead to a pull of events 1..3 and a push of the rebased event at 4. (Without live pull the
        // leader does not pull its own confirmation, so the event stays pending locally; that is existing behaviour.)
        yield* waitForUpstreamHead(leader, 3, '10 seconds')
        const pushed = yield* backend.pushedEvents.pipe(Stream.take(1), Stream.runCollect, Effect.timeout('10 seconds'))
        expect(pushed.map((event) => event.seqNum)).toEqual([4])
      }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test, { timeout: 30_000 })),
    30_000,
  )

  Vitest.live(
    'a live pull stream that ends is restarted from the cursor',
    (test) =>
      Effect.gen(function* () {
        const makeSqliteDb = yield* makeSqlite
        const backend = yield* makeMockSyncBackend({ startConnected: true })
        yield* backend.advance(...makeWriterEvents({ count: 5 }))

        let livePulls = 0
        const leader = yield* bootLeader({
          makeSqliteDb,
          dbs: yield* makeClientDbs(makeSqliteDb),
          clientId: 'agent',
          syncOptions: {
            livePull: true,
            backend: () =>
              backend.makeSyncBackend.pipe(
                Effect.map(
                  (inner): SyncBackend.SyncBackend => ({
                    ...inner,
                    // The first live pull ends after catching up, like a transport that lost its live subscription.
                    pull: (cursor, options) =>
                      options?.live === true && livePulls++ === 0
                        ? inner.pull(cursor, { live: false })
                        : inner.pull(cursor, options),
                  }),
                ),
              ),
          },
        })
        yield* waitForUpstreamHead(leader, 5)

        yield* backend.advance(...makeWriterEvents({ startSeq: 6, count: 5 }))
        yield* waitForUpstreamHead(leader, 10, '10 seconds')
        expect(livePulls).toBeGreaterThanOrEqual(2)
      }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test, { timeout: 30_000 })),
    30_000,
  )
})
