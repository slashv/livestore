import { expect } from 'vitest'

import { makeMockSyncBackend, type MockSyncBackend, type SqliteDb, type SyncBackend } from '@livestore/common'
import { LeaderThreadCtx, makeLeaderThreadLayer } from '@livestore/common/leader-thread'
import { EventSequenceNumber, LiveStoreEvent, SystemTables } from '@livestore/common/schema'
import { EventFactory } from '@livestore/common/testing'
import { loadSqlite3Wasm } from '@livestore/sqlite-wasm/load-wasm'
import { sqliteDbFactory } from '@livestore/sqlite-wasm/node'
import { Vitest } from '@livestore/utils-dev/node-vitest'
import { Deferred, Effect, FetchHttpClient, Layer, Stream, WebChannel } from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import { events, makeSqliteServicesLayer, schema } from './fixture.ts'

Vitest.describe('leader restart pending set', { timeout: 30_000 }, () => {
  /**
   * Own events that were rebased (rebase generation 1) and then confirmed keep that generation in the eventlog, while
   * the in-memory local head adopts the backend's copy (generation 0). Events committed afterwards therefore carry
   * generation 0. On restart, the pending set must still start after the backend head: the confirmed event at the
   * backend head is not pending just because its row has a higher rebase generation than the local head.
   */
  Vitest.live('does not treat a rebased-then-confirmed event at the backend head as pending after a restart', (test) =>
    Effect.gen(function* () {
      const sqlite3 = yield* Effect.promise(() => loadSqlite3Wasm())
      const makeSqliteDb = yield* sqliteDbFactory({ sqlite3 })
      const dbState = yield* makeSqliteDb({ _tag: 'in-memory' })
      const dbEventlog = yield* makeSqliteDb({ _tag: 'in-memory' })
      const mockBackend = yield* makeMockSyncBackend({ startConnected: true })
      const remote = EventFactory.makeFactory(events)({ client: EventFactory.clientIdentity('remote', 'session') })

      yield* mockBackend.advance(
        remote.todoCreated.next({ id: 'remote-1', text: 'remote', completed: false }),
        remote.todoCreated.next({ id: 'remote-2', text: 'remote', completed: false }),
      )

      // First run: commit before the first pull so the events are rebased onto the backend (e3r1..e5r1) and confirmed.
      const firstGate = yield* Deferred.make<void>()
      const firstPushOnline = { value: true }
      yield* Effect.gen(function* () {
        const leader = yield* LeaderThreadCtx
        yield* leader.syncProcessor.push(yield* makeLocalEvents(['local-1', 'local-2', 'local-3']))
        yield* Deferred.succeed(firstGate, undefined)
        yield* waitForSyncState((state) => state.pending.length === 0 && state.upstreamHead.global === 5)

        // Offline: commit two more events that never reach the backend. They follow the confirmed head (`e5`).
        firstPushOnline.value = false
        yield* leader.syncProcessor.push(yield* makeLocalEvents(['local-4', 'local-5']))
        yield* waitForSyncState((state) => state.localHead.global === 7)
      }).pipe(
        Effect.provide(
          makeLeader({ makeSqliteDb, dbState, dbEventlog, mockBackend, gate: firstGate, push: firstPushOnline }),
        ),
        Effect.scoped,
      )

      // Meanwhile the backend moves on, so the restarted leader must rebase its two pending events.
      remote.todoCreated.advanceTo(6, 5)
      yield* mockBackend.advance(
        remote.todoCreated.next({ id: 'remote-6', text: 'remote', completed: false }),
        remote.todoCreated.next({ id: 'remote-7', text: 'remote', completed: false }),
        remote.todoCreated.next({ id: 'remote-8', text: 'remote', completed: false }),
      )

      const secondGate = yield* Deferred.make<void>()
      yield* Effect.gen(function* () {
        const leader = yield* LeaderThreadCtx
        const bootState = yield* leader.syncProcessor.syncState.get
        expect(bootState.upstreamHead.global).toBe(5)
        expect(bootState.pending.map((event) => (event.args as { id: string }).id)).toEqual(['local-4', 'local-5'])

        yield* Deferred.succeed(secondGate, undefined)
        yield* waitForSyncState((state) => state.pending.length === 0 && state.upstreamHead.global === 10)
      }).pipe(
        Effect.provide(
          makeLeader({ makeSqliteDb, dbState, dbEventlog, mockBackend, gate: secondGate, push: { value: true } }),
        ),
        Effect.scoped,
      )

      // Every backend event, including the confirmed `local-3` at 5, appears exactly once in the eventlog.
      expect(eventlogIds(dbEventlog)).toEqual([
        [1, 'remote-1'],
        [2, 'remote-2'],
        [3, 'local-1'],
        [4, 'local-2'],
        [5, 'local-3'],
        [6, 'remote-6'],
        [7, 'remote-7'],
        [8, 'remote-8'],
        [9, 'local-4'],
        [10, 'local-5'],
      ])
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test)),
  )
})

/**
 * Boots a leader on the given databases. Backend calls wait for `gate`, so local commits can land before the first
 * pull, and pushes hang while `push.value` is false to model an offline client.
 */
const makeLeader = ({
  makeSqliteDb,
  dbState,
  dbEventlog,
  mockBackend,
  gate,
  push,
}: {
  makeSqliteDb: Parameters<typeof makeLeaderThreadLayer>[0]['makeSqliteDb']
  dbState: SqliteDb
  dbEventlog: SqliteDb
  mockBackend: MockSyncBackend
  gate: Deferred.Deferred<void>
  push: { value: boolean }
}) =>
  Effect.gen(function* () {
    const backend: SyncBackend.SyncBackendConstructor = () =>
      Effect.gen(function* () {
        const syncBackend = yield* mockBackend.makeSyncBackend
        return {
          ...syncBackend,
          pull: (cursor, options) =>
            Stream.fromEffect(Deferred.await(gate)).pipe(Stream.flatMap(() => syncBackend.pull(cursor, options))),
          push: (batch) =>
            Deferred.await(gate).pipe(
              Effect.andThen(Effect.suspend(() => (push.value === true ? syncBackend.push(batch) : Effect.never))),
            ),
        }
      })

    return makeLeaderThreadLayer({
      schema,
      storeId: 'restart-pending',
      clientId: 'local',
      syncPayloadEncoded: undefined,
      syncPayloadSchema: undefined,
      makeSqliteDb,
      syncOptions: { backend, livePull: true },
      devtoolsOptions: { enabled: false },
      shutdownChannel: yield* WebChannel.noopChannel<any, any>(),
    }).pipe(
      Layer.provide(
        Layer.mergeAll(
          makeSqliteServicesLayer({ dbState, dbEventlog }),
          FetchHttpClient.layer,
          PlatformNode.NodeFileSystem.layer,
        ),
      ),
    )
  }).pipe(Layer.unwrap)

/** Builds the next global events after the leader's local head, as a client session would. */
const makeLocalEvents = (ids: ReadonlyArray<string>) =>
  Effect.gen(function* () {
    const leader = yield* LeaderThreadCtx
    let head = (yield* leader.syncProcessor.syncState.get).localHead
    return ids.map((id) => {
      const { seqNum, parentSeqNum } = EventSequenceNumber.Client.nextPair({ seqNum: head, isClientOnly: false })
      head = seqNum
      return LiveStoreEvent.Client.Encoded.make({
        name: events.todoCreated.name,
        args: { id, text: 'local', completed: false },
        seqNum,
        parentSeqNum,
        clientId: 'local',
        sessionId: 'session',
      })
    })
  })

const waitForSyncState = (
  predicate: (state: {
    pending: ReadonlyArray<unknown>
    upstreamHead: EventSequenceNumber.Client.Composite
    localHead: EventSequenceNumber.Client.Composite
  }) => boolean,
) =>
  Effect.gen(function* () {
    const leader = yield* LeaderThreadCtx
    yield* leader.syncProcessor.syncState.changes.pipe(
      Stream.takeUntil(predicate),
      Stream.runDrain,
      Effect.timeout('10 seconds'),
    )
  })

const eventlogIds = (dbEventlog: SqliteDb) =>
  dbEventlog
    .select<{ seqNumGlobal: number; argsJson: string }>(
      `SELECT seqNumGlobal, argsJson FROM ${SystemTables.EVENTLOG_META_TABLE} ORDER BY seqNumGlobal, seqNumClient`,
    )
    .map((row) => [row.seqNumGlobal, (JSON.parse(row.argsJson) as { id: string }).id])
