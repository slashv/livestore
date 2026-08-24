import { expect } from 'vitest'

import {
  type BootStatus,
  EventlogSqliteDb,
  MATERIALIZATION_JOURNAL_META_TABLE,
  MaterializationJournal,
  StateHead,
  StateSqliteDb,
} from '@livestore/common'
import { Eventlog, LeaderSyncCommitter, makeMaterializeEvent, recreateDb } from '@livestore/common/leader-thread'
import { EventSequenceNumber, LiveStoreEvent } from '@livestore/common/schema'
import { loadSqlite3Wasm } from '@livestore/sqlite-wasm/load-wasm'
import { sqliteDbFactory } from '@livestore/sqlite-wasm/node'
import { Vitest } from '@livestore/utils-dev/node-vitest'
import { Effect, Layer, Option, Queue } from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import { events, schema } from './fixture.ts'

Vitest.describe.concurrent('LeaderSyncCommitter', () => {
  Vitest.live('commits local events without mutating the plan and returns an immutable receipt', (test) =>
    Effect.gen(function* () {
      const { committer, dbEventlog, dbState, stateHead } = yield* setup
      const event = makeTodoEvent({ global: 1, id: 'local', text: 'Local' })

      const receipt = yield* committer.commitLocal({ events: [event] })

      expect(dbState.select('SELECT id, text FROM todos')).toEqual([{ id: 'local', text: 'Local' }])
      expect(dbEventlog.select<{ name: string }>('SELECT name FROM eventlog')).toEqual([
        { name: events.todoCreated.name },
      ])
      expect(yield* stateHead.get).toEqual(event.seqNum)

      expect(receipt.committedEvents[0]).not.toBe(event)
      expect(event.meta.materializerHashLeader).toEqual(Option.none())
      expect(Object.isFrozen(receipt)).toBe(true)
      expect(Object.isFrozen(receipt.committedEvents)).toBe(true)
      expect(Object.isFrozen(receipt.committedEvents[0])).toBe(true)
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test)),
  )

  Vitest.live('rolls back both databases when an upstream batch fails during materialization', (test) =>
    Effect.gen(function* () {
      const { committer, dbEventlog, dbState, stateHead } = yield* setup
      const first = makeTodoEvent({ global: 1, id: 'duplicate', text: 'First' })
      const duplicate = makeTodoEvent({ global: 2, parentSeqNum: first.seqNum, id: 'duplicate', text: 'Second' })

      const error = yield* committer
        .commitUpstream({
          pulledEvents: [first, duplicate],
          events: [first, duplicate],
          rollbackEvents: [],
          confirmedEvents: [],
          backendHead: duplicate.seqNum,
        })
        .pipe(Effect.flip)

      expect(error._tag).toEqual('MaterializeError')
      expect(dbState.select('SELECT id FROM todos')).toEqual([])
      expect(dbEventlog.select('SELECT name FROM eventlog')).toEqual([])
      expect(Eventlog.getBackendHeadFromDb(dbEventlog)).toEqual(EventSequenceNumber.Client.ROOT.global)
      expect(yield* stateHead.get).toEqual(EventSequenceNumber.Client.ROOT)
      expect(dbState.select(`SELECT * FROM ${MATERIALIZATION_JOURNAL_META_TABLE}`)).toEqual([])
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test)),
  )

  Vitest.live('does not expose event inserts when backend-head persistence fails', (test) =>
    Effect.gen(function* () {
      const { committer, dbEventlog, dbState, stateHead } = yield* setup
      const event = makeTodoEvent({ global: 1, id: 'head-failure', text: 'Head failure' })
      dbEventlog.execute(`
        CREATE TRIGGER fail_backend_head
        BEFORE UPDATE ON __livestore_sync_status
        BEGIN
          SELECT RAISE(ABORT, 'backend head write failed');
        END
      `)

      const error = yield* committer
        .commitUpstream({
          pulledEvents: [event],
          events: [event],
          rollbackEvents: [],
          confirmedEvents: [],
          backendHead: event.seqNum,
        })
        .pipe(Effect.flip)

      expect(error._tag).toEqual('MaterializeError')
      expect(dbEventlog.select('SELECT name FROM eventlog')).toEqual([])
      expect(Eventlog.getBackendHeadFromDb(dbEventlog)).toEqual(EventSequenceNumber.Client.ROOT.global)
      expect(dbState.select('SELECT id FROM todos')).toEqual([])
      expect(yield* stateHead.get).toEqual(EventSequenceNumber.Client.ROOT)
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test)),
  )

  Vitest.live('replaces rolled-back history and advances the backend head in one upstream commit', (test) =>
    Effect.gen(function* () {
      const { committer, dbEventlog, dbState, stateHead } = yield* setup
      const original = makeTodoEvent({ global: 1, id: 'original', text: 'Original' })
      yield* committer.commitLocal({ events: [original] })

      const replacement = makeTodoEvent({
        global: 1,
        rebaseGeneration: 1,
        id: 'replacement',
        text: 'Replacement',
        syncMetadata: Option.some({ cursor: 'upstream-1' }),
      })

      const receipt = yield* committer.commitUpstream({
        pulledEvents: [replacement],
        events: [replacement],
        rollbackEvents: [original],
        confirmedEvents: [],
        backendHead: replacement.seqNum,
      })

      expect(dbState.select('SELECT id, text FROM todos')).toEqual([{ id: 'replacement', text: 'Replacement' }])
      expect(
        dbEventlog.select<{ seqNumRebaseGeneration: number }>('SELECT seqNumRebaseGeneration FROM eventlog'),
      ).toEqual([{ seqNumRebaseGeneration: 1 }])
      expect(Eventlog.getBackendHeadFromDb(dbEventlog)).toEqual(1)
      expect(yield* stateHead.get).toEqual(replacement.seqNum)
      expect(dbState.select(`SELECT * FROM ${MATERIALIZATION_JOURNAL_META_TABLE}`)).toEqual([])
      expect(receipt.rolledBackEventNums).toEqual([original.seqNum])
      expect(receipt.committedEvents[0]).not.toBe(replacement)
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test)),
  )

  Vitest.live('persists confirmation metadata with the matching backend head', (test) =>
    Effect.gen(function* () {
      const { committer, dbEventlog, dbState } = yield* setup
      const pending = makeTodoEvent({ global: 1, id: 'confirmed', text: 'Confirmed' })
      yield* committer.commitLocal({ events: [pending] })

      const confirmed = cloneEvent(pending, { syncMetadata: Option.some({ cursor: 'confirmed-1' }) })
      yield* committer.commitUpstream({
        pulledEvents: [confirmed],
        events: [],
        rollbackEvents: [],
        confirmedEvents: [pending],
        backendHead: confirmed.seqNum,
      })

      const cursorInfo = yield* Eventlog.getSyncBackendCursorInfoForDb(dbEventlog, {
        remoteHead: confirmed.seqNum.global,
      })
      expect(Option.getOrThrow(cursorInfo)).toEqual({
        eventSequenceNumber: 1,
        metadata: Option.some({ cursor: 'confirmed-1' }),
      })
      expect(Eventlog.getBackendHeadFromDb(dbEventlog)).toEqual(1)
      expect(dbState.select(`SELECT * FROM ${MATERIALIZATION_JOURNAL_META_TABLE}`)).toEqual([])
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test)),
  )
})

const setup = Effect.gen(function* () {
  const sqlite3 = yield* Effect.promise(() => loadSqlite3Wasm())
  const makeSqliteDb = yield* sqliteDbFactory({ sqlite3 })
  const dbState = yield* makeSqliteDb({ _tag: 'in-memory' })
  const dbEventlog = yield* makeSqliteDb({ _tag: 'in-memory' })
  yield* Eventlog.initEventlogDb(dbEventlog)

  const sqliteDbLayer = Layer.mergeAll(StateSqliteDb.layer(dbState), EventlogSqliteDb.layer(dbEventlog))
  const stateServicesLayer = Layer.mergeAll(StateHead.layer, MaterializationJournal.layer).pipe(
    Layer.provide(sqliteDbLayer),
  )
  const servicesLayer = Layer.mergeAll(sqliteDbLayer, stateServicesLayer)
  const materializeEvent = yield* makeMaterializeEvent({ schema }).pipe(Effect.provide(servicesLayer))

  const bootStatusQueue = yield* Queue.unbounded<BootStatus>()
  yield* recreateDb({ schema, bootStatusQueue, materializeEvent }).pipe(Effect.provide(servicesLayer))
  yield* Queue.shutdown(bootStatusQueue)

  const committer = yield* LeaderSyncCommitter.make({ materializeEvent }).pipe(Effect.provide(servicesLayer))
  const stateHead = yield* StateHead.make.pipe(Effect.provideService(StateSqliteDb.StateSqliteDb, dbState))

  return { committer, dbEventlog, dbState, stateHead }
})

const makeTodoEvent = ({
  global,
  rebaseGeneration = 0,
  parentSeqNum = EventSequenceNumber.Client.ROOT,
  id,
  text,
  syncMetadata = Option.none(),
}: {
  global: number
  rebaseGeneration?: number
  parentSeqNum?: EventSequenceNumber.Client.Composite
  id: string
  text: string
  syncMetadata?: Option.Option<{ cursor: string }>
}) =>
  new LiveStoreEvent.Client.EncodedWithMeta({
    name: events.todoCreated.name,
    args: { id, text },
    seqNum: EventSequenceNumber.Client.Composite.make({ global, client: 0, rebaseGeneration }),
    parentSeqNum,
    clientId: 'test-client',
    sessionId: 'test-session',
    meta: {
      syncMetadata,
      materializerHashLeader: Option.none(),
      materializerHashSession: Option.none(),
    },
  })

const cloneEvent = (
  event: LiveStoreEvent.Client.EncodedWithMeta,
  metaPatch: Partial<LiveStoreEvent.Client.EncodedWithMeta['meta']>,
) => new LiveStoreEvent.Client.EncodedWithMeta({ ...event, meta: { ...event.meta, ...metaPatch } })
