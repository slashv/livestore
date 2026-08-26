import type { Schema } from '@livestore/utils/effect'
import { Context, Effect, Layer, Option } from '@livestore/utils/effect'

import { MaterializeError, SqliteError, type SqliteDb, UnknownError } from '../adapter-types.ts'
import * as EventlogSqliteDb from '../EventlogSqliteDb.ts'
import * as MaterializationJournal from '../MaterializationJournal.ts'
import { EventSequenceNumber, LiveStoreEvent } from '../schema/mod.ts'
import { EVENTLOG_META_TABLE, SYNC_STATUS_TABLE } from '../schema/state/sqlite/system-tables/eventlog-tables.ts'
import * as StateHead from '../StateHead.ts'
import * as StateSqliteDb from '../StateSqliteDb.ts'
import { sql } from '../util.ts'
import * as Eventlog from './eventlog.ts'
import type { MaterializeEvent } from './types.ts'

export const TypeId = '~@livestore/common/LeaderSyncCommitter' as const
export type TypeId = typeof TypeId

export interface LocalCommitPlan {
  readonly events: ReadonlyArray<LiveStoreEvent.Client.Encoded>
}

export interface PulledEvent {
  readonly event: LiveStoreEvent.Client.Encoded
  readonly syncMetadata: Option.Option<Schema.Json>
}

export interface UpstreamCommitPlan {
  /** The events received in the backend chunk, including metadata used to confirm pending events. */
  readonly pulledEvents: ReadonlyArray<PulledEvent>
  /** The merged events to materialize, including any locally rebased pending suffix. */
  readonly events: ReadonlyArray<LiveStoreEvent.Client.Encoded>
  readonly rollbackEvents: ReadonlyArray<LiveStoreEvent.Client.Encoded>
  readonly confirmedEvents: ReadonlyArray<LiveStoreEvent.Client.Encoded>
  readonly backendHead: EventSequenceNumber.Client.Composite
}

export interface LocalCommitReceipt {
  readonly _tag: 'local-commit'
  readonly committedEvents: ReadonlyArray<LiveStoreEvent.Client.Encoded>
  readonly materializerHashes: ReadonlyArray<LiveStoreEvent.Client.MaterializerHash>
  readonly stateHead: EventSequenceNumber.Client.Composite
}

export interface UpstreamCommitReceipt {
  readonly _tag: 'upstream-commit'
  readonly committedEvents: ReadonlyArray<LiveStoreEvent.Client.Encoded>
  readonly materializerHashes: ReadonlyArray<LiveStoreEvent.Client.MaterializerHash>
  readonly rolledBackEventNums: ReadonlyArray<EventSequenceNumber.Client.Composite>
  readonly stateHead: EventSequenceNumber.Client.Composite
  readonly backendHead: EventSequenceNumber.Client.Composite
}

export type CommitError = MaterializeError | MaterializationJournal.MaterializationJournalError

export interface Service {
  readonly [TypeId]: TypeId
  readonly commitLocal: (plan: LocalCommitPlan) => Effect.Effect<LocalCommitReceipt, CommitError>
  readonly commitUpstream: (plan: UpstreamCommitPlan) => Effect.Effect<UpstreamCommitReceipt, CommitError>
  readonly resetLocalDatabases: Effect.Effect<void, UnknownError>
}

/**
 * Durable SQLite boundary for leader-sync transitions.
 *
 * `commitLocal` and `commitUpstream` handle rollback, materialization, journal maintenance, eventlog writes, and head
 * updates. They return immutable receipts so the processor publishes exactly what was committed, without changing the
 * caller's plan in place.
 *
 * Provider and session queues, retries, publication, acknowledgements, and in-memory sync state stay in
 * `LeaderSyncProcessor`. The two SQLite databases cannot be crash-atomic together; state is committed first so the
 * eventlog never claims that a state transition was durable when the state commit itself failed.
 */
export class LeaderSyncCommitter extends Context.Service<LeaderSyncCommitter, Service>()(
  '@livestore/common/LeaderSyncCommitter',
) {}

export const make = ({ materializeEvent }: { materializeEvent: MaterializeEvent }) =>
  Effect.gen(function* () {
    const dbState = yield* StateSqliteDb.StateSqliteDb
    const dbEventlog = yield* EventlogSqliteDb.EventlogSqliteDb
    const materializationJournal = yield* MaterializationJournal.MaterializationJournal
    const stateHead = yield* StateHead.StateHead

    const materializeEvents = (
      events: ReadonlyArray<LiveStoreEvent.Client.Encoded>,
      pulledEvents: ReadonlyArray<PulledEvent> = [],
    ) =>
      Effect.forEach(events, (event) =>
        Effect.gen(function* () {
          // Materialization may enrich the event, so work on a copy and return that committed version in the receipt.
          // This keeps the caller's plan immutable and makes publication use the exact durable value.
          const eventToMaterialize = cloneEvent(event)
          const syncMetadata =
            pulledEvents.find(({ event: pulledEvent }) =>
              EventSequenceNumber.Client.isEqual(event.seqNum, pulledEvent.seqNum),
            )?.syncMetadata ?? Option.none()
          const { hash } = yield* materializeEvent(eventToMaterialize, { syncMetadata })
          return {
            event: freezeEvent(eventToMaterialize),
            materializerHash: deepFreeze(
              LiveStoreEvent.Client.MaterializerHash.make({
                eventNum: freezeSeqNum(eventToMaterialize.seqNum),
                hash,
              }),
            ),
          }
        }),
      )

    const commitLocal: Service['commitLocal'] = ({ events }) =>
      withCoordinatedTransactions(
        { dbState, dbEventlog },
        Effect.gen(function* () {
          const materialized = yield* materializeEvents(events)
          const persistedStateHead = yield* stateHead.get.pipe(
            Effect.mapError((cause) => MaterializeError.make({ cause })),
          )

          return freezeReceipt({
            _tag: 'local-commit' as const,
            committedEvents: freezeArray(materialized.map(({ event }) => event)),
            materializerHashes: freezeArray(materialized.map(({ materializerHash }) => materializerHash)),
            stateHead: freezeSeqNum(persistedStateHead),
          })
        }),
      ).pipe(
        Effect.withSpan('@livestore/common:LeaderSyncCommitter:commitLocal', {
          attributes: { batchSize: events.length },
        }),
      )

    const commitUpstream: Service['commitUpstream'] = (plan) =>
      withCoordinatedTransactions(
        { dbState, dbEventlog },
        Effect.gen(function* () {
          const rollbackEventNums = plan.rollbackEvents.map((event) => event.seqNum)

          if (rollbackEventNums.length > 0) {
            // A rebase first restores the old materialized state, then removes the events that no longer belong
            // in the eventlog. The replacement events can then be applied from the restored head.
            const headAfterRollback = plan.rollbackEvents[0]!.parentSeqNum
            yield* materializationJournal.rollback(rollbackEventNums)
            yield* stateHead.set(headAfterRollback).pipe(Effect.mapError((cause) => MaterializeError.make({ cause })))
            yield* Eventlog.deleteEvents(dbEventlog, rollbackEventNums).pipe(
              Effect.mapError((cause) => MaterializeError.make({ cause })),
            )
          }

          const materialized = yield* materializeEvents(plan.events, plan.pulledEvents)

          if (plan.confirmedEvents.length > 0) {
            // Confirmed local events are already materialized. We only add the metadata learned from the backend.
            const confirmedPulledEvents = plan.pulledEvents.filter(({ event }) =>
              plan.confirmedEvents.some((confirmedEvent) => isSameEventPosition(event.seqNum, confirmedEvent.seqNum)),
            )
            yield* Eventlog.updateSyncMetadataForDb(dbEventlog, confirmedPulledEvents).pipe(
              Effect.mapError((cause) => MaterializeError.make({ cause })),
            )
          }

          // Once the backend has reached this head, journal entries before it are no longer needed for a future rebase.
          yield* materializationJournal.discardUpTo(plan.backendHead)

          // The backend head and the corresponding event inserts share this eventlog transaction.
          yield* updateBackendHead(dbEventlog, plan.backendHead)

          const persistedStateHead = yield* stateHead.get.pipe(
            Effect.mapError((cause) => MaterializeError.make({ cause })),
          )

          return freezeReceipt({
            _tag: 'upstream-commit' as const,
            committedEvents: freezeArray(materialized.map(({ event }) => event)),
            materializerHashes: freezeArray(materialized.map(({ materializerHash }) => materializerHash)),
            rolledBackEventNums: freezeArray(rollbackEventNums.map(freezeSeqNum)),
            stateHead: freezeSeqNum(persistedStateHead),
            backendHead: freezeSeqNum(plan.backendHead),
          })
        }),
      ).pipe(
        Effect.withSpan('@livestore/common:LeaderSyncCommitter:commitUpstream', {
          attributes: {
            batchSize: plan.events.length,
            rollbackCount: plan.rollbackEvents.length,
            confirmedCount: plan.confirmedEvents.length,
          },
        }),
      )

    const resetLocalDatabases = Effect.try({
      try: () => {
        // A backend identity change means none of the local materialized state can be trusted. Clearing both
        // databases lets the normal boot path rebuild them from the new backend.
        dbEventlog.execute(sql`DELETE FROM ${EVENTLOG_META_TABLE}`)
        dbEventlog.execute(sql`DELETE FROM ${SYNC_STATUS_TABLE}`)
        const tables = dbState.select<{ name: string }>(
          sql`SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'`,
        )
        for (const { name } of tables) dbState.execute(`DROP TABLE IF EXISTS "${name}"`)
      },
      catch: (cause) => UnknownError.make({ cause, note: 'Failed to reset local databases after backend mismatch' }),
    })

    return LeaderSyncCommitter.of({ [TypeId]: TypeId, commitLocal, commitUpstream, resetLocalDatabases })
  })

export const layer = (options: { materializeEvent: MaterializeEvent }) =>
  Layer.effect(LeaderSyncCommitter, make(options))

const withCoordinatedTransactions = <A, E, R>(
  { dbState, dbEventlog }: { dbState: SqliteDb; dbEventlog: SqliteDb },
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | MaterializeError, R> => {
  // These flags let cleanup roll back whichever transaction was actually opened or left behind by a failed commit.
  let stateTransactionOpen = false
  let eventlogTransactionOpen = false

  const rollbackOpenTransactions = Effect.gen(function* () {
    if (eventlogTransactionOpen === true) {
      yield* executeTransactionStatement(dbEventlog, 'ROLLBACK').pipe(Effect.ignore)
      eventlogTransactionOpen = false
    }
    if (stateTransactionOpen === true) {
      yield* executeTransactionStatement(dbState, 'ROLLBACK').pipe(Effect.ignore)
      stateTransactionOpen = false
    }
  })

  return Effect.gen(function* () {
    yield* executeTransactionStatement(dbState, 'BEGIN TRANSACTION')
    stateTransactionOpen = true
    yield* executeTransactionStatement(dbEventlog, 'BEGIN TRANSACTION')
    eventlogTransactionOpen = true

    const result = yield* effect

    // SQLite cannot atomically commit independent databases. Commit state first so a failed state
    // commit never publishes an eventlog head that claims the state transition is durable.
    yield* executeTransactionStatement(dbState, 'COMMIT')
    stateTransactionOpen = false
    yield* executeTransactionStatement(dbEventlog, 'COMMIT')
    eventlogTransactionOpen = false

    return result
  }).pipe(
    Effect.ensuring(rollbackOpenTransactions),
    // Do not allow cancellation between the two commits and the cleanup that follows a failure.
    Effect.uninterruptible,
  )
}

const executeTransactionStatement = (db: SqliteDb, statement: 'BEGIN TRANSACTION' | 'COMMIT' | 'ROLLBACK') =>
  Effect.try({
    try: () => db.execute(statement),
    catch: (cause) =>
      MaterializeError.make({
        cause: new SqliteError({ cause, query: { sql: statement, bindValues: [] } }),
        note: `Leader sync transaction failed during ${statement}`,
      }),
  })

const updateBackendHead = (dbEventlog: SqliteDb, head: EventSequenceNumber.Client.Composite) =>
  Effect.try({
    try: () => Eventlog.updateBackendHead(dbEventlog, head),
    catch: (cause) =>
      MaterializeError.make({
        cause: new SqliteError({ cause }),
        note: 'Failed to persist the leader sync backend head',
      }),
  })

const cloneEvent = (event: LiveStoreEvent.Client.Encoded) =>
  LiveStoreEvent.Client.Encoded.make({
    ...event,
    args: structuredClone(event.args),
    seqNum: EventSequenceNumber.Client.Composite.make({ ...event.seqNum }),
    parentSeqNum: EventSequenceNumber.Client.Composite.make({ ...event.parentSeqNum }),
  })

const freezeEvent = (event: LiveStoreEvent.Client.Encoded) => deepFreeze(event)

const freezeSeqNum = (seqNum: EventSequenceNumber.Client.Composite) =>
  Object.freeze(EventSequenceNumber.Client.Composite.make({ ...seqNum }))

const freezeArray = <A>(items: ReadonlyArray<A>): ReadonlyArray<A> => Object.freeze([...items])

const freezeReceipt = <A extends object>(receipt: A): Readonly<A> => Object.freeze(receipt)

const isSameEventPosition = (left: EventSequenceNumber.Client.Composite, right: EventSequenceNumber.Client.Composite) =>
  left.global === right.global && left.client === right.client

const deepFreeze = <A>(value: A): A => {
  if (typeof value !== 'object' || value === null || Object.isFrozen(value) === true) return value
  for (const nested of Object.values(value)) deepFreeze(nested)
  return Object.freeze(value)
}
