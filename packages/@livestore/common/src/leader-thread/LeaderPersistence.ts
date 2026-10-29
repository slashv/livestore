import type { Schema } from '@livestore/utils/effect'
import { Context, Effect, Option, Semaphore } from '@livestore/utils/effect'

import { MaterializeError, SqliteError, type SqliteDb, UnknownError } from '../adapter-types.ts'
import * as EventlogSqliteDb from '../EventlogSqliteDb.ts'
import * as MaterializationJournal from '../MaterializationJournal.ts'
import { EventSequenceNumber, LiveStoreEvent } from '../schema/mod.ts'
import { EVENTLOG_META_TABLE } from '../schema/state/sqlite/system-tables/eventlog-tables.ts'
import * as StateHead from '../StateHead.ts'
import * as StateSqliteDb from '../StateSqliteDb.ts'
import * as Eventlog from './eventlog.ts'
import type { MaterializeEvent } from './types.ts'

export const TypeId = '~@livestore/common/LeaderPersistence' as const
export type TypeId = typeof TypeId

export interface LocalPersistPlan {
  readonly events: ReadonlyArray<LiveStoreEvent.Client.Encoded>
}

export interface PulledEvent {
  readonly event: LiveStoreEvent.Client.Encoded
  readonly syncMetadata: Option.Option<Schema.Json>
}

export interface UpstreamPersistPlan {
  /** The events received in the backend chunk, including metadata used to confirm pending events. */
  readonly pulledEvents: ReadonlyArray<PulledEvent>
  /** The merged events to materialize, including any locally rebased pending suffix. */
  readonly events: ReadonlyArray<LiveStoreEvent.Client.Encoded>
  readonly rollbackEvents: ReadonlyArray<LiveStoreEvent.Client.Encoded>
  readonly confirmedEvents: ReadonlyArray<LiveStoreEvent.Client.Encoded>
  readonly backendHead: EventSequenceNumber.Client.Composite
}

export interface LocalPersistReceipt {
  readonly _tag: 'local-persist'
  readonly persistedEvents: ReadonlyArray<LiveStoreEvent.Client.Encoded>
  readonly materializerHashes: ReadonlyArray<LiveStoreEvent.Client.MaterializerHash>
  readonly stateHead: EventSequenceNumber.Client.Composite
}

export interface UpstreamPersistReceipt {
  readonly _tag: 'upstream-persist'
  readonly persistedEvents: ReadonlyArray<LiveStoreEvent.Client.Encoded>
  readonly materializerHashes: ReadonlyArray<LiveStoreEvent.Client.MaterializerHash>
  readonly rolledBackEventNums: ReadonlyArray<EventSequenceNumber.Client.Composite>
  readonly stateHead: EventSequenceNumber.Client.Composite
  readonly backendHead: EventSequenceNumber.Client.Composite
}

export type PersistError = MaterializeError | MaterializationJournal.MaterializationJournalError

/**
 * A capture of the state database taken at a moment consistent with the eventlog position it reflects.
 * `data` is whatever the capture callback of {@link Service.exportSnapshotWith} returned.
 */
export interface StateSnapshotCapture<T> {
  readonly data: T
  /** Global sequence number of the last event reflected by the captured state. Always a synced position. */
  readonly seqNum: EventSequenceNumber.Global.Type
  /** Sync metadata of the event at `seqNum`, used by booting clients as the pull cursor metadata. */
  readonly cursorMetadata: Option.Option<Schema.Json>
  readonly backendId: string
  readonly exportMs: number
}

/** A state database export consistent with the eventlog position it reflects. */
export interface StateSnapshotExport extends Omit<StateSnapshotCapture<never>, 'data'> {
  /** Serialized state database (rollback-journal format). */
  readonly bytes: Uint8Array<ArrayBuffer>
}

export interface Service {
  readonly [TypeId]: TypeId
  readonly persistLocal: (plan: LocalPersistPlan) => Effect.Effect<LocalPersistReceipt, PersistError>
  readonly persistUpstream: (plan: UpstreamPersistPlan) => Effect.Effect<UpstreamPersistReceipt, PersistError>
  /**
   * Exports the state database for publication as a snapshot. Runs between persist transactions, so the export never
   * contains a half-applied batch. Fails unless state reflects exactly the backend head (no pending or client-only
   * events), which holds for a checkpointer that never pushes.
   */
  readonly exportSnapshot: Effect.Effect<StateSnapshotExport, UnknownError>
  /**
   * Same validations and consistency as {@link exportSnapshot}, but hands the consistent moment to `capture` instead of
   * serializing the database. `capture` runs synchronously while the persistence lock is held and no persist
   * transaction is open, so every committed page has reached the database's storage. Platforms whose state database
   * lives in external storage use it to copy pages there without materializing the state in memory.
   */
  readonly exportSnapshotWith: <T>(
    capture: (dbState: SqliteDb) => T,
  ) => Effect.Effect<StateSnapshotCapture<T>, UnknownError>
  /**
   * Serializes the state database between persist transactions. Exports taken while a transaction has yielded to other
   * fibers would contain a half-applied batch, so every leader-side state export should go through this.
   */
  readonly exportState: Effect.Effect<{
    readonly bytes: Uint8Array<ArrayBuffer>
    /** State head reflected by `bytes`, read under the same lock. */
    readonly head: EventSequenceNumber.Client.Composite
  }>
}

/**
 * Durable SQLite boundary for leader-sync transitions: rollback, materialization, journal maintenance, eventlog writes
 * and head updates. Queues, retries, publication and acknowledgements stay in `LeaderSyncProcessor`, which publishes
 * only what a successful receipt reports.
 */
export class LeaderPersistence extends Context.Service<LeaderPersistence, Service>()(
  '@livestore/common/LeaderPersistence',
) {}

export const make = ({ materializeEvent }: { materializeEvent: MaterializeEvent }) =>
  Effect.gen(function* () {
    const dbState = yield* StateSqliteDb.StateSqliteDb
    const dbEventlog = yield* EventlogSqliteDb.EventlogSqliteDb
    const materializationJournal = yield* MaterializationJournal.MaterializationJournal
    const stateHead = yield* StateHead.StateHead
    // Serializes persist transactions with snapshot exports. Effect may yield to other fibers inside a transaction,
    // and an export taken then would include uncommitted writes.
    const lock = yield* Semaphore.make(1)
    // Client-only events never reach the backend, yet their effects stay in state after they leave the pending set. A
    // leader that recorded any can therefore never export a snapshot that equals synced history. Computed lazily (one
    // eventlog scan on the first export) and kept current by `persistLocal`.
    let hasClientOnlyEvents: boolean | undefined

    const materializeEvents = (
      events: ReadonlyArray<LiveStoreEvent.Client.Encoded>,
      pulledEvents: ReadonlyArray<PulledEvent> = [],
    ) =>
      Effect.forEach(events, (event) =>
        Effect.gen(function* () {
          const syncMetadata =
            pulledEvents.find(({ event: pulledEvent }) =>
              EventSequenceNumber.Client.isEqual(event.seqNum, pulledEvent.seqNum),
            )?.syncMetadata ?? Option.none()
          const { hash } = yield* materializeEvent(event, { syncMetadata })
          return LiveStoreEvent.Client.MaterializerHash.make({ eventNum: event.seqNum, hash })
        }),
      )

    const persistLocal: Service['persistLocal'] = ({ events }) =>
      withCoordinatedTransactions(
        lock,
        { dbState, dbEventlog },
        Effect.gen(function* () {
          if (events.some((event) => event.seqNum.client !== EventSequenceNumber.Client.DEFAULT) === true) {
            hasClientOnlyEvents = true
          }
          const materializerHashes = yield* materializeEvents(events)
          const persistedStateHead = yield* stateHead.get.pipe(
            Effect.mapError((cause) => MaterializeError.make({ cause })),
          )

          return {
            _tag: 'local-persist' as const,
            persistedEvents: events,
            materializerHashes,
            stateHead: persistedStateHead,
          }
        }),
      ).pipe(
        Effect.withSpan('@livestore/common:LeaderPersistence:persistLocal', {
          attributes: { batchSize: events.length },
        }),
      )

    const persistUpstream: Service['persistUpstream'] = (plan) =>
      withCoordinatedTransactions(
        lock,
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

          const materializerHashes = yield* materializeEvents(plan.events, plan.pulledEvents)

          if (plan.confirmedEvents.length > 0) {
            // Confirmed local events are already materialized. We only add the metadata learned from the backend.
            const confirmedPulledEvents = plan.pulledEvents.filter(({ event }) =>
              plan.confirmedEvents.some((confirmedEvent) =>
                isSameSequencePosition(event.seqNum, confirmedEvent.seqNum),
              ),
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

          return {
            _tag: 'upstream-persist' as const,
            persistedEvents: plan.events,
            materializerHashes,
            rolledBackEventNums: rollbackEventNums,
            stateHead: persistedStateHead,
            backendHead: plan.backendHead,
          }
        }),
      ).pipe(
        Effect.withSpan('@livestore/common:LeaderPersistence:persistUpstream', {
          attributes: {
            batchSize: plan.events.length,
            rollbackCount: plan.rollbackEvents.length,
            confirmedCount: plan.confirmedEvents.length,
          },
        }),
      )

    const exportSnapshotWith: Service['exportSnapshotWith'] = Effect.fn(
      '@livestore/common:LeaderPersistence:exportSnapshot',
    )(
      function* <T>(capture: (dbState: SqliteDb) => T) {
        const start = performance.now()
        const head = yield* stateHead.get.pipe(UnknownError.mapToUnknownError)
        const backendHead = Eventlog.getBackendHeadFromDb(dbEventlog)

        if (head.client !== EventSequenceNumber.Client.DEFAULT || head.global !== backendHead) {
          return yield* UnknownError.make({
            cause: `State head ${EventSequenceNumber.Client.toString(head)} is not the backend head ${backendHead}; only a client without local events can export a snapshot`,
          })
        }

        hasClientOnlyEvents ??=
          dbEventlog.select<{ found: number }>(
            `SELECT 1 AS found FROM ${EVENTLOG_META_TABLE} WHERE seqNumClient > ${EventSequenceNumber.Client.DEFAULT} LIMIT 1`,
          ).length > 0
        if (hasClientOnlyEvents === true) {
          return yield* UnknownError.make({
            cause:
              'This client recorded client-only events, so its state is not pure synced history and cannot be exported',
          })
        }

        const backendId = Eventlog.getBackendIdFromDb(dbEventlog)
        if (Option.isNone(backendId) === true) {
          return yield* UnknownError.make({
            cause: 'Cannot export a snapshot before the sync backend identity is known',
          })
        }

        const cursorInfo = yield* Eventlog.getSyncBackendCursorInfoForDb(dbEventlog, { remoteHead: head.global })
        const data = yield* Effect.try(() => capture(dbState))

        return {
          data,
          seqNum: head.global,
          cursorMetadata: Option.flatMap(cursorInfo, (info) => info.metadata),
          backendId: backendId.value,
          exportMs: performance.now() - start,
        } satisfies StateSnapshotCapture<T>
      },
      lock.withPermits(1),
      UnknownError.mapToUnknownError,
    )

    const exportSnapshot: Service['exportSnapshot'] = exportSnapshotWith((db) => db.export()).pipe(
      Effect.map(({ data, ...rest }) => ({ ...rest, bytes: data })),
    )

    const exportState: Service['exportState'] = Effect.gen(function* () {
      const head = yield* stateHead.get.pipe(Effect.orDie)
      return { bytes: dbState.export(), head }
    }).pipe(lock.withPermits(1))

    return LeaderPersistence.of({
      [TypeId]: TypeId,
      persistLocal,
      persistUpstream,
      exportSnapshot,
      exportSnapshotWith,
      exportState,
    })
  })

/**
 * Parent linkage identifies a position in the event chain. Rebase generation describes the version of optimistic
 * history, which the backend does not preserve, so it is not part of position identity.
 */
export const isSameSequencePosition = (
  left: EventSequenceNumber.Client.Composite,
  right: EventSequenceNumber.Client.Composite,
) => left.global === right.global && left.client === right.client

const withCoordinatedTransactions = <A, E, R>(
  lock: Semaphore.Semaphore,
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
    lock.withPermits(1),
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
