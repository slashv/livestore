import { Context, Effect, Layer, Option } from '@livestore/utils/effect'

import { MaterializeError, SqliteError, type SqliteDb } from '../adapter-types.ts'
import * as EventlogSqliteDb from '../EventlogSqliteDb.ts'
import * as MaterializationJournal from '../MaterializationJournal.ts'
import { EventSequenceNumber, LiveStoreEvent } from '../schema/mod.ts'
import * as StateHead from '../StateHead.ts'
import * as StateSqliteDb from '../StateSqliteDb.ts'
import * as Eventlog from './eventlog.ts'
import type { MaterializeEvent } from './types.ts'

export const TypeId = '~@livestore/common/LeaderSyncCommitter' as const
export type TypeId = typeof TypeId

export interface LocalCommitPlan {
  readonly events: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
}

export interface UpstreamCommitPlan {
  /** The events received in the backend chunk, including metadata used to confirm pending events. */
  readonly pulledEvents: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
  /** The merged events to materialize, including any locally rebased pending suffix. */
  readonly events: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
  readonly rollbackEvents: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
  readonly confirmedEvents: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
  readonly backendHead: EventSequenceNumber.Client.Composite
}

export interface LocalCommitReceipt {
  readonly _tag: 'local-commit'
  readonly committedEvents: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
  readonly stateHead: EventSequenceNumber.Client.Composite
}

export interface UpstreamCommitReceipt {
  readonly _tag: 'upstream-commit'
  readonly committedEvents: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>
  readonly rolledBackEventNums: ReadonlyArray<EventSequenceNumber.Client.Composite>
  readonly stateHead: EventSequenceNumber.Client.Composite
  readonly backendHead: EventSequenceNumber.Client.Composite
}

export type CommitError = MaterializeError | MaterializationJournal.MaterializationJournalError

export interface Service {
  readonly [TypeId]: TypeId
  readonly commitLocal: (plan: LocalCommitPlan) => Effect.Effect<LocalCommitReceipt, CommitError>
  readonly commitUpstream: (plan: UpstreamCommitPlan) => Effect.Effect<UpstreamCommitReceipt, CommitError>
}

/** Owns the durable state/eventlog boundary for leader sync transitions. */
export class LeaderSyncCommitter extends Context.Service<LeaderSyncCommitter, Service>()(
  '@livestore/common/LeaderSyncCommitter',
) {}

export const make = ({ materializeEvent }: { materializeEvent: MaterializeEvent }) =>
  Effect.gen(function* () {
    const dbState = yield* StateSqliteDb.StateSqliteDb
    const dbEventlog = yield* EventlogSqliteDb.EventlogSqliteDb
    const materializationJournal = yield* MaterializationJournal.MaterializationJournal
    const stateHead = yield* StateHead.StateHead

    const materializeEvents = (events: ReadonlyArray<LiveStoreEvent.Client.EncodedWithMeta>) =>
      Effect.forEach(events, (event) =>
        Effect.gen(function* () {
          const eventToMaterialize = cloneEvent(event)
          const { hash } = yield* materializeEvent(eventToMaterialize)
          return freezeEvent(cloneEvent(eventToMaterialize, { materializerHashLeader: hash }))
        }),
      )

    const commitLocal: Service['commitLocal'] = ({ events }) =>
      withCoordinatedTransactions(
        { dbState, dbEventlog },
        Effect.gen(function* () {
          const committedEvents = yield* materializeEvents(events)
          const persistedStateHead = yield* stateHead.get.pipe(
            Effect.mapError((cause) => MaterializeError.make({ cause })),
          )

          return freezeReceipt({
            _tag: 'local-commit' as const,
            committedEvents: freezeArray(committedEvents),
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
            const headAfterRollback = plan.rollbackEvents[0]!.parentSeqNum
            yield* materializationJournal.rollback(rollbackEventNums)
            yield* stateHead.set(headAfterRollback).pipe(Effect.mapError((cause) => MaterializeError.make({ cause })))
            yield* Eventlog.deleteEvents(dbEventlog, rollbackEventNums).pipe(
              Effect.mapError((cause) => MaterializeError.make({ cause })),
            )
          }

          const committedEvents = yield* materializeEvents(plan.events)

          if (plan.confirmedEvents.length > 0) {
            const confirmedPulledEvents = plan.pulledEvents.filter((event) =>
              plan.confirmedEvents.some((confirmedEvent) =>
                EventSequenceNumber.Client.isEqual(event.seqNum, confirmedEvent.seqNum),
              ),
            )
            yield* Eventlog.updateSyncMetadataForDb(dbEventlog, confirmedPulledEvents).pipe(
              Effect.mapError((cause) => MaterializeError.make({ cause })),
            )
          }

          yield* materializationJournal.discardUpTo(plan.backendHead)

          // The backend head and the corresponding event inserts share this eventlog transaction.
          yield* updateBackendHead(dbEventlog, plan.backendHead)

          const persistedStateHead = yield* stateHead.get.pipe(
            Effect.mapError((cause) => MaterializeError.make({ cause })),
          )

          return freezeReceipt({
            _tag: 'upstream-commit' as const,
            committedEvents: freezeArray(committedEvents),
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

    return LeaderSyncCommitter.of({ [TypeId]: TypeId, commitLocal, commitUpstream })
  })

export const layer = (options: { materializeEvent: MaterializeEvent }) =>
  Layer.effect(LeaderSyncCommitter, make(options))

const withCoordinatedTransactions = <A, E, R>(
  { dbState, dbEventlog }: { dbState: SqliteDb; dbEventlog: SqliteDb },
  effect: Effect.Effect<A, E, R>,
): Effect.Effect<A, E | MaterializeError, R> => {
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
  }).pipe(Effect.ensuring(rollbackOpenTransactions), Effect.uninterruptible)
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

const cloneEvent = (
  event: LiveStoreEvent.Client.EncodedWithMeta,
  metaPatch?: Partial<LiveStoreEvent.Client.EncodedWithMeta['meta']>,
) =>
  new LiveStoreEvent.Client.EncodedWithMeta({
    ...event,
    args: structuredClone(event.args),
    seqNum: EventSequenceNumber.Client.Composite.make({ ...event.seqNum }),
    parentSeqNum: EventSequenceNumber.Client.Composite.make({ ...event.parentSeqNum }),
    meta: {
      syncMetadata: Option.map(event.meta.syncMetadata, structuredClone),
      materializerHashLeader: Option.map(event.meta.materializerHashLeader, (hash) => hash),
      materializerHashSession: Option.map(event.meta.materializerHashSession, (hash) => hash),
      ...metaPatch,
    },
  })

const freezeEvent = (event: LiveStoreEvent.Client.EncodedWithMeta) => deepFreeze(event)

const freezeSeqNum = (seqNum: EventSequenceNumber.Client.Composite) =>
  Object.freeze(EventSequenceNumber.Client.Composite.make({ ...seqNum }))

const freezeArray = <A>(items: ReadonlyArray<A>): ReadonlyArray<A> => Object.freeze([...items])

const freezeReceipt = <A extends object>(receipt: A): Readonly<A> => Object.freeze(receipt)

const deepFreeze = <A>(value: A): A => {
  if (typeof value !== 'object' || value === null || Object.isFrozen(value) === true) return value
  for (const nested of Object.values(value)) deepFreeze(nested)
  return Object.freeze(value)
}
