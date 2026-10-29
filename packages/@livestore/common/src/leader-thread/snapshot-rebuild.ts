import { type HttpClient, type Scope, Effect, Option, Queue, Schema, Stream } from '@livestore/utils/effect'

import { type BootStatus, type MakeSqliteDb, type SqliteDb, UnknownError } from '../adapter-types.ts'
import type { MigrationsReport } from '../defs.ts'
import * as EventlogSqliteDb from '../EventlogSqliteDb.ts'
import type * as MaterializationJournal from '../MaterializationJournal.ts'
import { rematerializeFromEventlog } from '../rematerialize-from-eventlog.ts'
import type { LiveStoreSchema } from '../schema/mod.ts'
import { EventSequenceNumber, LiveStoreEvent, SystemTables, UNKNOWN_EVENT_SCHEMA_HASH } from '../schema/mod.ts'
import type { SnapshotBootOptions } from '../snapshot/snapshot.ts'
import * as SnapshotKey from '../snapshot/SnapshotKey.ts'
import * as SqliteDbHelper from '../sqlite-db-helper.ts'
import * as StateSqliteDb from '../StateSqliteDb.ts'
import type * as SyncBackend from '../sync/sync-backend.ts'
import { sql } from '../util.ts'
import * as Eventlog from './eventlog.ts'
import { markStateAsCompleted, recreateDb } from './recreate-db.ts'
import { fetchSnapshot, installState } from './snapshot-boot.ts'
import type { MaterializeEvent } from './types.ts'

/**
 * Rebuilds state for an eventlog that begins at a snapshot base (e.g. after a state schema change). The local eventlog
 * lacks the events at and below the base, so a plain replay would produce wrong state. Two exact strategies:
 *
 * 1. Re-seed: install the snapshot for the new schema key at position `p` with `base <= p <= backendHead`, then replay
 *    the local events after `p` (pending and client-only events included). Only used when no client-only event of this
 *    client sits below `p`, because the snapshot cannot contain it.
 * 2. Backfill: pull the missing events from the sync backend into the eventlog, drop the base and replay everything.
 *    Needs the backend to be reachable; otherwise boot fails, since no correct state can be produced offline.
 */
export const rebuildFromEventlogBase = ({
  schema,
  storeId,
  clientId,
  payload,
  base,
  snapshotOptions,
  syncBackend,
  makeSqliteDb,
  bootStatusQueue,
  materializeEvent,
  stateRebuildBatchSize,
}: {
  schema: LiveStoreSchema
  storeId: string
  clientId: string
  payload: unknown
  base: Eventlog.EventlogBase
  snapshotOptions: SnapshotBootOptions<any> | undefined
  syncBackend: SyncBackend.SyncBackend | undefined
  makeSqliteDb: MakeSqliteDb
  bootStatusQueue: Queue.Queue<BootStatus>
  materializeEvent: MaterializeEvent
  stateRebuildBatchSize?: number
}): Effect.Effect<
  { migrationsReport: MigrationsReport },
  UnknownError,
  | EventlogSqliteDb.EventlogSqliteDb
  | StateSqliteDb.StateSqliteDb
  | MaterializationJournal.MaterializationJournal
  | Scope.Scope
  | HttpClient.HttpClient
> =>
  Effect.gen(function* () {
    if (snapshotOptions !== undefined) {
      const reseeded = yield* reseedFromSnapshot({
        schema,
        storeId,
        clientId,
        payload,
        base,
        snapshotOptions,
        makeSqliteDb,
        bootStatusQueue,
        materializeEvent,
        ...(stateRebuildBatchSize !== undefined ? { stateRebuildBatchSize } : {}),
      })
      if (reseeded === true) return { migrationsReport: { migrations: [] } }
    }

    yield* backfillEventlogBelowBase({ schema, base, syncBackend })

    return yield* recreateDb({
      schema,
      bootStatusQueue,
      materializeEvent,
      ...(stateRebuildBatchSize !== undefined ? { stateRebuildBatchSize } : {}),
    }).pipe(UnknownError.mapToUnknownError)
  }).pipe(Effect.withSpan('@livestore/common:leader-thread:rebuildFromEventlogBase'))

const reseedFromSnapshot = ({
  schema,
  storeId,
  clientId,
  payload,
  base,
  snapshotOptions,
  makeSqliteDb,
  bootStatusQueue,
  materializeEvent,
  stateRebuildBatchSize,
}: {
  schema: LiveStoreSchema
  storeId: string
  clientId: string
  payload: unknown
  base: Eventlog.EventlogBase
  snapshotOptions: SnapshotBootOptions<any>
  makeSqliteDb: MakeSqliteDb
  bootStatusQueue: Queue.Queue<BootStatus>
  materializeEvent: MaterializeEvent
  stateRebuildBatchSize?: number
}) =>
  Effect.gen(function* () {
    const dbState = yield* StateSqliteDb.StateSqliteDb
    const dbEventlog = yield* EventlogSqliteDb.EventlogSqliteDb
    const key = SnapshotKey.toString(SnapshotKey.make(schema))
    const source = yield* snapshotOptions.source({ storeId, clientId, payload })

    const fetched = yield* fetchSnapshot({ source, key, storeId }).pipe(Effect.option, Effect.map(Option.flatten))
    if (Option.isNone(fetched) === true) return false

    const { manifest, bytes } = fetched.value
    const backendHead = Eventlog.getBackendHeadFromDb(dbEventlog)
    const backendId = Eventlog.getBackendIdFromDb(dbEventlog)
    const usable =
      manifest.seqNum >= base.seqNumGlobal &&
      manifest.seqNum <= backendHead &&
      Option.contains(backendId, manifest.backendId) &&
      hasClientOnlyEventsBelow(dbEventlog, manifest.seqNum) === false

    if (usable === false) {
      yield* Effect.logDebug(
        `Snapshot at ${manifest.seqNum} cannot re-seed an eventlog based at ${base.seqNumGlobal} with backend head ${backendHead}`,
      )
      return false
    }

    yield* installState({ dbState, bytes, manifest, makeSqliteDb })
    yield* rematerializeFromEventlog({
      dbState,
      schema,
      materializeEvent,
      since: Eventlog.baseToSeqNum(EventSequenceNumber.Global.make(manifest.seqNum)),
      onProgress: ({ done, total }) =>
        Queue.offer(bootStatusQueue, { stage: 'rehydrating', progress: { done, total } }),
      ...(stateRebuildBatchSize !== undefined ? { batchSize: stateRebuildBatchSize } : {}),
    })
    yield* markStateAsCompleted(dbState)

    return true
  }).pipe(
    // Any failure leaves the backfill strategy, which starts from an empty state database anyway.
    Effect.catch((error) => Effect.logWarning('Re-seeding state from a snapshot failed', error).pipe(Effect.as(false))),
  )

/** Client-only events below `seqNumGlobal` exist only locally, so a snapshot cannot stand in for them. */
const hasClientOnlyEventsBelow = (dbEventlog: SqliteDb, seqNumGlobal: number) =>
  dbEventlog.select<{ found: number }>(
    sql`SELECT 1 AS found FROM ${SystemTables.EVENTLOG_META_TABLE}
        WHERE seqNumClient > ${EventSequenceNumber.Client.DEFAULT} AND seqNumGlobal < ${seqNumGlobal} LIMIT 1`,
  ).length > 0

/**
 * Pulls the backend events up to and including the base into the eventlog and removes the base, making the eventlog
 * complete again so the regular full replay applies.
 */
const backfillEventlogBelowBase = Effect.fn('@livestore/common:leader-thread:backfillEventlogBelowBase')(function* ({
  schema,
  base,
  syncBackend,
}: {
  schema: LiveStoreSchema
  base: Eventlog.EventlogBase
  syncBackend: SyncBackend.SyncBackend | undefined
}) {
  const dbEventlog = yield* EventlogSqliteDb.EventlogSqliteDb

  if (syncBackend === undefined) {
    return yield* UnknownError.make({
      cause: 'Cannot rebuild state: the eventlog starts at a snapshot base and no sync backend is configured',
    })
  }

  let reachedBase = false
  yield* syncBackend.pull(Option.none(), { live: false }).pipe(
    Stream.takeUntil(() => reachedBase),
    Stream.runForEach(({ batch }) =>
      Effect.gen(function* () {
        const missing = batch.filter(({ eventEncoded }) => eventEncoded.seqNum <= base.seqNumGlobal)
        yield* Effect.forEach(
          missing,
          ({ eventEncoded, metadata }) => {
            const event = LiveStoreEvent.Client.fromGlobal(eventEncoded)
            const eventDef = schema.eventsDefsMap.get(event.name)
            return Eventlog.insertIntoEventlog(
              event,
              dbEventlog,
              eventDef === undefined ? UNKNOWN_EVENT_SCHEMA_HASH : Schema.hash(eventDef.schema),
              event.clientId,
              event.sessionId,
              metadata,
            )
          },
          { discard: true },
        ).pipe(SqliteDbHelper.withSavepoint(dbEventlog))

        if (batch.some(({ eventEncoded }) => eventEncoded.seqNum >= base.seqNumGlobal) === true) reachedBase = true
      }),
    ),
    Effect.mapError(
      (cause) =>
        new UnknownError({
          cause,
          note: 'Cannot rebuild state: backfilling the events below the snapshot base from the sync backend failed',
        }),
    ),
  )

  if (reachedBase === false) {
    return yield* UnknownError.make({
      cause: `Sync backend did not return the events up to the snapshot base ${base.seqNumGlobal}`,
    })
  }

  yield* Eventlog.clearEventlogBase(dbEventlog)
})
