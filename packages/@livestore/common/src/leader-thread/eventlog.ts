import { LS_DEV, shouldNeverHappen } from '@livestore/utils'
import { Effect, Option, ReadonlyArray, Schema } from '@livestore/utils/effect'

import { type SqliteDb, UnknownError } from '../adapter-types.ts'
import * as EventlogSqliteDb from '../EventlogSqliteDb.ts'
import { migrateTable } from '../schema-management/migrations.ts'
import * as EventSequenceNumber from '../schema/EventSequenceNumber/mod.ts'
import * as LiveStoreEvent from '../schema/LiveStoreEvent/mod.ts'
import {
  EVENTLOG_BASE_TABLE,
  EVENTLOG_META_TABLE,
  eventlogMetaTable,
  eventlogSystemTables,
  SYNC_STATUS_TABLE,
} from '../schema/state/sqlite/system-tables/eventlog-tables.ts'
import { insertRow, updateRows } from '../sql-queries/sql-queries.ts'
import * as SqliteDbHelper from '../sqlite-db-helper.ts'
import type { PreparedBindValues } from '../util.ts'
import { sql } from '../util.ts'
import { execSql } from './connection.ts'
import type { InitialSyncInfo, StreamEventsOptions } from './types.ts'
import { STREAM_EVENTS_BATCH_SIZE_DEFAULT } from './types.ts'

export const initEventlogDb = (dbEventlog: SqliteDb) =>
  Effect.gen(function* () {
    for (const tableDef of eventlogSystemTables) {
      yield* migrateTable({
        db: dbEventlog,
        behaviour: 'create-if-not-exists',
        tableAst: tableDef.sqliteDef.ast,
        skipMetaTable: true,
      })
    }

    // Create sync status row if it doesn't exist
    yield* execSql(
      dbEventlog,
      sql`INSERT INTO ${SYNC_STATUS_TABLE} (head)
          SELECT ${EventSequenceNumber.Client.ROOT.global}
          WHERE NOT EXISTS (SELECT 1 FROM ${SYNC_STATUS_TABLE})`,
      {},
    )
  })

/**
 * Exclusive of the "since event"
 */
export const getEventsSince = ({
  dbEventlog,
  since,
}: {
  dbEventlog: SqliteDb
  since: EventSequenceNumber.Client.Composite
}): ReadonlyArray<LiveStoreEvent.Client.Encoded> => {
  const pendingEvents = dbEventlog.select(eventlogMetaTable.where('seqNumGlobal', '>=', since.global))

  return pendingEvents
    .map((eventlogEvent) => {
      return LiveStoreEvent.Client.Encoded.make({
        name: eventlogEvent.name,
        args: eventlogEvent.argsJson,
        seqNum: {
          global: eventlogEvent.seqNumGlobal,
          client: eventlogEvent.seqNumClient,
          rebaseGeneration: eventlogEvent.seqNumRebaseGeneration,
        },
        parentSeqNum: {
          global: eventlogEvent.parentSeqNumGlobal,
          client: eventlogEvent.parentSeqNumClient,
          rebaseGeneration: eventlogEvent.parentSeqNumRebaseGeneration,
        },
        clientId: eventlogEvent.clientId,
        sessionId: eventlogEvent.sessionId,
      })
    })
    .filter((_) => EventSequenceNumber.Client.compare(_.seqNum, since) > 0)
    .toSorted((a, b) => EventSequenceNumber.Client.compare(a.seqNum, b.seqNum))
}

/**
 * Deletes eventlog entries at the requested logical event positions.
 *
 * @remarks
 * Positions are matched by their global and client components, so every rebase-generation incarnation at a matching
 * position is removed. Deletions are batched within one savepoint; if any batch fails, earlier batches are rolled back.
 *
 * @param dbEventlog - Eventlog database whose entries should be removed
 * @param eventNums - Logical event positions to remove
 */
export const deleteEvents = (dbEventlog: SqliteDb, eventNums: ReadonlyArray<EventSequenceNumber.Client.Composite>) =>
  Effect.gen(function* () {
    // Split into batches to keep each DELETE statement and its bound parameter count manageable.
    const eventNumChunks = ReadonlyArray.chunksOf(100)(eventNums)

    for (const eventNumChunk of eventNumChunks) {
      // A global/client pair identifies the logical event position. Deleting it intentionally purges
      // every rebase-generation incarnation that may remain at that position.
      const placeholders = eventNumChunk.map(() => '(?, ?)').join(', ')
      const bindValues = eventNumChunk.flatMap((key) => [key.global, key.client])

      yield* execSql(
        dbEventlog,
        sql`DELETE FROM ${EVENTLOG_META_TABLE}
            WHERE (seqNumGlobal, seqNumClient) IN (${placeholders})`,
        bindValues,
      )
    }
  }).pipe(SqliteDbHelper.withSavepoint(dbEventlog))

export const getEventsFromEventlog = ({
  dbEventlog,
  options,
}: {
  dbEventlog: SqliteDb
  options: StreamEventsOptions
}): Effect.Effect<ReadonlyArray<LiveStoreEvent.Client.Encoded>> =>
  Effect.gen(function* () {
    const since = options.since ?? EventSequenceNumber.Client.ROOT
    const batchSize = options.batchSize ?? STREAM_EVENTS_BATCH_SIZE_DEFAULT

    const makeQuery = () => {
      let query = eventlogMetaTable.where('seqNumGlobal', '>', since.global)

      if (options.until !== undefined) {
        query = query.where('seqNumGlobal', '<=', options.until.global)
      }

      if (options.filter !== undefined && options.filter.length > 0) {
        query = query.where({ name: { op: 'IN', value: options.filter } })
      }

      if (options.clientIds !== undefined && options.clientIds.length > 0) {
        query = query.where({ clientId: { op: 'IN', value: options.clientIds } })
      }

      if (options.sessionIds !== undefined && options.sessionIds.length > 0) {
        query = query.where({ sessionId: { op: 'IN', value: options.sessionIds } })
      }

      if (options.includeClientOnly !== true) {
        query = query.where('seqNumClient', '<=', EventSequenceNumber.Client.DEFAULT)
      }

      return query
        .orderBy([
          { col: 'seqNumGlobal', direction: 'asc' },
          { col: 'seqNumClient', direction: 'asc' },
        ])
        .limit(batchSize)
    }

    const eventlogEvents = yield* Effect.sync(() => dbEventlog.select(makeQuery()))

    if (eventlogEvents.length === 0) {
      return []
    }

    const spanAttributes = {
      'livestore.eventLog.since': since.global,
      'livestore.eventLog.until': options.until?.global,
    }

    return yield* Effect.sync(() => {
      const encodedEvents = eventlogEvents.map((eventlogEvent) => {
        return LiveStoreEvent.Client.Encoded.make({
          name: eventlogEvent.name,
          args: eventlogEvent.argsJson,
          seqNum: {
            global: eventlogEvent.seqNumGlobal,
            client: eventlogEvent.seqNumClient,
            rebaseGeneration: eventlogEvent.seqNumRebaseGeneration,
          },
          parentSeqNum: {
            global: eventlogEvent.parentSeqNumGlobal,
            client: eventlogEvent.parentSeqNumClient,
            rebaseGeneration: eventlogEvent.parentSeqNumRebaseGeneration,
          },
          clientId: eventlogEvent.clientId,
          sessionId: eventlogEvent.sessionId,
        })
      })

      return encodedEvents
    }).pipe(Effect.withSpan('@livestore/common:eventlog:getEventsFromEventlog', { attributes: spanAttributes }))
  })

export const getClientHeadFromDb = (dbEventlog: SqliteDb): EventSequenceNumber.Client.Composite => {
  const res = dbEventlog.select<{
    seqNumGlobal: EventSequenceNumber.Global.Type
    seqNumClient: EventSequenceNumber.Client.Type
    seqNumRebaseGeneration: number
  }>(
    sql`select seqNumGlobal, seqNumClient, seqNumRebaseGeneration from ${EVENTLOG_META_TABLE} order by seqNumGlobal DESC, seqNumClient DESC limit 1`,
  )[0]

  if (res !== undefined) {
    return { global: res.seqNumGlobal, client: res.seqNumClient, rebaseGeneration: res.seqNumRebaseGeneration }
  }

  // A snapshot-booted eventlog without events after its base is positioned at the base, not at the root.
  return getEventlogBaseFromDb(dbEventlog).pipe(
    Option.match({
      onNone: () => EventSequenceNumber.Client.ROOT,
      onSome: (base) => baseToSeqNum(base.seqNumGlobal),
    }),
  )
}

/** Where a snapshot-booted eventlog begins. See `eventlogBaseTable`. */
export interface EventlogBase {
  readonly seqNumGlobal: EventSequenceNumber.Global.Type
  readonly syncMetadata: Option.Option<Schema.Json>
  readonly snapshotKey: string
}

const decodeSyncMetadataJson = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.toCodecJson(Schema.Option(Schema.Json))),
)
const encodeSyncMetadataJson = Schema.encodeSync(Schema.fromJsonString(Schema.toCodecJson(Schema.Option(Schema.Json))))

/** The base is a synced position, so it never carries a client or rebase-generation component. */
export const baseToSeqNum = (seqNumGlobal: EventSequenceNumber.Global.Type): EventSequenceNumber.Client.Composite =>
  EventSequenceNumber.Client.Composite.make({
    global: seqNumGlobal,
    client: EventSequenceNumber.Client.DEFAULT,
    rebaseGeneration: EventSequenceNumber.Client.REBASE_GENERATION_DEFAULT,
  })

/** Returns the eventlog base, or `None` when the eventlog starts at the root (including databases without the table). */
export const getEventlogBaseFromDb = (dbEventlog: SqliteDb): Option.Option<EventlogBase> => {
  const hasTable =
    dbEventlog.select<{ name: string }>(
      sql`SELECT name FROM sqlite_master WHERE type = 'table' AND name = '${EVENTLOG_BASE_TABLE}'`,
    ).length === 1
  if (hasTable === false) return Option.none()

  const row = dbEventlog.select<{ seqNumGlobal: number; syncMetadataJson: string; snapshotKey: string }>(
    sql`SELECT seqNumGlobal, syncMetadataJson, snapshotKey FROM ${EVENTLOG_BASE_TABLE} WHERE id = 1`,
  )[0]
  if (row === undefined) return Option.none()

  return Option.some({
    seqNumGlobal: EventSequenceNumber.Global.make(row.seqNumGlobal),
    syncMetadata: decodeSyncMetadataJson(row.syncMetadataJson),
    snapshotKey: row.snapshotKey,
  })
}

/**
 * Positions an empty eventlog at a snapshot: records the base and moves the backend head and identity to it.
 * Runs in one eventlog transaction so a crash never leaves a base without its head (or the reverse).
 */
export const installEventlogBase = (
  dbEventlog: SqliteDb,
  { base, backendId }: { base: EventlogBase; backendId: string },
) =>
  Effect.gen(function* () {
    yield* execSql(dbEventlog, sql`DELETE FROM ${EVENTLOG_BASE_TABLE}`, {})
    yield* execSql(
      dbEventlog,
      sql`INSERT INTO ${EVENTLOG_BASE_TABLE} (id, seqNumGlobal, syncMetadataJson, snapshotKey, installedAt)
          VALUES (1, $seqNumGlobal, $syncMetadataJson, $snapshotKey, $installedAt)`,
      {
        seqNumGlobal: base.seqNumGlobal,
        syncMetadataJson: encodeSyncMetadataJson(base.syncMetadata),
        snapshotKey: base.snapshotKey,
        installedAt: new Date().toISOString(),
      },
    )
    yield* execSql(dbEventlog, sql`UPDATE ${SYNC_STATUS_TABLE} SET head = $head, backendId = $backendId`, {
      head: base.seqNumGlobal,
      backendId,
    })
  }).pipe(SqliteDbHelper.withSavepoint(dbEventlog))

/** Removes the base, e.g. after the events below it were backfilled or the local databases were reset. */
export const clearEventlogBase = (dbEventlog: SqliteDb) =>
  Effect.try({
    try: () => {
      if (Option.isSome(getEventlogBaseFromDb(dbEventlog)) === true) {
        dbEventlog.execute(sql`DELETE FROM ${EVENTLOG_BASE_TABLE}`)
      }
    },
    catch: (cause) => new UnknownError({ cause, note: 'Failed to clear the eventlog base' }),
  })

/**
 * True when nothing has been recorded locally yet: no events, no backend progress and no snapshot base.
 * Only such an eventlog may be positioned at a snapshot.
 */
export const isEventlogEmpty = (dbEventlog: SqliteDb): boolean =>
  dbEventlog.select<{ count: number }>(
    sql`SELECT COUNT(*) AS count FROM (SELECT 1 FROM ${EVENTLOG_META_TABLE} LIMIT 1)`,
  )[0]!.count === 0 &&
  getBackendHeadFromDb(dbEventlog) === EventSequenceNumber.Client.ROOT.global &&
  Option.isNone(getEventlogBaseFromDb(dbEventlog))

export const getBackendHeadFromDb = (dbEventlog: SqliteDb): EventSequenceNumber.Global.Type =>
  dbEventlog.select<{ head: EventSequenceNumber.Global.Type }>(sql`select head from ${SYNC_STATUS_TABLE}`)[0]?.head ??
  EventSequenceNumber.Client.ROOT.global

// TODO use prepared statements
export const updateBackendHead = (dbEventlog: SqliteDb, head: EventSequenceNumber.Client.Composite) =>
  dbEventlog.execute(sql`UPDATE ${SYNC_STATUS_TABLE} SET head = ${head.global}`)

export const getBackendIdFromDb = (dbEventlog: SqliteDb): Option.Option<string> =>
  Option.fromNullishOr(
    dbEventlog.select<{ backendId: string | null }>(sql`select backendId from ${SYNC_STATUS_TABLE}`)[0]?.backendId,
  )

export const updateBackendId = (dbEventlog: SqliteDb, backendId: string) =>
  dbEventlog.execute(sql`UPDATE ${SYNC_STATUS_TABLE} SET backendId = '${backendId}'`)

export const insertIntoEventlog = (
  eventEncoded: LiveStoreEvent.Client.Encoded,
  dbEventlog: SqliteDb,
  eventDefSchemaHash: number,
  clientId: string,
  sessionId: string,
  syncMetadata: Option.Option<Schema.Json> = Option.none(),
) =>
  Effect.gen(function* () {
    // Check history consistency during LS_DEV
    if (LS_DEV === true && eventEncoded.parentSeqNum.global !== EventSequenceNumber.Client.ROOT.global) {
      const parentEventExists =
        dbEventlog.select<{ count: number }>(
          `SELECT COUNT(*) as count FROM ${EVENTLOG_META_TABLE} WHERE seqNumGlobal = ? AND seqNumClient = ?`,
          [eventEncoded.parentSeqNum.global, eventEncoded.parentSeqNum.client] as any as PreparedBindValues,
        )[0]!.count === 1

      // The first event after a snapshot base has the base as its parent, which has no row.
      const parentIsBase =
        eventEncoded.parentSeqNum.client === EventSequenceNumber.Client.DEFAULT &&
        Option.exists(
          getEventlogBaseFromDb(dbEventlog),
          (base) => base.seqNumGlobal === eventEncoded.parentSeqNum.global,
        )

      if (parentEventExists === false && parentIsBase === false) {
        shouldNeverHappen(
          `Parent event ${eventEncoded.parentSeqNum.global},${eventEncoded.parentSeqNum.client} does not exist in eventlog`,
        )
      }
    }

    // TODO use prepared statements
    yield* execSql(
      dbEventlog,
      ...insertRow({
        tableName: EVENTLOG_META_TABLE,
        columns: eventlogMetaTable.sqliteDef.columns,
        values: {
          seqNumGlobal: eventEncoded.seqNum.global,
          seqNumClient: eventEncoded.seqNum.client,
          seqNumRebaseGeneration: eventEncoded.seqNum.rebaseGeneration,
          parentSeqNumGlobal: eventEncoded.parentSeqNum.global,
          parentSeqNumClient: eventEncoded.parentSeqNum.client,
          parentSeqNumRebaseGeneration: eventEncoded.parentSeqNum.rebaseGeneration,
          name: eventEncoded.name,
          argsJson: eventEncoded.args ?? {},
          clientId,
          sessionId,
          schemaHash: eventDefSchemaHash,
          syncMetadataJson: syncMetadata,
        },
      }),
    )

    dbEventlog.debug.head = eventEncoded.seqNum
  })

export const updateSyncMetadata = (
  items: ReadonlyArray<{
    readonly event: LiveStoreEvent.Client.Encoded
    readonly syncMetadata: Option.Option<Schema.Json>
  }>,
) => EventlogSqliteDb.EventlogSqliteDb.pipe(Effect.flatMap((dbEventlog) => updateSyncMetadataForDb(dbEventlog, items)))

export const updateSyncMetadataForDb = (
  dbEventlog: SqliteDb,
  items: ReadonlyArray<{
    readonly event: LiveStoreEvent.Client.Encoded
    readonly syncMetadata: Option.Option<Schema.Json>
  }>,
) =>
  Effect.gen(function* () {
    // TODO try to do this in a single query
    for (let i = 0; i < items.length; i++) {
      const { event, syncMetadata } = items[i]!

      yield* execSql(
        dbEventlog,
        ...updateRows({
          tableName: EVENTLOG_META_TABLE,
          columns: eventlogMetaTable.sqliteDef.columns,
          where: { seqNumGlobal: event.seqNum.global, seqNumClient: event.seqNum.client },
          updateValues: { syncMetadataJson: syncMetadata },
        }),
      )
    }
  })

export const getSyncBackendCursorInfo = (args: { remoteHead: EventSequenceNumber.Global.Type }) =>
  EventlogSqliteDb.EventlogSqliteDb.pipe(
    Effect.flatMap((dbEventlog) => getSyncBackendCursorInfoForDb(dbEventlog, args)),
  )

export const getSyncBackendCursorInfoForDb = (
  dbEventlog: SqliteDb,
  { remoteHead }: { remoteHead: EventSequenceNumber.Global.Type },
) =>
  Effect.gen(function* () {
    if (remoteHead === EventSequenceNumber.Client.ROOT.global) return Option.none()

    const EventlogQuerySchema = Schema.Struct({
      syncMetadataJson: Schema.fromJsonString(Schema.toCodecJson(Schema.Option(Schema.Json))),
    }).pipe(Schema.pluck('syncMetadataJson'), Schema.Array, Schema.head)

    const syncMetadataOption = yield* Effect.sync(() =>
      dbEventlog.select<{ syncMetadataJson: string }>(
        sql`SELECT syncMetadataJson FROM ${EVENTLOG_META_TABLE} WHERE seqNumGlobal = ${remoteHead} ORDER BY seqNumClient ASC LIMIT 1`,
      ),
    ).pipe(
      Effect.andThen(Schema.decodeEffect(EventlogQuerySchema)),
      // A snapshot-booted eventlog has no row at its base; the base row carries the cursor metadata instead.
      Effect.map((rowMetadata) =>
        Option.isSome(rowMetadata) === true
          ? Option.flatten(rowMetadata)
          : getEventlogBaseFromDb(dbEventlog).pipe(
              Option.filter((base) => base.seqNumGlobal === remoteHead),
              Option.flatMap((base) => base.syncMetadata),
            ),
      ),
      Effect.orDie,
    )

    return Option.some({
      eventSequenceNumber: remoteHead,
      metadata: syncMetadataOption,
    }) satisfies InitialSyncInfo
  }).pipe(Effect.withSpan('@livestore/common:eventlog:getSyncBackendCursorInfo', { attributes: { remoteHead } }))
