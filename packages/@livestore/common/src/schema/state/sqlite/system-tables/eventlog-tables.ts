import { Schema } from '@livestore/utils/effect'

import * as EventSequenceNumber from '../../../EventSequenceNumber/mod.ts'
import { SqliteDsl } from '../db-schema/mod.ts'
import { table } from '../table-def.ts'

/**
 * EVENTLOG DATABASE SYSTEM TABLES
 *
 * ⚠️  CRITICAL: NEVER modify eventlog schemas without bumping `liveStoreStorageFormatVersion`!
 * Eventlog is the source of truth - schema changes cause permanent data loss.
 *
 * TODO: Implement proper eventlog versioning system to prevent accidental data loss
 */

export const EVENTLOG_META_TABLE = 'eventlog'

/**
 * Main client-side event log storing all events (global and local/rebased).
 */
export const eventlogMetaTable = table({
  name: EVENTLOG_META_TABLE,
  columns: {
    // TODO Adjust modeling so a global event never needs a client id component
    seqNumGlobal: SqliteDsl.integer({ primaryKey: true, schema: EventSequenceNumber.Global.Schema }),
    seqNumClient: SqliteDsl.integer({ primaryKey: true, schema: EventSequenceNumber.Client.Schema }),
    seqNumRebaseGeneration: SqliteDsl.integer({ primaryKey: true }),
    parentSeqNumGlobal: SqliteDsl.integer({ schema: EventSequenceNumber.Global.Schema }),
    parentSeqNumClient: SqliteDsl.integer({ schema: EventSequenceNumber.Client.Schema }),
    parentSeqNumRebaseGeneration: SqliteDsl.integer({}),
    /** Event definition name */
    name: SqliteDsl.text({}),
    argsJson: SqliteDsl.text({ schema: Schema.fromJsonString(Schema.Any) }),
    clientId: SqliteDsl.text({}),
    sessionId: SqliteDsl.text({}),
    schemaHash: SqliteDsl.integer({}),
    syncMetadataJson: SqliteDsl.text({ schema: Schema.fromJsonString(Schema.toCodecJson(Schema.Option(Schema.Json))) }),
  },
  indexes: [
    { columns: ['seqNumGlobal'], name: 'idx_eventlog_seqNumGlobal' },
    { columns: ['seqNumGlobal', 'seqNumClient', 'seqNumRebaseGeneration'], name: 'idx_eventlog_seqNum' },
  ],
})

export type EventlogMetaRow = typeof eventlogMetaTable.Type

export const SYNC_STATUS_TABLE = '__livestore_sync_status'

/**
 * Tracks sync status including the remote head position and backend identity.
 */
// TODO support sync backend identity (to detect if sync backend changes)
export const syncStatusTable = table({
  name: SYNC_STATUS_TABLE,
  columns: {
    head: SqliteDsl.integer({ primaryKey: true }),
    // Null means the sync backend is not yet connected and we haven't yet seen a backend ID
    backendId: SqliteDsl.text({ nullable: true }),
  },
})

export type SyncStatusRow = typeof syncStatusTable.Type

export const EVENTLOG_BASE_TABLE = '__livestore_eventlog_base'

/**
 * Single-row marker for where the local eventlog begins when the client booted from a state snapshot.
 *
 * @remarks
 * A snapshot-booted client installs state at a known head and keeps only the events after it, so the eventlog has no
 * rows at or below `seqNumGlobal`. The row stands in for the missing event at the base: it is the parent of the first
 * event after the base and carries the sync metadata the backend cursor needs at that position. No row means the
 * eventlog starts at the root.
 *
 * Added after storage format 6 as a separate table that is created when missing, so existing eventlogs open unchanged
 * and no storage format bump is needed.
 */
export const eventlogBaseTable = table({
  name: EVENTLOG_BASE_TABLE,
  columns: {
    id: SqliteDsl.integer({ primaryKey: true }),
    seqNumGlobal: SqliteDsl.integer({ schema: EventSequenceNumber.Global.Schema }),
    syncMetadataJson: SqliteDsl.text({ schema: Schema.fromJsonString(Schema.toCodecJson(Schema.Option(Schema.Json))) }),
    /** Snapshot key of the installed snapshot, for diagnostics. */
    snapshotKey: SqliteDsl.text({}),
    /** ISO date format */
    installedAt: SqliteDsl.text({}),
  },
})

export type EventlogBaseRow = typeof eventlogBaseTable.Type

export const eventlogSystemTables = [eventlogMetaTable, syncStatusTable, eventlogBaseTable] as const

/** Tables whose presence marks an existing eventlog database. The base table is optional because it was added later. */
export const eventlogRequiredSystemTables = [eventlogMetaTable, syncStatusTable] as const
