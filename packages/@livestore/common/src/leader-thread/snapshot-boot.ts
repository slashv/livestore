import { type HttpClient, type Schema, type Scope, Duration, Effect, Option } from '@livestore/utils/effect'

import { type MakeSqliteDb, type SqliteDb, UnknownError } from '../adapter-types.ts'
import * as EventlogSqliteDb from '../EventlogSqliteDb.ts'
import type { LiveStoreSchema } from '../schema/mod.ts'
import { EventSequenceNumber, SystemTables } from '../schema/mod.ts'
import {
  sha256Hex,
  SnapshotError,
  type SnapshotBootOptions,
  type SnapshotManifest,
  type SnapshotSource,
} from '../snapshot/snapshot.ts'
import * as SnapshotKey from '../snapshot/SnapshotKey.ts'
import * as StateSqliteDb from '../StateSqliteDb.ts'
import { sql } from '../util.ts'
import { configureConnection } from './connection.ts'
import * as Eventlog from './eventlog.ts'
import { hasCompletedState } from './recreate-db.ts'

/** Timings and identity of an installed snapshot, reported for diagnostics and benchmarks. */
export interface SnapshotInstallResult {
  readonly manifest: SnapshotManifest
  readonly timings: {
    readonly manifestMs: number
    readonly downloadMs: number
    readonly verifyMs: number
    readonly installMs: number
  }
}

const DEFAULT_TIMEOUT = Duration.seconds(60)

/**
 * Installs the latest snapshot for the schema's key into an empty client: the state database becomes the snapshot and
 * the eventlog is positioned at the snapshot head, so the sync processor pulls only the events after it.
 *
 * Returns `None` (after logging why) whenever no usable snapshot can be installed; the caller then continues with the
 * regular boot, i.e. a full replay from the sync backend. A failed install never leaves partial state behind: the
 * state database is wiped again and the eventlog is only touched after the state install succeeded.
 *
 * Must run before the sync backend client is constructed, because providers read the persisted backend identity at
 * construction and need it when pulling with a cursor.
 */
export const bootFromSnapshot = ({
  schema,
  storeId,
  clientId,
  payload,
  options,
  makeSqliteDb,
}: {
  schema: LiveStoreSchema
  storeId: string
  clientId: string
  payload: unknown
  options: SnapshotBootOptions<any>
  makeSqliteDb: MakeSqliteDb
}): Effect.Effect<
  Option.Option<SnapshotInstallResult>,
  never,
  StateSqliteDb.StateSqliteDb | EventlogSqliteDb.EventlogSqliteDb | Scope.Scope | HttpClient.HttpClient
> =>
  Effect.gen(function* () {
    const dbState = yield* StateSqliteDb.StateSqliteDb
    const dbEventlog = yield* EventlogSqliteDb.EventlogSqliteDb
    const key = SnapshotKey.toString(SnapshotKey.make(schema))

    const source = yield* options.source({ storeId, clientId, payload })
    const fetched = yield* fetchSnapshot({ source, key, storeId }).pipe(
      Effect.timeout(options.timeout ?? DEFAULT_TIMEOUT),
      Effect.mapError((cause) =>
        cause._tag === 'SnapshotError'
          ? cause
          : new SnapshotError({ reason: 'unavailable', message: 'Timed out fetching the snapshot', cause }),
      ),
    )
    if (Option.isNone(fetched) === true) return Option.none()

    const { manifest, bytes, manifestMs, downloadMs, verifyMs } = fetched.value

    const installStart = performance.now()
    yield* installState({ dbState, bytes, manifest, makeSqliteDb })
    yield* Eventlog.installEventlogBase(dbEventlog, {
      base: {
        seqNumGlobal: EventSequenceNumber.Global.make(manifest.seqNum),
        syncMetadata: Option.fromUndefinedOr(manifest.cursorMetadata),
        snapshotKey: manifest.key,
      },
      backendId: manifest.backendId,
    }).pipe(
      Effect.mapError(
        (cause) => new SnapshotError({ reason: 'internal', message: 'Failed to position the eventlog', cause }),
      ),
    )
    const installMs = performance.now() - installStart

    yield* Effect.annotateCurrentSpan({
      'livestore.snapshot.seqNum': manifest.seqNum,
      'livestore.snapshot.byteLength': manifest.byteLength,
      'livestore.snapshot.downloadMs': downloadMs,
      'livestore.snapshot.installMs': installMs,
    })
    yield* Effect.logDebug(
      `Booted from snapshot at ${manifest.seqNum} (${manifest.byteLength} bytes, download ${downloadMs.toFixed(0)}ms, install ${installMs.toFixed(0)}ms)`,
    )

    return Option.some({ manifest, timings: { manifestMs, downloadMs, verifyMs, installMs } })
  }).pipe(
    Effect.catch((error) =>
      Effect.gen(function* () {
        yield* Effect.logWarning('Snapshot boot failed, falling back to full replay', error)
        // Leave no trace of a partially installed snapshot, so the regular boot path rebuilds state from scratch.
        yield* wipeState(makeSqliteDb).pipe(Effect.orDie)
        return Option.none()
      }),
    ),
    Effect.withSpan('@livestore/common:leader-thread:bootFromSnapshot'),
  )

/**
 * Fetches the manifest and blob for `key` and verifies them. `None` means no snapshot is published for the key.
 */
export const fetchSnapshot = ({ source, key, storeId }: { source: SnapshotSource; key: string; storeId: string }) =>
  Effect.gen(function* () {
    const manifestStart = performance.now()
    const manifestOption = yield* source.getManifest({ key })
    const manifestMs = performance.now() - manifestStart

    if (Option.isNone(manifestOption) === true) {
      yield* Effect.logDebug(`No snapshot published for key ${key}`)
      return Option.none()
    }

    const manifest = manifestOption.value
    if (manifest.key !== key || manifest.storeId !== storeId || manifest.manifestVersion !== 1) {
      return yield* new SnapshotError({
        reason: 'incompatible',
        message: `Manifest for ${manifest.storeId}/${manifest.key} does not match ${storeId}/${key}`,
      })
    }

    const downloadStart = performance.now()
    const bytes = yield* source.download(manifest)
    const downloadMs = performance.now() - downloadStart

    const verifyStart = performance.now()
    if (bytes.byteLength !== manifest.byteLength) {
      return yield* new SnapshotError({
        reason: 'integrity',
        message: `Snapshot has ${bytes.byteLength} bytes, manifest says ${manifest.byteLength}`,
      })
    }
    const digest = sha256Hex(bytes)
    if (digest !== manifest.sha256) {
      return yield* new SnapshotError({ reason: 'integrity', message: `Snapshot checksum mismatch (${digest})` })
    }
    const verifyMs = performance.now() - verifyStart

    return Option.some({ manifest, bytes, manifestMs, downloadMs, verifyMs })
  })

/**
 * Replaces the state database with the snapshot and checks that it is a completed state at the manifest's head.
 * Rollback records of the producer are dropped: they describe the producer's history, not this client's.
 */
export const installState = ({
  dbState,
  bytes,
  manifest,
  makeSqliteDb,
}: {
  dbState: SqliteDb
  bytes: Uint8Array<ArrayBuffer>
  manifest: SnapshotManifest
  makeSqliteDb: MakeSqliteDb
}) =>
  Effect.gen(function* () {
    yield* Effect.try({
      // `import` deserializes into memory and copies pages into the target with the backup API: one pass over the
      // bytes regardless of the target VFS, without the devtools import's shutdown and reboot.
      try: () => dbState.import(bytes),
      catch: (cause) => new SnapshotError({ reason: 'integrity', message: 'Snapshot is not a SQLite database', cause }),
    })

    const fail = (message: string) =>
      Effect.gen(function* () {
        yield* wipeState(makeSqliteDb).pipe(Effect.orDie)
        return yield* new SnapshotError({ reason: 'incompatible', message })
      })

    if (hasCompletedState(dbState) === false) {
      return yield* fail('Snapshot does not contain a completed state database')
    }

    const head = dbState.select<{ seqNumGlobal: number; seqNumClient: number }>(
      sql`SELECT seqNumGlobal, seqNumClient FROM ${SystemTables.STATE_HEAD_META_TABLE} WHERE id = 1`,
    )[0]
    if (head === undefined || head.seqNumGlobal !== manifest.seqNum || head.seqNumClient !== 0) {
      return yield* fail(
        `Snapshot state head ${head?.seqNumGlobal},${head?.seqNumClient} does not match manifest seqNum ${manifest.seqNum}`,
      )
    }

    yield* Effect.try({
      try: () => dbState.execute(sql`DELETE FROM ${SystemTables.MATERIALIZATION_JOURNAL_META_TABLE}`),
      catch: (cause) => new SnapshotError({ reason: 'internal', message: 'Failed to reset the journal', cause }),
    })
    yield* configureConnection(dbState, { foreignKeys: true }).pipe(
      Effect.mapError(
        (cause) => new SnapshotError({ reason: 'internal', message: 'Failed to configure the connection', cause }),
      ),
    )
  })

/** Empties the state database in place, preserving the open connection (same approach as a state rebuild). */
const wipeState = (makeSqliteDb: MakeSqliteDb) =>
  Effect.gen(function* () {
    const dbState = yield* StateSqliteDb.StateSqliteDb
    yield* Effect.acquireUseRelease(
      makeSqliteDb({ _tag: 'in-memory' }),
      (emptyDb) =>
        configureConnection(emptyDb, { foreignKeys: false }).pipe(
          Effect.andThen(Effect.sync(() => dbState.import(emptyDb))),
        ),
      (emptyDb) => Effect.sync(() => emptyDb.close()),
    )
  }).pipe(UnknownError.mapToUnknownError)
