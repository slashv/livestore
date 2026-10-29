import {
  type Scope,
  Cause,
  Duration,
  Effect,
  Option,
  Schedule,
  type Schema,
  Semaphore,
  SubscriptionRef,
} from '@livestore/utils/effect'

import { UnknownError } from '../adapter-types.ts'
import * as EventlogSqliteDb from '../EventlogSqliteDb.ts'
import type { LiveStoreSchema } from '../schema/mod.ts'
import { digestSha256Hex, type SnapshotManifest, type SnapshotSink } from '../snapshot/snapshot.ts'
import * as SnapshotKey from '../snapshot/SnapshotKey.ts'
import * as SnapshotTrigger from '../snapshot/trigger.ts'
import * as StateSqliteDb from '../StateSqliteDb.ts'
import * as Eventlog from './eventlog.ts'
import { LeaderThreadCtx } from './types.ts'

/** @experimental */
export interface CheckpointerOptions {
  readonly schema: LiveStoreSchema
  readonly storeId: string
  readonly sink: SnapshotSink
  /** Overrides for the publication cost model. */
  readonly trigger?: Partial<SnapshotTrigger.TriggerParams>
  /**
   * How often the checkpointer re-evaluates the trigger. Evaluation is cheap (two pragmas and a comparison).
   *
   * @default 1 second
   */
  readonly pollInterval?: Duration.Input
  /** Clock for tests. */
  readonly now?: () => number
}

/** Timings of one published snapshot, in milliseconds. */
export interface PublishTimings {
  readonly seqNum: number
  readonly byteLength: number
  readonly exportMs: number
  readonly hashMs: number
  readonly uploadMs: number
  readonly totalMs: number
  /** Events between the previous snapshot and this one. */
  readonly tailEvents: number
}

/** @experimental */
export interface CheckpointerStats {
  readonly key: string
  readonly publishedCount: number
  readonly failedCount: number
  readonly latestManifest: Option.Option<SnapshotManifest>
  readonly lastPublish: Option.Option<PublishTimings>
  /** Highest backend position the checkpointer has applied. */
  readonly head: number
  /** Events the latest snapshot is behind the checkpointer head. */
  readonly snapshotLag: number
  readonly lastDecision: SnapshotTrigger.TriggerDecision | undefined
}

/** @experimental */
export interface Checkpointer {
  readonly stats: SubscriptionRef.SubscriptionRef<CheckpointerStats>
  /** Publishes a snapshot now if the head moved past the latest snapshot, bypassing the cost model. */
  readonly publishNow: Effect.Effect<Option.Option<SnapshotManifest>, UnknownError>
}

/**
 * Runs the checkpointer loop on a leader thread: a LiveStore client that never pushes, follows the backend log with
 * live pull and periodically publishes its state as a snapshot.
 *
 * The leader must not have local events: `exportSnapshot` refuses to export state that is not exactly the backend
 * head, so only synced history ever reaches a snapshot. The loop runs until the surrounding scope closes.
 *
 * @experimental
 */
export const makeCheckpointer = (
  options: CheckpointerOptions,
): Effect.Effect<Checkpointer, never, LeaderThreadCtx | StateSqliteDb.StateSqliteDb | Scope.Scope> =>
  Effect.gen(function* () {
    const { syncProcessor, exportSnapshot, exportSnapshotWith } = yield* LeaderThreadCtx
    const dbState = yield* StateSqliteDb.StateSqliteDb
    const { schema, storeId, sink } = options
    const now = options.now ?? (() => Date.now())
    const params = { ...SnapshotTrigger.defaultTriggerParams, ...options.trigger }
    const key = SnapshotKey.toString(SnapshotKey.make(schema))

    // A restarted checkpointer continues from what it published before, so a quiet store stays quiet.
    const latestManifest = yield* sink
      .getManifest({ key })
      .pipe(
        Effect.catch((error) =>
          Effect.logWarning('Checkpointer could not read the latest manifest', error).pipe(Effect.as(Option.none())),
        ),
      )

    const stats = yield* SubscriptionRef.make<CheckpointerStats>({
      key,
      publishedCount: 0,
      failedCount: 0,
      latestManifest,
      lastPublish: Option.none(),
      head: 0,
      snapshotLag: 0,
      lastDecision: undefined,
    })

    const currentHead = syncProcessor.syncState.get.pipe(Effect.map((syncState) => syncState.upstreamHead.global))

    /** Estimates the next snapshot size from the live database instead of exporting it. */
    const estimateSnapshotBytes = Effect.sync(() => {
      const [pageCount] = dbState.select<{ page_count: number }>('PRAGMA page_count')
      const [pageSize] = dbState.select<{ page_size: number }>('PRAGMA page_size')
      return (pageCount?.page_count ?? 0) * (pageSize?.page_size ?? 0)
    })

    const manifestDraft = (exported: {
      backendId: string
      seqNum: number
      cursorMetadata: Option.Option<Schema.Json>
    }) => ({
      manifestVersion: 1 as const,
      storeId,
      key,
      backendId: exported.backendId,
      seqNum: exported.seqNum,
      ...(Option.isSome(exported.cursorMetadata) === true ? { cursorMetadata: exported.cursorMetadata.value } : {}),
      createdAt: now(),
    })

    // After a backend reset the log restarts, so the previous backend's snapshot says nothing about this one.
    const previousSeqNumFor = (previous: Option.Option<SnapshotManifest>, backendId: string) =>
      Option.match(previous, {
        onNone: () => 0,
        onSome: (manifest) => (manifest.backendId === backendId ? manifest.seqNum : 0),
      })

    /** Serializes the state database into one buffer, hashes it and hands it to `sink.publish`. */
    const exportAndPublishBuffered = Effect.gen(function* () {
      const previous = (yield* SubscriptionRef.get(stats)).latestManifest
      const exported = yield* exportSnapshot
      const previousSeqNum = previousSeqNumFor(previous, exported.backendId)
      if (exported.seqNum <= previousSeqNum) return Option.none()

      const hashStart = performance.now()
      const digest = yield* digestSha256Hex(exported.bytes)
      const hashMs = performance.now() - hashStart

      const uploadStart = performance.now()
      const manifest = yield* sink
        .publish({
          manifest: { ...manifestDraft(exported), byteLength: exported.bytes.byteLength, sha256: digest },
          bytes: exported.bytes,
        })
        .pipe(UnknownError.mapToUnknownError)
      const uploadMs = performance.now() - uploadStart

      return Option.some({ manifest, previousSeqNum, exportMs: exported.exportMs, hashMs, uploadMs })
    })

    /** Lets the sink copy the state at the consistent moment and stream it, without a full in-memory export. */
    const exportAndPublishStaged = (capture: NonNullable<SnapshotSink['capture']>) =>
      // The staged copy is owned by this flow until `publish` starts, and `publish` releases it on every exit. Keeping
      // the hand-off uninterruptible means an interruption can never strand a staged copy in between.
      Effect.uninterruptibleMask((restore) =>
        Effect.gen(function* () {
          const previous = (yield* SubscriptionRef.get(stats)).latestManifest
          // Decide inside the lock, before copying anything: staging a position that is already published is wasted
          // work proportional to the state size.
          const exported = yield* exportSnapshotWith((db, position) =>
            position.seqNum > previousSeqNumFor(previous, position.backendId)
              ? Option.some(capture(db))
              : Option.none(),
          )
          if (Option.isNone(exported.data) === true) return Option.none()

          const previousSeqNum = previousSeqNumFor(previous, exported.backendId)
          const uploadStart = performance.now()
          const manifest = yield* restore(exported.data.value.publish(manifestDraft(exported))).pipe(
            UnknownError.mapToUnknownError,
          )
          const uploadMs = performance.now() - uploadStart

          // Hashing happens while streaming, so it is part of `uploadMs`.
          return Option.some({ manifest, previousSeqNum, exportMs: exported.exportMs, hashMs: 0, uploadMs })
        }),
      )

    const publish = Effect.gen(function* () {
      const totalStart = performance.now()
      const published = yield* sink.capture === undefined
        ? exportAndPublishBuffered
        : exportAndPublishStaged(sink.capture)
      if (Option.isNone(published) === true) return Option.none()
      const { manifest, previousSeqNum, exportMs, hashMs, uploadMs } = published.value

      const timings: PublishTimings = {
        seqNum: manifest.seqNum,
        byteLength: manifest.byteLength,
        exportMs,
        hashMs,
        uploadMs,
        totalMs: performance.now() - totalStart,
        tailEvents: manifest.seqNum - previousSeqNum,
      }

      yield* SubscriptionRef.update(stats, (current) => ({
        ...current,
        publishedCount: current.publishedCount + 1,
        latestManifest: Option.some(manifest),
        lastPublish: Option.some(timings),
      }))
      yield* Effect.logDebug(
        `Checkpointer published snapshot at ${manifest.seqNum} (${manifest.byteLength} bytes, ${timings.totalMs.toFixed(0)}ms)`,
      )

      return Option.some(manifest)
    }).pipe(
      Effect.tapCause((cause) =>
        Cause.hasInterruptsOnly(cause) === true
          ? Effect.void
          : SubscriptionRef.update(stats, (current) => ({ ...current, failedCount: current.failedCount + 1 })),
      ),
      Effect.withSpan('@livestore/common:checkpointer:publish'),
    )

    // Publications are serialized: a manual `publishNow` never races the loop.
    const publishLock = yield* Semaphore.make(1)
    const publishSerialized = publish.pipe(publishLock.withPermits(1))

    const evaluate = Effect.gen(function* () {
      const head = yield* currentHead
      const current = yield* SubscriptionRef.get(stats)
      // A head below the latest snapshot means the backend was reset and its log restarted. (`publish` compares backend
      // identities exactly; this only keeps the trigger from waiting for the old position.)
      const latestSeqNum = Option.match(current.latestManifest, {
        onNone: () => 0,
        onSome: (m) => (m.seqNum <= head ? m.seqNum : 0),
      })
      const decision = SnapshotTrigger.decide(
        {
          tailEvents: head - latestSeqNum,
          snapshotBytes: yield* estimateSnapshotBytes,
          msSinceLastSnapshot: Option.match(current.latestManifest, {
            onNone: () => Number.POSITIVE_INFINITY,
            onSome: (manifest) => now() - manifest.createdAt,
          }),
        },
        params,
      )

      yield* SubscriptionRef.update(stats, (s) => ({
        ...s,
        head,
        snapshotLag: head - latestSeqNum,
        lastDecision: decision,
      }))

      if (decision._tag === 'publish') yield* publishSerialized
    }).pipe(
      // A failed attempt (including defects from a sink) must not end the loop; the next tick retries.
      Effect.catchCauseIf(
        (cause) => Cause.hasInterruptsOnly(cause) === false,
        (cause) => Effect.logWarning('Checkpointer failed to evaluate or publish a snapshot', cause),
      ),
    )

    yield* evaluate.pipe(
      Effect.repeat(Schedule.spaced(options.pollInterval ?? Duration.seconds(1))),
      Effect.interruptible,
      Effect.withSpan('@livestore/common:checkpointer:loop'),
      Effect.forkScoped,
    )

    return {
      stats,
      publishNow: publishSerialized,
    } satisfies Checkpointer
  })
