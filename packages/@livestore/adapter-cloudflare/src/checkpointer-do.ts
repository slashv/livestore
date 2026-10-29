/// <reference types="@cloudflare/workers-types" />

import { DurableObject } from 'cloudflare:workers'

import { Snapshot, type SqliteDb } from '@livestore/common'
import type { CfTypes, ClientDoWithRpcCallback, HelperTypes } from '@livestore/common-cf'
import {
  type Checkpointer,
  type CheckpointerStats,
  Eventlog,
  makeCheckpointer,
  type PublishTimings,
  ShutdownChannel,
} from '@livestore/common/leader-thread'
import type { LiveStoreSchema } from '@livestore/common/schema'
import { stageVfsFile } from '@livestore/sqlite-wasm/cf'
import {
  type CheckpointerWakeInterface,
  makeR2SnapshotSink,
  type SyncBackendRpcInterface,
} from '@livestore/sync-cf/cf-worker'
import { handleSyncUpdateRpc, makeDoRpcSync } from '@livestore/sync-cf/client'
import { checkpointerDurableObjectName } from '@livestore/sync-cf/common'
import {
  type Duration,
  Effect,
  Exit,
  FetchHttpClient,
  type LogLevel,
  Option,
  Queue,
  References,
  Scope,
  SubscriptionRef,
  WebChannel,
} from '@livestore/utils/effect'

import { type DoLeaderThread, makeDoLeaderThread } from './make-leader-thread.ts'

/**
 * RPC surface of a checkpointer Durable Object (experimental snapshot boot, LS.SYS-R06).
 *
 * Every method takes the `storeId` because a Durable Object doesn't know its own name; the first call persists it so
 * alarms can resume the checkpointer after eviction.
 *
 * @experimental
 */
export interface CheckpointerDoRpcInterface extends CheckpointerWakeInterface, ClientDoWithRpcCallback {
  /** Current checkpointer state (boots the checkpointer if needed). For monitoring, tests and benchmarks. */
  stats(storeId: string): Promise<CheckpointerDoStats>
  /** Publishes a snapshot now if the checkpointer's head moved past the latest one, bypassing the cost model. */
  publishNow(storeId: string): Promise<Snapshot.SnapshotManifest | null>
}

/** JSON-friendly {@link CheckpointerStats} plus runtime diagnostics. */
export interface CheckpointerDoStats {
  readonly key: string
  readonly publishedCount: number
  readonly failedCount: number
  readonly latestManifest: Snapshot.SnapshotManifest | null
  readonly lastPublish: PublishTimings | null
  readonly head: number
  readonly snapshotLag: number
  readonly lastDecision: CheckpointerStats['lastDecision'] | null
  readonly runtime: {
    /** Milliseconds the most recent boot of the headless leader took (including the initial pull). */
    readonly bootMs: number
    /** Wasm linear memory of the leader (high-water mark, it never shrinks). */
    readonly wasmHeapBytes: number
    /** Size of the state database (`page_count * page_size`), i.e. the size of the next snapshot. */
    readonly stateDbBytes: number
  }
}

export interface MakeCheckpointerDurableObjectOptions<TEnv> {
  /** Schema of the clients that boot from the published snapshots. Determines the snapshot key. */
  schema: LiveStoreSchema
  /** Binding name of the sync backend Durable Object. */
  syncBackendBinding: HelperTypes.ExtractDurableObjectKeys<TEnv>
  /** Binding name of the R2 bucket for snapshot blobs (the one the worker's snapshot routes serve). */
  bucketBinding: HelperTypes.ExtractR2BucketKeys<TEnv>
  /** Binding name of this checkpointer class; the sync backend calls back through it for live pull. */
  bindingName: HelperTypes.ExtractDurableObjectKeys<TEnv>
  /** Overrides for the publication cost model. */
  trigger?: Partial<Snapshot.SnapshotTrigger.TriggerParams>
  /** How often the trigger is evaluated while the checkpointer is awake. @default 1 second */
  pollInterval?: Duration.Input
  /** How long replaced snapshot blobs stay readable. @default 5 minutes */
  graceMs?: number
  keepAlive?: {
    /** Interval of the keep-alive alarm while the log moves. @default 5 seconds */
    alarmIntervalMs?: number
    /** The checkpointer stops re-arming its alarm after the head has not moved for this long. @default 60 seconds */
    idleTimeoutMs?: number
  }
  /**
   * How snapshots leave the Durable Object:
   * - `streamed`: under the persistence lock the state file's pages are copied inside DO storage, then streamed to R2
   *   as a multipart upload. Memory stays around one part (5 MiB) regardless of state size, at the cost of one extra
   *   DO row write per 8 KiB page per snapshot.
   * - `buffered`: the state database is serialized into wasm and JS memory (about twice the state size) and uploaded
   *   with one `put`. Only viable for state of a few tens of MB under the 128 MB isolate limit.
   *
   * @default 'streamed'
   */
  exportMode?: 'streamed' | 'buffered'
  /** @default 'Info' */
  logLevel?: LogLevel.LogLevel
}

/**
 * Creates a Durable Object class that checkpoints a store: a headless LiveStore leader that follows the sync backend
 * with live pull (persisted in the DO's own storage, so a woken instance only pulls the tail) and publishes state
 * snapshots to R2 according to the snapshot trigger.
 *
 * Instances are named `${storeId}/${snapshotKey}` (see {@link getCheckpointerStub}). The checkpointer stays awake
 * through alarms while the head moves and idles out on a quiet store. It wakes again when the sync backend delivers
 * new events through the live-pull callback, or when the sync DO's `snapshots` option calls `ensureRunning`.
 *
 * @example
 * ```ts
 * export class CheckpointerDO extends makeCheckpointerDurableObject<Env>({
 *   schema,
 *   syncBackendBinding: 'SYNC_BACKEND_DO',
 *   bucketBinding: 'SNAPSHOT_BUCKET',
 *   bindingName: 'CHECKPOINTER_DO',
 * }) {}
 *
 * export class SyncBackendDO extends makeDurableObject({
 *   snapshots: { checkpointerBinding: 'CHECKPOINTER_DO', snapshotKeys: [Snapshot.SnapshotKey.toString(Snapshot.SnapshotKey.make(schema))] },
 * }) {}
 * ```
 *
 * @experimental
 */
export const makeCheckpointerDurableObject = <TEnv>(
  options: MakeCheckpointerDurableObjectOptions<TEnv>,
): new (ctx: CfTypes.DurableObjectState, env: TEnv) => CfTypes.DurableObject & CheckpointerDoRpcInterface => {
  const alarmIntervalMs = options.keepAlive?.alarmIntervalMs ?? 5_000
  const idleTimeoutMs = options.keepAlive?.idleTimeoutMs ?? 60_000

  return class CheckpointerDurableObject extends DurableObjectBase implements CheckpointerDoRpcInterface {
    __DURABLE_OBJECT_BRAND = 'CheckpointerDurableObject' as never
    readonly #ctx: CfTypes.DurableObjectState
    readonly #env: TEnv
    #running: Promise<Running> | undefined
    #lastActivityAt = Date.now()
    #lastSeenHead = -1
    #resetPersistenceOnBoot = false

    constructor(ctx: CfTypes.DurableObjectState, env: TEnv) {
      super(ctx, env)
      this.#ctx = ctx
      this.#env = env
    }

    async ensureRunning(storeId: string, ensureOptions?: { backendId?: string }): Promise<void> {
      this.#lastActivityAt = Date.now()
      const running = await this.#ensure(storeId)
      const followedBackendId = Eventlog.getBackendIdFromDb(running.leader.dbEventlog)
      if (
        ensureOptions?.backendId !== undefined &&
        Option.isSome(followedBackendId) === true &&
        followedBackendId.value !== ensureOptions.backendId
      ) {
        // The backend was reset. Its new history is unrelated to the local copy, so start over from an empty store.
        await this.#restart(running, { resetPersistence: true })
        await this.#ensure(storeId)
      }
      await this.#armAlarm()
    }

    async syncUpdateRpc(payload: Uint8Array<ArrayBuffer>, storeId: string): Promise<void> {
      this.#lastActivityAt = Date.now()
      if (this.#running === undefined) {
        // The sync DO awaits this callback before acknowledging the push, so don't block it on a cold boot.
        // The booting leader pulls from its persisted cursor, which covers the dropped payload.
        this.#ctx.waitUntil(this.ensureRunning(storeId).catch((error: unknown) => console.error(error)))
        return
      }
      await this.#ensure(storeId)
      await this.#armAlarm()
      await handleSyncUpdateRpc(this.#ctx, payload)
    }

    override async alarm(): Promise<void> {
      const storeId = this.#ctx.storage.kv.get<string>(STORE_ID_KEY)
      if (storeId === undefined) return

      const running = await this.#ensure(storeId)
      const { head } = await runPromise(SubscriptionRef.get(running.checkpointer.stats), options.logLevel)
      if (head !== this.#lastSeenHead) {
        this.#lastSeenHead = head
        this.#lastActivityAt = Date.now()
      }

      // Re-arm only while the log moves; a quiet store lets the instance idle out (and be evicted).
      if (Date.now() - this.#lastActivityAt < idleTimeoutMs) {
        await this.#ctx.storage.setAlarm(Date.now() + alarmIntervalMs)
      }
    }

    async stats(storeId: string): Promise<CheckpointerDoStats> {
      const running = await this.#ensure(storeId)
      const stats = await runPromise(SubscriptionRef.get(running.checkpointer.stats), options.logLevel)
      return {
        key: stats.key,
        publishedCount: stats.publishedCount,
        failedCount: stats.failedCount,
        latestManifest: Option.getOrNull(stats.latestManifest),
        lastPublish: Option.getOrNull(stats.lastPublish),
        head: stats.head,
        snapshotLag: stats.snapshotLag,
        lastDecision: stats.lastDecision ?? null,
        runtime: {
          bootMs: running.bootMs,
          wasmHeapBytes: running.leader.wasmHeapBytes(),
          stateDbBytes: stateDbBytes(running.leader),
        },
      }
    }

    async publishNow(storeId: string): Promise<Snapshot.SnapshotManifest | null> {
      const running = await this.#ensure(storeId)
      return runPromise(running.checkpointer.publishNow.pipe(Effect.map(Option.getOrNull)), options.logLevel)
    }

    async #restart(running: Running, { resetPersistence }: { resetPersistence: boolean }): Promise<void> {
      if (this.#running !== undefined && (await this.#running) === running) this.#running = undefined
      await runPromise(Scope.close(running.scope, Exit.void), options.logLevel)
      this.#resetPersistenceOnBoot = resetPersistence
    }

    #ensure(storeId: string): Promise<Running> {
      const persistedStoreId = this.#ctx.storage.kv.get<string>(STORE_ID_KEY)
      if (persistedStoreId === undefined) {
        this.#ctx.storage.kv.put(STORE_ID_KEY, storeId)
      } else if (persistedStoreId !== storeId) {
        return Promise.reject(new Error(`Checkpointer belongs to store '${persistedStoreId}', called for '${storeId}'`))
      }

      if (this.#running === undefined) {
        const running = this.#boot(storeId)
        // A failed boot must not poison the instance: the next call retries.
        running.catch(() => {
          if (this.#running === running) this.#running = undefined
        })
        this.#running = running
      }
      return this.#running
    }

    async #armAlarm(): Promise<void> {
      if ((await this.#ctx.storage.getAlarm()) === null) {
        await this.#ctx.storage.setAlarm(Date.now() + alarmIntervalMs)
      }
    }

    #boot(storeId: string): Promise<Running> {
      const ctx = this.#ctx
      const resetPersistence = this.#resetPersistenceOnBoot
      this.#resetPersistenceOnBoot = false
      const env = this.#env as Record<string, unknown>
      const syncBackendNamespace = env[options.syncBackendBinding] as
        | CfTypes.DurableObjectNamespace<SyncBackendRpcInterface>
        | undefined
      const bucket = env[options.bucketBinding] as CfTypes.R2Bucket | undefined
      const onStopped = (running: Promise<Running>) => {
        if (this.#running === running) this.#running = undefined
      }

      const boot = Effect.gen(function* () {
        if (syncBackendNamespace === undefined) {
          return yield* Effect.die(new Error(`Sync backend binding '${options.syncBackendBinding}' not found on env`))
        }
        if (bucket === undefined) {
          return yield* Effect.die(new Error(`R2 bucket binding '${options.bucketBinding}' not found on env`))
        }

        const start = performance.now()
        const getSyncBackendStub = () => syncBackendNamespace.get(syncBackendNamespace.idFromName(storeId))
        // Lives as long as this instance runs the checkpointer; closed when the leader stops.
        const scope = yield* Scope.make()
        const shutdown = yield* WebChannel.queueChannelProxy({ schema: ShutdownChannel.All }).pipe(Scope.provide(scope))

        const leader = yield* makeDoLeaderThread({
          storage: ctx.storage,
          schema: options.schema,
          storeId,
          // The checkpointer never pushes, so a fixed client id is fine and keeps the persisted eventlog stable.
          clientId: 'livestore-checkpointer',
          resetPersistence,
          syncOptions: {
            backend: makeDoRpcSync({
              getSyncBackendStub,
              durableObjectState: ctx,
              durableObjectContext: { bindingName: options.bindingName, durableObjectId: ctx.id.toString() },
            }),
            livePull: true,
          },
          shutdownChannel: shutdown.webChannel,
        }).pipe(Scope.provide(scope))

        const checkpointer = yield* makeCheckpointer({
          schema: options.schema,
          storeId,
          sink: makeR2SnapshotSink({
            bucket,
            storeId,
            getSyncBackendStub,
            ...(options.graceMs !== undefined ? { graceMs: options.graceMs } : {}),
            ...(options.exportMode === 'buffered'
              ? {}
              : {
                  stage: (dbState: SqliteDb) =>
                    stageVfsFile({
                      sql: ctx.storage.sql,
                      fileName: leader.stateFileName,
                      // The pager's page count, read under the same lock, is what `sqlite3_serialize` would export.
                      pageCount: dbState.select<{ page_count: number }>('PRAGMA page_count')[0]?.page_count ?? 0,
                    }),
                }),
          }),
          ...(options.trigger !== undefined ? { trigger: options.trigger } : {}),
          ...(options.pollInterval !== undefined ? { pollInterval: options.pollInterval } : {}),
        }).pipe(Effect.provide(leader.context), Scope.provide(scope))

        return {
          storeId,
          leader,
          checkpointer,
          scope,
          shutdownQueue: shutdown.sendQueue,
          bootMs: performance.now() - start,
        }
      }).pipe(
        Effect.provide(FetchHttpClient.layer),
        Effect.withSpan('@livestore/adapter-cloudflare:checkpointer:boot', { attributes: { storeId } }),
      )

      const running: Promise<Running> = runPromise(boot, options.logLevel).then((booted) => {
        // When the leader stops (e.g. a backend reset made it clear its storage), drop this instance's checkpointer;
        // the next call or alarm boots a fresh one against the new backend.
        Effect.runFork(
          Queue.take(booted.shutdownQueue).pipe(
            Effect.flatMap((cause) =>
              Effect.logWarning('Checkpointer leader stopped, will reboot on next wake', cause),
            ),
            Effect.andThen(Effect.sync(() => onStopped(running))),
            Effect.andThen(Scope.close(booted.scope, Exit.void)),
          ),
        )
        return booted
      })
      return running
    }
  }
}

/** Stub of the checkpointer for a store and schema, named `${storeId}/${snapshotKey}`. @experimental */
export const getCheckpointerStub = <T extends CheckpointerDoRpcInterface>({
  namespace,
  storeId,
  schema,
}: {
  namespace: CfTypes.DurableObjectNamespace<T>
  storeId: string
  schema: LiveStoreSchema
}) =>
  namespace.get(
    namespace.idFromName(
      checkpointerDurableObjectName({
        storeId,
        key: Snapshot.SnapshotKey.toString(Snapshot.SnapshotKey.make(schema)),
      }),
    ),
  )

interface Running {
  readonly storeId: string
  readonly leader: DoLeaderThread
  readonly checkpointer: Checkpointer
  readonly scope: Scope.Closeable
  readonly shutdownQueue: Queue.Dequeue<typeof ShutdownChannel.All.Type>
  readonly bootMs: number
}

const STORE_ID_KEY = 'livestore-checkpointer:storeId'

// oxlint-disable-next-line typescript-eslint(no-unsafe-type-assertion) -- `cloudflare:workers` types the base class against the ambient env type
const DurableObjectBase = DurableObject as unknown as new (
  ctx: CfTypes.DurableObjectState,
  env: unknown,
) => CfTypes.DurableObject

const runPromise = <A, E>(effect: Effect.Effect<A, E>, logLevel: LogLevel.LogLevel | undefined): Promise<A> =>
  effect.pipe(
    Effect.tapCauseLogPretty,
    Effect.provideService(References.MinimumLogLevel, logLevel ?? 'Info'),
    Effect.runPromise,
  )

const stateDbBytes = (leader: DoLeaderThread) => {
  const [pageCount] = leader.dbState.select<{ page_count: number }>('PRAGMA page_count')
  const [pageSize] = leader.dbState.select<{ page_size: number }>('PRAGMA page_size')
  return (pageCount?.page_count ?? 0) * (pageSize?.page_size ?? 0)
}
