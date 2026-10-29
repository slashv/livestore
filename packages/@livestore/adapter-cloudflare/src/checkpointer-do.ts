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
import { dropStagedVfsFiles, type StagedVfsFile, stageVfsFile } from '@livestore/sqlite-wasm/cf'
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
  Fiber,
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
  /** Whether a checkpointer runs in this instance, without booting one. */
  status(): Promise<CheckpointerDoStatus>
  /** Publishes a snapshot now if the checkpointer's head moved past the latest one, bypassing the cost model. */
  publishNow(storeId: string): Promise<Snapshot.SnapshotManifest | null>
}

/** Cheap probe that never boots the checkpointer. */
export interface CheckpointerDoStatus {
  /** Store this instance checkpoints, once any call told it. */
  readonly storeId: string | null
  /** Whether this instance currently runs (or boots) a leader. */
  readonly running: boolean
  /** `ensureRunning` calls received by this instance (e.g. wake-ups from the sync backend on push). */
  readonly ensureRunningCalls: number
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
    /**
     * Set while a running checkpointer shuts down. `#ensure` waits for it, so a call arriving mid-shutdown never boots
     * a second leader on the same storage.
     */
    #transition: Promise<void> | undefined
    /** Boot generation of `#running`, so a finished shutdown only clears the instance it stopped. */
    #runningGeneration = 0
    #lastSeenHead = -1
    #resetPersistenceOnBoot = false
    #ensureRunningCalls = 0
    /** Staged snapshot copies not yet released, i.e. streamed publications in flight. */
    #stagedInFlight = 0
    /** Highest backend head reported by wake-ups, and since when the leader has been behind it. */
    #expectedHead = 0
    #behindSince: number | undefined

    constructor(ctx: CfTypes.DurableObjectState, env: TEnv) {
      super(ctx, env)
      this.#ctx = ctx
      this.#env = env
    }

    async ensureRunning(storeId: string, ensureOptions?: { backendId?: string; head?: number }): Promise<void> {
      this.#ensureRunningCalls++
      if (ensureOptions?.head !== undefined && ensureOptions.head > this.#expectedHead) {
        this.#expectedHead = ensureOptions.head
      }
      this.#markActive()
      const running = await this.#ensure(storeId)
      const followedBackendId = Eventlog.getBackendIdFromDb(running.leader.dbEventlog)
      if (
        ensureOptions?.backendId !== undefined &&
        Option.isSome(followedBackendId) === true &&
        followedBackendId.value !== ensureOptions.backendId
      ) {
        // The backend was reset. Its new history is unrelated to the local copy, so start over from an empty store.
        await this.#stop(running, { resetPersistence: true })
        await this.#ensure(storeId)
      }
      await this.#armAlarm()
    }

    async syncUpdateRpc(payload: Uint8Array<ArrayBuffer>, storeId: string): Promise<void> {
      this.#markActive()
      // The sync DO awaits this callback during push broadcast, so never wait for a boot here. Routing is synchronous:
      // a running pull takes the update, a pull still catching up buffers it, and with no pull it is dropped (the
      // leader pulls from its persisted cursor when it boots).
      await handleSyncUpdateRpc(this.#ctx, payload)
      this.#ctx.waitUntil(
        this.#ensure(storeId)
          .then(() => this.#armAlarm())
          .catch((error: unknown) => console.error('Checkpointer failed to start', error)),
      )
    }

    override async alarm(): Promise<void> {
      const storeId = this.#ctx.storage.kv.get<string>(STORE_ID_KEY)
      if (storeId === undefined) return

      // A leftover alarm on a quiet, evicted store must not boot a whole leader just to find nothing to do.
      if (this.#running === undefined && this.#idleForMs() >= idleTimeoutMs) return

      const running = await this.#ensure(storeId)
      const { head } = await runPromise(SubscriptionRef.get(running.checkpointer.stats), options.logLevel)
      if (head !== this.#lastSeenHead) {
        this.#lastSeenHead = head
        this.#markActive()
      }

      // A lost live-pull callback is only detected (as a gap) when the next one arrives. If the leader stays behind the
      // head a wake-up reported, restart it so it pulls from its persisted cursor.
      if (head >= this.#expectedHead) {
        this.#behindSince = undefined
      } else if (this.#behindSince === undefined) {
        this.#behindSince = Date.now()
      } else if (Date.now() - this.#behindSince > 2 * alarmIntervalMs) {
        this.#behindSince = undefined
        console.warn(`Checkpointer stuck at ${head}, backend head is ${this.#expectedHead}; re-pulling`)
        await this.#stop(running, { resetPersistence: false })
        await this.#ensure(storeId)
      }

      // Re-arm while the log moves or a streamed publication is still uploading; a quiet store lets the instance idle
      // out (and be evicted).
      if (this.#idleForMs() < idleTimeoutMs || this.#stagedInFlight > 0 || this.#behindSince !== undefined) {
        await this.#ctx.storage.setAlarm(Date.now() + alarmIntervalMs)
      }
    }

    async status(): Promise<CheckpointerDoStatus> {
      return {
        storeId: this.#ctx.storage.kv.get<string>(STORE_ID_KEY) ?? null,
        running: this.#running !== undefined,
        ensureRunningCalls: this.#ensureRunningCalls,
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
      // Runs inside the leader's scope, so stopping the checkpointer interrupts it instead of letting it outlive the
      // leader (and race the next leader's staging).
      return runPromise(
        running.checkpointer.publishNow.pipe(
          Effect.map(Option.getOrNull),
          Effect.forkIn(running.scope),
          Effect.flatMap(Fiber.join),
        ),
        options.logLevel,
      )
    }

    /**
     * Shuts a running checkpointer down. The reset flag is set first and `#running` is cleared only once its scope has
     * closed, all while `#transition` holds back `#ensure`, so no second leader boots on the same storage meanwhile.
     */
    #stop(running: Running, { resetPersistence }: { resetPersistence: boolean }): Promise<void> {
      // Recorded even when joining an in-flight shutdown, so a reset requested meanwhile is not lost.
      if (resetPersistence === true) this.#resetPersistenceOnBoot = true
      if (this.#transition !== undefined) return this.#transition
      const transition = runPromise(Scope.close(running.scope, Exit.void), options.logLevel)
        .catch((error: unknown) => console.error('Checkpointer shutdown failed', error))
        .finally(() => {
          if (this.#runningGeneration === running.generation) this.#running = undefined
          this.#transition = undefined
        })
      this.#transition = transition
      return transition
    }

    async #ensure(storeId: string): Promise<Running> {
      const persistedStoreId = this.#ctx.storage.kv.get<string>(STORE_ID_KEY)
      if (persistedStoreId === undefined) {
        this.#ctx.storage.kv.put(STORE_ID_KEY, storeId)
      } else if (persistedStoreId !== storeId) {
        throw new Error(`Checkpointer belongs to store '${persistedStoreId}', called for '${storeId}'`)
      }

      while (this.#transition !== undefined) await this.#transition

      if (this.#running === undefined) {
        const generation = ++this.#runningGeneration
        const running = this.#boot(storeId, generation)
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

    /** Last activity is persisted (throttled to once per second) so alarms after eviction can tell a quiet store. */
    #markActive() {
      const now = Date.now()
      const persisted = this.#ctx.storage.kv.get<number>(LAST_ACTIVITY_KEY) ?? 0
      if (now - persisted >= 1_000) this.#ctx.storage.kv.put(LAST_ACTIVITY_KEY, now)
    }

    #idleForMs() {
      return Date.now() - (this.#ctx.storage.kv.get<number>(LAST_ACTIVITY_KEY) ?? 0)
    }

    #boot(storeId: string, generation: number): Promise<Running> {
      const ctx = this.#ctx
      const resetPersistence = this.#resetPersistenceOnBoot
      this.#resetPersistenceOnBoot = false
      const env = this.#env as Record<string, unknown>
      const syncBackendNamespace = env[options.syncBackendBinding] as
        | CfTypes.DurableObjectNamespace<SyncBackendRpcInterface>
        | undefined
      const bucket = env[options.bucketBinding] as CfTypes.R2Bucket | undefined
      const onStopped = (running: Running) => this.#stop(running, { resetPersistence: false })
      const trackStaged = (staged: StagedVfsFile): StagedVfsFile => {
        this.#stagedInFlight++
        let released = false
        return {
          ...staged,
          release: () => {
            if (released === true) return
            released = true
            this.#stagedInFlight--
            staged.release()
          },
        }
      }

      const boot = Effect.gen(function* () {
        if (syncBackendNamespace === undefined) {
          return yield* Effect.die(new Error(`Sync backend binding '${options.syncBackendBinding}' not found on env`))
        }
        if (bucket === undefined) {
          return yield* Effect.die(new Error(`R2 bucket binding '${options.bucketBinding}' not found on env`))
        }

        const start = performance.now()
        // Copies of an earlier instance that was evicted mid-upload are never released by anyone else.
        dropStagedVfsFiles(ctx.storage.sql)
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
                    trackStaged(
                      stageVfsFile({
                        sql: ctx.storage.sql,
                        fileName: leader.stateFileName,
                        // The pager's page count, read under the same lock, is what `sqlite3_serialize` would export.
                        pageCount: dbState.select<{ page_count: number }>('PRAGMA page_count')[0]?.page_count ?? 0,
                      }),
                    ),
                }),
          }),
          ...(options.trigger !== undefined ? { trigger: options.trigger } : {}),
          ...(options.pollInterval !== undefined ? { pollInterval: options.pollInterval } : {}),
        }).pipe(Effect.provide(leader.context), Scope.provide(scope))

        return {
          generation,
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

      return runPromise(boot, options.logLevel).then((booted) => {
        // When the leader stops (e.g. a backend reset made it clear its storage), shut this checkpointer down; the next
        // call or alarm boots a fresh one against the new backend.
        Effect.runFork(
          Queue.take(booted.shutdownQueue).pipe(
            Effect.flatMap((cause) =>
              Effect.logWarning('Checkpointer leader stopped, will reboot on next wake', cause),
            ),
            Effect.andThen(Effect.promise(() => onStopped(booted))),
          ),
        )
        return booted
      })
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
  readonly generation: number
  readonly storeId: string
  readonly leader: DoLeaderThread
  readonly checkpointer: Checkpointer
  readonly scope: Scope.Closeable
  readonly shutdownQueue: Queue.Dequeue<typeof ShutdownChannel.All.Type>
  readonly bootMs: number
}

const STORE_ID_KEY = 'livestore-checkpointer:storeId'
const LAST_ACTIVITY_KEY = 'livestore-checkpointer:lastActivityAt'

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
