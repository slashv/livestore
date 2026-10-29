import {
  type Adapter,
  ClientSessionLeaderThreadProxy,
  type LockStatus,
  makeClientSession,
  type SyncOptions,
  UnknownError,
} from '@livestore/common'
import type { CfTypes } from '@livestore/common-cf'
import { type DevtoolsOptions, LeaderThreadCtx, streamEventsWithSyncState } from '@livestore/common/leader-thread'
import { Effect, FetchHttpClient, Queue, SubscriptionRef } from '@livestore/utils/effect'

import { makeDoLeaderThread } from './make-leader-thread.ts'

export const makeAdapter =
  ({
    storage,
    clientId,
    syncOptions,
    sessionId,
    resetPersistence = false,
  }: {
    storage: CfTypes.DurableObjectStorage
    clientId: string
    syncOptions: SyncOptions
    sessionId: string
    resetPersistence?: boolean
  }): Adapter =>
  (adapterArgs) =>
    Effect.gen(function* () {
      const {
        storeId,
        /* devtoolsEnabled, shutdown, bootStatusQueue,  */
        syncPayloadEncoded,
        syncPayloadSchema,
        schema,
        params,
      } = adapterArgs

      const devtoolsOptions = { enabled: false } as DevtoolsOptions

      const { context, dbEventlog, makeSqliteDb } = yield* makeDoLeaderThread({
        storage,
        schema,
        storeId,
        clientId,
        syncOptions,
        resetPersistence,
        syncPayloadEncoded,
        syncPayloadSchema,
        params,
      })

      // The client session queries an in-memory copy of the state, seeded from the leader's state below.
      const syncInMemoryDb = yield* makeSqliteDb({ _tag: 'in-memory', configureDb: () => {} }).pipe(
        UnknownError.mapToUnknownError,
      )

      const { leaderThread, initialSnapshot } = yield* Effect.gen(function* () {
        const { syncProcessor, extraIncomingMessagesQueue, initialState, networkStatus, exportState } =
          yield* LeaderThreadCtx

        // Snapshot and head come from one locked read, so the session never pairs state with a different position.
        const { bytes: initialSnapshot, head: initialLeaderHead } = yield* exportState

        const leaderThread = ClientSessionLeaderThreadProxy.of(
          {
            events: {
              pull: ({ cursor }) => syncProcessor.pull({ cursor }),
              push: syncProcessor.push,
              stream: (options) =>
                streamEventsWithSyncState({
                  dbEventlog,
                  syncState: syncProcessor.syncState,
                  options,
                }),
            },
            initialState: {
              leaderHead: initialLeaderHead,
              migrationsReport: initialState.migrationsReport,
              storageMode: 'persisted',
            },
            export: exportState.pipe(Effect.map(({ bytes }) => bytes)),
            getEventlogData: Effect.sync(() => dbEventlog.export()),
            syncState: syncProcessor.syncState,
            sendDevtoolsMessage: (message) => Queue.offer(extraIncomingMessagesQueue, message),
            networkStatus,
          },
          {
            // overrides: testing?.overrides?.clientSession?.leaderThreadProxy
          },
        )

        return { leaderThread, initialSnapshot }
      }).pipe(Effect.provide(context))

      syncInMemoryDb.import(initialSnapshot)

      const lockStatus = yield* SubscriptionRef.make<LockStatus>('has-lock')

      const clientSession = yield* makeClientSession({
        ...adapterArgs,
        sqliteDb: syncInMemoryDb,
        webmeshMode: 'proxy',
        connectWebmeshNode: Effect.fnUntraced(function* ({ webmeshNode }) {
          if (devtoolsOptions.enabled === true) {
            console.log('connectWebmeshNode', { webmeshNode })
            //   yield* Webmesh.connectViaWebSocket({
            //     node: webmeshNode,
            //     url: `ws://${devtoolsOptions.host}:${devtoolsOptions.port}`,
            //     openTimeout: 500,
            //   }).pipe(Effect.tapCauseLogPretty, Effect.forkScoped)
          }
        }),
        leaderThread,
        lockStatus,
        clientId,
        sessionId,
        isLeader: true,
        // Not really applicable for node as there is no "reload the app" concept
        registerBeforeUnload: (_onBeforeUnload) => () => {},
        origin: undefined,
      })

      return clientSession
    }).pipe(
      Effect.withSpan('@livestore/adapter-cloudflare:makeAdapter', { attributes: { clientId, sessionId } }),
      Effect.provide(FetchHttpClient.layer),
    )
