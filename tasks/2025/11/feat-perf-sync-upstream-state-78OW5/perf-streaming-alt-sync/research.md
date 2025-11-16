# Research

## Context
- The perf-streaming harness currently depends on Cloudflare Durable Object sync (`tests/perf-streaming/src/cf-worker/index.ts`) and a WebSocket transport (`test-app/src/livestore.worker.ts`).
- Leader thread streaming relies on `Eventlog.updateBackendHead` invoked by the sync processor when pull chunks arrive (`packages/@livestore/common/src/leader-thread/LeaderSyncProcessor.ts`).

## Alternative confirmation hooks
- `packages/@livestore/common/src/sync/mock-sync-backend.ts` provides an in-process sync backend that queues pushed events and replays them through a live pull stream, advancing internal `syncEventSequenceNumberRef` and feeding `syncPullQueue`.
- The mock backend exposes `makeSyncBackend` returning a fully-fledged `SyncBackend` compatible with `makeWorker` expectations, including `connect`, `pull`, `push`, `ping`, and `isConnected` wiring.
- `MockSyncBackend` automatically enqueues pushed events for downstream pull streams, which causes `backgroundBackendPulling` to merge them and call `Eventlog.updateBackendHead`, satisfying `streamEventsWithSyncState` requirements.

## Supporting considerations
- `makeWorker` accepts any `SyncBackendConstructor`, so the perf harness can inject a custom constructor without touching shared packages.
- Keeping everything inside the perf harness avoids touching adapter internals and fits the "localized changes" guidance.
- We must ensure the mock backend boots connected (`startConnected: true`) so pushes do not stall waiting for connectivity.
- Since the harness only needs confirmation semantics, we can skip Cloudflare-specific payload validation and the wrangler dev server.
