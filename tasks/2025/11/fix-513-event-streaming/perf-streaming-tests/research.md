# Research Notes - perf-streaming-tests

## Existing Perf Harness
- `tests/perf/test-app/` renders a LiveStore-powered table app with bulk commit buttons (1k/10k create, append, update, clear) wired through synced events defined in `schema.ts`.
- Playwright suites under `tests/perf/tests/suites/` reuse `repeatSuite` to repeat latency runs 15x and memory runs once. Latency relies on Event Timing API for measurements; memory uses `Runtime.getHeapUsage` after requesting GC.
- Metrics are emitted through `tests/perf/tests/measurements-reporter.ts`, tagging annotations and printing tables plus OTLP summaries. `playwright.config.ts` boots the app via `pnpm test-app` (`vite preview`).

## Streaming Mechanics
- `packages/@livestore/common/src/leader-thread/stream-events.ts` paginates eventlog batches behind a queue that blocks once the cursor catches the upstream head. Streaming only resumes when `SyncState` pushes a newer head; optional `until` halts emission once reached.
- `packages/@livestore/common/src/leader-thread/eventlog.ts` exposes `getEventsFromEventlog` with filters and enforces `seqNumGlobal` <= upstream head, `updateBackendHead` persists the durable head value.
- `tests/perf/scripts/stream-events-benchmark.ts` fabricates `syncState` updates, importing sqlite snapshots, manually raises upstream head to unblock `streamEventsWithSyncState`, and reports throughput / first-event latency.

## Existing Streaming UI Example
- `examples/web-todomvc-sync-cf/` demonstrates Cloudflare Durable Object backed syncing. `MainSection.tsx` consumes `store.events()` via an async iterator while the app talks to the DO defined in `src/cf-worker/index.ts`.
- Wrangler config (`wrangler.toml`) provisions the DO and dev/prod environments; the Vite dev server launches front-end and worker together.

## Constraints / Gaps
- Current perf harness lacks any streaming UI and no backend confirmation, so Playwright cannot observe event arrival timing.
- Streaming requires confirmed upstream head advance; without a backend, `streamEventsWithSyncState` stalls once cursor reaches head. A real worker or manual head advancement is required.
- We need deterministic, queryable DOM markers for Playwright to detect “last streamed event” plus consistent dataset sizing to keep metrics comparable with existing perf dashboards.

## Reset mechanics discovery (2025-11-16)
- Web adapter exposes a `resetPersistence` flag propagated from `makePersistedAdapter` down to the worker, shared worker, and client session. When true during boot, the client session sends an `IntentionalShutdownCause` with `reason: 'adapter-reset'` via `makeShutdownChannel(storeId)`, then calls `resetPersistedDataFromClientSession({ storageOptions, storeId })` which clears the OPFS directory derived from the store id.
- `resetPersistedDataFromClientSession` ultimately deletes the OPFS subtree via `navigator.storage.getDirectory()`; it retires until successful, so it is safe to reuse in custom flows as long as the relevant workers are shut down first.
- Shared worker hosts the leader thread and caches invariants. It listens on the same shutdown broadcast channel and tears down the leader worker scope when `IntentionalShutdownCause` arrives.
- `LiveStoreProvider` keeps the active store reference in `ctxValueRef`; calling `store.shutdownPromise()` (or letting the broadcast channel deliver the shutdown) transitions provider state to `stage: 'shutdown'`, allowing a remount with fresh adapter options to boot a clean store in the same tab.
- Cloudflare adapter’s `makeAdapter` honors `resetPersistence` by deleting matching `vfs_*` rows for state/eventlog file patterns within the durable object transaction. This mirrors the OPFS reset but leaves any other keyed data intact.
- Durable object storage layer `SyncStorage.resetStore` is implemented as `ctx.storage.deleteAll()` in `@livestore/sync-cf/src/cf-worker/do/sync-storage.ts`. The helper isn’t wired to public routes but is available if we add an admin endpoint or RPC.
- Integration worker fixture (`tests/integration/.../TestStoreDo`) demonstrates how to expose `/store/reset` that boots the DO with `resetPersistence: true`, captures before/after snapshots, and returns them for assertions.
- `SyncMessage` schema already defines `AdminResetRoomRequest/AdminResetRoomResponse`, hinting at an intended remote reset RPC that we could surface via HTTP/WS transport with an authenticated secret.

## Local reset strategy evaluation
- **Cold remount with `resetPersistence` (page reload or React key swap)**
  - Trigger: wrap `LiveStoreProvider` in a component keyed off a `resetCounter`; when reset is requested, increment counter, render provider with `makePersistedAdapter({ resetPersistence: true })` for that mount only.
  - Guarantees: adapter deletes OPFS data, shared worker is torn down, new store boots from clean snapshot. Cloudflare DO is untouched, so replayed events will rehydrate once sync runs unless backend is cleared separately.
  - Costs: requires tearing down React tree; UI flickers unless we hide during reset. Need to ensure generator/stream timers are stopped beforehand (already handled in `handleResetHarness`).
  - Implementation detail: we can reuse existing adapter factory but pass `resetPersistence: resetFlag` on demand since the helper already accepts it. After boot finishes, revert flag to avoid paying OPFS delete cost on every run.
- **Runtime wipe without remounting React tree**
  - Steps: (1) Acquire `store` from context, call `store.shutdownPromise()` to let LiveStore cleanly flush fibers. (2) Call `makeShutdownChannel(STORE_ID)` and send `IntentionalShutdownCause.make({ reason: 'adapter-reset' })` so worker/shared-worker exit. (3) Await promises, then call `resetPersistedDataFromClientSession({ storageOptions: { type: 'opfs' }, storeId })` directly to delete files. (4) Instantiate a new adapter + store (either by re-running `createStore` manually or by toggling provider state) and resume streaming.
  - Pros: can keep the outer React shell alive; we control exact timing and can show progress states.
  - Cons: `resetPersistedDataFromClientSession` deletes directories synchronously with retries; we must ensure no worker holds file handles or the deletion fails. Without provider remount we need custom wiring to create a new store instance and provide it to components (e.g. store registry or manual `LiveStoreProvider` swap). Similar complexity to cold remount, so only worth it if we need UI continuity.
  - Caveats: Any outstanding `store.commit` futures (our harness uses synchronous commits) will reject if we already shut down. Need to guard generator/stream intervals before calling this flow.
- **Browser-level nuke (`navigator.storage.getDirectory().remove()` / clear-site-data)**
  - Works regardless of LiveStore, but requires devtools access or custom helper. Not automatable in Playwright without exposing JS bridge. More destructive than necessary and still leaves DO state intact.

## Sync backend reset strategy evaluation
- **Cloudflare Durable Object rebootstrap with `resetPersistence`**
  - Approach: mirror the integration test worker. Create an authenticated control route (e.g. `/internal/reset?token=...`) that calls `SyncBackend.handleSyncRequest`? Instead, instantiate the DO via `env.SYNC_BACKEND_DO.idFromName(storeId)` and send a custom admin RPC calling `ctx.storage.deleteAll()` or re-running the adapter with `resetPersistence: true`.
  - Implementation detail: extend our Worker fetch handler to detect `/internal/reset` and dispatch to a helper that constructs a new `createStoreDoPromise` with `{ resetPersistence: true }` (similar to docs snippet). Because the DO host already exposes `SyncBackendDO`, we can create a temporary store client inside the worker to trigger the adapter’s reset logic which deletes the VFS rows for both state and eventlog.
  - Guarantees: resets both state + event log persistence for the store id. Any clients reconnecting will start from an empty log.
  - Caveats: booting a throwaway store per reset is heavier (~100ms) but acceptable for perf harness. Need to guard route with shared secret to avoid unauthenticated resets.
- **Direct `ctx.storage.deleteAll()` admin endpoint**
  - Approach: augment `SyncBackendDO` subclass with a method exposed via HTTP or admin RPC that wraps `syncStorage.resetStore` (already exported by `makeStorage`). We can add a branch in worker fetch (e.g. `/internal/reset-eventlog`) that resolves DO stub and invokes a new `reset()` RPC implemented via DO WebSocket/HTTP channel.
  - Pros: minimal overhead; no need to spin up adapter. We can pass store id and optional backend identity, and the deletion is executed inside DO storage transaction.
  - Cons: `makeDurableObject` currently returns a class without extra methods, but we can extend the generated class to add a handler for our admin path. Requires careful wiring to keep generic `makeDurableObject` unaffected.
- **`AdminResetRoomRequest` over existing transports**
  - Approach: implement handling for `AdminResetRoomRequest` (currently unused) in sync backend transports. The schema already lives in `SyncMessage`. We could modify `makePush`/`makePull` or RPC server to accept admin message and call `resetStore` after verifying `ADMIN_SECRET`.
  - Pros: would work across HTTP/WS/DO-RPC consistently; reuse existing serialization.
  - Cons: more invasive change to shared sync provider library; overkill for harness-only need unless we upstream support for devtools.
- **Store-id rotation**
  - Approach: generate a new random `storeId` and pass it to both LiveStoreProvider and sync requests. Guarantees a clean slate with no backend changes.
  - Drawbacks: leaves old OPFS directory + DO eventlog orphaned. Over time this pollutes storage and makes metrics harder to compare. Requires restarting the sync backend stub or ensuring router accepts multiple active stores.

## Fallback comparison
- Store-id rotation is the lowest-effort escape hatch: we can reuse existing harness mechanics by updating `STORE_ID` and letting both app + worker read a dynamic id. However, this only hides accumulated state—Durable Object still holds old eventlogs and the client leaves OPFS directories behind. Eventually we would need tooling to prune these artifacts, and performance metrics might drift because the backend has to manage many historical rooms.
- Relying solely on local `resetPersistence` without touching the backend resets browser state but immediately replays old events from DO. Useful for quick local visual cleanup, but Playwright assertions about empty event list would fail once sync reconnects.
- Backend-only reset (`ctx.storage.deleteAll()`) without clearing OPFS leaves the client eventlog inconsistent; on next connect, the client pushes its local backlog back into the fresh DO. For a true clean slate we must coordinate both sides.
- Combined local + backend reset (either via cold remount + authenticated backend reset endpoint, or via manual broadcast + OPFS delete + backend reset) is the only path that guarantees both logs and derived state are empty. Slightly more wiring, but keeps storage tidy and metrics reproducible.

## Recommended reset flow
| Step | Action | Notes |
| --- | --- | --- |
| 1 | Stop harness timers and call `store.shutdownPromise()` | Ensure generator/stream loops are halted before shutdown. |
| 2 | Broadcast `IntentionalShutdownCause.make({ reason: 'adapter-reset' })` via `makeShutdownChannel(STORE_ID)` | Shared worker + leader thread exit, releasing OPFS handles. |
| 3 | Call `resetPersistedDataFromClientSession({ storageOptions: { type: 'opfs' }, storeId })` | Deletes local state/eventlog databases. Wrap in try/catch to surface failure in UI. |
| 4 | Hit Cloudflare worker admin endpoint (new) that runs `ctx.storage.deleteAll()` or boots `createStoreDoPromise(... resetPersistence: true ...)` | Requires secret `ADMIN_SECRET`; ensures upstream eventlog is wiped. |
| 5 | Recreate adapter/provider (e.g. bump a `resetKey` state) so LiveStore boots fresh and reconnects to cleared backend | React remount keeps UX manageable; once booted we can reseed events. |

### Suggested implementation steps
- Add a `POST /internal/reset` route to `tests/perf-streaming/src/cf-worker/index.ts` guarded by `ADMIN_SECRET`. Inside, instantiate a temporary store with `resetPersistence: true`, await completion, and return a JSON body confirming counts.
- Extend the test app with a `useResetHarness` hook that wraps the five steps above. Use a modal or inline status to avoid confusing the user during reset.
- For Playwright, expose a `window.__streamPerfReset()` helper that triggers the hook so tests can guarantee a clean slate between runs.
- Optionally, add a fallback path to rotate `storeId` for debugging, but hide it behind a query param to avoid polluting metrics.
