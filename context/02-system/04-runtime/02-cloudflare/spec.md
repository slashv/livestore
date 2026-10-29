# Cloudflare Runtime — Spec

This document specifies the Cloudflare adapter
(`packages/@livestore/adapter-cloudflare`). It builds on
[requirements.md](./requirements.md); the mechanism-agnostic contract is in
[../spec.md](../spec.md).

## Status

Draft.

## Model

`createStoreDo` (`create-store-do.ts`) boots a full LiveStore client inside a
Durable Object:

```
Durable Object (one client)
  createStore(schema, adapter)          @livestore/livestore
    └─ makeAdapter(...)                 make-adapter.ts
         ├─ session SQLite (in-memory, DO storage-backed factory)
         ├─ makeLeaderThreadLayer       leader colocated in the DO
         │    ├─ dbEventlog / dbState   make-sqlite-db.ts (DO storage VFS,
         │    │                          CF_SQL_VFS_REQUIRED_PRAGMAS)
         │    └─ sync backend           via SYNC_BACKEND_DO RPC stub
         └─ ClientSessionLeaderThreadProxy (in-process)
```

Inputs: `schema`, `storeId`, `clientId`, `sessionId`, the DO's own
`ctx`/`env`/`bindingName` (so the sync backend can call back for live pull),
and `getSyncBackendStub` (returns a `@livestore/sync-cf/cf-worker` RPC stub;
called for every sync call so a retry gets a fresh stub, see sync
[decision 0006](../../03-sync/03-cf/.decisions/0006-do-rpc-transport-failures.md)).
`livePull: false` is the default (LS.SYS.RT.CF-R03).

`createStoreDo.params.stateRebuildBatchSize` forwards the per-client rebuild
setting. A Durable Object can choose a smaller batch than clients with more
memory, trading lower per-batch resource use for more queries, savepoints and
storage writes. The event-count limit is not a whole-isolate memory guarantee.

Persistence keys are versioned with `liveStoreStorageFormatVersion` and the
schema hash, so schema changes recreate state rather than migrate it in
place.

## Platform Adaptations

Colocation and the `SqlStorage` API force several degenerate or adapted
behaviors versus the portable contract:

- **No shutdown channel** — `WebChannel.noopChannel`; with a single context
  there is nothing to broadcast to (degenerate case of LS.SYS.RT-R06).
- **Devtools disabled** — `devtoolsOptions.enabled` is hardcoded false; the
  websocket webmesh connect is commented out (stub). `webmeshMode` is
  `'proxy'` (web adapters use `'direct'`).
- **Transaction control is dropped** — `BEGIN`/`COMMIT`/`ROLLBACK`/
  `SAVEPOINT` statements are silently discarded on the `SqlStorage` path
  (`make-sqlite-db.ts`). Safe because the eventlog is append-only and
  idempotent, state is rebuildable, and the DO is single-threaded — but it
  means SQLite transaction semantics do not exist on this realization.
  Why the state DB runs WASM SQLite over a VFS instead of `SqlStorage`
  directly is recorded in
  [.decisions/0001-sqlite-over-vfs.md](./.decisions/0001-sqlite-over-vfs.md).
- **`export()`/`import()` are no-ops** — `SqlStorage` has no
  serialize/deserialize; the session's initial snapshot import is therefore
  also a no-op (leader and session share the isolate anyway).
- **`resetPersistence` spans four tables** — `vfs_pages` (state VFS),
  `__livestore_state_files` (adapter ownership), `eventlog`, and
  `__livestore_sync_status` (direct), inside
  `storage.transactionSync`.

## Obsolete State Cleanup

Before opening a state database, register its exact VFS path in the adapter-owned
`__livestore_state_files` table. Registration failure stops boot before opening the
file. Existing registrations are reused without writing rows. This registry is
adapter metadata, separate from the eventlog and the materialized state schema.

After adapter boot succeeds, remove pages belonging to other registered paths
and their ownership records together in `storage.transactionSync`. Keep the
current state, unregistered VFS files, eventlog and sync metadata. Failed replay
or migration hooks retain every registered file. Cleanup failure logs a warning
and leaves completed state available, with ownership intact for retry on the
next successful boot. Run cleanup on completed-state reuse too.

Filename shape is not proof of ownership. Old files that predate registration
remain untouched unless the adapter subsequently opens and registers that exact
file. Consequently this prevents new orphan accumulation without automatically
reclaiming all historical orphans. See
[the ownership decision](./.decisions/0002-state-file-ownership.md).

Once clean, later boots write no rows for registration or cleanup. Deleting a
previous state means returning to that schema rebuilds from the eventlog. Cleanup
consumes billed row writes and does not explicitly compact the underlying DO
database. Because it follows successful boot, it cannot recover a database that
is already too full to rebuild.

## Snapshot Checkpointer

**Maturity: experimental**

`@livestore/adapter-cloudflare` hosts the snapshot checkpointer
([`../../03-sync/04-snapshots/`](../../03-sync/04-snapshots/spec.md#checkpointer)):
`makeCheckpointerDurableObject({ schema, syncBackendBinding, bucketBinding,
bindingName, trigger?, pollInterval?, graceMs?, keepAlive?, exportMode?,
logLevel? })` returns a Durable Object class. Each instance, named
`${storeId}/${key}` (`getCheckpointerStub`), runs a headless leader
(`makeDoLeaderThread({ headless: true })`, fixed client id
`livestore-checkpointer`) that live-pulls from the sync Durable Object over
DO RPC, never pushes, and runs `makeCheckpointer` with an R2 sink plus the sync
DO's manifest RPC. Each schema line therefore gets its own checkpointer.

- **RPC.** `ensureRunning(storeId, { backendId?, head? })` boots the leader if
  needed and arms the alarm; a different `backendId` than the one it followed
  (backend reset) stops it, resets its persistence and boots it fresh.
  `stats(storeId)` returns the checkpointer stats plus runtime diagnostics
  (boot time, wasm heap size, state database size) and boots if needed;
  `status()` reports `{ storeId, running, ensureRunningCalls }` without
  booting; `publishNow(storeId)` publishes inside the leader's scope,
  bypassing the cost model. The first call persists the `storeId`, so alarms
  can resume after eviction; a call for another store fails.
- **Alarms and idling.** Live-pull callbacks and `ensureRunning` mark activity
  (persisted, throttled to once per second) and arm an alarm every
  `keepAlive.alarmIntervalMs` (default 5 seconds). The alarm re-arms while the
  head moved within `keepAlive.idleTimeoutMs` (default 60 seconds), while a
  streamed upload is in flight, or while the leader is behind the head a
  wake-up reported; otherwise the instance idles out. A leftover alarm on an
  idle, evicted instance does not boot a leader. A leader that stays behind a
  reported head for more than two alarm intervals (a lost live-pull callback)
  is restarted so it re-pulls from its persisted cursor.
- **Lifecycle.** Shutdowns and restarts go through one transition that new
  calls await, so no second leader boots on the same storage. When the leader
  stops on its own (e.g. a backend reset cleared its storage), the instance
  shuts the checkpointer down and the next call or alarm boots a fresh one.
- **Export.** `exportMode: 'streamed'` (default) passes `stage` to the R2 sink:
  under the persistence lock, `stageVfsFile` (`@livestore/sqlite-wasm/cf`)
  copies the state file's pages (the pager's `page_count`) into a staging
  table inside DO storage, and the sink streams them to R2 as a multipart
  upload. Memory stays around one 5 MiB part (wasm heap ~17 MB at state sizes
  measured up to 521 MB), at the cost of one extra DO row write per 8 KiB page
  per snapshot. Missing pages are staged as zeros so the copy is dense; reads
  fail on any missing row. Leftover staged copies of an evicted instance are
  dropped (`dropStagedVfsFiles`) when a checkpointer leader boots.
  `exportMode: 'buffered'` serializes the database into wasm and JS memory
  (about twice the state size) and is viable to about 30–45 MB of state under
  the 128 MB isolate limit.

The export reads the VFS-backed state database, not `SqlStorage`, so the
`export()`/`import()` no-ops above do not apply to it. A client Durable Object
can also opt into snapshot boot as a client (e.g. with
`makeDoRpcSnapshotSource`); the install then deserializes the snapshot in the
wasm heap and imports it into the same VFS-backed state database, so the
installable state size is bounded by the isolate's memory
(LS.SYS.SYNC.SNAP-DQ3). `makeAdapter` seeds its client session from
`LeaderThreadCtx.exportState` (locked bytes plus head), and resetting
persistence also clears `__livestore_eventlog_base`.

## Eviction and Resume

The DO adapter has no eviction-specific handling (no alarms, no hibernation
hooks): the store is created lazily per DO instance (typically cached on the
DO class between requests), leader and single client session are colocated
in the same isolate, and all persistence goes through the DO's SQL storage
(`ctx.storage`). Commits materialize and persist to the eventlog before the
background backend push, so an evicted DO recovers on the next request
through the ordinary leader boot rehydration path
([../spec.md](../spec.md) Leadership Handover): upstream head and pending
events are re-derived from DO storage and pending events are re-pushed.

Live-pull delivery has its own reconstruction path. The sync backend
delivers updates by calling the client DO's `syncUpdateRpc(payload, storeId)`
reverse-RPC (`ClientDoWithRpcCallback`), a call that also wakes an evicted
DO. Because the backend passes the subscription's `storeId`, a store-less
wake can re-boot its store (`getStore(storeId)`, idempotent; its boot runs a
catch-up pull) before forwarding the payload to
`handleSyncUpdateRpc(ctx, payload)` — so a reconstructed client recovers the
pushed update rather than dropping it (LS.SYS.RT.CF-R03). Opting out (no
re-boot) drops that one live update and recovers lazily on the next store
access. The provider side that threads `storeId` is `03-sync/03-cf`'s concern
([../../03-sync/03-cf/.decisions/0003-reverse-rpc-storeid-recovery.md](../../03-sync/03-cf/.decisions/0003-reverse-rpc-storeid-recovery.md)).

## Open Design Questions

- **LS.SYS.RT.CF-DQ2 Free-plan capacity and memory headroom.** The adapter should
  support representative workloads on Workers Free with substantial memory
  headroom for application state and concurrent work. The supported workload
  envelope and regression budget are not yet established: local full-boot
  profiles for several replay fixtures exceed the documented isolate ceiling,
  even with 100-event replay batches. Track production-equivalent profiling,
  allocation reduction and deployed validation in
  [#1612](https://github.com/livestorejs/livestore/issues/1612). Account-wide
  read/write quotas must also leave room for normal operation; batch size alone
  cannot guarantee Free-plan compatibility.
- **LS.SYS.RT.CF-DQ1 Flush durability scope.** The commit-loss window is
  decided (accepted; LS.SYS.RT.CF-R06). What remains platform-trust: whether
  Cloudflare's "confirmed flushed to disk" implies geo/replicated durability
  or single-node disk — undocumented upstream (see
  [.reference/cloudflare-do-durability.md](./.reference/cloudflare-do-durability.md)).
  Blocked on: upstream documentation or vendor confirmation.
