# State Snapshots — Spec

This document specifies snapshot boot: the snapshot key and manifest, how a
client installs a snapshot and continues from an eventlog base, how state is
rebuilt when the local eventlog is partial, how the checkpointer produces
snapshots, and the in-repo realizations. It builds on
[requirements.md](./requirements.md), the
[sync boundary](../spec.md) and the [Cloudflare provider](../03-cf/spec.md).

## Status

Draft.

**Maturity: experimental.** Everything in this node is opt-in through
`syncOptions.snapshot` (LS.SYS.SYNC.SNAP-R01, LS.SYS-R06). The contract below
is implemented with tests on the snapshot-boot branch; until that lands on
`main`, and for the open items it lists, it is tracked in
[.delta/DELTA-001-snapshot-boot-implementation.md](./.delta/DELTA-001-snapshot-boot-implementation.md).

## Overview

**Maturity: experimental**

```
 sync backend (full, never compacted log) ── pull ──▶ checkpointer (never pushes)
        │ manifest pointer                                │ export state DB
        ▼                                                 ▼
 snapshot source  ◀──── publish manifest ──────────── snapshot sink ──▶ blob store
        │
        │ 1. getManifest(key)  2. download blob  3. verify + install
        ▼
 fresh client: state = snapshot @ p, eventlog base = p, then pull events > p
```

A client that starts with an empty eventlog would otherwise replay the store's
entire history. With snapshot boot it downloads a state database exported at a
known global position `p` and replays only the events after `p`. Startup cost
then depends on the state size and the tail since the latest snapshot rather
than on the history length (LS.SYS.SYNC.SNAP-A01). The backend log stays
complete (LS.SYS.SYNC.SNAP-R02): a snapshot is a cache of derived state, not a
replacement for history.

The public helpers are exported as the `Snapshot` namespace from
`@livestore/common` and re-exported from `@livestore/livestore`
(`SnapshotKey`, `SnapshotTrigger`, `SnapshotManifest`, `SnapshotError`,
`makeInMemorySnapshotStore`, and the source/sink types). Leader-side pieces
(`makeCheckpointer`, `Eventlog`) are exported from
`@livestore/common/leader-thread`.

## Snapshot Key

**Maturity: experimental**

`SnapshotKey.make(schema)` (`snapshot/SnapshotKey.ts`) derives:

| Field                  | Source                                                                                       |
| ---------------------- | -------------------------------------------------------------------------------------------- |
| `stateSchemaHash`      | `schema.state.sqlite.hash`, the compound state fingerprint (table ASTs, incl. system tables) |
| `eventDefsHash`        | SHA-256 over every event name with its arg-schema hash and `clientOnly` flag, sorted by name |
| `storageFormatVersion` | `liveStoreStorageFormatVersion`                                                              |

`SnapshotKey.toString(key)` is `v<fmt>.<stateSchemaHash>.<eventDefsHash>`. Both
hashes are unpadded base64url, so `.` is an unambiguous separator and the string
is safe in object-storage keys and Durable Object names.

The key identifies which clients can install a snapshot
(LS.SYS.SYNC.SNAP-R05). It deliberately does not cover materializer code: no
schema hash does (see
[schema management](../../02-state/01-sqlite/02-schema-management/spec.md#state-fingerprint-contract)).
Equivalence between a snapshot-booted client and a full replay therefore rests
on LS.SYS.SYNC.SNAP-A02: a materializer change ships under a new event name,
which changes `eventDefsHash`, and old events keep their old effect. The same
assumption already holds for persisted clients today, since a materializer-only
change does not change the state fingerprint and so does not rebuild their
state.

## Manifest

**Maturity: experimental**

`SnapshotManifest` (`snapshot/snapshot.ts`) is the small pointer a backend
serves; the blob lives in object storage.

| Field             | Meaning                                                                             |
| ----------------- | ----------------------------------------------------------------------------------- |
| `manifestVersion` | `1`                                                                                 |
| `storeId`, `key`  | store and `SnapshotKey.toString` of the producing schema                            |
| `backendId`       | backend identity the snapshot was derived from (LS.SYS.SYNC.SNAP-R06)               |
| `seqNum`          | global sequence number of the last event reflected by the state (the snapshot head) |
| `cursorMetadata?` | provider sync metadata of the event at `seqNum`; becomes the pull cursor metadata   |
| `byteLength`      | exact blob size                                                                     |
| `sha256`          | lowercase hex SHA-256 of the blob                                                   |
| `createdAt`       | export time, epoch milliseconds                                                     |
| `objectKey`       | opaque, provider-specific blob location                                             |

Failures are one tagged error, `SnapshotError`, with `reason ∈ { unavailable,
not-found, integrity, incompatible, conflict, internal }`.

## Source and Sink Contract

**Maturity: experimental**

The snapshot contract is separate from the sync provider contract
(LS.SYS.SYNC.SNAP-R03):

```ts
SnapshotSource = {                       // booting clients
  getManifest: ({ key }) => Effect<Option<SnapshotManifest>, SnapshotError>
  download: (manifest) => Effect<Uint8Array, SnapshotError>  // caller verifies
}
SnapshotSourceConstructor = ({ storeId, clientId, payload }) =>
  Effect<SnapshotSource, UnknownError, Scope | HttpClient>

SnapshotSink = {                         // checkpointer
  publish: ({ manifest /* without objectKey */, bytes }) => Effect<SnapshotManifest, SnapshotError>
  getManifest: ({ key }) => Effect<Option<SnapshotManifest>, SnapshotError>
  capture?: (dbState: SqliteDb) => StagedSnapshot   // optional streaming path
}
StagedSnapshot = {
  publish: (draft: SnapshotManifestDraft) => Effect<SnapshotManifest, SnapshotError>
  discard: Effect<void>
}
```

- A source never returns a manifest whose blob it cannot serve, and should only
  return manifests whose `backendId` is the backend's current identity (a
  fresh client cannot check it before installing). The in-memory store does
  not filter by identity; the Cloudflare sync Durable Object does.
- `publish` uploads the blob first and moves the manifest pointer second, so a
  reader never sees a manifest without its blob. Positions are ordered per
  backend identity: a manifest older than the published one of the **same**
  `backendId` and key is rejected (`conflict`); an equal position is accepted,
  and a manifest of a new identity (after a backend reset) replaces the old
  one regardless of position. The replaced blob stays readable for a grace
  period so that in-flight downloads complete, and is deleted afterwards
  (LS.SYS.SYNC.SNAP-R14).
- `getManifest` on the sink lets a restarted checkpointer resume its trigger
  state from the latest published snapshot.
- `capture` is the streaming path for state too large to serialize as one
  buffer. The checkpointer calls it synchronously under the leader's
  persistence lock; it must take a copy that stays consistent after the lock is
  released without holding the state in memory. The returned
  `StagedSnapshot.publish` receives a `SnapshotManifestDraft` (the manifest
  without `objectKey`, `byteLength` and `sha256`, which the sink computes while
  uploading) and releases the copy on every exit; `discard` releases it
  without publishing.

Client configuration is `syncOptions.snapshot = { source, timeout? }`
(`SnapshotBootOptions`); `timeout` bounds constructing the source and fetching
and verifying the manifest and blob (default 60 seconds).

## Client Boot

**Maturity: experimental**

Snapshot boot runs in the leader during `makeLeaderThreadLayer`
(`leader-thread/snapshot-boot.ts`, `bootFromSnapshot`), **before** the sync
backend client is constructed: providers read the persisted backend identity
at construction (the Cloudflare client requires it when pulling with a cursor).
A successful install is exposed as `LeaderThreadCtx.snapshotBoot`
(`Option<SnapshotInstallResult>`: the manifest plus manifest, download, verify
and install timings).

Eligibility (LS.SYS.SYNC.SNAP-R04): `syncOptions.snapshot` is set and
`Eventlog.isEventlogEmpty` holds: the local eventlog has no events, its
upstream head is the root, and it has no eventlog base. A client with any
local history skips snapshot boot and keeps its current boot path, so the
feature never discards local work.

Steps:

1. `getManifest({ key: SnapshotKey.toString(SnapshotKey.make(schema)) })`.
   `None` ends snapshot boot. A manifest whose `storeId`, `key` or
   `manifestVersion` does not match is `incompatible`.
2. `download(manifest)`; verify `byteLength` and `sha256` (`integrity`).
3. Import the bytes into a temporary in-memory database and validate it: it is
   a SQLite database, the rebuild-complete marker (`__livestore_rebuild`) is
   present, and the state head row equals `{ global: manifest.seqNum,
client: 0 }` (LS.SYS.SYNC.SNAP-R07).
4. In the temporary database, clear the materialization journal (the producer's
   rollback records) and the rebuild-complete marker, then replace the state
   database with it in one step (`dbState.import`, an SQLite backup/page copy).
   The state database is now the snapshot but marked incomplete.
5. Write the eventlog in one savepoint (`Eventlog.installEventlogBase`): the
   base row, sync status head `= manifest.seqNum`, and
   `backendId = manifest.backendId`.
6. Mark the state complete.

Fallback (LS.SYS.SYNC.SNAP-R08): any error or timeout in steps 1–6 is logged,
the state database is wiped again, and boot continues on today's path, a full
replay from the backend.

Crash safety: until step 6 the state counts as incomplete, so the next boot
wipes it. If the crash happened before step 5, the eventlog is still empty and
snapshot boot simply retries; after step 5, the eventlog has its base and the
rebuild path below repairs state. No crash point leaves a complete state next to
an eventlog that does not describe it.

After a successful install, leader boot proceeds as usual: the upstream head is
the base, there are no pending events, and the sync processor pulls the events
after the base.

Install needs the whole snapshot in the SQLite wasm heap (step 3 deserializes
it there), so the state size a client can install is bounded by that heap; see
[Limits](#limits).

## Eventlog Base

**Maturity: experimental**

The eventlog database gains `__livestore_eventlog_base`
(`schema/state/sqlite/system-tables/eventlog-tables.ts`), a single-row table:
`id = 1`, `seqNumGlobal`, `syncMetadataJson`, `snapshotKey`, `installedAt`. No
row means the eventlog starts at the root. The row stands in for the missing
event at the base (LS.SYS.SYNC.SNAP-R09):

- The local head falls back to `{ base, 0, 0 }` when the eventlog has no rows
  (`getClientHeadFromDb`), so the boot assertion `backendHead <= localHead`
  (LS.SYS.RT-R13) holds.
- The backend cursor uses the base's sync metadata when the upstream head
  equals the base and no event row exists at it
  (`getSyncBackendCursorInfoForDb`).
- The `LS_DEV` parent check accepts the base as the parent of the first event
  after it.
- Clearing the eventlog for a backend reset (`Eventlog.resetEventlog`) also
  clears the base, as does resetting a Durable Object's persistence.

The table is created when missing and is optional for detecting an existing
eventlog (`eventlogRequiredSystemTables` excludes it). It is additive: existing
eventlogs open unchanged and `liveStoreStorageFormatVersion` is not bumped.
This is a deliberate, reviewed exception to the "never modify eventlog schemas
without a format bump" rule in
[schema management](../../02-state/01-sqlite/02-schema-management/spec.md#state-vs-eventlog-versioning--asymmetry):
no existing table or column changes, and older readers ignore the extra table.

`store.events` and the events stream of a snapshot-booted client start at the
base; events below it are not local (LS.SYS.SYNC.SNAP-T02, LS.SYS.SYNC.SNAP-DQ2).

## Rebuild With a Partial Eventlog

**Maturity: experimental**

A state rebuild (new state fingerprint, missing or incomplete state; see
[schema management](../../02-state/01-sqlite/02-schema-management/spec.md#rebuild-sequence))
normally replays the whole local eventlog. With a base above the root, the
local eventlog lacks the events at and below the base, so a plain replay would
produce wrong state. `rebuildFromEventlogBase`
(`leader-thread/snapshot-rebuild.ts`) runs instead of `recreateDb`, after the
sync backend client is constructed, and uses one of two exact strategies
(LS.SYS.SYNC.SNAP-R11):

- **A — Re-seed** (only with `syncOptions.snapshot`). Fetch and verify the
  snapshot for the **new** key at position `p'`. It is used only when
  `base <= p' <= backendHead` (the persisted upstream head), its `backendId`
  equals the persisted one, and the local eventlog holds no client-only event
  below `p'` (a snapshot cannot contain a client's client-only effects; one at
  `(p', c>0)` is after `p'` and replayed). Install it as in Client Boot steps
  3–4, rematerialize the local eventlog events after `p'` (pending and
  client-only events included), and mark the state complete. The base stays
  where it was: the rows in `(base, p']` remain valid history. Any failure
  wipes state and falls through to B.
- **B — Backfill.** Delete leftover rows below the base and the row at
  `(base, 0)` (only an interrupted earlier backfill can have written them),
  pull the backend from the root without a cursor (`live: false`) until the
  base is reached, insert those events with their sync metadata, delete the
  base row, and run the normal full replay (`recreateDb`). The deletion first
  makes it idempotent across crashes. It needs a configured and reachable
  backend; otherwise boot fails with a clear error, because no correct state
  can be produced locally (LS-DQ3). If the backend's identity changed (a
  `BackendIdMismatchError`, or the provider adopting a new identity on the
  cursor-less pull), the history below the base no longer exists: with
  `onBackendIdMismatch: 'reset'` (the default) the eventlog is reset and the
  client replays from the new backend; otherwise boot fails.

Re-seed is preferred when a compatible snapshot exists; backfill is the
fallback. Both preserve LS.SYS.SYNC.SNAP-R10.

## Checkpointer

**Maturity: experimental**

The checkpointer is an ordinary LiveStore leader for the store that never
pushes, live-pulls the backend, and periodically exports its state
(LS.SYS.SYNC.SNAP-R12). Because it commits nothing, its state contains exactly
the confirmed synced events up to its upstream head; client-only events of
other clients never reach it. A checkpointer leader is created with
`makeLeaderThreadLayer({ headless: true })`: it serves no client sessions, so it
does not retain pull items for sessions that might connect later (that cache
otherwise grows with every applied event).

### Consistent export

`LeaderPersistence` (`leader-thread/LeaderPersistence.ts`) serializes exports
with persist transactions through one lock, since a transaction can yield to
other fibers between `BEGIN` and `COMMIT` (LS.SYS.SYNC.SNAP-R13). Exposed on
`LeaderThreadCtx`:

- `exportSnapshotWith(capture)` validates, under the lock, that the state head
  has client part `0` and equals the persisted backend head, that the leader
  never recorded a client-only event, and that the backend identity is known.
  It then calls `capture(dbState, { seqNum, backendId })` synchronously and
  returns `{ data, seqNum, cursorMetadata, backendId, exportMs }`;
  `cursorMetadata` is the sync metadata of the eventlog row (or base) at
  `seqNum`. Client-only events are refused because they leave the pending set
  once later events are confirmed, but their effects stay in state; the check
  is one eventlog scan on the first export, then kept current by
  `persistLocal`.
- `exportSnapshot` is `exportSnapshotWith(db => db.export())` and returns
  `{ bytes, seqNum, cursorMetadata, backendId, exportMs }`.
- `exportState` returns `{ bytes, head }` under the same lock without the
  snapshot validations. Session boot and the adapters' database export in the
  web (worker and in-memory) and Cloudflare adapters use it, so they never
  capture a half-applied persist transaction. The leader devtools
  `SnapshotReq` handler still calls `dbState.export()` directly.

### Loop

`makeCheckpointer({ schema, storeId, sink, trigger?, pollInterval?, now? })`
(`leader-thread/checkpointer.ts`) runs on a leader thread (it needs
`LeaderThreadCtx` and `StateSqliteDb`) and returns `{ stats, publishNow }`:

- On start it reads `sink.getManifest` for its key so a restarted checkpointer
  resumes from what it published.
- Every `pollInterval` (default 1 second) it evaluates the trigger with
  `tailEvents = upstreamHead − latest.seqNum` (or the whole head when the
  latest manifest is above the head, i.e. the backend was reset) and the
  estimated snapshot size `page_count × page_size`. A failed evaluation or
  publish, including a sink defect, is logged and counted; the loop keeps
  running.
- A publish uses `sink.capture` when the sink has it (staged, streamed) and
  otherwise `exportSnapshot` plus a SHA-256 over the bytes and `sink.publish`
  (buffered). It skips a position that is not above the latest manifest of the
  same `backendId`, deciding inside the lock before copying anything. The
  hand-off from capture to `StagedSnapshot.publish` is uninterruptible.
- `publishNow` publishes if the head moved past the latest snapshot, bypassing
  the cost model. Loop and manual publishes are serialized.
- `stats` is a `SubscriptionRef<CheckpointerStats>`: key, published and failed
  counts, latest manifest, last publish timings (export, hash, upload, total,
  tail), head, snapshot lag and the last trigger decision.

`makeCheckpointer` derives one key from its schema. Hosts run one checkpointer
per `(storeId, key)`: a schema deploy that changes the key needs a checkpointer
for the new key, and the old key's checkpointer keeps serving clients still on
the old schema.

### Trigger and cost model

`SnapshotTrigger.decide` (`snapshot/trigger.ts`) publishes iff

```
tailEvents > 0
∧ tailEvents ≥ minTailEvents
∧ msSinceLastSnapshot ≥ minIntervalMs
∧ tailEvents × replayCostPerEventMs > alpha × snapshotBytes × downloadCostPerByteMs
```

A booting client pays `download(snapshot) + replay(tail)`; publishing once the
tail replay would exceed a fraction `alpha` of the download keeps every boot
within about `(1 + alpha)` of the snapshot download alone
(LS.SYS.SYNC.SNAP-T01, LS.SYS.SYNC.SNAP-R15). The checkpointer's work per
snapshot is proportional to the state size, and snapshots are spaced by a tail
proportional to that size, so its amortized cost per appended event stays
roughly constant as state grows. A quiet store publishes nothing.

| Parameter               | Default                   |
| ----------------------- | ------------------------- |
| `alpha`                 | `0.5`                     |
| `replayCostPerEventMs`  | `0.5`                     |
| `downloadCostPerByteMs` | `0.00004` (about 25 MB/s) |
| `minIntervalMs`         | `10_000`                  |
| `minTailEvents`         | `100`                     |

`replayCostPerEventMs` is the measured full-replay cost of about 0.5 ms CPU
per event on a Node leader; the constants are calibrated by the
[snapshot boot benchmarks](../../../../contributor-docs/perf/snapshot-boot-benchmarks.md).

### Grace period

A replaced blob stays readable for the sink's grace period, long enough for a
client that fetched the previous manifest to finish its download: 60 seconds
by default for the in-memory store, 5 minutes for the R2 sink, which in
addition always keeps the current and previous manifests' blobs. A client that
loses the race sees an integrity or not-found failure and falls back
(LS.SYS.SYNC.SNAP-R08).

## Realizations

**Maturity: experimental**

### Cloudflare

Sync side (`@livestore/sync-cf`, on top of [03-cf](../03-cf/spec.md)):

- **Manifest table.** The sync Durable Object stores one row per
  `(storeId, key)` in `snapshot_manifests_<PERSISTENCE_FORMAT_VERSION>`
  (`cf-worker/do/snapshot-manifests.ts`): `backendId`, `seqNum`, the current
  `manifest` JSON and the `previousManifest` JSON. Only writes create the
  table; reads treat a missing table as empty.
- **Put** (`putSnapshotManifest(manifestJson)`) is rejected as `incompatible`
  for a store without backend history in this instance, another `storeId`,
  another `backendId` than the current one, or a `seqNum` above the backend
  head, and as `conflict` when older than the current row of the same
  `backendId`. It runs as one synchronous transaction. An accepted put moves
  the current manifest to `previousManifest` (same identity only) and returns
  `{ _tag: 'Accepted', previousObjectKey }`; a rejection returns
  `{ _tag: 'Rejected', reason, message }`. Both are plain objects so they
  survive DO RPC cloning.
- **Get** (`getSnapshotManifest({ storeId, key })`) returns the current
  manifest JSON only when its `backendId` is the current identity, else
  `null`. It never initializes a backend identity for an unknown store.
- **Servable check** (`isSnapshotObjectServable({ storeId, objectKey })`) is
  true only for the blob of a current or previous manifest of the store's
  current identity.
- **Blobs** live in R2 under
  `snapshots/<encodeURIComponent(storeId)>/<encodeURIComponent(key)>/<seqNum>-<nanoid>.sqlite`
  (content type `application/vnd.sqlite3`, custom metadata including the
  owning `storeId`). The encoding keeps one store's prefix from being a prefix
  of another's.
- **Sink.** `makeR2SnapshotSink({ bucket, storeId, getSyncBackendStub,
graceMs?, now?, stage?, partSizeBytes? })` uploads the blob (single `put`
  with R2-verified SHA-256), then calls `putSnapshotManifest`. A rejected
  manifest's blob is deleted at once. Afterwards it prunes blobs of the same
  key replaced more than `graceMs` (default 5 minutes) ago, keeping the current
  and previous blobs; pruning failures never fail the publish. With `stage`,
  the sink also implements `capture`: the staged copy is streamed as an R2
  multipart upload in parts of `partSizeBytes` (default and minimum 5 MiB, a
  multiple of the staged page size), hashed on the fly with
  `crypto.DigestStream`, and the upload is aborted on any non-success exit. R2
  does not verify a SHA-256 for multipart uploads, so streamed blobs rely on
  the client's verification. Blobs of other keys are never pruned (use an R2
  lifecycle rule).
- **Worker routes** (opt-in with `makeWorker({ snapshots: { bucketBinding } })`,
  or `matchSnapshotRequest` + `handleSnapshotRequest` in a custom `fetch`),
  matched as path suffixes so the sync endpoint can be mounted under a prefix:
  - `GET …/snapshot/manifest?storeId&key&payload`: the manifest JSON, `404`
    when none applies, `Cache-Control: no-store`.
  - `GET …/snapshot/blob?storeId&objectKey&payload`: `403` for object keys
    outside the store's prefix; `404` unless the sync DO reports the object
    servable and its R2 metadata names the store; otherwise the blob is
    streamed from R2 with `Content-Length`.

  Both run the worker's `validatePayload` like the sync transports (`400` on
  rejection). Storage or RPC failures answer `503` with CORS headers, which
  clients treat as `unavailable` and fall back.

- **Wake-ups.** `makeDurableObject({ snapshots: { checkpointerBinding,
snapshotKeys, wakeIntervalMs? } })` calls the checkpointer's
  `ensureRunning(storeId, { backendId, head })` after successful pushes,
  fire-and-forget and at most once per `wakeIntervalMs` (default 10 seconds)
  per checkpointer and sync DO instance. Live-pull callbacks to subscriber
  Durable Objects (checkpointers included) fan out concurrently, each bounded
  by a 5-second timeout, so a slow subscriber never holds up push admission.
  All `snapshotKeys` are woken through the one `checkpointerBinding`, while a
  checkpointer class is bound to the one schema it was created with; nothing
  checks that an instance's name matches its class's key.

Client sources (`@livestore/sync-cf/client`): `makeHttpSnapshotSource({ url,
headers? })` reads the worker routes (`ws(s)://` is mapped to `http(s)://`; a
`Content-Length` that disagrees with the manifest is refused before buffering
unless the response is content-encoded), and `makeDoRpcSnapshotSource({
getSyncBackendStub, bucket })`, for clients inside Durable Objects, reads the
manifest over DO RPC and the blob straight from R2 with the same prefix and
owning-store checks (bypassing `validatePayload`).

Checkpointer host (`@livestore/adapter-cloudflare`, see
[04-runtime/02-cloudflare](../../04-runtime/02-cloudflare/spec.md#snapshot-checkpointer)):
`makeCheckpointerDurableObject({ schema, syncBackendBinding, bucketBinding,
bindingName, ... })`, one instance per `${storeId}/${key}`, running a headless
leader with DO-RPC live pull and the R2 sink. Its default `exportMode:
'streamed'` stages the state file's pages inside DO storage and streams them,
so its wasm heap stays around 17 MB at state sizes measured up to 521 MB;
`'buffered'` serializes the database in memory and is viable to about 30–45 MB
of state under the 128 MB isolate limit.

### Node and tests

`makeInMemorySnapshotStore({ storeId, graceMs?, now? })`
(`snapshot/in-memory-store.ts`) returns `{ sink, source, makeSource, testing }`.
It follows the object-storage contract (blob before pointer, ordering per
backend identity, grace-period retention) and is used by tests and Node
benchmarks; `testing` can replace manifests and corrupt or delete blobs to
exercise fallback. `makeCheckpointer` runs on any leader, so a Node
checkpointer is a Node leader plus `makeCheckpointer` and a sink; no Node host
or file-system sink ships in the repo.

## Limits

**Maturity: experimental**

Measured by the
[snapshot boot benchmarks](../../../../contributor-docs/perf/snapshot-boot-benchmarks.md)
and the Cloudflare limits test (`tests/sync-provider/src/snapshot-do-limits.test.ts`):

| Path                                          | Holds in memory          | Measured ceiling                                              |
| --------------------------------------------- | ------------------------ | ------------------------------------------------------------- |
| Client install (`bootFromSnapshot`)           | whole state in wasm heap | ~1.6 GB works, 2.16 GB fails (Node wasm build, 2 GiB heap)    |
| Buffered export (`exportSnapshot`)            | whole state in wasm + JS | same as install in Node; ~30–45 MB in a 128 MB Durable Object |
| Streamed DO export (`exportMode: 'streamed'`) | one 5 MiB part           | wasm heap ~17 MB at up to 521 MB of state                     |

A client Durable Object that boots from a snapshot is bounded by its isolate
memory in the same way as any client install.

## Verification

**Maturity: experimental**

Evidence for this node (owned with
[09-verification](../../09-verification/spec.md)), in
`tests/package-common/src/snapshot/` unless noted:

- Trigger: `checkpointer-trigger.test.ts` (inequality, ordering of guards,
  random inputs).
- Boot and rebuild: `snapshot-boot.test.ts` (snapshot + tail equals full
  replay, local history kept, fallback per fault, re-seed and backfill),
  `snapshot-faults.test.ts` (stale manifests, timeouts, grace period,
  restarted checkpointer never regresses), `snapshot-regressions.test.ts`
  (crash between install and eventlog positioning, interrupted backfill,
  per-identity ordering, client-only export refusal, loop survives sink
  defects).
- Equivalence: `snapshot-oracle.test.ts` and `snapshot-fuzz.test.ts`
  (randomized schedules with concurrent writers, checkpointers publishing
  while pulling, kills, restarts, offline clients and a schema change).
- Leader sync: `sync-findings.test.ts` (restarts after rebased-then-confirmed
  events) and `tests/package-common/src/leader-thread/pull-restart.test.ts`
  (pull restarts for parked pushes and ended live pulls).
- Cloudflare (`tests/sync-provider/src/`): `snapshot-manifest.test.ts`
  (routes, DO-RPC source, rejects, `validatePayload` and prefix checks,
  staging, previous-identity manifests, pruning), `snapshot-boot-e2e.test.ts`
  (boot from the checkpointer DO, backend reset), and the opt-in
  `snapshot-do-limits.test.ts`.
- Benchmarks: `tests/package-common/src/snapshot/bench/`, reported in
  [snapshot-boot-benchmarks.md](../../../../contributor-docs/perf/snapshot-boot-benchmarks.md).

## Open Design Questions

- **LS.SYS.SYNC.SNAP-DQ1 Snapshot graduation.** What evidence (boot benchmarks on
  the agent workload, conformance across providers, production use) graduates
  snapshot boot from experimental, and whether its source/sink contract should
  become part of the provider realization registry.
- **LS.SYS.SYNC.SNAP-DQ2 History below the base.** A snapshot-booted client's
  `store.events`, events stream, and devtools eventlog view start at the base
  (LS-R13 inspectability is reduced locally). Should these read through to the
  backend for older events, backfill lazily, or keep the base as an explicit,
  documented boundary?
- **LS.SYS.SYNC.SNAP-DQ3 Snapshot size ceiling.** The streamed Cloudflare
  export removed the checkpointer's memory ceiling, but every client install
  (and every buffered export) still deserializes the whole state into the
  SQLite wasm heap ([Limits](#limits)). Should install stream pages into the
  target VFS, should snapshots be compressed, or is a documented size limit
  enough?
- **LS.SYS.SYNC.SNAP-DQ4 Blob distribution.** Blobs are served from one R2 bucket
  through the worker. Multi-region caching (CDN in front of immutable blob
  keys, regional buckets) and its interaction with the grace period is open.
- **Offline rebuild of a snapshot-booted client** is a root question: see
  [LS-DQ3](../../../open-questions.md).
