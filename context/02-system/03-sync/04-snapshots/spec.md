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
`syncOptions.snapshot` (LS.SYS.SYNC.SNAP-R01, LS.SYS-R06). Implementation is in
progress; parts of this contract without code yet are tracked in
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
}
```

- A source never returns a manifest whose blob it cannot serve, nor one whose
  `backendId` differs from the backend's current identity.
- `publish` uploads the blob first and moves the manifest pointer second, so a
  reader never sees a manifest without its blob. It rejects a manifest older
  than the published one for the same key (`conflict`), so a key's position
  only advances. The replaced blob stays readable for a grace period so that
  in-flight downloads complete, and is deleted afterwards
  (LS.SYS.SYNC.SNAP-R14).
- `getManifest` on the sink lets a restarted checkpointer resume its trigger
  state from the latest published snapshot.

Client configuration is `syncOptions.snapshot = { source, timeout? }`
(`SnapshotBootOptions`); `timeout` bounds fetching the manifest and blob
(default 60 seconds).

## Client Boot

**Maturity: experimental**

Snapshot boot runs in the leader during `makeLeaderThreadLayer`
(`leader-thread/snapshot-boot.ts`, `bootFromSnapshot`), **before** the sync
backend client is constructed: providers read the persisted backend identity
at construction (the Cloudflare client requires it when pulling with a cursor).

Eligibility (LS.SYS.SYNC.SNAP-R04): `syncOptions.snapshot` is set, and the
local eventlog has no events, its upstream head is the root, and it has no
eventlog base. A client with any local history skips snapshot boot and keeps
its current boot path, so the feature never discards local work.

Steps:

1. `getManifest({ key: SnapshotKey.toString(SnapshotKey.make(schema)) })`.
   `None` ends snapshot boot.
2. `download(manifest)`; verify `byteLength` and `sha256`.
3. Import the bytes into a temporary in-memory database and validate it: the
   state head row equals `{ global: manifest.seqNum, client: 0 }` and the
   rebuild-complete marker (`__livestore_rebuild`) is present
   (LS.SYS.SYNC.SNAP-R07).
4. In the temporary database, clear the materialization journal (the producer's
   rollback records) and the rebuild-complete marker, then replace the state
   database with it in one step (`dbState.import`, an SQLite backup/page copy).
   The state database is now the snapshot but marked incomplete.
5. Write the eventlog in one transaction: the base row, sync status head
   `= manifest.seqNum`, and `backendId = manifest.backendId`.
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
- Clearing the eventlog for a backend reset also clears the base.

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
(`leader-thread/snapshot-rebuild.ts`) uses one of two exact strategies
(LS.SYS.SYNC.SNAP-R11):

- **A — Re-seed.** Fetch the snapshot for the **new** key at position `p'`
  with `base <= p' <= backendHead`, the same `backendId`, and no client-only
  event of this client below `p'` (a snapshot cannot contain this client's
  client-only effects; one at `(p', c>0)` is after `p'` and replayed). Install
  it as in Client Boot, then rematerialize the local eventlog events after
  `p'`, including pending and client-only events. The base stays where it was:
  the rows in `(base, p']` remain valid history. A failed re-seed wipes state
  and falls through to B.
- **B — Backfill.** Drop leftover rows below the base (only an interrupted
  earlier backfill can have written them), pull the backend events from the
  root up to the base, insert them into the eventlog with their sync metadata,
  delete the base row, and run the normal full replay. Idempotent across
  crashes. Needs the backend to be reachable; offline, boot fails with a clear
  error because no correct state can be produced locally (LS-DQ3). If the
  backend's identity changed (a `BackendIdMismatchError`, or the provider
  adopting a new identity on the cursor-less pull), the history below the base
  no longer exists: with `onBackendIdMismatch: 'reset'` the eventlog is reset
  and the client boots fresh from the new backend; otherwise boot fails.

Re-seed is preferred when a compatible snapshot exists; backfill is the
fallback. Both preserve LS.SYS.SYNC.SNAP-R10.

## Checkpointer

**Maturity: experimental**

The checkpointer is an ordinary LiveStore leader for the store that never
pushes, live-pulls the backend, and periodically exports its state
(LS.SYS.SYNC.SNAP-R12). Because it commits nothing, its state contains exactly
the confirmed synced events up to its upstream head; client-only events of
other clients never reach it.

- **Consistent export** (LS.SYS.SYNC.SNAP-R13):
  `LeaderThreadCtx.exportSnapshot` runs under a lock shared with
  `LeaderPersistence` transactions and returns
  `{ bytes, head, cursorMetadata, backendId }`. `head` comes from the state
  head row and must equal the backend head with client part `0`;
  `cursorMetadata` is the eventlog row's sync metadata at `head`. Export is
  refused for a leader that ever recorded a client-only event: such events
  leave the pending set once later events are confirmed, but their effects
  stay in state. The same lock backs `LeaderThreadCtx.exportState`, which every
  leader-side state export (session boot, devtools) uses, so no export ever
  captures a half-applied persist transaction.
- **Loop:** `makeCheckpointer` (generic, `leader-thread/checkpointer.ts`) evaluates
  the trigger after pulls, exports, and calls `sink.publish`. It runs one
  checkpointer per `(storeId, key)`: a schema deploy that changes the key
  starts a new checkpointer, and the old key's checkpointer keeps serving
  clients still on the old schema.

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

The cost constants are calibrated by the [snapshot boot benchmarks](../../../../contributor-docs/perf/snapshot-boot-benchmarks.md).

### Grace period

A replaced blob stays readable for the sink's grace period (in-memory default
60 seconds), long enough for a client that fetched the previous manifest to
finish its download. A client that loses the race sees an integrity or
not-found failure and falls back (LS.SYS.SYNC.SNAP-R08).

## Realizations

**Maturity: experimental**

### Cloudflare

Sync side (`@livestore/sync-cf`, on top of [03-cf](../03-cf/spec.md)):

- The sync Durable Object stores manifests in a `snapshot_manifests_<V>` table
  next to its eventlog. It serves only manifests whose `backendId` equals its
  current backend identity, and accepts a put only when the position advances
  for the key. RPC methods for the checkpointer's sink:
  `putSnapshotManifest(manifestJson)` (returns `Accepted { previousObjectKey }`
  or `Rejected { reason, message }` as a plain object so it survives DO RPC
  cloning) and `getSnapshotManifest({ storeId, key })`.
- Blobs live in R2 under `snapshots/<storeId>/<key>/<seqNum>-<id>.sqlite`,
  written by `makeR2SnapshotSink`
  (content type `application/vnd.sqlite3`). Replaced blobs are deleted after
  the grace period.
- Opt-in worker HTTP routes, matched as path suffixes so the sync endpoint can
  be mounted under a prefix:
  - `GET …/snapshot/manifest?storeId&key&payload`
  - `GET …/snapshot/blob?storeId&objectKey&payload`

  Both run the worker's `validatePayload` like the sync transports, and need
  an R2 binding. The blob route refuses object keys outside
  `snapshots/<storeId>/` and blobs whose size does not match the manifest.

Client sources: `makeHttpSnapshotSource` (worker routes) and
`makeDoRpcSnapshotSource` (for Durable Object clients).

Checkpointer host (`@livestore/adapter-cloudflare`, see
[04-runtime/02-cloudflare](../../04-runtime/02-cloudflare/spec.md#snapshot-checkpointer)):
a headless-leader Durable Object named `${storeId}/${key}` with an R2 sink. An
alarm keeps it awake while the log moves. The state database of a checkpointer
must fit the Durable Object's memory (LS.SYS.SYNC.SNAP-DQ3).

### Node and tests

`makeInMemorySnapshotStore({ storeId, graceMs })` (`snapshot/in-memory-store.ts`)
returns `{ sink, source, makeSource, testing }`. It follows the
object-storage contract (blob before pointer, grace-period retention) and is
used by tests and Node benchmarks; `testing` can corrupt or delete blobs to
exercise fallback. A Node checkpointer runs the same `makeCheckpointer` loop
with an in-memory or file-system sink.

## Verification

**Maturity: experimental**

Evidence expected for this node (owned with
[09-verification](../../09-verification/spec.md)): unit tests for key
derivation, trigger, and in-memory store; leader tests that a snapshot-booted
client reaches the same state as a full replay, falls back on every failure
reason, and rebuilds via re-seed and backfill; Cloudflare tests for the
manifest RPC and routes; and boot benchmarks that calibrate the trigger
constants.

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
- **LS.SYS.SYNC.SNAP-DQ3 Checkpointer memory ceiling.** The Cloudflare checkpointer
  holds the state database inside one Durable Object (about 128 MB of memory).
  How should stores near that limit be handled: streaming export, a Node or
  container checkpointer, or a documented size limit?
- **LS.SYS.SYNC.SNAP-DQ4 Blob distribution.** Blobs are served from one R2 bucket
  through the worker. Multi-region caching (CDN in front of immutable blob
  keys, regional buckets) and its interaction with the grace period is open.
- **Offline rebuild of a snapshot-booted client** is a root question: see
  [LS-DQ3](../../../open-questions.md).
