# Snapshot Boot

> **Status: accepted as an experiment — folded into the intent layer (2026-10-29).** The durable contract lives in
> [`context/02-system/03-sync/04-snapshots/`](../../context/02-system/03-sync/04-snapshots/spec.md), with the decision and
> rejected alternatives in
> [`.decisions/0001-snapshot-boot-over-compaction.md`](../../context/02-system/03-sync/04-snapshots/.decisions/0001-snapshot-boot-over-compaction.md),
> the amended root requirements LS-R04 and LS-R06 in [`context/requirements.md`](../../context/requirements.md), and the
> coined terms in [`context/ontology.md`](../../context/ontology.md). Parts not yet implemented at fold-in are tracked in
> [`DELTA-001`](../../context/02-system/03-sync/04-snapshots/.delta/DELTA-001-snapshot-boot-implementation.md). Where the
> shipped code differs from this text, the intent layer records the code. This RFC is retained as a historical record
> (see the fold-in rule in `context/05-contributing/`).

## Implementation Notes (2026-10-29)

The implementation on the snapshot-boot branch differs from the proposal below in these points; the
[spec](../../context/02-system/03-sync/04-snapshots/spec.md) is authoritative:

- **Crash-safe install.** The state stays marked incomplete until the eventlog base is written, and only then is it
  marked complete (a sixth step). A crash in between is wiped and repaired on the next boot.
- **Rebuild.** Re-seed requires no client-only event below `p'` (one at `(p', c>0)` is replayed). Backfill first drops
  rows left by an interrupted backfill, so it is idempotent, and treats a backend identity change as a reset.
- **Export.** `LeaderThreadCtx.exportSnapshot` returns `{ bytes, seqNum, cursorMetadata, backendId, exportMs }` and is
  refused for a leader that ever recorded a client-only event. `exportSnapshotWith(capture)` and the optional
  `SnapshotSink.capture` add a streaming path. `exportState` gives every session-boot export the same lock.
- **Ordering.** Manifests are ordered per backend identity: a sink rejects an older position of the same `backendId`,
  and a new identity replaces the old manifest.
- **Cloudflare.** Object keys URI-encode the store id and key:
  `snapshots/<encodeURIComponent(storeId)>/<encodeURIComponent(key)>/<seqNum>-<id>.sqlite`. The blob route serves only
  the current or previous manifest's blob. The checkpointer Durable Object streams its export from staged DO pages by
  default (wasm heap ~17 MB at up to 521 MB of state); the buffered export is viable to about 30–45 MB. No Node host or
  file-system sink ships; `makeCheckpointer` runs on any leader with any sink.
- **Trigger.** The default `replayCostPerEventMs` is `0.5`, measured by the
  [snapshot boot benchmarks](../perf/snapshot-boot-benchmarks.md).
- **Remaining limit.** Client install still deserializes the whole snapshot in the wasm heap (~1.6 GB works, 2.16 GB
  fails in Node).

## Context

- A LiveStore client derives its state by materializing every event in its eventlog. A client with an empty eventlog
  pulls the store's whole history from the sync backend and replays it before its state is current.
- The sync backend holds the totally ordered log of a store. It does not interpret events (LS.SYS.SYNC-R02).
- The state database is a SQLite database. Its identity is a fingerprint of the state table ASTs; a different
  fingerprint selects a fresh database that is rebuilt from the eventlog
  ([schema management](../../context/02-system/02-state/01-sqlite/02-schema-management/spec.md)).
- No schema hash covers materializer code. The state fingerprint covers table ASTs, and the event-definition hash covers
  the args schema. By convention, an event whose behavior changes gets a new, versioned name.
- `sync/next/` contains an experimental event compaction prototype (`compact-events.ts`) that is not part of the shipping
  contract.

## Problem

The target workload for this proposal is many AI agents per system, on the order of 100, each a LiveStore client.
Agents append events at high rates and run in fresh sandboxes that restart often. Every restart is a cold start with an
empty eventlog, so every restart replays the complete history. Boot time and backend egress grow with the history
length even when the state that the agent needs stays small.

The constraints:

- History must be kept. Inspection, future read models (LS-T04), and debugging depend on it, and the backend cannot
  judge which events are safe to drop.
- The sync provider contract and its realizations should not change for clients that do not opt in (LS.SYS-R06).
- A failure in the new path must never make boot fail where it succeeds today.

## Proposed Solution

Make startup cost depend on state size instead of history length. A **checkpointer** next to the sync backend
periodically exports its state database as a **state snapshot**. A fresh client downloads the latest compatible snapshot,
installs it as its state, and pulls only the events after it.

### Client API

```ts
makeWorker({
  schema,
  sync: {
    backend: makeWsSync({ url }),
    snapshot: {
      source: makeHttpSnapshotSource({ url }), // experimental
      timeout: '30 seconds',
    },
  },
})
```

`SyncOptions.snapshot` is the only opt-in. Without it nothing changes. (`makeHttpSnapshotSource` is the Cloudflare HTTP source; see the intent layer for the current API.)

### Snapshot key and manifest

A snapshot is usable only by clients with the same **snapshot key**:
`v<storageFormatVersion>.<stateSchemaHash>.<eventDefsHash>`, where `eventDefsHash` hashes every event name, its
arg-schema hash, and its `clientOnly` flag. The manifest that points to a snapshot blob records the store, key,
`backendId`, the global position `seqNum` of the last reflected event, the provider cursor metadata at that position,
the blob's byte length and SHA-256, the creation time, and an opaque object key.

Materializer code is not part of the key. The design relies on the convention that a materializer change ships under a
new event name. Materializer changes are therefore forward-only: events already in the log keep their old effect. This
is already true for persisted clients today, because a materializer-only change does not rebuild their state.

### Client boot

Only a client with an empty eventlog is eligible. Before the sync backend client is constructed, the leader:

1. fetches the manifest for its key;
2. downloads the blob and verifies size and SHA-256;
3. imports it into a temporary database and checks that its state head equals `{ seqNum, 0 }` and that its
   rebuild-complete marker is present;
4. clears the materialization journal and replaces its state database with the snapshot;
5. writes an **eventlog base** row (position, cursor metadata, key), the sync head, and the `backendId`.

Any failure or timeout logs and falls back to today's full replay. Afterwards the client behaves like a client whose
upstream head is the base. The eventlog base is a new, additive table in the eventlog database, so no storage format
bump is needed.

### Rebuild with a partial eventlog

If a snapshot-booted client must rebuild state (for example after a state schema change), its local eventlog lacks the
events at and below the base. Two exact strategies:

- **Re-seed:** install the snapshot for the new key at a position `p'` between the base and the backend head (provided
  no client-only event of this client lies at or below `p'`), then rematerialize local events after `p'`.
- **Backfill:** pull the missing history from the backend, remove the base, and run a normal full replay. This needs
  the network; offline, boot fails with a clear error.

### Checkpointer

The checkpointer is an ordinary LiveStore leader that live-pulls and never pushes. It exports under a lock shared with
leader persistence, so the export reflects exactly the confirmed events up to its head. A cost model decides when to
publish:

```
tailEvents × replayCostPerEventMs > alpha × snapshotBytes × downloadCostPerByteMs
```

with a minimum tail and a minimum interval. This keeps a client's boot within about `(1 + alpha)` of the snapshot
download, and keeps the checkpointer's amortized cost per event roughly constant as state grows. A quiet store publishes
nothing. Publication uploads the blob before moving the manifest pointer and keeps the replaced blob for a grace period.

### Cloudflare realization

- The sync Durable Object stores manifests (it knows the current `backendId` and enforces monotonic positions) and
  exposes them to the checkpointer over DO RPC.
- Blobs live in R2 under `snapshots/<storeId>/<key>/<seqNum>-<id>.sqlite`.
- Opt-in worker routes `GET …/snapshot/manifest` and `GET …/snapshot/blob` serve clients, guarded by the same
  `validatePayload` as the sync transports.
- The checkpointer runs as a Durable Object named `${storeId}/${key}` with an alarm that keeps it awake while the log
  moves. A Node checkpointer with an in-memory or file-system sink covers tests, benchmarks, and stores too large for a
  Durable Object.

## Alternatives Considered

- **Event compaction / log rewriting.** Replace-by-key stubs, prefix truncation, or local compaction of client-only
  events. Safe compaction needs declared, checked laws on event definitions, stub-aware push confirmation, and cursor
  handling, and it changes the eventlog's append-only contract and the provider contract. It also still replays every
  surviving event and loses history. Not chosen.
- **Incremental page-level replication** of the state database (a Litestream-like second sync layer). It adds a second
  replication protocol coupled to SQLite page layout and the storage format. Full exports are simpler and verifiable
  with one hash. Not chosen.
- **Detecting materializer changes** by hashing materializer code. Function source is not a stable identity across
  bundlers, closures, and platforms. Not chosen; the versioned event name convention is made explicit instead.
- **Materializing inside the sync backend.** Breaks the schema-blind backend and needs a second materializer runtime.
  Not chosen.
- **Letting any client publish snapshots.** Clients hold pending and client-only events and may run different app
  versions. Not chosen.

## Open Questions

- `store.events`, the events stream, and devtools on a snapshot-booted client start at the base. Should they read older
  events through the backend?
- How should a snapshot-booted client that is offline handle a state schema change (today: fail boot with a clear
  error)?
- The Cloudflare checkpointer keeps the state database in one Durable Object's memory. What is the size ceiling and the
  path beyond it?
- Should blobs be cached closer to clients (CDN, regional buckets), and how does that interact with the grace period?
- What evidence graduates the feature from experimental?
