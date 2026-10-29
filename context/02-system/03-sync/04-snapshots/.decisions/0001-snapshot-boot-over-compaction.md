# 0001 — Bound startup cost with state snapshots instead of compacting the eventlog

Status: accepted as an experiment (maintainer confirmation, Victor Nyberg,
2026-10-29; RFC 0005 accepted and folded in). Evidence: the event compaction
research report (maintainer-local, `tmp/research/reports/LiveStore event
compaction.md`, 2026-10), and the committed interfaces in
`packages/@livestore/common/src/snapshot/` (key derivation, manifest, trigger
cost model, in-memory store). Graduation evidence is open
(LS.SYS.SYNC.SNAP-DQ1).

## Context

The motivating workload is many short-lived clients per system, on the order
of 100 AI agents, appending at high rates and running in fresh sandboxes that
restart often (LS.SYS.SYNC.SNAP-A01). Every restart is a cold boot with an
empty local eventlog, so boot pulls and materializes the whole history. Boot
cost grows with history length even when state stays small.

The research report examined compaction in depth. Its findings: a schema-blind
backend owns the total order but cannot read events; safe compaction needs
app-declared, checked laws (for example replace-by-key) on event definitions;
compaction interacts with positional pending confirmation, pull cursors inside
a rewritten range, and rebuild under future materializer versions (LS-T04).
Shipped compactors in other systems only drop records superseded per key,
behind a lag horizon, with a snapshot or reset fallback for lagging readers.

## Decision

1. **Keep the full history; snapshot state.** The backend's eventlog is never
   compacted or truncated (LS-R04, LS.SYS.SYNC.SNAP-R02). Fresh clients boot
   from a state snapshot at a known global position and pull only the tail.
   Startup cost becomes a function of state size plus a bounded tail.
2. **Full-export snapshots.** A snapshot is a complete SQLite export of the
   state database, published as an immutable blob with a small manifest
   pointer. Clients replace their (empty) state with it in one page copy.
3. **Key composition.** A snapshot is keyed by state schema fingerprint,
   event-definition hash (names, arg-schema hashes, `clientOnly` flags) and
   storage format version (LS.SYS.SYNC.SNAP-R05), plus the backend identity
   in the manifest (LS.SYS.SYNC.SNAP-R06). Any schema deploy that changes
   either hash selects a different snapshot line.
4. **Materializer code is not keyed.** The design assumes a materializer
   change ships under a new event name (LS.SYS.SYNC.SNAP-A02,
   LS.SYS.STATE.SQLITE-A01), so materializer changes are forward-only. This
   makes the existing versioned-name convention load-bearing.
5. **Checkpointer is an ordinary client.** Snapshots are produced by a
   LiveStore leader that pulls but never pushes (LS.SYS.SYNC.SNAP-R12), so
   the export uses the same materializers and SQLite build as clients and
   cannot affect the canonical log. Publication is governed by a cost model
   that bounds boot cost to about `(1 + alpha)` times the snapshot download
   (LS.SYS.SYNC.SNAP-R15).
6. **Hosting.** On Cloudflare, the sync Durable Object stores manifests next
   to the log (so it can enforce `backendId` and monotonic positions), blobs
   live in R2, and the checkpointer runs as its own Durable Object per
   `(storeId, key)`. Node and tests use the same checkpointer loop with an
   in-memory or file-system sink, which is also the fallback for stores too
   large for a Durable Object (LS.SYS.SYNC.SNAP-DQ3).
7. **Placement.** The contract lives under `03-sync/` because its center is a
   sync-boundary concern: where a client's eventlog and backend cursor begin.
   Leader boot integration and the Cloudflare checkpointer host are realized
   in `04-runtime/`, which depends on this node, never the reverse.

## Alternatives Considered

| Alternative                                                                                         | Why it was not chosen                                                                                                                                                                                                                                                                                                                      |
| --------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| Event compaction or log rewriting (replace-by-key stubs, prefix truncation, client-only compaction) | Needs per-event declared laws, a law checker, stub-aware confirmation, and cursor handling; changes LS-R04, LS.SYS.EVT-R07 and the provider contract. It shrinks bytes but still replays every surviving event, and destroys history that inspection and future read models may need. Snapshots get the boot win without touching history. |
| Incremental page-level replication (a Litestream-like second sync layer for the state DB)           | A second replication protocol with its own ordering, consistency, and retention, coupled to SQLite page layout and the storage format. Full exports are simpler, verifiable by one hash, and their cost is bounded by the trigger.                                                                                                         |
| Detecting materializer changes (hashing materializer source or bytecode)                            | Function source is not a stable identity (bundlers, closures, imported helpers, minification), and differs across platforms. The versioned-name convention already governs persisted clients today.                                                                                                                                        |
| Checkpointing inside the sync backend (backend materializes events)                                 | Breaks the schema-blind backend (LS.SYS.SYNC-R02) and would need a second materializer runtime. A checkpointer client reuses the client runtime.                                                                                                                                                                                           |
| Snapshots produced by arbitrary clients                                                             | Clients hold pending and client-only events and run untrusted versions; a passive, dedicated checkpointer exports only confirmed synced state.                                                                                                                                                                                             |
| Snapshot boot on by default                                                                         | Violates LS.SYS-R06; needs graduation evidence first.                                                                                                                                                                                                                                                                                      |

## Consequences

- A snapshot-booted client has a partial local eventlog that starts at a base
  (LS.SYS.SYNC.SNAP-R09). Rebuilds need a re-seed or a backfill
  (LS.SYS.SYNC.SNAP-R11); offline rebuild is an open root question (LS-DQ3).
- LS-R04 and LS-R06 are restated so they distinguish the canonical backend log
  from a client's local copy.
- The eventlog database gains an additive table without a storage format bump.
- Operators run and pay for checkpointers and blob storage.
- The experimental `sync/next/` compaction prototype is not on this path;
  its graduation stays under LS.SYS.SYNC-DQ2.
