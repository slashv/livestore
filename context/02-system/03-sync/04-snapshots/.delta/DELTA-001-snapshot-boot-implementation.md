# DELTA-001 — Snapshot boot contract ahead of its implementation

Status: open (2026-10-29)

## Divergence

RFC 0005 was accepted and folded into this node while the implementation was
still landing. At fold-in, the committed code covered the core interfaces only
(`packages/@livestore/common/src/snapshot/`: key, manifest, source/sink,
trigger, in-memory store; `SyncOptions.snapshot`). The leader boot path, the
eventlog base, rebuild with a partial eventlog, the checkpointer, and the
Cloudflare realization were in progress.

## VRS

Contracts in [requirements.md](../requirements.md) LS.SYS.SYNC.SNAP-R04…R15
and the Client Boot, Eventlog Base, Rebuild, Checkpointer, and Realizations
sections of [spec.md](../spec.md).

## Close Condition

Close when all of the following hold on `main`:

- `bootFromSnapshot`, the eventlog base table and its eventlog functions, and
  `rebuildFromEventlogBase` (re-seed and backfill) are implemented, with tests
  that a snapshot-booted client matches a full replay, falls back on every
  failure reason, and rebuilds through both strategies.
- `LeaderThreadCtx.exportSnapshot` and `Checkpointer.make` exist, with a
  consistency test against concurrent pulls.
- The Cloudflare sync DO manifest RPC and HTTP routes, the R2 sink, and the
  checkpointer Durable Object exist with tests.
- API names and route paths in [spec.md](../spec.md) match the code.
