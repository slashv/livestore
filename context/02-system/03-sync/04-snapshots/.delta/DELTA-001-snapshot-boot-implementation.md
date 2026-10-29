# DELTA-001 — Snapshot boot contract ahead of its implementation

Status: open (2026-10-29; implemented with tests on the snapshot-boot branch,
not yet on `main`)

## Divergence

RFC 0005 was accepted and folded into this node while the implementation was
still landing. At fold-in, the committed code covered the core interfaces only
(`packages/@livestore/common/src/snapshot/`: key, manifest, source/sink,
trigger, in-memory store; `SyncOptions.snapshot`). The leader boot path, the
eventlog base, rebuild with a partial eventlog, the checkpointer, and the
Cloudflare realization were in progress.

Branch status (`claude/snapshot-boot-6d51c8`, 2026-10-29): every item of the
close condition below is implemented and tested there, and
[spec.md](../spec.md) was reconciled with that code (API names, signatures,
route paths, object-key layout, defaults, streamed export, measured limits).
The divergence that remains is that none of it is on `main` yet.

Open items found while implementing, which stay open after merge unless
resolved first:

- **Session pull-item cache.** A leader that serves client sessions still
  retains every pull item for sessions that might connect later
  (`cachedPullItems` in `LeaderSyncProcessor.ts`, about 430 B per applied
  event). Only headless leaders (`makeLeaderThreadLayer({ headless: true })`,
  used by checkpointers) skip it. Owned by
  [02-processors](../../02-processors/spec.md#backpressure-and-known-gaps).
- **Offline schema-change rebuild.** A snapshot-booted client that needs a
  state rebuild while offline can neither re-seed nor backfill and fails boot
  with a clear error ([LS-DQ3](../../../../open-questions.md)).
- **HTTP 5xx is terminal in the HTTP sync client.** HTTP status failures of
  the sync RPC map to `UnknownError`, not `IsOfflineError`
  (`sync-cf/src/client/transport/rpc-client-error.ts`); only transport loss
  is retried. This is existing policy, not introduced by snapshot boot, but a
  client on the HTTP transport behind a flaky worker stops syncing on a 5xx
  instead of retrying. The Cloudflare checkpointer uses DO RPC, and 5xx answers of the snapshot routes only make a client fall back to full replay.
- **Client install memory ceiling.** Install deserializes the whole snapshot
  into the SQLite wasm heap (~1.6 GB works, 2.16 GB fails in the Node build;
  far less in a 128 MB Durable Object), see
  [spec.md Limits](../spec.md#limits) and LS.SYS.SYNC.SNAP-DQ3.
- **Checkpointer instance/key check.** A checkpointer class does not verify that
  the instance name's snapshot key matches the schema it was created with;
  wiring a key to the wrong class publishes nothing usable (its manifests carry
  its own key) but wastes a Durable Object.

## VRS

Contracts in [requirements.md](../requirements.md) LS.SYS.SYNC.SNAP-R04…R15
and the Client Boot, Eventlog Base, Rebuild, Checkpointer, Realizations and
Limits sections of [spec.md](../spec.md).

## Close Condition

Close when all of the following hold on `main`:

- `bootFromSnapshot`, the eventlog base table and its eventlog functions, and
  `rebuildFromEventlogBase` (re-seed and backfill) are implemented, with tests
  that a snapshot-booted client matches a full replay, falls back on every
  failure reason, and rebuilds through both strategies.
- `LeaderThreadCtx.exportSnapshot` and `makeCheckpointer` exist, with a
  consistency test against concurrent pulls.
- The Cloudflare sync DO manifest RPC and HTTP routes, the R2 sink, and the
  checkpointer Durable Object exist with tests.
- API names and route paths in [spec.md](../spec.md) match the code.

The open items above are not part of the close condition; when this delta
closes, move any still unresolved into their owning nodes (open questions or
their own deltas).
