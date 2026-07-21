# DELTA-001 — Browser session rebase can fail while rematerializing a pending create

Status: open

## Divergence

The `shared-todo-workday` scenario exposes a reproducible failure in the
persistent browser profile. Alice's phone disconnects after the shared backend
head reaches `e33`, performs six local actions including a `TodoCreated` for
`todo-27`, while the two online laptops advance the backend to `e61`, and then
reconnects.

The browser Client's Leader reaches rebased position `e40r1` with upstream head
`e61`, but its session fails while applying the pull. The captured worker error
is a `SqliteError` caused by `UNIQUE constraint failed: todos.id`, with the
failure originating from `ClientSessionSyncProcessor`'s materialize-event path.
The session remains at `e40r1`; the other Clients and backend remain at `e61`.
A restart probe against the same persisted state also observed a boot invariant
failure in which the stored backend head was ahead of the local head.

The same portable workload passes through the in-process mock profile and the
isolated-process SQLite profile backed by local sync-cf. Browser controls also
pass when tested independently with only the long pull backlog or only offline
local pushes. The failure requires the divergent combination: locally
materialized pending events plus concurrent backend advancement and rebase in
the browser/OPFS topology.

The leading hypothesis is that the session rebase critical section does not
fully reverse the locally materialized create before rematerializing its
rebased event. That hypothesis is not yet a proven root cause; the retained
artifact establishes the observed failure boundary and stack, not the precise
internal defect.

## VRS

[spec.md](../spec.md), **Client Session Sync Processor / Rebase critical
section**. The specified rollback-before-rematerialization sequence should
allow the rebased pending events to materialize once and then converge.

## Implementation Contract

Fix the processor or persistence interaction only after isolating the missing
rollback/changeset invariant. Add a focused regression that uses a persisted
browser session with at least one locally materialized create and a concurrently
advanced backend, then proves:

1. session changesets reverse every rolled-back pending event exactly once;
2. rebased events rematerialize without primary-key duplication;
3. Leader and session heads converge to the authoritative backend head;
4. restarting the persistent Client preserves valid local/backend head
   ordering; and
5. the scenario runner retains a structured failed artifact if any of these
   conditions regress.

The smaller `offline-writer-recovery` corpus case reproduces the same defect
with only two application events: Client A creates one Todo while offline,
Client B creates another against the backend, and Client A reconnects. This is
the preferred focused regression. Before runtime-failure capture was added,
that browser test incorrectly passed because the sampled global heads satisfied
the coarse convergence predicate after the session had already reported its
materialization error.

No scenario-side retry, timeout extension, participant restart, or materializer
change counts as closing this delta.
