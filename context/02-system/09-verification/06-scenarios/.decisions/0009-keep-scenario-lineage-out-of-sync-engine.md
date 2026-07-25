# 0009 — Keep Scenario Event lineage out of the sync engine for now

Status: accepted (maintainer review of a removed sync-transition instrumentation
spike, 2026-07-25)

Refines [decision 0002](./0002-observed-state-and-event-identity.md) and
[decision 0004](./0004-causal-order-and-calibrated-time.md).

## Context

Exact Event lineage across rebase, confirmation, rejection, disappearance, and
equivalent Events cannot be recovered from sampled Eventlog observations. A
design spike made the engine implication concrete: `SyncState.merge` returned
explicit before/after rebase mappings, and the Client session sync processor
accepted an observer that emitted rebase and application transitions. Even the
session-only slice added a new merge-result contract and placed observation on
the synchronization execution path; complete lineage would additionally need
Leader, backend, process, and browser observation paths.

The evidence would primarily strengthen Scenario traces and replay
visualization. Current Eventlog-convergence correctness does not depend on it:
portable equality compares authoritative ordered Event facts and ignores
inferred event references.

## Options

- **Add merge mappings and sync-processor observers now (rejected).** This can
  produce authoritative transition evidence, but adds interface and execution-
  path complexity to the sync engine for a Scenario-specific capability.
- **Infer stable lineage from sampled Event facts (rejected).** Equivalent
  Events from one origin can be rejected, removed, or reordered, so occurrence
  matching cannot prove identity.
- **Keep lineage unavailable and advertise the capability limit (chosen).**
  Preserve sampled correlation as explicitly inferred visualization evidence;
  do not claim exact lineage or derive portable correctness or causal edges from
  it.

## Decision

Do not change `SyncState`, the sync processors, the Event schema, or the sync
wire protocol solely to provide exact Scenario Event lineage. Hosts do not
advertise `event-lineage`; sampled event references remain inferred, and
portable oracles ignore them.

Exact transition or lineage evidence remains a possible future direction when
the owning sync or runtime subsystem has an independent product, debugging, or
observability need for the same seam. An already-justified browser-safe
observation surface could then be consumed by Scenario hosts without making
Scenario verification the reason for sync-engine complexity.

## Consequences

- Scenario traces cannot currently prove Event identity across rebase,
  confirmation, rejection, disappearance, or repeated equivalent Events.
- Replay surfaces continue to label matching event references as inferred
  correlation, and sampled transitions retain `firstObserved` semantics.
- Exact receive-versus-apply latency and lineage-sensitive properties remain
  unsupported capabilities rather than silently weakened claims.
- DELTA-008 closes as an accepted capability limit. It may be reopened only
  when an independently justified engine observation seam changes the trade-off.
