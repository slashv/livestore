# 0016 — Cover Leader turnover through session lifecycle

Status: accepted (maintainer review and browser scenario evidence, 2026-07-26)

Refines [decision 0003](./0003-local-process-and-browser-realizations.md) and
[decision 0014](./0014-add-participants-through-explicit-plan-steps.md).

## Context

The Scenario architecture originally treated explicit Leader-role lifecycle as
a missing portable surface. In the persisted browser runtime, however, one
Client session holds a store-scoped Web Lock and runs the current Leader worker.
The SharedWorker routes sibling sessions to that Leader and accepts a new port
when another session acquires leadership.

Closing the lock-holding browser tab is the representative product event that
causes Leader turnover. Existing `stop-session`, `restart-session`, dynamic
session addition, application actions, and Settlement operations can already
exercise that behavior. A separate `stop-leader` request would require the host
to identify or terminate a role below the normal browser lifecycle boundary.

## Options

- **Add explicit Leader stop/restart operations (rejected).** This duplicates a
  real session-loss trigger and requires a new adapter or browser-runtime
  control seam solely for Scenario orchestration.
- **Claim Leader identity from operation order in the portable trace
  (rejected).** Fixture startup order makes the initial holder deterministic,
  but generic trace consumers do not observe the Web Lock owner or the exact
  handoff boundary.
- **Exercise turnover through the initial holder's session lifecycle (chosen).**
  Keep the product-shaped trigger and prove that a sibling remains writable and
  the Client later converges.

## Decision

Do not add a portable Leader lifecycle operation or host capability. The
`browser-multi-session-recovery` Scenario starts its first session to completion
before adding the second, relying on the fixture's blocking Web Lock election
to make the first session the deterministic initial holder. It then stops the
first session, completes an application action through the second session,
restarts the first, and evaluates pending resolution, Eventlog equality, and
State convergence.

This evidence establishes behavioral recovery through the production browser
leadership-election path. It does not establish portable facts naming the old
and new Leader sessions, the exact handoff instant, or the absence of every
possible transient overlap. Add authoritative Leader identity only when an
owning runtime or observability need independently justifies the observation
seam. Until then, the trace and viewer must not infer identity from session
creation order.

## Consequences

- DELTA-005 no longer treats explicit Leader-role lifecycle syntax as missing.
- The browser corpus covers the practical tab-close turnover without sync
  engine, adapter, or transport control changes.
- Session lifecycle remains the portable trigger; leadership identity remains
  an explicitly unobserved implementation detail.
- Stronger split-brain or exact-handoff claims require future authoritative
  runtime evidence rather than additional Scenario instructions alone.
