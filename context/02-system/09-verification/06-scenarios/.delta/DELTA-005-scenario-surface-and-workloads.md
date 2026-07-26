# DELTA-005 — Scenario surface and reusable workloads are incomplete

Status: open — reusable seeded workloads and dynamic participant addition are
resolved; the remaining surface additions are intentionally unimplemented.

## Resolved Slice

The serializable AST now retains a compact `workload` node with stable name,
JSON input, declared targets, and bounded count. Application-owned workload
definitions are resolved and deterministically expanded from a seed derived
from the recorded Scenario seed and stable plan identity before any Client is
created. Each emitted application action and the enclosing workload retain
stable instruction/outcome evidence. The same contract passes through
in-process, process, and browser hosts without changing their transport or sync
engine surfaces.

[Decision 0012](../.decisions/0012-resolve-seeded-workloads-by-name.md)
records the registry boundary, preflight rules, seed derivation, sequential v1
execution, and rejected callback/host-expansion alternatives.

Direct schema-event steps were evaluated and removed from the intended
portable surface. Named actions already permit a fixture to expose a thin,
test-only wrapper around a schema event without duplicating the AST, host,
capability, trace, and conformance contracts. [Decision
0013](../.decisions/0013-keep-application-mutations-behind-named-actions.md)
records that narrowing and the concrete needs that could justify revisiting
it.

Dynamic Client creation and session addition are now explicit sequential plan
steps with stateful preflight validation, derived capabilities, stable
instruction/outcome evidence, terminal snapshot inclusion, and topology
projection. Client creation passes the shared in-process, process, and browser
host contract; session addition is a browser capability that opens a new page
inside the existing Client context. [Decision
0014](../.decisions/0014-add-participants-through-explicit-plan-steps.md)
records the initial-topology boundary and keeps removal separate.

Generic Client/session removal was subsequently removed from the intended
surface. Disconnect, session stop, Client-runtime termination, local-data
deletion, authorization revocation, and Settlement membership have different
effects and must not share one ambiguous operation. Existing `stop-session`
already models closing a browser tab while retaining its restartable identity.
[Decision
0015](../.decisions/0015-reject-generic-participant-removal.md) records this
terminology and capability narrowing.

Explicit Leader-role lifecycle syntax was also removed from the intended
surface. The browser's real leadership-election path is exercised by stopping
the deterministic initial lock-holding session while a sibling session remains
live and successfully writes before convergence. [Decision
0016](../.decisions/0016-cover-leader-turnover-through-session-lifecycle.md)
records why that behavioral evidence is useful without claiming portable
Leader-session identity.

## Remaining Divergence

The AST still does not retain rematerialization operations as portable scenario
syntax. Workloads do not yet express rates, stop conditions, generated parallel
scheduling, or nesting.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R03,
LS.SYS.VER.SCEN-R07, LS.SYS.VER.SCEN-R09`.

## Implementation Contract

Add a remaining surface only when a concrete Scenario requires it. Each new
variant needs capability validation and stable trace instructions/outcomes.
Lifecycle or rematerialization operations must retain participant identity and
an explicit terminal or indefinite outcome. Future workload scheduling must
preserve the compact data-only AST, derived-seed reproduction, stable generated
action identities, and explicit v1 sequential semantics rather than silently
changing them.
