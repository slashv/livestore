# DELTA-005 — Scenario surface and reusable workloads are incomplete

Status: open

## Divergence

The serializable AST supports named application actions, initial Client/session
topology, selected lifecycle controls, and explicit bounded parallel groups.
Generated corpus helpers expand eagerly into ordinary steps before execution.
It does not yet retain direct schema-event steps, dynamic Client/session
addition and removal, Leader-role lifecycle, reusable seeded workload nodes, or
rematerialization operations as portable scenario syntax.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R03,
LS.SYS.VER.SCEN-R07, LS.SYS.VER.SCEN-R09`.

## Implementation Contract

Add these surfaces as versioned, serializable AST variants with capability
validation and stable trace instructions/outcomes. Workload expansion must be
derived from the recorded seed and remain reproducible without embedding
callbacks or pre-expanding an unbounded action list in the scenario source.
Each lifecycle or rematerialization operation must retain participant identity
and an explicit terminal or indefinite outcome.
