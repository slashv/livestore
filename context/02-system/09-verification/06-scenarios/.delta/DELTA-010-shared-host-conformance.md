# DELTA-010 — Host conformance is not fully shared and capability-driven

Status: open

## Divergence

The host contract is transport-neutral, and focused tests cover real in-process,
process, and browser execution plus portable failure categories. Conformance is
still distributed across profile-specific tests. There is no single
capability-driven suite that every host implementation runs for all advertised
operation, lifecycle, observation, outcome-certainty, and cleanup semantics.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R04,
LS.SYS.VER.SCEN-R05`.

## Implementation Contract

Extract a shared host-conformance suite parameterized by a profile factory and
its advertised capabilities. It must verify operation identity, Control
acknowledgements, definite versus indefinite failure, lifecycle isolation,
observation contracts, settlement prerequisites, and resource cleanup. A host
must fail capability validation before execution when required behavior is not
advertised.
