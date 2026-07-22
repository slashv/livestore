# DELTA-002 — Scenario operation history is incomplete

Status: open

## Divergence

The runner retains stable operation correlation IDs, explicit
instruction-to-Control-acknowledgement dependencies, and failure-only Operation
outcome records. Its derived Scenario operation history truthfully projects
successful, definite-failure, indefinite, and still-pending outcomes for
runner instructions present in the trace.

The runner still executes application steps sequentially. Internal inspection
requests do not yet retain full invocation/outcome boundaries, nested
settlement fault-removal commands are represented primarily by the enclosing
settlement operation, and no history-based checker consumes overlapping
operations. The projection is therefore useful evidence but not a complete
concurrent operation history.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R21` and
[decision 0007](../.decisions/0007-operation-evidence-and-property-vocabulary.md).

## Implementation Contract

Retain explicit invocation and success/definite-failure/indefinite outcome
boundaries for every operation family required by a checker; add overlapping
operation scheduling with preserved identities; declare projection coverage;
and add history-based properties/oracles only after their required boundaries
are present.
