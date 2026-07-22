# DELTA-003 — Recovery and quiescence evidence is implicit

Status: open

## Divergence

Settlement currently removes named disconnect faults, repeatedly samples the
selected participants, and requires the heads/pending/isSynced convergence
predicate to hold twice with the same signature. This is a valid bounded
convergence barrier, and reconnect acknowledgements are no longer described as
proof of Recovery.

The trace does not yet emit first-class Fault injection/removal, Recovery, or
Quiescence evidence records. Quiescence is presently guaranteed structurally
by the serial runner rather than checked against an explicit in-flight-work
projection, and Recovery is inferred from later convergence observations
rather than represented as its own progression.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R11,
LS.SYS.VER.SCEN-R15` and
[decision 0007](../.decisions/0007-operation-evidence-and-property-vocabulary.md).

## Implementation Contract

Represent supported Scenario fault models and their injection/removal
boundaries explicitly; project relevant runner-controlled in-flight work for a
Quiescence check; and retain Recovery progression independently from both
Fault removal and final Convergence.
