# DELTA-009 — The portable oracle catalogue remains narrow

Status: open

## Divergence

The runner evaluates eventlog/state convergence, no-pending-events, expected
State, and declared operation-history properties after settlement. It does not
yet provide portable checks for rebase preservation, additional safety and
liveness properties, rematerialization equivalence, resource bounds, or
performance evidence.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R14,
LS.SYS.VER.SCEN-R16, LS.SYS.VER.SCEN-R17`.

## Implementation Contract

Add each property as an explicit serializable oracle with typed verdict evidence
and declared trace/capability prerequisites. A checker must reject insufficient
evidence rather than pass on unavailable observations. Performance verdicts
must use wall-clock evidence and remain distinct from logical scheduling time.
