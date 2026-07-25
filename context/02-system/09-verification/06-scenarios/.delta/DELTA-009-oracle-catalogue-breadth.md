# DELTA-009 — The portable oracle catalogue remains narrow

Status: open

## Divergence

The runner does not yet provide portable checks for rebase preservation,
additional safety and liveness properties, rematerialization equivalence,
resource bounds, or performance evidence.

The initial `eventlog-convergence` evidence gap is resolved. The oracle now
selects a complete backend/participant observation capture, retains the settled
head and pending-event predicates, and compares each selected participant's
confirmed, ordered Event facts with the authoritative backend Eventlog. Equal
heads with a replaced Event fail with evidence references and the first
divergent position; missing complete capture evidence also fails.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R12,
LS.SYS.VER.SCEN-R14, LS.SYS.VER.SCEN-R16, LS.SYS.VER.SCEN-R17`.

## Implementation Contract

Add each further property as an explicit serializable oracle with typed verdict
evidence and declared trace/capability prerequisites. A checker must reject
insufficient evidence rather than pass on unavailable observations. Performance
verdicts must use wall-clock evidence and remain distinct from logical
scheduling time.
