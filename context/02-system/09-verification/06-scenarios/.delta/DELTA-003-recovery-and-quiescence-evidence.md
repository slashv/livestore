# DELTA-003 — Recovery and quiescence evidence is implicit

Status: closed (2026-07-22) — resolved by explicit disconnect-fault,
Quiescence, and Recovery trace evidence.

## Resolution

The runner now retains `fault.injected` and `fault.removed` only after both the
corresponding Control acknowledgement and a system observation confirm the
requested connectivity state. A reconnect acknowledgement alone therefore
does not claim Fault removal or Recovery.

Before settlement polling, the runner projects instruction/outcome boundaries
and emits `quiescence.reached` only when no other modifying Scenario operation
is in flight. After observed Fault removal, stable-poll samples emit
`recovery.observed`; `recovery.completed` is emitted only when the convergence
predicate holds twice with the same signature, immediately before the distinct
`settlement.completed` boundary. Failed recovery retains observations without
manufacturing a completion record.

This closes the delta for the currently supported disconnect/reconnect fault
model. Broader fault families remain in DELTA-001, and overlapping execution
with complete histories remains in DELTA-002.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R11,
LS.SYS.VER.SCEN-R15` and
[decision 0007](../.decisions/0007-operation-evidence-and-property-vocabulary.md).
