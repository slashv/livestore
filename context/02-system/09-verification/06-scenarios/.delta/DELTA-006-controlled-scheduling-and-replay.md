# DELTA-006 — Controlled scheduling and replay are incomplete

Status: closed (2026-07-25) — resolved by accepting controlled interleaving
replay as an unavailable capability rather than adding Scenario-only gates to
sync and runtime delivery paths.

## Resolution

Phase order provides sequence, and `parallel` joins a bounded set of retained
child operations after releasing their host requests from a shared invocation
barrier. The runner intentionally does not claim control over the subsequent
session↔Leader, Leader↔backend, Effect scheduler, browser event-loop, or concrete
backend interleaving.

Artifacts retain the normalized plan, seed, requested operations, execution
configuration, and observed trace. Their `seeded` reproduction mode means the
same inputs and requested choices, not the same internal delivery order. A
rerun may therefore fail to reproduce a timing-sensitive defect.

[Decision 0010](../.decisions/0010-keep-controlled-replay-out-of-sync-paths.md)
rejects adding delivery gates to adapters, transports, processors, or the sync
engine solely for Scenario replay. Recorded boundary replay remains a possible
future capability if an owning subsystem independently introduces suitable
control seams.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R09,
LS.SYS.VER.SCEN-R10, LS.SYS.VER.SCEN-R16, LS.SYS.VER.SCEN-R18`.

## Accepted Contract

Seed reproduction must never be reported as interleaving replay. Any future
controlled-replay profile must explicitly advertise the capability, name the
boundaries it controls, record their release decisions, and fail at the first
unavailable or incompatible replay decision.
