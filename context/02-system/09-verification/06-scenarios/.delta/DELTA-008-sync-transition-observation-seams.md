# DELTA-008 — Exact sync-transition observation seams are missing

Status: closed (2026-07-25) — resolved by accepting exact Event lineage as an
unavailable capability rather than adding Scenario-only sync-engine
instrumentation.

## Resolution

Process and browser profiles emit participant-local sequence and monotonic time,
and controller calibration maps observations to explicit uncertainty intervals.
Sync/eventlog facts are still sampled as `firstObserved`; the hosts do not emit
exact receive/apply transitions or stable lineage when equivalent events are
rejected, disappear, or reorder. The browser profile also lacks a stable
browser-safe Leader/backend observation seam.

Current hosts do not advertise `event-lineage`. They retain
fingerprint-and-occurrence references as explicitly inferred visualization
evidence, and portable Eventlog equality ignores those references.

A session-side design spike demonstrated a technically viable direction:
return explicit before/after rebase mappings from `SyncState.merge` and emit
rebase/application transitions from the Client session sync processor. It was
removed after review because even that partial slice added a new merge-result
contract and placed observation on the synchronization execution path; complete
coverage would also require Leader, backend, process, and browser paths.

[Decision 0009](../.decisions/0009-keep-scenario-lineage-out-of-sync-engine.md)
accepts the current capability limit. Exact lineage remains a possible future
direction if the owning sync or runtime subsystem independently introduces a
suitable observation seam. Until then, sampled correlation remains explicitly
non-authoritative and no lineage-sensitive capability is advertised.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R12,
LS.SYS.VER.SCEN-R13`.
