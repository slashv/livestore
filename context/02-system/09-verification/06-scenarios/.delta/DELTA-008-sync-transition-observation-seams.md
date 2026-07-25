# DELTA-008 — Exact sync-transition observation seams are missing

Status: open

## Divergence

Process and browser profiles emit participant-local sequence and monotonic time,
and controller calibration maps observations to explicit uncertainty intervals.
Sync/eventlog facts are still sampled as `firstObserved`; the hosts do not emit
exact receive/apply transitions or stable lineage when equivalent events are
rejected, disappear, or reorder. The browser profile also lacks a stable
browser-safe Leader/backend observation seam.

Current hosts no longer advertise `event-lineage`. They retain
fingerprint-and-occurrence references as explicitly inferred visualization
evidence, and portable Eventlog equality ignores those references. The
remaining delta is the exact transition and stable identity evidence needed to
support the stronger capability.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R12,
LS.SYS.VER.SCEN-R13`.

## Implementation Contract

Instrument stable semantic boundaries for event receipt and application while
preserving event lineage across rebases and rejection. Retain participant-local
ordering and calibrated uncertainty without inventing cross-participant causal
edges. Provide a portable browser-safe Leader observation surface or advertise
its absence as a capability limit.
