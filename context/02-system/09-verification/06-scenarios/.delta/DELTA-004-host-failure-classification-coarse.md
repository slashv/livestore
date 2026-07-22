# DELTA-004 — Participant-host failure classification remains coarse

Status: closed (2026-07-22) — resolved by a portable participant-host failure
taxonomy with separate outcome certainty.

## Resolution

Participant-host failures now use stable categories for host infrastructure,
request rejection, invalid response, response timeout, and transport failure.
The process and browser profiles map their native boundaries into those
categories while retaining profile-specific detail in the diagnostic message.
`capability-unavailable` remains reserved for unsupported profile surfaces.

Failure category is independent from Operation outcome certainty. A process
timeout, child exit with a request in flight, or ambiguous browser mutation is
indefinite; an already-closed IPC channel, returned request rejection, invalid
read response, or pre-dispatch infrastructure failure is definite. Shared
conformance coverage exercises every portable category and both certainty
values for transport failure.

The trace and artifact schemas already encode failure codes as strings, so the
new categories require no trace-version change or saved-artifact migration.
Existing version-3 traces and version-4 artifacts remain loadable.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R05,
LS.SYS.VER.SCEN-R21` and
[decision 0007](../.decisions/0007-operation-evidence-and-property-vocabulary.md).
