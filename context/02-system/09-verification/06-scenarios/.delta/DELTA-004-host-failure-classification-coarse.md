# DELTA-004 — Participant-host failure classification remains coarse

Status: open

## Divergence

Process and browser request/runtime failures are no longer mislabeled as
capability unavailability. Process timeouts and child exits with requests in
flight, plus ambiguous browser action/evaluate completion, produce indefinite
Operation outcomes; known precondition and returned request failures remain
definite.

Most remaining adapter failures still collapse into the broad
`host-request-failed` code. The trace cannot yet distinguish transport closure,
participant-runtime rejection, schema/response decoding, fixture startup, and
host infrastructure failure as stable portable categories.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R05,
LS.SYS.VER.SCEN-R21` and
[decision 0007](../.decisions/0007-operation-evidence-and-property-vocabulary.md).

## Implementation Contract

Define a small portable host-failure taxonomy, map each execution profile's
native errors without overstating certainty, and add shared host-conformance
tests for definite versus indefinite request outcomes.
