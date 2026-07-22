# DELTA-011 — The visualizer is replay-only

Status: open

## Divergence

Completed version-4 artifacts can be loaded into the visualizer, which projects
topology, semantic records, calibrated timing, captures, failures, and explicit
causal edges. It cannot stream an active run, issue runner controls, pause at a
declared scheduling boundary, or save the live session as a replayable artifact.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R19,
LS.SYS.VER.SCEN-R20`.

## Implementation Contract

Define a versioned transport between the runner and visualizer for incremental
trace records, run state, and capability-scoped control commands. Headless
execution remains authoritative. Live control must use the same declared
scheduling boundaries as controlled replay, and a captured live run must remain
loadable through the ordinary artifact decoder.
