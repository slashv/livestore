# DELTA-001 — Scenario verification subsystem not built

Status: open

## Divergence

The contracts in `LS.SYS.VER.SCEN-R01…R18` are accepted intent, but no private
`tests/scenarios/` workspace, declarative scenario model, participant host,
production-shaped in-process profile, scenario trace, scenario oracle set,
settlement mechanism, reproducible artifact, or visualizer exists.

Existing focused sync and integration tests encode setup, timing, faults, and
assertions directly in their harnesses. They do not provide the shared
serializable scenario semantics or evidence boundary required by this node.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R01…R18`, accepted via
[RFC 0003](../../../../../contributor-docs/rfcs/0003-scenario-based-sync-verification.md).

## Implementation Contract

Establish the first coherent headless subsystem in `tests/scenarios/`:

1. versioned scenario AST, typed authoring surface, and real-schema application
   definitions;
2. transport-neutral participant-host contract and host-conformance suite;
3. production-shaped controlled in-process host using real Stores, processors,
   materializers, SQLite, and a mock backend;
4. stable Client/Client-session topology, explicit actions, lifecycle controls,
   basic disconnect/reconnect and backend-availability faults;
5. seeded scheduling plus controlled-boundary recording/replay;
6. stable scenario trace, core safety/convergence/pending-resolution oracles,
   bounded settlement, and reproducible run artifacts; and
7. requirement traceability annotations on the evidence that implements these
   contracts.

Close this umbrella delta when that coherent baseline runs headlessly. In the
same change, create narrower deltas for every accepted requirement that remains
unimplemented, such as richer workloads/faults, optional trace capabilities,
or live/replay visualization. Optional participant profiles, backend
realizations, and cross-profile comparisons do not create deltas merely because
they have not been implemented.
