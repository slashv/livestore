# DELTA-001 — Scenario verification baseline incomplete

Status: open

## Divergence

The first vertical slice now exists in the private `tests/scenarios/`
workspace. It provides a versioned serializable AST, schema-backed named
actions and inspectors, a transport-neutral host, a production-shaped
in-process profile with one real Store session per Client, controlled
disconnect/reconnect, bounded stable-poll settlement, semantic trace records,
four core oracles, and a schema-validated run artifact. The initial corpus
scenario proves offline and online writers converge through real processors,
materializers, SQLite State, and the shared mock backend.

The coherent baseline is still incomplete. The current slice does not provide
direct schema-event steps, dynamic Client/session/Leader lifecycle, reusable
workloads, generated scheduling, runner-controlled delivery gates and replay,
backend-availability or latency faults, rematerialization, the broader oracle
catalog, artifact persistence, a shared capability-driven conformance suite,
or a live/replay visualizer. One session per Client is an advertised v1 host
limit rather than hidden profile parity.

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
