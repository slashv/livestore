# DELTA-001 — Scenario verification baseline incomplete

Status: open

## Divergence

Several vertical slices now exist in the private `tests/scenarios/` workspace.
They provide a versioned serializable AST, schema-backed named actions and
inspectors, a transport-neutral host, controlled disconnect/reconnect, bounded
parallel operation groups, stable-poll settlement, semantic trace records,
five core oracles, and a schema-validated run artifact. The portable
offline-writer scenario runs
unchanged through a production-shaped in-process host, one isolated Node
process per Client, or one persistent Chromium context per Client. The local
profiles connect to the real sync-cf Worker and SQLite Durable Object through
the production WebSocket client under local workerd.

The browser profile uses one page per Client session with the production web
adapter, SharedWorker, Web Locks, and OPFS. A browser-specific scenario proves
that two sessions share a Client, one session can stop and restart, and the
entire persistent Client can restart without losing materialized State before
settling. Completed artifacts can be selected by CLI profile, persisted, and
loaded into the replay visualizer. Its observation-index cursor projects actual
backend, Client, Leader-role, and session sync/eventlog observations, and its
event markers expose pending, confirmed, and rebased positions. The trace now
distinguishes controller events, instructions, acknowledgements,
`firstObserved` samples, and verdicts; preserves non-atomic observation-capture
identity; records explicit instruction→acknowledgement edges; and carries the
controller's local monotonic sequence and calibrated scenario-time point. The
viewer offers capture-aligned flow and calibrated-time layouts over those same
records, fitted and raw elapsed-time scales with explicit gap compression, a
raw-time trace carpet, and matching-event highlighting without inferring
propagation arrows.

The runner now retains independently identified child operations across
bounded parallel groups, derives histories with declared coverage, and uses a
history oracle to verify actual overlap. The coherent baseline is nevertheless
still incomplete. The remaining gaps are decomposed into narrow contracts for
the [scenario surface and workloads](DELTA-005-scenario-surface-and-workloads.md),
[fault-model breadth](DELTA-007-fault-model-breadth.md),
[oracle breadth](DELTA-009-oracle-catalogue-breadth.md),
[live visualizer control](DELTA-011-live-visualizer-control.md).

Process and browser observation responses emit
participant-local sequence and monotonic time; controller round-trip samples
calibrate those occurrences into explicit uncertainty intervals. They do not
expose exact sync boundary receive/apply transitions. Component
facts therefore remain `firstObserved` samples rather than application-time
claims. The flow view can align captures and display explicit control
causation, while exact propagation stages and sync arrows are outside the
capabilities currently advertised by these hosts.

Event references in this first slice are
correlated from ordered occurrences of actual eventlog observations. An
exact lineage seam was explored and deliberately left outside the sync engine;
the hosts consequently do not advertise `event-lineage`. The browser's public
Store observation exposes session↔Leader status, so its portable Leader/backend
view is reconciled with the actual authoritative backend eventlog. One session
per Client remains an advertised v1 limit of the in-process and process hosts
rather than hidden profile parity.

All three profiles now run one capability-parameterized host-conformance suite.
The runner derives structural requirements and session limits before execution,
so unsupported topology, operations, observations, or oracles fail before any
Client is created.

Controlled interleaving replay was evaluated and deliberately left outside the
current capability set. Seeded runs reproduce declared inputs and requested
choices, not session↔Leader or Leader↔backend delivery order; adding gates to
those paths solely for Scenario replay is not part of the baseline.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R01…R21`, accepted via
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
5. seeded reproduction with explicit non-guarantees for internal interleaving;
6. stable scenario trace, core safety/convergence/pending-resolution oracles,
   bounded settlement, and reproducible run artifacts; and
7. requirement traceability annotations on the evidence that implements these
   contracts.

Close this umbrella delta when the remaining baseline hard blocker—a backend-
availability fault—runs headlessly. The other accepted extensions may remain
in their narrow deltas. Optional participant profiles, backend realizations,
and cross-profile comparisons do not create deltas merely because they have
not been implemented.
