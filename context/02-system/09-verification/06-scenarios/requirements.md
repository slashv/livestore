# Scenario-Based Sync Verification — Requirements

Role: owns reproducible, system-wide verification scenarios spanning clients,
sessions, sync backends, workloads, faults, recovery, materialization, and
runtime boundaries.

## Context

Builds on [../requirements.md](../requirements.md). Product behavior remains
owned by sync, runtime, state, Store, and observability nodes; this node owns
the evidence architecture that composes those behaviors. The accepted
architecture originates in [RFC 0003](../../../../contributor-docs/rfcs/0003-scenario-based-sync-verification.md).

In-process, isolated-process, local sync-cf, and persistent-browser
scenario-runner slices exist; the remaining baseline divergence is recorded in
[DELTA-001](./.delta/DELTA-001-scenario-verification-not-built.md).

## Requirements

- **LS.SYS.VER.SCEN-R01 Declarative scenario model:** Contributors author
  typed TypeScript scenario modules through Effect Schema-backed declarative
  constructors that normalize to a versioned, serializable scenario AST. Run
  control flow, time, randomness, faults, and assertions are explicit AST data,
  not arbitrary orchestration callbacks. Stable names, schema validation, and
  canonical serialization keep human- and agent-authored scenarios reviewable.
  `refines: LS-R11, LS.SYS-R02`
- **LS.SYS.VER.SCEN-R02 Application definition:** A scenario application
  definition wraps the actual `LiveStoreSchema` and reuses its event types,
  Store type, and materializers. Named actions and state inspectors cross the
  host boundary by stable name and schema-encoded values; the scenario model
  never redeclares or invokes materializers. `refines: LS-R11`
- **LS.SYS.VER.SCEN-R03 Topology and lifecycle:** The scenario model represents
  a sync backend separately from one or more Clients. A Client is the stable
  top-level participant containing one active Leader role and one or more
  Client-session participants. Plans can add, stop, restart, and remove
  supported participants or roles while preserving their scenario identities.
  `refines: LS.SYS-R04`
- **LS.SYS.VER.SCEN-R04 Explicit plans and workloads:** Plans declaratively
  compose application actions, participant lifecycle, connectivity and faults,
  workloads, scheduling, observed conditions, phases, and settlement. Reusable
  workload patterns expand deterministically and every emitted application
  action appears in the scenario trace.
- **LS.SYS.VER.SCEN-R05 Participant-host boundary:** The runner controls
  Clients and Client sessions only through a transport-neutral host contract.
  Control operations, acknowledgements, capability descriptions, application
  actions, and trace records crossing that boundary are serializable; the
  runner never holds participant Stores, processors, adapters, or databases.
- **LS.SYS.VER.SCEN-R06 Capability-based execution:** An execution
  configuration composes a participant execution profile, a sync-backend
  realization, and optional state capabilities. Profiles advertise supported
  controls and fault semantics before execution, and the runner rejects a
  scenario whose required capabilities are unavailable. The contract does not
  require every profile/backend combination.
- **LS.SYS.VER.SCEN-R07 Production-shaped controlled profile:** The primary
  in-process correctness profile uses real Stores, session and leader sync
  processors, materializers, and SQLite databases behind controlled in-memory
  boundaries with a mock backend. A processor-only model does not satisfy this
  profile. `refines: LS-R03, LS-R05, LS-R06`
- **LS.SYS.VER.SCEN-R08 Profile conformance and evidence scope:** Every
  implemented participant execution profile passes one shared host-conformance
  suite for its claimed capabilities. Compatible scenarios remain unchanged
  across profiles, but each result makes claims only about its selected
  profile; cross-profile comparison is optional and declares the properties it
  compares.
- **LS.SYS.VER.SCEN-R09 Reproduction:** Every profile records a seed governing
  generated inputs and requested choices. The controlled in-process profile
  additionally records and replays runner-controlled boundary decisions,
  reporting the first divergence instead of claiming a false reproduction.
- **LS.SYS.VER.SCEN-R10 Time semantics:** Correctness runs default to logical
  time for runner-owned scheduling and delivery controls; performance evidence
  uses wall-clock time. Logical time is never reported as performance evidence.
- **LS.SYS.VER.SCEN-R11 Valid fault semantics:** Fault injection occurs at the
  highest boundary that still exercises the behavior under test and respects
  the selected realization's guarantees. Impossible corruption, duplication,
  or reordering requires an explicitly adversarial realization or capability.
- **LS.SYS.VER.SCEN-R12 Sync/state separation:** Eventlog safety and convergence
  are independently verifiable from materialized-state convergence and
  rematerialization. SQLite is required by the initial full-stack profile but
  is not part of scenario-level sync semantics. `refines: LS-R05, LS-R06,
LS-R10`
- **LS.SYS.VER.SCEN-R13 Scenario trace protocol:** Every run emits a versioned
  scenario trace with a stable run descriptor, ordered semantic records,
  participant and boundary identities, correlation and causation, and typed
  payloads. Namespaced implementation diagnostics may extend the trace, but
  portable consumers ignore unknown diagnostics and do not depend on them.
- **LS.SYS.VER.SCEN-R14 Scenario oracles:** Safety, ordering, convergence,
  pending resolution, rebase preservation, state, rematerialization, liveness,
  and optional resource/performance checks are explicit scenario oracles that
  produce bounded verdicts and evidence references. `refines: LS-R03, LS-R05`
- **LS.SYS.VER.SCEN-R15 Settlement:** A settle phase stops new work, heals its
  named faults, identifies the expected convergence group, and evaluates an
  explicit profile-appropriate settlement barrier and timeout. Unresolved
  pending events or unacknowledged control work prevent successful settlement;
  there is no hidden global meaning of “eventually.”
- **LS.SYS.VER.SCEN-R16 Reproducible artifacts:** A scenario run artifact
  contains the normalized scenario, application and source identity, execution
  configuration, component versions, seed, controlled decisions when present,
  scenario trace, verdicts, and relevant snapshots needed to explain or replay
  the run.
- **LS.SYS.VER.SCEN-R17 Headless authority:** Headless execution is the
  authoritative local and CI mode. Live and replay visualization consume the
  scenario trace or run artifact and may issue controls only through an
  explicit runner API; visualizers never mutate or inspect participants
  directly. `refines: LS-R13`
- **LS.SYS.VER.SCEN-R18 Repository boundary:** Scenario orchestration, hosts,
  backend realizations, traces, oracles, artifacts, corpus, and visualization
  live in one private `tests/scenarios/` workspace. It may depend on product
  packages; product packages never depend on it.
