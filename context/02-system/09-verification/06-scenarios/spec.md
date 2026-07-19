# Scenario-Based Sync Verification — Spec

This document specifies reproducible system-wide verification scenarios. It
builds on [requirements.md](./requirements.md); rationale lives in
[intuition.md](./intuition.md).

## Status

Draft.

## Scope and Ownership

This node owns scenario semantics, plans, participant-host control, execution
configuration, faults, scheduling, reproduction, settlement, scenario traces,
scenario oracles, run artifacts, profile conformance, cross-profile evidence,
and runner/visualizer separation.

Sync, runtime, state, Store, and observability nodes own the product behavior
being exercised. Scenario code may request the smallest general observation or
control seam from those owners, using an explicit internal testing export when
the seam is not product API.

The architecture was selected in
[decision 0001](./.decisions/0001-declarative-scenario-verification.md). Its
first in-process vertical slice is implemented, while the coherent baseline
remains incomplete
([DELTA-001](./.delta/DELTA-001-scenario-verification-not-built.md)).

This node does not redesign the sync protocol, require an eventlog-only product
profile before sync/State separation exists, emulate packet-level networks,
require every production provider or profile/backend combination, define global
performance budgets, or make schema-invalid/wire-invalid inputs a default mode.

## Architecture

```text
typed scenario source
        │
        ▼
validation + normalization
        │
        ▼
versioned scenario AST ──▶ scenario runner ──▶ participant hosts
                                  │                  + backend realization
                                  ▼
                            scenario trace
                                  │
                 ┌────────────────┼────────────────┐
                 ▼                ▼                ▼
           scenario oracles  run artifact  live/replay visualizer
```

Scenario semantics, runner control, trace protocol, oracles, and consumers are
separate contracts. Headless execution is authoritative; visualization is a
consumer of the same evidence.

## Scenario Semantic Model (LS.SYS.VER.SCEN-R01)

The primary authoring surface is a typed TypeScript module that uses Effect
Schema-backed declarative constructors. It normalizes to a versioned,
serializable AST. JSON may encode that AST in artifacts or across transports,
but YAML, JSON, and arbitrary TypeScript/Effect programs are not primary
scenario authoring formats.

The AST carries:

| Area         | Contract                                                        |
| ------------ | --------------------------------------------------------------- |
| Identity     | Stable scenario ID, description, version, and tags              |
| Reproduction | Seed, scheduling mode, execution configuration                  |
| Application  | Stable application-definition reference                         |
| Topology     | Backend, Clients, Client sessions, boundaries, connectivity     |
| Lifecycle    | Initial and dynamically added, restarted, or removed identities |
| Workloads    | Explicit actions and named parameterized patterns               |
| Schedule     | Logical time, dependencies, observed conditions, phases         |
| Faults       | Requested failures and healing operations                       |
| Completion   | Explicit terminal action or bounded settle phase                |
| Assertions   | Selected scenario oracles and their assumptions                 |
| Capture      | Trace detail, snapshots, measurements, artifact policy          |

All run-specific control remains represented by the normalized AST. Executable
application definitions and named workload libraries are dependencies resolved
by stable identity rather than embedded callbacks.

Stable names for participants, phases, patterns, faults, and oracles, useful
schema-validation errors, seeded defaults, and canonical serialization keep
generated scenarios reviewable by contributors and agents.

## Application Definitions (LS.SYS.VER.SCEN-R02)

An application definition wraps the actual `LiveStoreSchema`; it does not
redeclare event definitions or materializers. From that schema it exposes:

- direct schema-event commits, optionally restricted by the application;
- named higher-level actions with Effect Schema-encoded inputs;
- optional state inspectors returning schema-encoded normalized values; and
- stable application and schema identities used during reproduction.

Action implementations execute inside the target participant host with its
real typed Store. Only the action name and encoded input cross the scenario
boundary. Reproducible actions use runner-provided randomness and time;
uncontrolled external effects cannot claim deterministic replay.

State inspectors read already-materialized State. They are not materializers.
Rematerialization replays the authoritative eventlog through the application's
normal schema and materializers into fresh State before applying an inspector.

## Topology and Plans (LS.SYS.VER.SCEN-R03, R04)

The topology reflects LiveStore's two product boundaries:

```text
sync backend
    ▲
    │ provider boundary
Client
  ├─ Leader role
  └─ Client session(s) ── leader-proxy boundary ──▶ Leader role
```

A Client is the stable top-level scenario participant and owns shared local
data, one active Leader role, and one or more Client-session participants. The
Leader is a controllable and observable role within its Client, not a separate
participant. The sync backend is a separate topology component.

Plans use these stable step families:

| Family             | Meaning                                                                         |
| ------------------ | ------------------------------------------------------------------------------- |
| Application        | Commit a schema event or invoke a named action                                  |
| Lifecycle          | Add, stop, restart, or remove a supported participant or role                   |
| Connectivity/fault | Request or heal a supported failure                                             |
| Workload           | Run a named seeded pattern, repetition, or burst                                |
| Scheduling         | Sequence, parallelism, logical timing, repetition, condition wait               |
| Settlement         | Stop work, heal named faults, establish a convergence group, evaluate a barrier |

Instructions and observations are distinct. A requested lifecycle or fault
transition succeeds only after the appropriate acknowledgement or observation;
issuing the instruction is not proof that it took effect.

Workload patterns declare compatible application actions, parameters, targets,
rate/count, and stopping condition. Their deterministic expansion is compact in
the AST, while each emitted application action is recorded in the trace.

## Execution Configuration (LS.SYS.VER.SCEN-R05…R08)

An execution configuration composes three independent selections:

```text
participant execution profile
        +
sync-backend realization
        +
optional State profile/capabilities
```

The composition is semantic, not a requirement to implement every Cartesian
combination. Scenarios declare required capabilities; unsupported
configurations fail validation before execution.

### Participant-host contract

Every participant execution profile realizes one transport-neutral host
contract that can:

- create a Client and add a Client session;
- dispatch a serialized named action to a target session;
- stop or restart supported sessions, Clients, or Leader roles;
- request and heal supported faults;
- acknowledge lifecycle and control operations;
- advertise capabilities before the run; and
- emit stable trace records without exposing participant objects.

Capabilities describe real guarantees rather than pretending all profiles have
feature parity. Platform-specific scenarios may request controls such as Web
Locks, OPFS, or process termination explicitly.

### Participant execution profiles

| Profile        | Evidence scope                                                                                   |
| -------------- | ------------------------------------------------------------------------------------------------ |
| In-process     | Primary dense correctness and stress execution using controlled boundaries                       |
| Worker/process | Optional evidence about isolation, lifecycle, and transport boundaries                           |
| Browser        | Optional evidence about the web adapter, worker topology, Web Locks, OPFS, and browser lifecycle |

The required in-process profile is production-shaped: every Client has one
actual leader and one or more real Client sessions; sessions use real Stores,
session processors, materializers, and in-memory SQLite State; the Client uses
the real leader processor, eventlog database, and leader State database behind
an in-memory proxy. A direct processor-only harness is subordinate evidence,
not this profile.

### Sync-backend realizations

| Realization    | Evidence scope                                                                         |
| -------------- | -------------------------------------------------------------------------------------- |
| Mock/in-memory | Required controlled correctness, fault injection, and high participant counts          |
| Local concrete | Optional real provider/backend serialization, persistence, and reconnection evidence   |
| Deployed       | Optional authentication, network, persistence, platform-limit, and deployment evidence |

### Conformance and cross-profile evidence

Every implemented participant host passes a shared suite for the capabilities
it claims: creation, action dispatch, lifecycle control, capability rejection,
stable identities, core trace families, control/fault failure reporting, and
valid artifacts.

A scenario using only shared capabilities remains unchanged across compatible
profiles. Results are profile-scoped. Cross-profile comparison is optional;
when selected, it names the semantic properties being compared. A failure in a
platform-realized profile is evidence about that configuration, not automatic
proof of host non-conformance or a requirement for one-to-one run equivalence.

## Time, Scheduling, and Reproduction (LS.SYS.VER.SCEN-R09, R10)

Logical time controls correctness-run timers, scheduled faults, workload rates,
and runner-owned delivery delays. Wall-clock time supplies throughput, latency,
CPU, and memory evidence. The two are never conflated.

Every generated choice derives from the recorded seed. This reproduces inputs,
requested timing, workloads, and fault choices, but does not promise identical
host interleaving.

The controlled in-process profile additionally records the order in which the
runner dispatches actions/lifecycle operations, activates or heals faults,
releases controlled session↔leader and leader↔backend deliveries or backend
responses, and advances logical time. Replay gates those same boundaries. If a
recorded operation cannot become available or its preconditions differ, replay
reports the first divergent decision.

Exact Effect fiber scheduling, browser event-loop scheduling, remote backend
ordering, and byte-identical traces are outside this guarantee unless a future
profile explicitly advertises them.

## Fault Semantics (LS.SYS.VER.SCEN-R11)

Initial supported fault families are disconnect/reconnect, backend
unavailability/recovery, delayed responses, bounded latency/jitter, constrained
throughput, supported participant or Leader-role termination/restart, and
stale-head or concurrent-push conditions produced through valid protocol
behavior.

Faults are injected at the highest boundary that still exercises the behavior
under test. Corruption, duplication, or arbitrary reordering is legal only when
the selected transport can exhibit it or the scenario selects an adversarial
realization/capability.

## Sync and State Evidence (LS.SYS.VER.SCEN-R12)

- **Sync correctness:** expected participants converge on the authoritative
  eventlog order without silent loss or duplication.
- **Full-stack correctness:** after eventlog convergence, normalized
  materialized State converges and can be reproduced from that eventlog.

Eventlog convergence does not require a State oracle. State convergence and
rematerialization require compatible application inspectors and profile
capabilities. The initial profile nevertheless runs SQLite materialization for
every participant because current rebase, changeset, transaction, and
materializer-failure behavior depends on it.

## Scenario Trace Protocol (LS.SYS.VER.SCEN-R13)

One versioned scenario trace serves live observation and replay. Its stable run
descriptor records scenario/run IDs, scenario and trace versions, source and
application identity, execution configuration, component versions,
capabilities, seed, and reproduction mode.

Stable records share an envelope containing:

- run ID and monotonic runner-observation index;
- origin: instruction, acknowledgement, observation, or verdict;
- participant, role, and boundary identities where applicable;
- logical and wall-clock time where provided;
- correlation and causation identifiers where applicable; and
- stable record kind plus typed, versioned payload.

The observation index orders runner receipt; it does not assert an atomic
distributed order. Stable semantic families cover run/phase lifecycle,
participant lifecycle, control acknowledgements, application actions,
connectivity and boundary batches, event disposition, eventlog positions,
advance/rebase transitions, fault activation/healing, settlement progress,
oracle verdicts, and structured failures.

Private queues, raw depths, scheduler state, SQLite details, provider payloads,
Web Lock/OPFS internals, OTel spans, stack traces, and performance entries are
namespaced diagnostics. Portable oracles ignore unknown diagnostics unless the
scenario explicitly requires their capability. Additive optional fields are
compatible; removal or semantic change requires a protocol-version change.

## Oracles and Settlement (LS.SYS.VER.SCEN-R14, R15)

Scenario oracles are explicit configuration and return bounded verdicts with
evidence references. Families include safety, ordering, convergence, pending
resolution, rebase preservation, State convergence, rematerialization,
liveness, resource bounds, and optional wall-clock performance thresholds.

A settle phase:

1. stops new workload actions and awaits dispatched-action acknowledgements;
2. heals the faults named by the phase;
3. declares the convergence group and any intentional exclusions; and
4. stops new writes from that group while evaluating a bounded barrier.

Successful settlement requires the expected participants to hold the same
authoritative order through backend head `H`, with local/upstream heads at `H`,
no unexplained pending events, no unacknowledged controls, no held due
controlled delivery that can change the verdict, and all requested State
oracles passing.

The controlled profile releases due work, advances logical time until no
immediately due controlled work can affect the verdict, observes every expected
participant at `H`, confirms backend stability, and re-evaluates predicates.
Other profiles may use repeated observation or a bounded wall-clock stability
window and must advertise and record that weaker mechanism. Open streams and
future polling alone do not prevent settlement.

## Artifacts, Headless Runs, and Visualization (LS.SYS.VER.SCEN-R16, R17)

A scenario run artifact contains the normalized AST, application/source
identity, component versions, execution configuration, environment metadata,
seed, controlled schedule when available, scenario trace, oracle verdicts and
failure explanation, relevant eventlog/State snapshots, and wall-clock
measurements when enabled.

The minimum replay needs only the artifact and matching source revision. Every
profile supports seeded replay; profiles with controlled boundaries may also
support recorded boundary replay.

The visualizer consumes either a live trace or completed artifact. Its system
view shows topology, connectivity, traffic, pressure, and convergence; its
timeline view shows application actions and causal transitions by participant.
Any runner control goes through an explicit API. The UI does not directly
inspect or mutate participants and is never required for headless execution.

## Repository Placement (LS.SYS.VER.SCEN-R18)

All scenario-specific code lives in one private `tests/scenarios/` workspace:
model, runner, hosts, backend realizations, trace, oracles, artifacts, corpus,
CLI, tests, and visualizer. Internal directories may evolve under this stable
dependency direction:

```text
tests/scenarios  ── uses ──▶  @livestore/*
@livestore/*     ── must not depend on ──▶  tests/scenarios
```

## Open Design Questions

- **LS.SYS.VER.SCEN-DQ1 Constructor ergonomics.** Which TypeScript constructors
  and combinators make the schema-backed AST concise while preserving static
  inspection and canonical formatting?
- **LS.SYS.VER.SCEN-DQ2 Application surface.** What exact API exposes optional
  actions, state inspectors, and event-surface restrictions from an application
  definition?
- **LS.SYS.VER.SCEN-DQ3 Fault-injection seams.** At which abstraction should
  latency and partitions be injected for each supported profile/backend
  combination?
- **LS.SYS.VER.SCEN-DQ4 Trace retention.** How are large scenario traces
  sampled, compressed, referenced, or streamed without losing causal evidence?
- **LS.SYS.VER.SCEN-DQ5 Adversarial inputs.** When do invalid events, malformed
  protocol payloads, and impossible transport behavior become supported modes?
- **LS.SYS.VER.SCEN-DQ6 Failure minimization.** How are generated failing runs
  minimized without destroying the causal interleaving?
- **LS.SYS.VER.SCEN-DQ7 Performance reuse.** Which correctness scenarios can
  also provide trustworthy wall-clock evidence, and which require distinct
  configurations?
