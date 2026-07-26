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
[decision 0001](./.decisions/0001-declarative-scenario-verification.md), with
the local fidelity mappings recorded in
[decision 0003](./.decisions/0003-local-process-and-browser-realizations.md).
Failed execution capture is specified by
[decision 0006](./.decisions/0006-preserve-failed-runs.md).
Operation, evidence, property, and convergence vocabulary is specified by
[decision 0007](./.decisions/0007-operation-evidence-and-property-vocabulary.md).
In-process, isolated-process, and persistent-browser vertical slices now run,
while the coherent baseline remains incomplete
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
                 ┌────────────────┼─────────────────────┐
                 ▼                ▼                     ▼
       operation history    scenario oracles     live/replay visualizer
                                  │                     │
                                  └──────▶ run artifact ◀┘
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
| Faults       | Fault injection and Fault removal operations                    |
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

| Family             | Meaning                                                                           |
| ------------------ | --------------------------------------------------------------------------------- |
| Application        | Commit a schema event or invoke a named action                                    |
| Lifecycle          | Add, stop, restart, or remove a supported participant or role                     |
| Connectivity/fault | Inject or remove a supported adverse condition                                    |
| Workload           | Run a named seeded pattern, repetition, or burst                                  |
| Scheduling         | Sequence, parallelism, logical timing, repetition, condition wait                 |
| Settlement         | Stop work, remove named faults, establish a convergence group, evaluate a barrier |

Instructions and observations are distinct. A Control acknowledgement proves
only completion of Participant-host request handling at the advertised
boundary. It is not proof of Sync backend confirmation, propagation, Recovery,
or the requested observed state. Scenario operations retain successful,
definite-failure, or indefinite outcomes; a timeout never proves non-execution.

Participant-host failures use a portable category independently from outcome
certainty:

| Category                    | Meaning                                                              |
| --------------------------- | -------------------------------------------------------------------- |
| Host infrastructure failure | The execution profile could not create, run, or clean up its host    |
| Request rejected            | The participant host returned a negative response for the request    |
| Invalid response            | A returned response did not satisfy the portable response contract   |
| Response timeout            | No response crossed the advertised boundary before its bounded limit |
| Transport failure           | The controller/participant channel failed while carrying the request |

`capability-unavailable` is reserved for a configuration or operation the
profile does not advertise, rather than a request that failed at runtime. A
category alone never establishes certainty: a transport failure before send
can be definite, while loss after dispatch is indefinite; a response timeout
is indefinite. Profile-native details remain in the diagnostic message.

Workload patterns declare compatible application actions, parameters, targets,
rate/count, and stopping condition. Their deterministic expansion is compact in
the AST, while each emitted application action is recorded in the trace.

The implemented `parallel` scheduling step contains two or more ordinary
non-settlement operations. Every child retains its own identity; the runner
records all child instructions before releasing their host requests, executes
the requests concurrently, and awaits every child outcome before advancing.
The container is orchestration rather than a synthetic Scenario operation.

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
combination. The runner derives required capabilities from Scenario topology,
operations, observations, and oracles, then unions them with explicitly
declared platform requirements. Unsupported configurations and per-Client
session counts fail validation before the run emits evidence or creates a
participant.

### Participant-host contract

Every participant execution profile realizes one transport-neutral host
contract that can:

- create a Client and add a Client session;
- dispatch a serialized named action to a target session;
- stop or restart supported sessions, Clients, or Leader roles;
- inject and remove supported faults;
- acknowledge lifecycle and control handling at the host boundary;
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

The first process realization assigns one Node child process to each Client and
carries serialized commands and observations over IPC. The first browser
realization assigns one persistent browser context to each Client and one page
to each Client session. Pages within a Client therefore share the production
SharedWorker leader, Web Locks, origin, and OPFS, while browser contexts isolate
Clients. Reopening one page preserves the Client; reopening the context with the
same profile directory models a persistent Client restart.

### Sync-backend realizations

| Realization    | Evidence scope                                                                         |
| -------------- | -------------------------------------------------------------------------------------- |
| Mock/in-memory | Required controlled correctness, fault injection, and high participant counts          |
| Local concrete | Optional real provider/backend serialization, persistence, and reconnection evidence   |
| Deployed       | Optional authentication, network, persistence, platform-limit, and deployment evidence |

The first local-concrete realization runs the repository's actual sync-cf
Worker and SQLite Durable Object under local workerd through Wrangler and uses
the production WebSocket sync client. It is isolated per scenario run and does
not claim deployed-service evidence.

### Conformance and cross-profile evidence

Every implemented participant host passes a shared suite for the capabilities
it claims: creation, action dispatch, lifecycle control, capability rejection,
stable identities, core trace families, control/fault failure reporting, and
valid artifacts. Shared failure conformance verifies the portable category and
the separate definite/indefinite outcome for each exposed failure boundary.
The same capability-parameterized suite runs against the in-process, process,
and browser factories and verifies scope cleanup of process and browser
resources after execution.

A scenario using only shared capabilities remains unchanged across compatible
profiles. Results are profile-scoped. Cross-profile comparison is optional;
when selected, it names the semantic properties being compared. A failure in a
platform-realized profile is evidence about that configuration, not automatic
proof of host non-conformance or a requirement for one-to-one run equivalence.

## Time, Scheduling, and Reproduction (LS.SYS.VER.SCEN-R09, R10, R19)

Logical time orders Scenario-owned plan and trace facts and may control
explicitly advertised runner-owned timers, scheduled faults, workload rates,
or delivery delays. Participant-local monotonic time and
calibrated scenario time describe observed elapsed time. Wall-clock time
supplies externally comparable throughput, latency, CPU, and memory evidence.
These notions are never conflated, and none participates in LiveStore's sync
semantics.

Every instrumented participant assigns a monotonically increasing local
sequence and local monotonic timestamp to its records. A profile that compares
elapsed time across processes calibrates each participant clock against the
scenario controller, records the calibration identity, and represents the
result as an estimated scenario-time interval whose uncertainty includes clock
offset and transport uncertainty. The controller receipt timestamp remains a
fallback observation, not a substitute for participant occurrence time.

Participant-local sequence establishes observed order within that participant.
Explicit instruction/acknowledgement, request/response, boundary-transition,
dependency, and causation records establish supported cross-participant
relationships. Correlation associates related evidence but creates no edge.
Timestamp order alone never creates a causal edge. Overlapping
calibrated intervals remain temporally unordered, and a timestamp that
contradicts an explicit causal edge beyond its uncertainty is reported as a
clock-calibration or instrumentation problem rather than used to rewrite the
causal evidence.

Every generated choice derives from the recorded seed. This reproduces inputs,
requested timing, workloads, and fault choices, but does not reproduce internal
host or sync interleaving. The current profiles do not advertise controlled
boundary replay and do not gate individual session↔Leader, Leader↔backend, or
backend-response deliveries.

A future profile may advertise recorded boundary replay only when it names and
controls those boundaries. It records each release decision and reports the
first decision whose operation is unavailable or whose preconditions differ.
Seed equality alone is never presented as interleaving replay.

Exact Effect fiber scheduling, browser event-loop scheduling, remote backend
ordering, and byte-identical traces are outside this guarantee unless a future
profile explicitly advertises them.

## Fault Semantics (LS.SYS.VER.SCEN-R11)

The implemented portable baseline supports Client disconnect/reconnect and
shared backend unavailability/recovery. Delayed responses, bounded
latency/jitter, constrained throughput, supported participant or Leader-role
termination/restart, and stale-head or concurrent-push conditions produced
through valid protocol behavior are future fault families and must not be
advertised by a profile that cannot realize them.

The local `sync-cf` realization places a Scenario-owned TCP proxy between every
participant and Wrangler. While unavailable, the proxy withholds traffic on
existing participant sockets and rejects new connections. Wrangler, the Worker,
and Durable Object remain live, and the evidence-only backend observer bypasses
the proxy. Removing the fault resumes the retained transport route; this
baseline does not claim recovery after TCP/WebSocket destruction, Worker or
Durable Object restart, or persisted-backend process death. The controlled mock
backend realizes the same portable fault through its availability control.

Faults are injected at the highest boundary that still exercises the behavior
under test. Corruption, duplication, or arbitrary reordering is legal only when
the selected transport can exhibit it or the scenario selects an adversarial
realization/capability.

Fault removal stops the injected condition. Recovery is a separate observed
progression toward the scenario's required operating or converged state; a
successful reconnect Control acknowledgement alone does not prove it.

## Sync and State Evidence (LS.SYS.VER.SCEN-R12)

- **Sync correctness:** expected participants converge on the authoritative
  eventlog order without silent loss or duplication.
- **Full-stack correctness:** after eventlog convergence, normalized
  materialized State converges and can be reproduced from that eventlog.

Eventlog convergence requires ordered equality between the authoritative
backend Eventlog through the settled head and every selected participant's
confirmed Eventlog through that head. Equality covers the retained Event facts
and their authoritative order; equal heads, equal entry counts, zero pending
Events, or equivalent materialized State are not substitutes. The oracle fails
on the first retained loss, duplication, reordering, or unequal Event and
rejects insufficient Eventlog evidence rather than treating it as a pass.

Eventlog convergence does not require a State oracle. State convergence and
rematerialization require compatible application inspectors and profile
capabilities. The initial profile nevertheless runs SQLite materialization for
every participant because current rebase, changeset, transaction, and
materializer-failure behavior depends on it.

## Scenario Trace Protocol (LS.SYS.VER.SCEN-R13, R19)

One versioned scenario trace serves live observation and replay. Its stable run
descriptor records scenario/run IDs, scenario and trace versions, source and
application identity, execution configuration, component versions,
capabilities, seed, and reproduction mode.

The ordering and timing evidence model is selected in
[decision 0004](./.decisions/0004-causal-order-and-calibrated-time.md).
Truth-preserving visibility and playback navigation are selected in
[decision 0005](./.decisions/0005-system-focus-and-playback-moments.md).
The current limit on engine instrumentation for Event lineage is selected in
[decision 0009](./.decisions/0009-keep-scenario-lineage-out-of-sync-engine.md).

Stable records share an envelope containing:

- run ID and monotonic runner-observation index;
- origin: instruction, acknowledgement, observation, or verdict;
- participant, role, and boundary identities where applicable;
- logical and wall-clock time where provided;
- participant-local sequence and monotonic time where emitted;
- optional calibrated scenario-time interval, calibration identity, and
  uncertainty;
- observation-capture identity for facts sampled in one collection pass;
- evidence semantics distinguishing boundary sent/received/applied transitions
  from state that was only first observed by sampling;
- correlation identifiers for association and explicit dependency/causation
  references where supported; and
- stable record kind plus typed, versioned payload.

The observation index orders runner receipt; it does not assert an atomic
distributed order. A scenario observation capture groups component facts
collected by one runner sampling pass, but the collection itself may read the
backend and participants at different instants. Capture membership therefore
means "indistinguishable at this sampling resolution," not simultaneous. A
capture that first observes an event everywhere cannot recover intermediate
propagation transitions that occurred between samples.

The trace's canonical ordering evidence is a partial order. It combines
participant-local sequence with explicit control and boundary-transition
dependency/causation references. A shared correlation identifier only groups
evidence and never creates an edge. Independent branches remain unordered even
when calibrated time shows that one completed much later. Calibrated time
annotates this graph with latency and scheduling evidence; it does not turn it
into a fabricated global total order.

Stable semantic families cover run/phase lifecycle,
participant lifecycle, Control acknowledgements, Operation outcomes,
application actions,
connectivity and boundary batches, event disposition, eventlog positions,
advance/rebase transitions, Fault injection/removal, settlement progress,
oracle verdicts, and structured failures.

The full Scenario trace is the evidence envelope. A Scenario operation history
is a derived projection of retained instruction and outcome boundaries for
history-based checks. Consumers must not call that projection complete unless
it covers the required failed, indefinite, and overlapping operations. The
current projection declares Client creation, application action, connectivity,
session/Client lifecycle, and settlement families across the
instruction-to-Control-outcome boundary. System/sync sampling and State
inspection are explicitly excluded from that application/control history.

Trace consumers reconstruct an **observed system state at cursor** from the
prefix ending at a selected observation index. That projection is the runner's
accumulated knowledge after one record, not an atomic distributed snapshot at a
wall-clock instant. Timeline layout, grouping, and playback never change these
cursor semantics. Logical time remains available for scheduling and
reproduction but is not a visual claim about when product transitions occurred.

Portable event observations embed the actual encoded LiveStore event facts
available at the observed component, including origin and current sequence and
parent positions, plus an opaque run-local event reference. Because a sequence
number is a mutable eventlog position rather than an immutable identity, a
profile claiming event-lineage must carry that reference through actual sync
transitions across session, Leader, and backend observations and explicitly map
rebase and confirmation position changes. Consumers do not infer identity from
event arguments, timestamps, or matching positions.

Every observed event carries its pending or confirmed disposition separately
from its base sequence-position string. Replay surfaces render pending events
with the canonical trailing-prime notation (`e3'`, `e3r1'`) and never infer
confirmation from a client component or rebase generation. A profile claiming
event-lineage capability captures the individual pending tail rather than only
an aggregate pending count. That capability guarantees that one run-local
event reference denotes the same actual Event across sampled components and
explicit transitions, including when one origin emits equivalent Events or an
Event rebases, confirms, is rejected, or disappears. Similar Event facts,
matching positions, and occurrence order are insufficient to claim lineage.

Without event-lineage capability, a host may still assign event references by
sampled correlation to support debugging and visual grouping. Such a reference
means only that observations are inferred to concern the same Event. Oracles
ignore it, causal projections cannot derive edges from it, and replay surfaces
must distinguish the inferred link from proven lineage.

Participant hosts and backend realizations derive these records from actual
LiveStore sync state, eventlogs, boundary operations, and transition observers;
the runner does not simulate product state from its instructions. Existing
DevTools or internal observation surfaces may supply those facts, and
Scenario-side wrappers should first instrument the existing session↔Leader and
Leader↔backend boundaries. A profile advertises the absence of exact transition
or lineage evidence when those surfaces are insufficient. Scenario verification
does not by itself justify adding merge mappings or hot-path observers to the
sync engine; a new product seam must be independently owned and justified by the
relevant LiveStore subsystem. Scenario-level references and envelopes make
available evidence portable without replacing LiveStore Event structure.

Private queues, raw depths, scheduler state, SQLite details, provider payloads,
Web Lock/OPFS internals, OTel spans, stack traces, and performance entries are
namespaced diagnostics. Portable oracles ignore unknown diagnostics unless the
scenario explicitly requires their capability. Additive optional fields are
compatible; removal or semantic change requires a protocol-version change.

## Properties, Oracles, and Settlement (LS.SYS.VER.SCEN-R14, R15, R21)

Scenario properties are explicit correctness or reliability claims under
declared assumptions. Scenario oracles evaluate those properties and return
bounded Scenario verdicts with evidence references. Families include safety,
ordering, convergence, pending resolution, rebase preservation, State
convergence, rematerialization, liveness, resource bounds, and optional
wall-clock performance thresholds.

The implemented operation-history oracle checks named operations for terminal
outcomes, can reject indefinite outcomes, and can require retained evidence of
overlapping invocation intervals. It evaluates only the projection's declared
coverage and does not imply linearizability, serializability, or another
general consistency model.

A settle phase:

1. stops new workload actions and awaits dispatched-action Control acknowledgements;
2. removes the injected faults named by the phase;
3. declares the convergence group and any intentional exclusions; and
4. stops new writes from that group while evaluating a bounded barrier.

Successful settlement requires the expected participants to report
local/upstream heads at backend head `H`, no unexplained pending Events, no
unacknowledged controls, and—when a future profile advertises controlled
delivery—no held due delivery that can change the convergence result. Repeated
observation establishes a stable catch-up point for later property evaluation.
The barrier does not by itself prove that the Eventlog contents behind `H` are
equal and does not include State or other property verdicts.

For a Client-session participant, the implemented predicate covers both that
session and its current Leader: every local/upstream component head must reach
`H`, and pending work in either component prevents settlement.

Current profiles use repeated observation and a bounded wall-clock stability
window. A future controlled-delivery profile must release due work and advance
logical time until no immediately due controlled work can affect convergence.
Open streams and future polling alone do not prevent settlement.

The runner joins each bounded parallel group before advancing to a later step,
projects outstanding instruction/outcome boundaries before settlement, and
emits `quiescence.reached` only when no modifying operation other than the
settlement itself remains in flight. For the supported disconnect and backend-
availability faults,
`fault.injected` and `fault.removed` are first-observed facts:
they follow both the Control acknowledgement and a system observation
confirming the requested connectivity state. Each later settlement poll emits
`recovery.observed` while that removed fault is recovering;
`recovery.completed` precedes `settlement.completed` only after the convergence
predicate holds twice with one stable signature. A removal acknowledgement or
a single converged sample therefore proves neither Recovery nor settlement.

If a settlement deadline expires, the runner records a structured
`settlement.failed` boundary containing the declared timeout and the latest
successfully sampled participant heads, pending counts, and synced flags. It
then records the active phase and step in a terminal `run.failed` boundary.
Increasing the timeout is not a substitute for preserving and exposing a
stable non-convergent state.

After settlement, Scenario oracles evaluate the declared properties. A failed
verdict may make the run fail without changing the completed convergence
barrier. Conversely, settlement failure prevents later property evaluation
when the required evidence cannot be captured.

The current snapshot-based oracles (`pending-resolution`,
`eventlog-convergence`, `state-convergence`, and `state-contains-ids`) share one
terminal Settlement evidence boundary. When any such oracle is selected, the
last Scenario step must be a settle phase whose convergence group includes
every participant selected by those oracles. No later modifying operation may
invalidate that boundary before final evidence capture and property
evaluation. Earlier settle phases remain legal checkpoints, but they cannot
stand in for the terminal boundary. An `operation-history`-only Scenario does
not require Settlement because its verdict is derived from retained operation
boundaries rather than a final system snapshot.

Scenario construction validates this relationship between the plan and its
oracles. Runner preflight repeats the invariant before creating participants so
a caller cannot bypass it by supplying a manually constructed typed AST.

The Eventlog-convergence oracle evaluates the selected participants against the
authoritative backend Eventlog captured at the settled head. It emits a passing
verdict only when the ordered confirmed contents are equal and cites the
compared evidence. A matching head with missing Eventlog contents is
insufficient evidence, not convergence.

Participant hosts expose runtime failures observable at their execution
boundary as participant-scoped `runtime.failure.observed` records. A browser
profile may retain a worker or page error reported through its browser console;
other profiles may provide equivalent process or runtime diagnostics. The
runner drains these observations during system capture and terminates promptly
after recording them. This diagnostic channel does not alter LiveStore sync
behavior or manufacture product causality.
Any such runtime failure makes the run fail even if the last sampled head and
pending-count predicates would otherwise satisfy settlement.

Runtime health and sync/eventlog convergence are independent projections. A
scoped runtime failure marks only the affected Leader role or session as
unhealthy from its first retained observation until an explicit restart or
recovery boundary. It does not invalidate confirmed eventlog observations on
that participant, mark a healthy sibling role as failed, or imply that a nearby
event caused the failure without explicit causal evidence. The later
settlement or run failure remains a separate global boundary.

## Artifacts, Headless Runs, and Visualization (LS.SYS.VER.SCEN-R16, R17, R20)

A scenario run artifact contains the normalized AST, application/source
identity, component versions, execution configuration, environment metadata,
seed, controlled schedule when available, scenario trace, oracle verdicts and
failure explanation, relevant eventlog/State snapshots, and wall-clock
measurements when enabled.

Once `run.started` has been emitted, an operation failure still produces a
valid artifact with `status: failed` and the complete trace prefix. Final
snapshots or oracle verdicts may be absent when their capture was unreachable.
Headless command-line execution persists and catalogs this artifact before
returning a non-zero status. Preflight configuration errors may fail without an
artifact because participant execution has not begun.

The minimum rerun needs the artifact and matching source revision. Every
profile supports seeded reproduction of inputs and requested choices; this is
not interleaving replay. Profiles with independently justified controlled
boundaries may additionally support recorded boundary replay.

The visualizer consumes either a live trace or completed artifact. Its system
view shows topology, connectivity, traffic, pressure, and convergence; its
timeline view shows application actions and causal transitions by participant.
Any runner control goes through an explicit API. The UI does not directly
inspect or mutate participants and is never required for headless execution.
Settlement and terminal run failures are system-focused playback moments and
appear as explicit failure boundaries in the timeline and its range overview.

The timeline offers two projections over the same records:

- **Causal-flow projection.** Horizontal stages expose event propagation and
  other supported partial-order relationships. Sibling branches may align as
  the same structural stage, but alignment does not claim simultaneity. Actual
  elapsed delay remains visible through wait segments, latency annotations,
  uncertainty, or diagnostic emphasis.
- **Elapsed-time projection.** Horizontal position uses calibrated scenario
  time. Delayed participants move later on the axis; overlapping uncertainty
  intervals are not forced into a false before/after order. Known causal links
  remain visible and take precedence over timestamp-based presentation. Its
  default fitted scale may compress long gaps only when each distortion is
  visibly marked with the real duration; an uncompressed linear-time scale
  remains available.

The timeline uses semantic zoom without changing either projection's evidence:
spacious observations receive labelled markers, denser observations become
points, and observations below the available pixel resolution become bounded
aggregates. Aggregation is local to a participant lane and projected visual
bin; neighbouring markers must not transitively collapse into a lane-wide
stack. Observations at one projected position may group in place, but the
visualizer does not invent horizontal or unbounded vertical separation. Hover
or selection exposes the observations represented by a point or aggregate.

A range navigator retains a whole-run density overview while its two handles
select the main timeline's visible window. Narrowing or panning that window
recomputes semantic detail from the visible density; it does not mutate the
artifact, projection evidence, or independently selected trace cursor. The
overview continues to locate that cursor when it falls outside the main
window.

A compact trace carpet may group all records by observation capture and expose
less prominent instructions, acknowledgements, observations, and verdicts.
Multiple records at one projected position stack rather than overwrite one
another. Selecting any aggregate or marker reveals its raw records, local
sequence, runner receipt index, capture, timing estimate and uncertainty, and
evidence semantics. A material capture moment also exposes a concise difference
between the projected system state before and after the capture, so an otherwise
anonymous observation marker explains which participant, connectivity, head,
pending suffix, or event state changed. In fitted elapsed-time mode the carpet
retains the raw linear-time distribution as context.

Moment inspection first presents its member records in retained order, grouped
by backend, Client, and session scope without implying capture atomicity or
causality. Each record exposes a concise semantic presentation derived from its
tagged payload, with the complete envelope and payload available through a
foldable JSON tree. Selecting a record for detail is independent from the trace
cursor: it does not project a partial capture or change the system state being
replayed. Inspector section visibility follows the operator while they compare
records, and each record's expanded JSON branches are restored when revisited
for the lifetime of the current viewer session only.

Trace visibility and playback stepping are independent viewer projections. A
system-focused visibility mode retains application actions, eventlog changes,
topology, connectivity, lifecycle, settlement, failure, and other material
system transitions while suppressing unchanged sampling and runner plumbing.
An all-records mode retains every raw trace record. Classification is semantic:
an acknowledgement such as `client.created` remains system-relevant even
though routine control acknowledgements may be suppressed.

Persistent system conditions are projected as intervals rather than isolated
markers. In particular, a disconnected Client is highlighted across its
Leader-role and session lanes from the acknowledged disconnect transition to
the acknowledged reconnect transition. When either explicit boundary is
absent, the first sampled connectivity observation may bound the interval only
when the UI marks that boundary as observational and therefore uncertain. The
viewer does not extend a condition backward beyond the evidence retained by
the trace.

Declared topology remains visible as a faint lane guide, but a backend,
Leader-role, or session lane becomes solid only from its first retained
creation or observation boundary. A stopped session returns to the declared
treatment until its acknowledged restart. Participant-scoped creation,
action, lifecycle, connectivity, and runtime-failure moments appear as compact
markers on the affected lane or Client group; global phase, settlement, and
run boundaries remain in the system layer. A runtime failure additionally
starts a participant-local unhealthy interval so its origin remains apparent
even when a later global failure boundary terminates the run.

Record playback visits every observation-index boundary. Moment playback
visits a derived list of material navigation points: semantic system
transitions and the final boundary of an observation capture whose accumulated
system projection changed. Records skipped by navigation are still reduced
into the state at every selected point. One moment may therefore represent
several raw records, and the UI retains both its moment ordinal and raw record
index. Filtering and moment derivation never rewrite the artifact or define a
second cursor.

Scrubbing selects an observation-index boundary and projects the trace prefix
into backend, Client, Leader-role, session, boundary, and event state. Timeline
arrows use explicit event references and dependency/causation records rather
than capture membership or temporal proximity. No arrow is drawn when the
trace lacks that evidence. Playback of a completed artifact advances this
cursor, either through every record or through derived moment boundaries; it is
distinct from rerunning the scenario. Optional checkpoints may accelerate
seeking but are derived cache data and never replace the authoritative trace.

The first replay visualizer projects sync and eventlog evidence only: topology,
connectivity, heads, confirmed and pending events, rebases, propagation, and
settlement/verdict state. Materialized application State remains available
through explicit inspector snapshots and is not captured at every cursor.

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
