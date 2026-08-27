# Effect Machine Sync Processors

> **Status:** Draft architectural proposal. This document describes an implementation experiment and does not replace
> accepted product intent.

This work builds on Igor Gassmann's `MaterializationJournal` and role-specific SQLite Effect service extraction. It
preserves that storage architecture and adds an explicit orchestration model above it. `LeaderSyncCommitter` becomes the
focused durable seam used by the leader processor, while Effect Machine owns the asynchronous lifecycles of both sync
processors.

## In One Minute

LiveStore has two sync processors:

- `ClientSessionSyncProcessor` keeps one Store responsive and reconciles it with the leader.
- `LeaderSyncProcessor` combines all client sessions with the sync backend.

On `main`, their behavior emerges from several transactional queues, semaphores, long-running fibers, mutable
references, and restart rules working together. To understand whether a push, pull, rebase, retry, or shutdown can run,
a maintainer has to reconstruct the state encoded across those mechanisms.

This proposal models those lifecycles with `@typeonce/effect-machine`. Each processor has a root machine for its
coordination policy and child machines for independently owned push and pull relationships. States own the Effects they
invoke, so leaving a state also defines when its network call, stream, timer, or reconciliation work is cancelled.

The leader machine is a private implementation detail of `LeaderSyncProcessor`, not a peer module connected through a
broad dependency object. The root topology and the local/upstream Effects it schedules are colocated. Provider push and
pull are separate deep modules because each owns a complete independently retrying lifecycle. The client-session
experiment retains a processor/machine split because it also has a synchronous Store-facing lane; that split remains an
area to evaluate separately.

This is a framework-driven state machine, but not a pure reducer plus command interpreter. Effect Machine transitions
select topology and update state, while invoked Effects perform the actual durable, network, publication, and shutdown
work in the state that owns them.

## Coordination State We No Longer Have to Reconstruct

The primary simplification is not that every queue, reference, or deferred disappears. It is that synchronization
primitives no longer jointly encode the current lifecycle. Domain queues become machine data, and concurrent work is
owned by explicit states.

### Leader processor

| Coordination on `main`                                     | What it encodes                                   | Proposed owner                                                            |
| ---------------------------------------------------------- | ------------------------------------------------- | ------------------------------------------------------------------------- |
| `localPushesQueue`                                         | local work waiting to be applied                  | `Running.localQueue`                                                      |
| `syncBackendPushQueue`                                     | events waiting for backend propagation            | provider-push child `Active.queued`                                       |
| `localPushBackendPullMutex`                                | exclusion between local and upstream durable work | mutually exclusive `CommittingLocal` and `CommittingUpstream` root states |
| `pushAdmissionSemaphore`                                   | atomic validation and reservation                 | one `Admitting` state-owned Effect                                        |
| `reservedLocalPushItems`                                   | admitted events not yet committed or rejected     | `Running.reservations`                                                    |
| the reservation-aware push-head calculation                | optimistic validation fence                       | the reservation tail, or `Running.syncState.localHead`                    |
| `pullMutexHeld`                                            | whether pagination blocks local work              | `Running.pullPagination`                                                  |
| an optionally initialized `SubscriptionRef` shared by work | observable state spanning independent workers     | canonical `Running.syncState` plus a write-only public read model          |
| a long-running local-apply worker                          | local draining, batching, and fairness            | one `Idle` scheduling transition plus semantic commit/completion states    |
| a restartable backend-push worker                          | push progress and replacement after rebase        | provider-push child topology                                              |
| `Effect.retry`, schedule state, and retry counters         | when a provider request can run again             | `BackingOff` states and state-owned timers                                |
| a long-running backend-pull worker                         | stream lifetime, pagination, and failure          | provider-pull child topology                                              |
| one acknowledgement `Deferred` per queued event            | completion of a caller's batch                    | one request registry entry with a remaining-item count                    |

The root machine is the sole owner of durable scheduling. Provider push and pull remain concurrent, but each is a
direct child of `Running`, with its queue, active request, retry timer, and failure state in one topology.

### Client-session processor

| Coordination on `main`                          | What it encodes                                        | Proposed owner                                               |
| ----------------------------------------------- | ------------------------------------------------------ | ------------------------------------------------------------ |
| `leaderPushQueue`                               | queued events and whether leader propagation is active | leader-push child `Active` data and leaf state               |
| `pullReconciliationMutex`                       | exclusion among pull, rejection recovery, and shutdown | root `ApplyingPull` and `Draining` states                    |
| `unresolvedRejection`                           | propagation waiting for corrective leader history      | `Running.rejection` and push-child `AwaitingReconciliation`  |
| `terminalPushCause`                             | background propagation failure                         | root `Failed` state                                          |
| `leaderPushingFiberHandle`                      | batching, active call, cancellation, and restart       | leader-push child and its state-owned invocation             |
| `pullingFiberHandle`                            | lifetime of the leader pull stream                     | leader-pull child `Streaming` state                          |
| `Effect.never` after rejection                  | a fence awaiting corrective history                    | `AwaitingReconciliation`                                     |
| clearing and rebuilding the transactional queue | invalidating an old push plan after rebase or recovery | `ReplacePlan` transition                                     |
| shutdown branches around queue and fiber state  | drain versus immediate cancellation                    | root `Draining` and `Stopping`, plus push-child `BeginDrain` |

Some adapter-level runtime machinery remains deliberately narrow:

- a synchronous `syncStateRef` preserves immediate Store visibility;
- deferred registries correlate an external pull item or caller with the machine turn that completes it;
- an observable queue publishes session state changes;
- a small guard makes the public shutdown operation idempotent.

These values bridge synchronous APIs and external streams. They do not decide which asynchronous lifecycle is active.

## The New Shape

```text
┌────────────────────────── one client session ───────────────────────────┐
│                                                                         │
│  Store.commit                                                           │
│      │                                                                  │
│      ▼                                                                  │
│  ClientSessionSyncProcessor                                             │
│    ├─ synchronous lane: merge → materialize → publish local state       │
│    ├─ Effect implementations and external correlation registries        │
│    └─ ClientSessionSyncMachine                                          │
│         ├─ root: pull reconciliation, failure, drain, shutdown          │
│         ├─ leader-push child                                             │
│         └─ leader-pull child                                             │
│                          │                                              │
└──────────────────────────┼──────────────────────────────────────────────┘
                           │ leaderThread.events.push / pull
                           ▼
┌───────────────────────────── leader thread ─────────────────────────────┐
│                                                                         │
│  LeaderSyncProcessor                                                    │
│    ├─ public service, correlations, and publication read model           │
│    ├─ private root machine: admission and durable scheduling             │
│    ├─ local/upstream commit and completion Effects                       │
│    ├─ LeaderSyncProviderPush child module ───────────► Sync backend      │
│    ├─ LeaderSyncProviderPull child module ───────────► Sync backend      │
│    └─ LeaderSyncCommitter                                                │
│         ├─ state DB                                                      │
│         └─ eventlog DB                                                   │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

The modules have distinct responsibilities:

| Module                         | Owns                                                                                     | Does not own                                                  |
| ------------------------------ | ---------------------------------------------------------------------------------------- | ------------------------------------------------------------- |
| `ClientSessionSyncProcessor`   | Store-facing API, synchronous optimistic state, materialization operations, correlations | asynchronous lifecycle topology                               |
| `ClientSessionSyncMachine`     | leader push/pull ownership, reconciliation, rejection recovery, failure, draining        | SQLite or leader-proxy construction                           |
| `LeaderSyncProcessor`          | public interface, root topology, durable scheduling, domain workflows, publication       | provider retry mechanics or SQLite transition details         |
| `LeaderSyncProviderPush`       | provider batching, connectivity gating, retries, server-ahead recovery, cancellation     | leader durable scheduling or publication                      |
| `LeaderSyncProviderPull`       | provider stream, page backpressure, retries, progress, cancellation                      | merging or committing an upstream page                        |
| `LeaderSyncCommitter`          | materialization, rollback, journal maintenance, heads, eventlog writes, commit receipts  | queues, retries, publication, acknowledgements, or lifecycle  |

### Why the leader processor and root machine share one module

A processor/machine split is useful only when the interface between them is substantially smaller and more stable than
either implementation. That was not true for the leader coordinator. The machine needed the initial `SyncState`, policy
configuration, validation, both durable workflows, request settlement, shutdown, and both provider Effects. Following
one local commit meant repeatedly crossing a wide dependency interface, while the processor and machine each remained
large.

The proposed implementation removes that shallow seam. `LeaderSyncProcessor.ts` contains the public Effect service,
the private root topology, and the local/upstream workflows selected by that topology. Their conceptual roles remain
distinct, but their code is local and there is no exported machine protocol to learn. The separate files are the deep
modules:

- `LeaderSyncProviderPush.ts` exposes a small command and parent-event protocol while hiding its complete lifecycle.
- `LeaderSyncProviderPull.ts` exposes the pulled-page protocol and acknowledgement operation while hiding its stream,
  cursor, retry, progress, and backpressure machinery.
- `LeaderSyncCommitter.ts` exposes durable commit operations while hiding the multi-database transition procedure.

This is the intended test for future extractions: a new module should hide meaningful implementation depth behind a
small interface, not merely move part of one workflow to another file.

## The Effect Machine Pattern

```text
external input
      │
      ▼
typed machine event
      │
      ▼
transition selects topology and state
      │
      ├───────────────► pure state update / child command
      │
      ▼
state-owned Effect, stream, or timer
      │
      ▼
onDone / onFailure transition
```

Effect Machine provides four properties used by this proposal:

1. **Hierarchical state:** lifecycle-wide policy lives on a root or compound state; detailed behavior lives on its leaf
   states.
2. **Child ownership:** independently concurrent push and pull relationships have their own statecharts without creating
   a Cartesian root state.
3. **State-owned work:** an invoked Effect, stream, or timer is cancelled when its owning state exits. A stale completion
   cannot transition a newer state instance.
4. **Typed protocols:** state data, accepted events, parent events, child events, and emitted events are declared with
   schemas and checked at machine boundaries.

Durable leader work is still serialized. `CommittingLocal` and `CommittingUpstream` are mutually exclusive root states,
and their invoked Effects complete before the root returns to scheduling. Provider requests and client leader calls are
genuinely concurrent child work and therefore remain independently cancellable.

## The States That Matter

### Leader root

```text
Starting ── Boot ──► Running
  Idle
  Admitting
  CommittingLocal
  CompletingLocal
  CommittingUpstream
  CompletingUpstream
      │
      └─ shutdown / reset / fatal failure
          ▼
Stopping ──► Stopped
```

`Running` owns the current `SyncState`, pending admissions, local queue, reservations, upstream pages, pagination mode,
and any terminal request. One eventless transition in `Idle` selects the next eligible turn in priority order: stop,
admission, upstream commit, then local commit. Upstream pages are chosen before local durable work, and `more-expected`
pagination keeps local work blocked between pages.

`Starting` is the bootstrap fence: provider children do not begin pulling until `boot` has installed the runtime and
accepted the `Boot` event. This preserves the public startup ordering without using an adapter flag.

Commit and completion are separate semantic states. `CommittingLocal` or `CommittingUpstream` establishes durable truth.
Its transition atomically installs the new canonical `Running.syncState`; only then does the corresponding `Completing`
state publish the read model, update provider propagation, release pull-page backpressure, or resolve callers. Scheduling
therefore never reads the public `SubscriptionRef`, and publication cannot become the coordinator's second source of
truth.

### Leader provider children

```text
provider push:
  Disabled
  Active
    Idle
    Pushing
    ClassifyingFailure
    BackingOff
    RestoringPlan
    AwaitingPull
    Failed

provider pull:
  Disabled
  Streaming
  BackingOff
  Completed
  Failed
```

The push child's `Append` event extends the current provider plan. `ReplacePlan` atomically invalidates active or queued
propagation and starts from the pending suffix produced by an upstream commit. `ServerAheadError` moves propagation to
`AwaitingPull`.

There is an important two-child race: the required pull may be committed immediately before the provider reports
`ServerAheadError`. The root compares its upstream head with the server's required head. If that history is already
applied, it replays the current replacement plan; otherwise the push child waits for the upcoming pull. This closes the
lost-wakeup window without coupling the two child machines.

### Client-session root

```text
Starting ──► Running ───────────────────────► Stopping ──► Stopped
              │   Active                         ▲
              │   ApplyingPull                   │
              │   Draining                       │
              └──────────────► Failed ───────────┘
```

The session retains one intentional exception to machine ownership: `Store.commit` must materialize and update local
state synchronously so an immediate UI read observes the commit. The resulting `LocalPushAdmitted` notification enters
the machine after that synchronous boundary.

### Client-session children

```text
leader push:
  Active
    Idle
    Dequeuing
    InFlight
    AwaitingReconciliation
    Suspending
    Suspended
    ReportingDrained
    ReportingDrainFailure
    Failed

leader pull:
  Streaming
  Disabled
  Failed
```

The push child owns the active leader call. A rebase sends it `Suspend`, causing it to leave `InFlight` and interrupt the
owned call before rollback begins. Reconciliation then replaces the complete plan from the live pending suffix, which
includes synchronous commits admitted while rollback was suspended.

If orderly shutdown arrives during `ApplyingPull`, the root records the drain request without leaving the state.
Reconciliation installs its replacement plan first, then raises shutdown and lets the push child drain. A failed
shutdown is immediate and interrupts owned work.

## Representative Call Stacks

### 1. A local Store commit reaches the backend

```text
Store.commit
  ├─ ClientSessionSyncProcessor.encodeEvents
  ├─ ClientSessionSyncProcessor.materializeEvents
  └─ ClientSessionSyncProcessor.push
       ├─ synchronously merge into optimistic session SyncState
       ├─ publish session SyncState
       └─ machine.send(LocalPushAdmitted)
            └─ leader-push child: Idle → Dequeuing → InFlight
                 └─ leaderThread.events.push
                      └─ LeaderSyncProcessor.push
                           ├─ register request acknowledgement
                           └─ machine.send(PushRequested)
                                ├─ Idle → Admitting
                                │    └─ validate and reserve complete batch
                                └─ Idle → CommittingLocal → CompletingLocal
                                     ├─ commitLocal
                                          ├─ SyncState.merge
                                          ├─ LeaderSyncCommitter.commitLocal
                                          │    ├─ materialize into state DB
                                          │    ├─ update journal and heads
                                          │    └─ coordinate SQLite commits
                                     ├─ atomically install canonical Running.syncState
                                     └─ completeLocal
                                          ├─ publish committed receipt to sessions
                                          ├─ provider-push child.send(Append)
                                          └─ resolve request acknowledgement

provider-push child: Idle → Pushing
  └─ syncBackend.push
       ├─ success → Idle
       └─ failure → ClassifyingFailure
```

The leader acknowledgement means the batch is durable, published, and scheduled for backend propagation. It does not
mean the backend has already accepted it.

### 2. An upstream page reaches all sessions

```text
provider-pull child: Streaming
  └─ syncBackend.pull
       └─ parent.send(UpstreamBatchReceived { batchId })
            └─ leader root: Idle → CommittingUpstream → CompletingUpstream
                 ├─ commitUpstream
                      ├─ SyncState.merge
                      ├─ LeaderSyncCommitter.commitUpstream
                      │    ├─ rollback divergent pending state when needed
                      │    ├─ materialize replacement history
                      │    ├─ update journal, eventlog, and heads
                      │    └─ coordinate SQLite commits
                 ├─ atomically install canonical Running.syncState
                 └─ completeUpstream
                      ├─ publish committed receipt to session pull queues
                      ├─ provider-push child.send(ReplacePlan)
                      └─ release this provider page

leader-pull child: Streaming
  └─ parent.send(PullItemReceived { requestId })
       └─ client root: Active → ApplyingPull
            ├─ leader-push child.send(Suspend)
            ├─ wait for active leader call to be interrupted
            ├─ rollback and materialize session state
            ├─ refresh affected tables
            ├─ publish live session SyncState
            └─ leader-push child.send(ReplacePlan)
```

Each pull stream waits for its correlated completion. A later page therefore cannot race ahead of the durable cursor or
session reconciliation for the current page.

### 3. A provider push fails and retries

```text
provider-push child: Pushing
  └─ syncBackend.push fails
       └─ ClassifyingFailure
            ├─ offline / unknown failure
            │    └─ BackingOff
            │         └─ state-owned timer → Pushing
            ├─ server ahead
            │    └─ RestoringPlan → AwaitingPull
            │         └─ ReplacePlan → Idle → Pushing
            └─ backend identity mismatch
                 └─ Failed
                      └─ parent applies reset / shutdown / ignore policy
```

There are no manually correlated retry fibers. Exiting `BackingOff` cancels its timer; replacing the push plan exits
the active branch and cancels any superseded provider call.

### 4. The leader rejects a session push

```text
leader-push child: InFlight
  └─ leaderThread.events.push rejects
       ├─ parent.send(PushRejected)
       │    ├─ corrective pull already recovered this batch?
       │    │    └─ ReplacePlan from live pending
       │    └─ otherwise record Running.rejection
       └─ AwaitingReconciliation
            └─ later PullItemReceived
                 ├─ apply advance or rebase
                 ├─ clear recovered rejection
                 ├─ ReplacePlan from live pending
                 └─ Active
```

The push child cannot accidentally continue draining behind a rejection fence. Recovery is a topology change plus a
complete plan replacement, not queue clearing coordinated with a parked worker.

### 5. Orderly client shutdown overlaps a rebase

```text
client root: ApplyingPull
  ├─ ShutdownRequested(drain)
  │    └─ retain ApplyingPull and set drainRequested
  ├─ complete rollback, materialization, and ReplacePlan
  └─ raise ShutdownRequested(drain)
       └─ Draining
            ├─ leader-pull child.send(Disable)
            ├─ leader-push child.send(BeginDrain)
            └─ PushDrained → Stopping → Stopped
```

The replacement plan is installed before draining begins, so shutdown cannot lose a rebased event.

## Why This Is Easier to Reason About

A maintainer can answer the main coordination questions from the machines:

- **What can happen now?** Inspect the active root and child state paths.
- **What inputs can change it?** Read the declared event schemas for that state and its ancestors.
- **What work is running?** Inspect the invocation, stream, or timer owned by the active state.
- **What cancels that work?** Follow the transition that exits its owner.
- **What makes a leader transition durable?** Follow `commitLocal` or `commitUpstream` into `LeaderSyncCommitter`.
- **Can stale work update a replacement lifecycle?** No; invocation outcomes belong to the state instance that started
  them.
- **Can publication or acknowledgement precede durability?** No; they follow a validated commit receipt.
- **Can independent push and pull relationships be understood separately?** Yes; they are child machines with typed
  parent protocols.

The machines do contain framework-specific nesting. That indentation expresses root, compound, and leaf ownership; it
is not incidental syntax. The leader root deliberately avoids topology that only translates a loop: its single `Idle`
choice exposes scheduling priority, while the remaining states name work with a real lifetime. Domain calculations sit
beside that topology in the same implementation module, so a reviewer can follow a commit without crossing a broad
processor/machine seam.

## Guarantees and Deliberate Limits

| Guarantee                                                                  | Status                                                         |
| -------------------------------------------------------------------------- | -------------------------------------------------------------- |
| One leader root state owns admission and durable scheduling                | guaranteed by the machine mailbox                              |
| At most one leader local or upstream durable operation is active           | guaranteed by mutually exclusive commit states                 |
| Local pushes are validated against committed plus reserved history         | guaranteed by `Running.reservations`                           |
| Upstream pagination takes precedence over new local durable work           | guaranteed by root selection and `pullPagination`              |
| Publication and acknowledgement follow a successful leader commit          | guaranteed                                                     |
| Provider and leader calls are cancelled when their owning state exits      | guaranteed by Effect Machine invocation ownership              |
| A provider push cannot miss an already-applied server-ahead reconciliation | guarded by the leader upstream-head handshake                  |
| A session commit is immediately visible to that session                    | guaranteed by the synchronous processor lane                   |
| Rebase plan replacement includes commits admitted during reconciliation    | guaranteed by re-reading the live pending suffix               |
| Orderly shutdown drains the post-reconciliation plan                       | guaranteed by deferred drain transition                        |
| Backend head and matching event inserts use the same eventlog transaction  | guaranteed                                                     |
| State DB and eventlog DB are crash-atomic together                         | **not guaranteed**                                             |
| A leader acknowledgement means backend acceptance                          | **not guaranteed**; it means durable, published, and scheduled |

The state and eventlog databases use separate SQLite connections. `LeaderSyncCommitter` coordinates normal success and
rollback paths, but it cannot make them atomic across a process crash. State is committed first so the eventlog does not
claim a transition that never reached materialized state. A crash or eventlog commit failure after the state commit can
still leave state ahead of eventlog truth and requires a separate recovery strategy.

## Costs and Trade-offs

- Effect Machine introduces a framework and its vocabulary: compound states, child machines, target scopes, invoked
  work, eventless transitions, and parent/child protocols.
- The declarative machine specifications are deeply nested where the topology is deeply nested. This is more verbose
  than a loop for simple cases, though it keeps lifecycle ownership visible.
- Machine schemas repeat some TypeScript domain shapes at runtime boundaries.
- `LeaderSyncProcessor.ts` remains a substantial implementation module. Its size is the cost of keeping root scheduling
  and the durable workflows it selects local; independent provider and durability lifecycles are extracted only where
  they form deep modules with small interfaces.
- The Effect cohort must move to the version required by Effect Machine. This proposal upgrades the workspace to Effect
  `4.0.0-rc.112` and adds `@typeonce/effect-machine` `0.26.2`.
- Effect Machine becomes critical synchronization infrastructure and must be evaluated for API stability, diagnostics,
  and long-term maintenance alongside the architectural benefits.

## Compatibility and Validation

This is an internal refactor. The Store-facing and leader-thread sync APIs remain unchanged. The experiment retains the
existing SQLite-backed behavior suites covering sync-state merging, leader-thread construction, backend behavior, and
the higher-level Store workflows that exercise admission, pagination, rejection recovery, rebase, and shutdown.

The implementation experiment passes the repository unit suite, TypeScript build, formatting and lint checks, Markdown
checks, and dependency-cycle checks.

## Alternatives Considered

- **Keep the coordination primitives on `main`.** This avoids a dependency and migration, but lifecycle state remains
  distributed across queues, semaphores, fiber handles, and mutable flags.
- **Use a hand-written serialized mailbox loop.** This addresses the same ownership problem with less framework syntax
  and can keep domain policy highly local. It must implement state hierarchy, child ownership, invocation cancellation,
  stale-completion handling, and runtime diagnostics itself.
- **Use a pure reducer plus command/result interpreter.** This maximizes transition purity, but represents each Effect
  twice and separates a durable operation from the invariant it completes.
- **Use one combined machine for root, push, and pull state.** This creates a Cartesian topology and obscures which
  relationships are independently concurrent.
- **Move synchronous client admission into the machine.** An asynchronous mailbox cannot preserve immediate UI
  visibility without another synchronous mutable seam, so the processor is the clearer owner.
- **Move provider or session orchestration into `LeaderSyncCommitter`.** This mixes durable truth with retry, network,
  publication, acknowledgement, and shutdown policy.

## Open Questions

- Does the framework-specific topology make common maintenance tasks faster enough to justify the dependency and
  learning cost?
- Should the client-session processor/machine split also be deepened or collapsed after evaluating the leader result?
- Should statechart snapshots or transition traces be exposed through LiveStore devtools?
- Should the provider and leader child protocols become reusable internal abstractions after production experience?
- What stability and upgrade policy should LiveStore require from Effect Machine before adopting it outside this
  experiment?
- Should a future storage format use one attached SQLite transaction or a recovery protocol for cross-database crash
  consistency?

## Reading Order

For code review or an architecture walkthrough, read the implementation in this order:

1. `LeaderSyncProcessor.ts`: public interface, private root topology, local/upstream workflows, and completion order.
2. `LeaderSyncProviderPush.ts`: provider batching, retry, and server-ahead recovery lifecycle.
3. `LeaderSyncProviderPull.ts`: provider stream, page backpressure, retry, and progress lifecycle.
4. `LeaderSyncCommitter.ts`: durable state/eventlog transition implementation.
5. `ClientSessionSyncMachine.ts`: session root topology and its leader push/pull children.
6. `ClientSessionSyncProcessor.ts`: synchronous Store lane and the Effects supplied to the machine.
7. `make-leader-thread-layer.test.ts`, `syncstate.test.ts`, and the repository Store suites: construction, merge,
   lifecycle, and SQLite-backed behavior evidence.
