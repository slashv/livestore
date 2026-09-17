# Serialized Sync Processors

> **Status:** Preferred direction for this fork as of September 17, 2026, not an accepted upstream RFC or product
> contract. The session uses approach C: one synchronous owner with yielding reconciliation, including the readability
> refactor at `66ca5d3e0`. The leader architecture is unchanged. Alternatives remain preserved on named branches.

This work builds on Igor Gassmann's `MaterializationJournal` and role-specific SQLite Effect service extraction. It
preserves that storage architecture and adds a more explicit orchestration model above it, with
`LeaderSyncCommitter` providing the focused durable seam used by the leader processor.

## In One Minute

LiveStore has two sync processors:

- `ClientSessionSyncProcessor` keeps one Store responsive and reconciles it with the leader.
- `LeaderSyncProcessor` combines all client sessions with the sync backend.

Previously, their behaviour emerged from several queues, semaphores, long-running fibers, mutable references, and
restart rules working together. To understand whether a push could run, a maintainer had to inspect all of them and
reconstruct their timing.

Now, named events enter one state-changing owner in each processor. The leader uses a serialized mailbox loop. It
delegates durable SQLite work to `LeaderSyncCommitter` and only publishes or acknowledges after that work succeeds.
The session uses a synchronous `dispatch`: local commits and network results enter the same owner. An asynchronous
runner handles waiting and applies incoming history one complete SQLite/model step at a time, yielding between steps.

This is not a framework-driven or fully pure state machine. The session owner is effectful and is not an input-FIFO
mailbox. It finishes state changes synchronously, then releases notifications and commands. Both processors make
ordering and the important states visible without introducing another production module.

## Coordination State We No Longer Have to Reconstruct

The main simplification is not that every queue or fiber disappeared. It is that queues, locks, fibers, booleans, and
references no longer jointly encode the current synchronization state.

### Leader processor

Before the refactor, the leader's control flow depended on all of these at once:

| Previous coordination mechanism                              | What it was encoding                                | What owns that information now                                                    |
| ------------------------------------------------------------ | --------------------------------------------------- | --------------------------------------------------------------------------------- |
| `localPushesQueue`                                           | local work waiting to be applied                    | `model.localQueue`                                                                |
| `syncBackendPushQueue`                                       | events waiting for backend propagation              | the tagged `model.push` state                                                     |
| `localPushBackendPullMutex`                                  | whether local or upstream durable work could run    | mailbox ordering and `processNextWork`                                            |
| `pushAdmissionSemaphore`                                     | atomic validation and reservation of local pushes   | the single mailbox handler                                                        |
| `reservedLocalPushItems`                                     | admitted events not yet committed or rejected       | `model.reservations`                                                              |
| `pushHeadRef`                                                | the validation fence including reservations         | the tail of `model.reservations`, or the durable local head                       |
| `pullMutexHeld`                                              | whether pagination was blocking local work          | `model.pull.pagination`                                                           |
| `ctxRef`                                                     | runtime dependencies needed by background effects   | explicit construction-time dependencies                                           |
| an optional `syncStateSref` initialized during boot          | state shared between independent background workers | `model.syncState` for decisions and an initialized observable ref for subscribers |
| a long-running local-apply worker                            | draining and batching local pushes                  | `ContinueWork` turns in the mailbox                                               |
| a restartable backend-push worker                            | push progress, retry, and replacement after rebase  | tagged push states plus correlated completion events                              |
| `Effect.retry`, schedule metadata, and a local retry counter | when and why a provider call would run again        | `retry-wait` states and retry events with identities                              |
| one acknowledgement `Deferred` per event                     | completion of a caller's batch                      | one request-level acknowledgement with a remaining-event count                    |

The queues and reservations have not disappeared as domain concepts. They are now fields in one model, changed by one
owner, rather than independent synchronization primitives that can disagree.

### Client-session processor

The session processor had a similar set of implicit controls:

| Previous coordination mechanism                   | What it was encoding                                          | What owns that information now                                     |
| ------------------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------------ |
| `leaderPushQueue`                                 | pending events and whether propagation was active             | the tagged `model.push` state                                      |
| `pullReconciliationMutex`                         | exclusion between pull, rejection recovery, and shutdown      | synchronous dispatch, reconciliation identity and lifecycle checks |
| `unresolvedRejection`                             | whether propagation was waiting for corrective leader history | `push: awaiting-reconciliation`                                    |
| `terminalPushCause`                               | whether the background worker had failed                      | `model.lifecycle` and `model.terminalCause`                        |
| a permanent push-drain worker                     | batching, propagation, and parking after rejection            | one finite push operation at a time                                |
| `Effect.never` in that worker                     | a rejection fence waiting for corrective history              | the explicit `awaiting-reconciliation` state                       |
| clearing and restarting that worker during rebase | invalidating an old push plan                                 | operation identities and rebuilding from live pending events       |

Some runtime machinery remains, but with narrower jobs:

- the leader mailbox carries events; the session command queue schedules asynchronous work;
- fiber handles own the lifetime of actual concurrent provider or leader calls;
- deferred values let callers await an acknowledgement or let a pull stream apply backpressure;
- shutdown state keeps repeated shutdown requests idempotent.

These are adapters around the model. They no longer compete with it as sources of orchestration truth.

## The New Shape

```text
┌────────────────────────── one client session ──────────────────────────┐
│                                                                        │
│  Store.commit                                                          │
│      │                                                                 │
│      ▼                                                                 │
│  ClientSessionSyncProcessor                                            │
│    ├─ one synchronous owner: local commits, pull steps, push results   │
│    └─ async runner: network, cancellation, refresh, yield, shutdown    │
│                         │                                              │
└─────────────────────────┼──────────────────────────────────────────────┘
                          │ leaderThread.events.push / pull
                          ▼
┌──────────────────────────── leader thread ─────────────────────────────┐
│                                                                        │
│  LeaderSyncProcessor                                                   │
│    ├─ one mailbox                                                      │
│    ├─ one explicit model                                               │
│    ├─ session publication and acknowledgements                         │
│    └─ provider pull, push, and retry orchestration                     │
│                  │                            │                        │
│                  ▼                            ▼                        │
│       LeaderSyncCommitter                  Sync backend                │
│       durable SQLite work                 pull / push                  │
│          │           │                                                 │
│          ▼           ▼                                                 │
│       state DB    eventlog DB                                          │
│                                                                        │
└────────────────────────────────────────────────────────────────────────┘
```

The three modules have distinct responsibilities:

| Module                       | Owns                                                                                                          | Does not own                                                           |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| `ClientSessionSyncProcessor` | optimistic session state, leader propagation, stepped reconciliation, pull refresh, session shutdown          | Store's local subscriber refresh, leader durability or backend retries |
| `LeaderSyncProcessor`        | event ordering, in-memory sync state, session publication, acknowledgements, provider work, retries, shutdown | SQLite transition details                                              |
| `LeaderSyncCommitter`        | materialization, rollback, journal maintenance, heads, eventlog writes, coordinated SQLite commits            | queues, publication, retries, acknowledgements, or lifecycle           |

## One Owner, Two Execution Patterns

### Leader: a serialized mailbox

```text
external input or async completion
              │
              ▼
        tagged Event
              │
              ▼
         mailbox queue
              │
              ▼
      handle one event fully
              │
       ┌──────┴──────┐
       │             │
       ▼             ▼
 update Model   start/await work
                     │
                     ▼
              result returns as
              a correlated Event
```

There is one important distinction between kinds of work:

- durable leader work is awaited inside the mailbox turn, so nothing can observe a half-applied transition;
- genuinely concurrent work, such as a provider request or retry timer, runs in a supervised fiber and reports its
  outcome back to the mailbox as an event.

Every concurrent operation has an identity. A late completion is ignored when its identity no longer matches the
current state.

### Session: a synchronous owner and an asynchronous runner

```text
Store.commit ──────────────┐
network result ────────────┤
runner: next pull step ────┤
shutdown request ──────────┘
                          │
                          ▼
                   dispatch(Event)
                          │
                   transition router
                          │
                   named owner workflow
                          │
                SQLite work when needed
                    → matching model
                          │
                    release owner
                          │
                 notifications / commands
                          │
                          ▼
                    async runner
                wait, cancel, refresh, yield
                          │
                          └──► next step / completion enters dispatch
```

All session model changes go through `dispatch`. It does not wait for network I/O, cancellation or a timer.
The command queue schedules work, not LiveStore event-log order. Sequence numbers and `SyncState.merge` still
determine which history is valid. A command or completion can become obsolete while waiting, so identities are
checked before a push starts and when its result arrives.

Notifications are deliberately staged until after the owner is released. An Effect Queue offer or Deferred completion
can resume another fiber immediately, even when automatic scheduler yielding is disabled. Sending it halfway through
a transition would let that caller see unfinished state or reenter the owner. Subscriber refresh also runs outside
the owner: Store refreshes after a local commit, and the runner refreshes after a pull step.

This separation preserves synchronous `Store.commit` without a second state-changing path. Browser input may still
wait for a synchronous pull step to finish before its handler can start. Once that handler calls `Store.commit`, the
local SQLite/model change completes before the call returns. Neither statement requires network I/O to be synchronous.

## The States That Matter

### Leader

```text
lifecycle:  starting ──► running ──► stopping
                            └──────► failed

pull:       disabled
            streaming { between-pages | more-expected }
            retry-wait
            completed

push:       disabled
            idle
            in-flight
            retry-wait
            awaiting-pull
```

The model also holds the current `SyncState`, local reservations, and the local and upstream work waiting for the next
mailbox turn. Upstream pages are processed before more local work. When a page says more pages are coming, local work
waits until that upstream sequence is complete.

### Client session

```text
lifecycle:  starting ──► running ──► stopping ──► stopped
                            └──────► failed

push:       idle
            in-flight
            awaiting-reconciliation

reconcile:  none
            active { id } ──► complete step ──► yield ──► next step
```

`Store.commit` calls `processor.commit(events)`, which enters the same `dispatch` used by pull steps and completions.
There is no session mailbox-ownership exception and no delayed `LocalPushAdmitted` event. Pull reconciliation yields
only after a complete SQLite/model step, and merges again from live pending events when it resumes.
The runner's cursor and cancellation flag describe traversal, not a second copy of the model.

Shutdown closes admission immediately, finishes the accepted pull, then drains the rebuilt pending suffix.
A fatal completion can be handled between steps, unlike A's whole mailbox turn. It invalidates reconciliation so late
steps cannot apply. Rejection recovery likewise consults current state at pull completion, not a snapshot from pull start.

### Safe session reconciliation steps

See [the original safety fix](./0003-session-reconciliation-validation.md) and
[C's design and validation history](./0003-session-single-owner-experiment.md) for evidence and limits.

The earlier split let a local commit land inside an unfinished rebase. It repaired the pending propagation queue, but
an older materialization could still overwrite the newer durable head. Independent inserts hid the corresponding
row-order problem. The full-Store regression tests now cover both head equality and noncommutative updates.

The single owner retains the safety rule established by fixed A: **every reconciliation pause exposes a complete
upstream prefix plus all current optimistic events, with matching SQLite state, journal, and in-memory head**.

1. Validate the complete incoming payload before applying a prefix.
2. If a step needs a rebase, cancel the old push while leaving the old committed model intact. New local commits can
   still finish during this wait. Recompute the merge afterward; the pre-cancellation result is not a durable plan.
3. Normally apply up to 32 upstream events against the live pending suffix. An explicit leader rebase's first step
   must replace enough history to reach the old upstream head before yielding. Rollback, incoming materialization, pending replay,
   and the state head share one savepoint. Suppress automatic Effect scheduler yields during this synchronous work.
4. After success, install the matching model, release the owner, and deliver staged notifications. The runner refreshes
   affected tables. Subscriber callbacks may commit at this point. Yield before the next step so other fibers and browser input can run.
5. Only the final step discards the journal through the payload's confirmed `globalHead`. Rebuild propagation from the
   final pending suffix after reconciliation completes. No replacement push starts during reconciliation, and there
   are no delayed admission messages to repair.

This does not add another state-machine layer. `ClientSessionSyncProcessor` retains orchestration;
the existing SQLite, journal and state-head services retain storage responsibilities. Within Store, those services
use the cache-aware SQLite adapter so changeset/savepoint rollback cannot leave query results cached from old history.
Because journal rollback does not return affected table names, a rollback conservatively refreshes all user tables.

The production materializer, journal and head effects must remain synchronous, as required by `Store.commit` too.
`PreventSchedulerYield` suppresses automatic yielding; it does not make a genuinely asynchronous effect synchronous.
No network wait, cancellation, timer, or test barrier belongs inside the savepoint/model step.

Consequential behavior and limits:

- Subscribers may observe complete intermediate prefixes instead of only the end of a pull batch. They never need
  to interpret a planned head whose rows have not been applied yet.
- If a later step fails, earlier committed prefixes remain. The failing step rolls back and the processor fails.
  This is step atomicity, not whole-payload atomicity. Cleanup failures still mean the connection cannot be trusted.
- Admission is checked before encoding/materialization under the same owner. A local batch uses one outer savepoint:
  a materialization failure rolls back the whole batch without installing its model or scheduling propagation.
- A pending event rebased onto the end of one prefix can be confirmed by a matching event in the next prefix. The
  tests require confirmation rather than duplicating that event. Arbitrary batch partitions are not guaranteed to
  produce identical merge outcomes under the old whole-batch algorithm.
- A step bounds incoming event count, not elapsed time: replaying a large pending suffix, rolling back a large leader
  rebase, expensive materializers, and subscriber work can still cause a long task. There is no hard frame-time bound.
- Session SQLite/head consistency is not a promise that an unacknowledged local event survives a crash. Leader
  acknowledgement and cross-database crash atomicity retain their existing meanings and limits.

## Representative Call Stacks

### 1. A local Store commit reaches the backend

```text
Store.commit
  ├─ ClientSessionSyncProcessor.commit
  │    └─ dispatch(Commit) → commitLocalEvents
  │         ├─ check admission, encode, merge
  │         ├─ savepoint: materialize batch, journal and head
  │         ├─ install matching model; reserve propagation if allowed
  │         └─ release owner; publish and enqueue commands
  └─ Store refreshes local subscribers; return synchronously

session command runner
  └─ Push command → startLeaderPush
       └─ leaderThread.events.push
            └─ LeaderSyncProcessor.push
                 ├─ register acknowledgement
                 └─ mailbox: LocalPushRequested
                      ├─ validate and reserve the batch
                      └─ mailbox: ContinueWork
                           └─ processLocalBatch
                                ├─ SyncState.merge
                                ├─ LeaderSyncCommitter.commitLocal
                                │    ├─ materialize into state DB
                                │    ├─ update journal and heads
                                │    └─ coordinate SQLite commits
                                ├─ publish committed receipt to sessions
                                ├─ add committed events to provider push state
                                └─ resolve acknowledgement
                                     └─ session dispatch(PushSucceeded)

provider push fiber
  └─ syncBackend.push
       └─ mailbox: PushSucceeded or PushFailed
```

The leader acknowledgement means the batch is durable, published, and scheduled for backend propagation. It does not
mean the backend has already accepted it.

### 2. An upstream page reaches all sessions

```text
provider pull fiber
  └─ syncBackend.pull
       └─ mailbox: UpstreamBatchReceived
            └─ mailbox: ContinueWork
                 └─ processUpstreamBatch
                      ├─ SyncState.merge
                      ├─ LeaderSyncCommitter.commitUpstream
                      │    ├─ rollback divergent pending state when needed
                      │    ├─ materialize the replacement history
                      │    ├─ update journal, eventlog, state head
                      │    ├─ persist backend head with event inserts
                      │    └─ coordinate SQLite commits
                      ├─ publish committed receipt to session pull queues
                      ├─ release this provider page
                      └─ rebuild the provider push plan from committed pending

session leader-pull fiber
  └─ dispatch(PullReceived) → acceptPull
       ├─ validate full payload; register reconciliation identity
       └─ release owner; enqueue Reconcile command
            └─ reconcile (async runner)
                 ├─ dispatch(PullStep) → applyPullStep
                 │    ├─ check identity and lifecycle; merge live pending
                 │    ├─ cancellation needed? return without applying
                 │    └─ applyPullToSqlite → install model → release owner → notify
                 ├─ cancellation requested? wait outside owner, then retry from live state
                 ├─ applied? refresh subscribers, yield, repeat
                 └─ dispatch(PullFinished) → finishPull
                      └─ release reconciliation; rebuild/reserve propagation
```

The provider stream waits for the leader's page-completion signal. A later page therefore cannot race ahead of the
durable cursor for the current page.

### 3. A provider push fails and retries

```text
syncBackend.push fails
  └─ mailbox: PushFailed { operationId }
       ├─ stale operationId? ignore
       ├─ server ahead? push = awaiting-pull
       ├─ backend identity mismatch? apply configured policy
       └─ transient failure? push = retry-wait
            └─ retry timer
                 └─ mailbox: PushRetryElapsed { retryId }
                      ├─ stale retryId? ignore
                      └─ start a new in-flight push
```

### 4. The leader rejects a session push

```text
leaderThread.events.push rejects
  └─ session dispatch(PushRejected { operationId }) → completePush
       ├─ stale operationId? ignore
       ├─ corrective pull already recovered it?
       │    └─ rebuild from live pending and continue
       └─ otherwise
            └─ push = awaiting-reconciliation
                 └─ accepted pull continues or a later PullReceived arrives
                      ├─ advance or rebase in complete steps
                      └─ PullFinished → finishPull
                           ├─ check current rejection against live pending
                           └─ rebuild/resume propagation if recovered
```

## Why This Is Easier to Reason About

A maintainer can now answer the main coordination questions in one place:

- **What can happen now?** Read the tagged lifecycle, pull, and push states.
- **What inputs can change it?** Read the closed `Event` union.
- **In what order can changes happen?** Follow the leader mailbox, or session dispatch and the runner's safe yield points.
- **What makes a transition durable?** Follow the two methods on `LeaderSyncCommitter`.
- **Can an old completion corrupt current work?** No; operation identities reject stale results.
- **Can the leader publish or acknowledge before durability?** No; those actions follow a successful commit receipt.
  Session publication is optimistic and only promises a completed local SQLite/model transition.

The processors still contain domain policy, because keeping that policy together is the point. The session's short
transition router leads to named workflows in the same file; `applyPullToSqlite` keeps the savepoint together, and the
async functions show exactly where waiting and callbacks can interleave. Commands and staged notifications add
concepts, but they do not add another production module or a continuation-event framework.

## Guarantees and Deliberate Limits

| Guarantee                                                                    | Status                                                             |
| ---------------------------------------------------------------------------- | ------------------------------------------------------------------ |
| One owner changes each processor's model                                     | leader mailbox; session synchronous dispatch                       |
| Session inputs are processed in input-FIFO mailbox order                     | **not guaranteed or required**; dispatch is synchronous            |
| Local leader pushes are validated against committed plus reserved history    | guaranteed                                                         |
| Upstream pagination takes precedence over new local durable work             | guaranteed                                                         |
| Publication and acknowledgement happen only after a successful leader commit | guaranteed                                                         |
| Late provider or leader-push completions cannot advance newer work           | guarded by operation identities                                    |
| A session commit is immediately visible to that session                      | guaranteed by the synchronous owner before the call returns        |
| Session observers can reenter only after a completed transition              | owner released before staged notifications and subscriber refresh  |
| Session rows, journal and head agree at reconciliation yield points          | guaranteed for the synchronous SQLite/materializer implementations |
| A complete session pull payload is applied atomically                        | **not guaranteed**; complete prefixes may be visible               |
| Backend head and matching event inserts use the same eventlog transaction    | guaranteed                                                         |
| State DB and eventlog DB are crash-atomic together                           | **not guaranteed**                                                 |
| A leader acknowledgement means backend acceptance                            | **not guaranteed**; it means durable and scheduled                 |
| Browser input has a hard frame-time bound                                    | **not guaranteed**; a complete step can still be expensive         |

The state and eventlog databases use separate SQLite connections. The committer coordinates their normal success and
rollback paths, but it cannot make them atomic across a process crash. State is committed first so the eventlog does not
claim a transition that never reached materialized state. A crash or eventlog commit failure after the state commit can
still leave state ahead of eventlog truth and requires a separate recovery strategy.

## Reading Order

For a code review or architecture walkthrough, read the implementation in this order:

1. [ClientSessionSyncProcessor.ts](../../packages/@livestore/common/src/sync/ClientSessionSyncProcessor.ts):
   `transition` for the workflow map, then `dispatch` for ownership and staged notifications.
   Follow `commitLocalEvents` and `applyPullStep` for updates, `applyPullToSqlite` for persistence, and
   `reconcile` / `runCommand` for waits, callbacks and yields. `boot` only acquires and starts the runtime.
   [Store.commit](../../packages/@livestore/livestore/src/store/store.ts) calls the processor and retains local refresh.
2. `LeaderSyncProcessor.ts`: the system-wide orchestration model, event vocabulary, and mailbox handler.
3. `LeaderSyncCommitter.ts`: the durable transition implementation.
4. `ClientSessionSyncProcessor.test.ts`, `LeaderSyncProcessor.test.ts`, and `LeaderSyncCommitter.test.ts`: races,
   transition invariants, and SQLite-backed behaviour.
5. `ClientSessionReconciliation.test.ts`: real-Store cancellation, ordered replay, durable heads, failure cleanup,
   intermediate prefixes, rollback cache invalidation, and subscriber commits.

## Choice, Alternatives and Evidence

The fork currently chooses C's single session owner with yielding steps. This is a human architecture decision after
the readability refactor, not proof that fewer state writers always make code simpler. The leader mailbox and
`LeaderSyncCommitter` are unchanged. This decision does not update accepted `context/` intent or imply upstream adoption.

| Alternative          | Preserved branch / checkpoint                                          | Why keep it                                                                                                                     |
| -------------------- | ---------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Fixed split-owner A  | `codex/split-owner-a` at `4ead601cd`                   | Direct local path plus mailbox, with the same coherent-step safety rule. Fewer owner/runner concepts, but two writers to audit. |
| Whole-batch B        | `experiment/session-sync-latency` at `e2edfa693`                       | One synchronous owner without stepwise yielding. Large batches delay UI input. Its mailbox comparison predates A's safety fix.  |
| Fixed A/B comparison | `experiment/session-sync-fix`                                          | Keeps the later comparison against the whole-batch alternative.                                                                 |
| Effect Machine       | `refactor/effect-machine-processors` and `codex/effect-machine-spike`  | Separate framework-based explorations, not dependencies of this design.                                                         |
| Preferred C          | `refactor/serialized-sync-processors`, implementation checkpoint `66ca5d3e0` | One synchronous owner, coherent-step yielding, named workflows and separate async execution in one file.                        |

The Effect Machine review refinements are checkpointed at `db978b595`; its earlier bounded spike is preserved at
`18c0273a1`. The fixed A/B comparison is preserved at `53a8cea42`.

Preserving a branch does not require keeping its worktree. `refactor/serialized-sync-processors` is the canonical
fork refactor branch and now follows C, including the fixed-A safety checkpoint. The original C experiment remains
preserved on `experiment/session-sync-owner`; fixed A remains on `codex/split-owner-a`.

The saved browser measurements compare **fixed A with C, not main**. All 130 samples passed checked invariants and
runtime checks; input delay and catch-up were closely comparable. They provide no measured performance win over main
and no hard latency bound. See [the measurement interpretation](../../tests/perf/session-sync/DECISION.md).
The readability refactor was separately rerun against the same baseline with 130 passing samples; original generated
results were preserved.

Validation at the refactored implementation checkpoint: root unit suite 129 passed / 1 skipped, focused session tests
45 passed, Common and LiveStore suites 352 passed / 1 skipped, TypeScript build and perf-fixture typecheck passed,
and full lint passed. [The companion](./0003-session-single-owner-experiment.md) records the review findings, fixes,
and the remaining notification-stage and scheduling trade-offs.
