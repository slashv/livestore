# Serialized Sync Processors

> **Status:** Preferred direction for this fork as of September 17, 2026, not an accepted upstream RFC or product
> contract. The session uses approach C: one synchronous owner with yielding reconciliation, including the readability
> refactor at `66ca5d3e0` and the explicit-state follow-up of September 24, 2026 (tagged lifecycle, a `cancelling` push
> state, enforced synchronous owner). The leader architecture is unchanged. Alternatives remain preserved on named
> branches.

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
The session uses a synchronous owner: local commits, pull steps and network results all run through `owned`, which
finishes each state change synchronously and fails the session loudly if one ever suspends. An asynchronous runner handles
waiting and applies incoming history one complete SQLite/model step at a time, yielding between steps.

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

| Previous coordination mechanism                   | What it was encoding                                          | What owns that information now                                    |
| ------------------------------------------------- | ------------------------------------------------------------- | ----------------------------------------------------------------- |
| `leaderPushQueue`                                 | pending events and whether propagation was active             | the tagged `model.push` state                                     |
| `pullReconciliationMutex`                         | exclusion between pull, rejection recovery, and shutdown      | the synchronous owner, reconciliation identity and lifecycle tags |
| `unresolvedRejection`                             | whether propagation was waiting for corrective leader history | `push: awaiting-reconciliation`                                   |
| `terminalPushCause`                               | whether the background worker had failed                      | `lifecycle: failed { cause }`                                     |
| a permanent push-drain worker                     | batching, propagation, and parking after rejection            | one finite push operation at a time                               |
| `Effect.never` in that worker                     | a rejection fence waiting for corrective history              | the explicit `awaiting-reconciliation` state                      |
| clearing and restarting that worker during rebase | invalidating an old push plan                                 | `push: cancelling`, `PushCancelled`, and rebuilding from pending  |

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

### Session: synchronous changes, asynchronous waiting

**Every change to the session model completes synchronously inside the owner. Work that must wait happens outside
that owner, then enters it again using the current state.** Local commits and incoming history use the same owner, so
neither can interrupt the other's unfinished SQLite/model work.

The file uses five terms:

- **Owner**: `owned(body)`, the only code allowed to write `model`. It is not a thread, a mailbox, or a lock held
  throughout a pull. Exclusion relies on the body being synchronous. A synchronous body always finishes before any
  microtask runs, so `owned` schedules one: if it still finds the body holding the owner, the body suspended. That
  body then fails the session with a named defect, and callers that meet the held owner get the same defect.
  Running the body on a separate synchronous fiber (`Effect.runSyncExitWith`) would catch this earlier, but it
  measurably slowed a 10,000-event commit in the perf suite, so it was rejected.
- **Event**: an input that changes the model without returning a result, entered through `dispatch(event)`.
  `commit` and one pull step are the two owner calls that do return a result (`writeTables`, or a `StepResult`).
- **Command**: asynchronous work the owner stages for the runner (`Push`, `Reconcile`, shutdown steps).
- **Notification**: a staged Queue offer or Deferred completion delivered after the owner is released.
- **Runner**: the Effect code that consumes commands, manages asynchronous operations and advances reconciliation.
  It has no second model to write; its only loop state is the offset into the pull being applied.

This is close to an Elm-style `update` returning `(model, commands)`, except the owner stages commands imperatively
with `stage(...)` instead of returning them. Updating in-memory sync state means assigning the new event heads,
pending local events and relevant push state after the SQLite changes succeed.

Read the following views separately. Boxes marked OWNER contain synchronous state changes; waits and callbacks
are outside those boxes. Time runs downward in the sequence view. Its lanes represent responsibilities in the
client, not separate operating-system threads.

#### 1. What does Store.commit wait for?

```text
                     Store.commit(events)
                              |
                              v
       +---------------------------------------------+
       | OWNER: dispatch(Commit)                     |
       |                                             |
       | Check admission; encode; merge              |
       | Apply local batch in savepoint              |
       | Update in-memory sync state to match SQLite |
       | Stage notifications / commands              |
       +---------------------------------------------+
                              |
                        release owner
                              |
                   deliver notifications
                              |
             +----------------+ - - - - - - - - - - +
             |                                       : eligible Push
             v                                       v
    Store refreshes subscribers             Runner starts leader push
    (callbacks may commit again)                     :
             |                                       : async result
             v                                       v
    Store.commit returns                    New dispatch handles result
    LOCAL CHANGE APPLIED                    (operation identity checked)

    No network acknowledgement awaited by Store.commit.
    After owner release, the two paths have no fixed ordering.
```

The solid path is the local call: `Store.commit` calls `dispatch(Commit)`, which checks admission, applies the local
batch, updates in-memory sync state to match SQLite, and stages notifications. After releasing the owner, dispatch
delivers those notifications. Store then refreshes local subscribers and returns. The local change does not wait for a leader or
backend acknowledgement. This diagram shows the successful path; a local materialization failure rolls back the
batch without updating in-memory sync state or scheduling its propagation.

The dashed branch shows eligible propagation. The owner reserves a push before releasing its command to the runner.
The runner starts the leader call; its success, rejection, or failure enters a new dispatch. Operation identities
prevent an obsolete command or result from advancing newer work. A push can be deferred, for example while
reconciliation is active; committing locally still completes immediately.

**Owner release is earlier than dispatch return.** Offering an Effect Queue or completing a Deferred resumes the
waiting fiber inline: Effect calls `fiber.evaluate` in the caller's stack. The resumed fiber uses its own scheduler
settings, so the owner's `PreventSchedulerYield` does not carry over. The two branches therefore have no fixed ordering
after release: a push can start before `Store.commit` returns, but the commit does not await its network result.
Staging notifications ensures any resumed caller sees finished state.

This has a consequence for Store: code resumed by a notification, such as a `syncState` subscriber or the runner's
next command, can run **inside** `Store.commit`, before that commit refreshes its own tables. If that code commits
again, the nested commit refreshes its tables first. Model state stays coherent; only the order of reactive refreshes
can be nested. Store's subscriber callbacks also run after owner release and can make another commit.

#### 2. Where can a local commit run during a pull?

```text
  App / subscriber              Shared dispatch owner               Async runner
          |                               |                               |
          |                               |<---------- PullStep ----------|
          |                  +------------+------------+                  |
          |                  | OWNER                   |                  |
          |                  | Merge live pending      |                  |
          |                  | Finish SQLite changes   |                  |
          |                  | Update in-memory state  |                  |
          |                  | to match SQLite         |                  |
          |                  +------------+------------+                  |
          |                               |                               |
          |                               |--- release; notify; return -->|
          |                               |                        refresh / yield
          |                               |                               |
          |------- Store.commit(L) ------>|                               |
          |                  +------------+------------+                  |
          |                  | SAME OWNER              |                  |
          |                  | Apply L locally         |                  |
          |                  | Update pending state    |                  |
          |                  +------------+------------+                  |
          |                               |                               |
          |<-- release; notify; return ---|                               |
  Store refreshes;                        |                               |
   commit returns                         |                               |
          |                               |                               |
          |                               |<------- next PullStep --------|
          |                  +------------+------------+                  |
          |                  | SAME OWNER              |                  |
          |                  | Merge CURRENT pending   |                  |
          |                  | (now accounting for L)  |                  |
          |                  | Finish SQLite changes   |                  |
          |                  | Update in-memory state  |                  |
          |                  | to match SQLite         |                  |
          |                  +------------+------------+                  |
          |                               |                               |
          |                               |--- release; notify; return -->|
          |                               |                        refresh / yield
          |                               |                               |

  One possible interleaving: L completes between two complete pull steps.
  No wait, yield or subscriber callback occurs inside an OWNER box.
```

This is one possible successful interleaving after a pull has been accepted. Each owner box represents
a complete synchronous SQLite/model step. After that step, the runner refreshes subscribers and yields before the
next step. A subscriber can commit during refresh; a browser handler may run when scheduling gives it an opportunity.
The sequence shows a commit between steps, without promising that one occurs at every yield. Notifications can also
resume other fibers immediately after owner release, before the step returns to the runner.

The next step **merges again from live pending events**. It cannot reuse a plan made before the local commit. A
concrete example, assuming neither incoming prefix confirms local event L:

| Completed transition                                                 | Materialized counter | Still-pending local work |
| -------------------------------------------------------------------- | -------------------- | ------------------------ |
| First incoming prefix leaves the counter at 1                        | 1                    | none                     |
| Local L increments the counter by 10                                 | 11                   | L: increment by 10       |
| Next incoming prefix sets the counter to 2; reconciliation replays L | 12                   | L: increment by 10       |

At every pause, rows, journal and head describe a complete incoming prefix plus the current optimistic events.
The reader can see intermediate states such as 11 and 12; it must never see a new head paired with unfinished rows.
If incoming history confirms L, the merge removes it from pending instead of applying it twice.

Only the last step discards the journal through the payload's confirmed global head. After the last refresh,
`dispatch(PullFinished)` finishes reconciliation and resumes eligible propagation using current pending state.
There is no yield required after the final step. A complete step or its subscriber work can still be expensive:
this design provides interleaving opportunities, not a hard browser frame-time bound.

#### 3. What if applying a step first requires asynchronous cancellation?

```text
    Runner runs a pull step in the owner
                   |
                   v
    +--------------------------------------------+
    | OWNER: merge detects cancellation needed   |
    | push: in-flight -> cancelling              |
    | Return cancel-push; nothing else changes   |
    +--------------------------------------------+
                   |
             release owner
                   |
    Runner interrupts old push and waits
                   :
                   :     Store.commit(L) may run during this wait
                   :     +--------------------------------------+
                   :     | SAME OWNER: apply L; queue behind    |
                   :     | the cancelling push                  |
                   :     +--------------------------------------+
                   :     Release; notify; refresh; commit returns
                   :
                   :     A late success or rejection of the
                   :     cancelling push is ignored; a fatal
                   :     failure still fails the session.
                   :
    Cancellation finishes
                   |
    dispatch(PushCancelled): cancelling -> idle
                   |
    Retry the SAME incoming prefix
                   |
                   v
    +--------------------------------------------+
    | OWNER: recompute merge from LIVE state     |
    |                                            |
    | One savepoint:                             |
    |   rollback -> materialize -> replay -> head|
    | Update in-memory sync state to match       |
    | SQLite (including L)                       |
    +--------------------------------------------+
                   |
             release owner
                   |
             notify / refresh

    The wait holds neither the owner nor a reconciliation savepoint.
    The merge computed before the wait is never reused as a write plan.
```

This view shows the rebase path with a local commit during the cancellation wait. The first attempt detects that
cancellation is needed, records `push: cancelling`, and returns `cancel-push` **before changing SQLite or sync
state**. The cancellation is therefore visible in the model, not only in the runner. The runner then interrupts the
old push outside the owner and reports `PushCancelled` for that operation. No reconciliation savepoint spans that wait, so a local commit can finish
against the previous coherent state.

Once cancellation finishes, the runner retries the same incoming prefix. The owner recomputes its merge, including
any local events committed during the wait, then applies rollback, incoming materialization, pending replay and head
updates together in one savepoint. It then updates in-memory sync state to match SQLite before releasing
notifications. If that SQLite step fails, its savepoint rolls back; earlier completed steps remain. A stopped or failed reconciliation is rejected
by the lifecycle/identity guard instead of applying a late step.

**The safety rule is the same in all three views:** finish the SQLite changes and update in-memory sync state to
match before permitting another caller to observe or change session state. Waiting, notifications and subscriber callbacks belong outside that
boundary. The command queue schedules work; sequence numbers and `SyncState.merge` determine valid event history.

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

Every field that affects a later decision is part of `Model`. Lifecycle and push are tagged unions, so combinations
such as "running with a terminal cause" cannot be constructed.

```text
lifecycle:
  starting ─────────► running { reconciliation? }
     │                    │
     └────────────────────┴──► shutdown-requested { exit, reconciliation? } ──► stopping { exit } ──► stopped
                                                                                                       ▲
  starting / running / shutdown-requested / stopping ──► failed { cause, shutdownExit? } ─────────────┘

push:
  idle { queued }                          ──► in-flight                 reserveNextPush
  in-flight { operationId, batch, queued } ──► idle                      PushSucceeded, or a recovered PushRejected
  in-flight                                ──► awaiting-reconciliation   PushRejected
  in-flight                                ──► cancelling                a pull step needs a rebase
  cancelling { operationId, batch, queued } ─► idle                      PushCancelled (batch requeued)
  awaiting-reconciliation { error, ... }   ──► idle                      rebase step, or PullFinished sees recovery
  any                                      ──► idle                      applied rebase step (queue = live pending)

reconciliation (inside running / shutdown-requested):
  undefined ──► { id, rebased } ──► complete step ──► yield ──► next step ──► undefined
```

Transitions that change `lifecycle` or `push`, and what each stages for the runner:

| Input               | From                            | To                                               | Staged                                           |
| ------------------- | ------------------------------- | ------------------------------------------------ | ------------------------------------------------ |
| `Started`           | starting                        | running                                          |                                                  |
| `commit`            | running (else defect)           | push `queued` grows (unless awaiting)            | sync-state update; `Push` if idle                |
| `PullReceived`      | running                         | reconciliation `{ id, rebased: false }`          | `Reconcile`                                      |
| pull step, rebase   | push in-flight                  | push cancelling                                  | (returns `cancel-push`)                          |
| `PushCancelled`     | push cancelling, same operation | push idle, batch requeued                        |                                                  |
| pull step, applied  | reconciliation active           | sync state; rebase: push idle, `rebased: true`   | sync-state update                                |
| `PullFinished`      | reconciliation active           | reconciliation cleared; push rebuilt if needed   | `Push` if eligible                               |
| `PushSucceeded`     | push in-flight, same operation  | push idle                                        | next `Push`, or `FinishShutdown` if done         |
| `PushRejected`      | push in-flight, same operation  | push idle (recovered) or awaiting-reconciliation | next `Push`; `FinishShutdown` if stopping        |
| `PushFailed`        | push in-flight or cancelling    | failed                                           | `NotifyFailure` or `FinishShutdown`              |
| `ShutdownRequested` | starting / running              | shutdown-requested                               | `BeginShutdown`                                  |
| `ShutdownRequested` | failed, not yet requested       | failed with `shutdownExit`                       | `FinishShutdown`                                 |
| `DrainStarted`      | shutdown-requested              | stopping                                         | next `Push` or `FinishShutdown`                  |
| `DrainStarted`      | failed                          | (unchanged)                                      | `FinishShutdown`                                 |
| `Failed`            | any live state                  | failed                                           | `NotifyFailure`, or `FinishShutdown` if stopping |
| `Stopped`           | any                             | stopped                                          | shutdown result                                  |

A failed session never re-enters `stopping`: nothing is drained after a fatal error, and push completions are ignored.

`Store.commit` calls `processor.commit(events)`, which enters the same owner used by pull steps and completions.
There is no session mailbox-ownership exception and no delayed `LocalPushAdmitted` event. Pull reconciliation yields
only after a complete SQLite/model step, and merges again from live pending events when it resumes. The runner handles
one command at a time, so `BeginShutdown`, `FinishShutdown` and `NotifyFailure` wait behind an active `Reconcile`,
including its cancellation wait and yields. A failure still takes effect at the next step, because it clears the
reconciliation and every later step is then obsolete.

Shutdown closes admission immediately, finishes the accepted pull, then drains the rebuilt pending suffix.
A fatal completion can be handled between steps, unlike A's whole mailbox turn. It invalidates reconciliation so late
steps cannot apply. Rejection recovery likewise consults current state at pull completion, not a snapshot from pull start.

In development builds the owner also checks, after every successful body, that a cancelling push only exists during
reconciliation and that the push queue is the unpushed suffix of pending events outside reconciliation.

### Safe session reconciliation steps

See [the original safety fix](./0005-session-reconciliation-validation.md) and
[C's design and validation history](./0005-session-single-owner-experiment.md) for evidence and limits.

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
4. After success, update in-memory sync state to match SQLite, release the owner, and deliver staged notifications.
   The runner refreshes affected tables. Subscriber callbacks may commit at this point. Yield before the next step so other fibers and browser input can run.
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
  a materialization failure rolls back the whole batch without updating in-memory sync state or scheduling propagation.
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
  │    └─ owned(commitLocalEvents)
  │         ├─ check admission, encode, merge
  │         ├─ savepoint: materialize batch, journal and head
  │         ├─ update in-memory sync state to match SQLite
  │         ├─ reserve propagation if allowed
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
                 ├─ owned(applyPullStep)
                 │    ├─ check identity and lifecycle; merge live pending
                 │    ├─ cancellation needed? push = cancelling; return without applying
                 │    ├─ applyPullToSqlite; update in-memory sync state to match SQLite
                 │    └─ release owner → notify
                 ├─ cancellation requested? wait outside owner, dispatch(PushCancelled), retry from live state
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
- **What inputs can change it?** Read the closed `Event` union; in the session, add the two owner calls that return
  results (`commit` and a pull step).
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
| Upstream commit receipts match the merged local head                         | by DAG position; the rebase generation is local                    |
| A session commit is immediately visible to that session                      | guaranteed by the synchronous owner before the call returns        |
| Session observers can reenter only after a completed transition              | owner released before staged notifications and subscriber refresh  |
| Session owner work cannot suspend while holding the owner                    | detected; a suspending body fails the session with a named defect  |
| Store refreshes a commit's tables before any code it resumes runs            | **not guaranteed**; resumed code can refresh (and commit) first    |
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

The backend confirms events without the leader's local rebase generation. When the leader rebases a pending event
before the backend accepts it (for example `e1` becomes `e2` with generation 1), the eventlog row and `StateHead` keep
generation 1 while the merged sync state adopts the backend's `e2`. The leader therefore checks upstream commit receipts
by global and client position, the same way the committer matches confirmed events. Which of the two orderings occurs
depends on fiber scheduling: Effect 4.0.0-rc.113 lets a local push reach the leader before concurrently forked backend
events, where beta.99 happened to order them the other way.

## Reading Order

For a code review or architecture walkthrough, read the implementation in this order:

1. [ClientSessionSyncProcessor.ts](../../packages/@livestore/common/src/sync/ClientSessionSyncProcessor.ts):
   the `Model`, `Lifecycle` and `LeaderPushState` types at the bottom, then `transition` for the event map and `owned`
   for ownership and staged notifications.
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

| Alternative          | Preserved branch / checkpoint                                                | Why keep it                                                                                                                     |
| -------------------- | ---------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------- |
| Fixed split-owner A  | `codex/split-owner-a` at `4ead601cd`                                         | Direct local path plus mailbox, with the same coherent-step safety rule. Fewer owner/runner concepts, but two writers to audit. |
| Whole-batch B        | `experiment/session-sync-latency` at `e2edfa693`                             | One synchronous owner without stepwise yielding. Large batches delay UI input. Its mailbox comparison predates A's safety fix.  |
| Fixed A/B comparison | `experiment/session-sync-fix`                                                | Keeps the later comparison against the whole-batch alternative.                                                                 |
| Effect Machine       | `refactor/effect-machine-processors` and `codex/effect-machine-spike`        | Separate framework-based explorations, not dependencies of this design.                                                         |
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
and full lint passed. After the explicit-state follow-up: root unit suite 131 passed / 1 skipped, focused session tests
47 passed (new: suspending materializer, rejection during push cancellation), Common and LiveStore suites 352 passed /
1 skipped. [The companion](./0005-session-single-owner-experiment.md) records the review findings, fixes,
and the remaining notification-stage and scheduling trade-offs.
