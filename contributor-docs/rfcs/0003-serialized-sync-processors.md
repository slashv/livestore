# Serialized Sync Processors

> **Status:** Draft local architectural proposal. This describes the current experiment and does not replace accepted
> product intent.

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

Now, asynchronous inputs become named events and enter one mailbox per processor. One loop handles those events in
order and is the sole owner of an explicit model. The leader delegates durable SQLite work to `LeaderSyncCommitter` and
only publishes or acknowledges after that work succeeds.

This is not a framework-driven or fully pure state machine. It is a deliberately small, state-machine-shaped loop that
makes ordering and the important states visible in the processors we already have.

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

| Previous coordination mechanism                   | What it was encoding                                          | What owns that information now                               |
| ------------------------------------------------- | ------------------------------------------------------------- | ------------------------------------------------------------ |
| `leaderPushQueue`                                 | pending events and whether propagation was active             | the tagged `model.push` state                                |
| `pullReconciliationMutex`                         | exclusion between pull, rejection recovery, and shutdown      | mailbox ordering                                             |
| `unresolvedRejection`                             | whether propagation was waiting for corrective leader history | `push: awaiting-reconciliation`                              |
| `terminalPushCause`                               | whether the background worker had failed                      | `model.lifecycle` and `model.terminalCause`                  |
| a permanent push-drain worker                     | batching, propagation, and parking after rejection            | one finite push operation at a time                          |
| `Effect.never` in that worker                     | a rejection fence waiting for corrective history              | the explicit `awaiting-reconciliation` state                 |
| clearing and restarting that worker during rebase | invalidating an old push plan                                 | operation identities and rebuilding from live pending events |

Some runtime machinery remains, but with narrower jobs:

- the mailbox queue carries events;
- fiber handles own the lifetime of actual concurrent provider or leader calls;
- deferred values let callers await an acknowledgement or let a pull stream apply backpressure;
- a small shutdown guard keeps the public shutdown operation idempotent.

These are adapters around the model. They no longer compete with it as sources of orchestration truth.

## The New Shape

```text
┌────────────────────────── one client session ──────────────────────────┐
│                                                                        │
│  Store.commit                                                          │
│      │                                                                 │
│      ▼                                                                 │
│  ClientSessionSyncProcessor                                            │
│    ├─ synchronous local lane: encode → materialize → update local state│
│    └─ asynchronous mailbox: push results, leader pulls, shutdown       │
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

| Module                       | Owns                                                                                                          | Does not own                                                 |
| ---------------------------- | ------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------ |
| `ClientSessionSyncProcessor` | optimistic session state, leader propagation, pull reconciliation, table refresh, session shutdown            | leader durability or backend retries                         |
| `LeaderSyncProcessor`        | event ordering, in-memory sync state, session publication, acknowledgements, provider work, retries, shutdown | SQLite transition details                                    |
| `LeaderSyncCommitter`        | materialization, rollback, journal maintenance, heads, eventlog writes, coordinated SQLite commits            | queues, publication, retries, acknowledgements, or lifecycle |

## The Common Event-Loop Pattern

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
```

The session keeps one intentional exception to mailbox ownership: `Store.commit` must materialize and update local
state synchronously so a UI read immediately after the commit sees the new value. The resulting propagation request is
then sent through the mailbox. Pull reconciliation re-reads the live pending suffix because a synchronous commit may
arrive while an asynchronous rebase is in progress.

## Representative Call Stacks

### 1. A local Store commit reaches the backend

```text
Store.commit
  ├─ ClientSessionSyncProcessor.encodeEvents
  ├─ ClientSessionSyncProcessor.materializeEvents
  └─ ClientSessionSyncProcessor.push
       ├─ merge into optimistic session SyncState
       ├─ publish session SyncState
       └─ mailbox: LocalPushAdmitted
            └─ startLeaderPush
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
                                               └─ session receives LeaderPushSucceeded

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
  └─ mailbox: PullItemReceived
       └─ handlePullItem
            ├─ merge advance or rebase
            ├─ rollback/materialize session state
            ├─ refresh affected tables
            ├─ publish live session SyncState
            └─ resume leader propagation when reconciliation is complete
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
  └─ session mailbox: LeaderPushRejected { operationId }
       ├─ stale operationId? ignore
       ├─ corrective pull already recovered it?
       │    └─ rebuild from live pending and continue
       └─ otherwise
            └─ push = awaiting-reconciliation
                 └─ later PullItemReceived
                      ├─ advance or rebase session state
                      ├─ rebuild from live pending
                      └─ resume leader propagation
```

## Why This Is Easier to Reason About

A maintainer can now answer the main coordination questions in one place:

- **What can happen now?** Read the tagged lifecycle, pull, and push states.
- **What inputs can change it?** Read the closed `Event` union.
- **In what order can changes happen?** Follow the one mailbox loop.
- **What makes a transition durable?** Follow the two methods on `LeaderSyncCommitter`.
- **Can an old completion corrupt current work?** No; operation identities reject stale results.
- **Can we publish or acknowledge before durability?** No; those actions follow a successful commit receipt.

The processor still contains domain policy, because keeping that policy together is the point. Extracting every step
into plans, commands, and completion types would add more hops without creating more leverage.

## Guarantees and Deliberate Limits

| Guarantee                                                                    | Status                                             |
| ---------------------------------------------------------------------------- | -------------------------------------------------- |
| One owner orders asynchronous transitions in each processor                  | guaranteed by the mailbox loop                     |
| Local leader pushes are validated against committed plus reserved history    | guaranteed                                         |
| Upstream pagination takes precedence over new local durable work             | guaranteed                                         |
| Publication and acknowledgement happen only after a successful leader commit | guaranteed                                         |
| Late provider or leader-push completions cannot advance newer work           | guarded by operation identities                    |
| A session commit is immediately visible to that session                      | guaranteed by the synchronous local lane           |
| Backend head and matching event inserts use the same eventlog transaction    | guaranteed                                         |
| State DB and eventlog DB are crash-atomic together                           | **not guaranteed**                                 |
| A leader acknowledgement means backend acceptance                            | **not guaranteed**; it means durable and scheduled |

The state and eventlog databases use separate SQLite connections. The committer coordinates their normal success and
rollback paths, but it cannot make them atomic across a process crash. State is committed first so the eventlog does not
claim a transition that never reached materialized state. A crash or eventlog commit failure after the state commit can
still leave state ahead of eventlog truth and requires a separate recovery strategy.

## Reading Order

For a code review or architecture walkthrough, read the implementation in this order:

1. `ClientSessionSyncProcessor.ts`: the Store-facing optimistic lane and session mailbox.
2. `LeaderSyncProcessor.ts`: the system-wide orchestration model, event vocabulary, and mailbox handler.
3. `LeaderSyncCommitter.ts`: the durable transition implementation.
4. `ClientSessionSyncProcessor.test.ts`, `LeaderSyncProcessor.test.ts`, and `LeaderSyncCommitter.test.ts`: races,
   transition invariants, and SQLite-backed behaviour.
