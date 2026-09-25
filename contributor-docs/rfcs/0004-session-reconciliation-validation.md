# Session reconciliation: safety and responsiveness

Status: historical validation of fixed split-owner A at `4ead601cd`, not accepted upstream intent. The fork now prefers
C, which retains this safety fix and its regressions under a single synchronous owner.
See [the current architecture RFC](./0004-serialized-sync-processors.md#safe-session-reconciliation-steps) and
[the C comparison](./0004-session-single-owner-experiment.md). The A/B measurements below are distinct from the later
fixed-A/C measurements, and neither is a comparison against main.

## What changed

The old split installed a planned model before finishing its SQLite work. A synchronous local commit could run during
cancellation or materialization, then have its durable head overwritten when the older reconciliation resumed. Merely
repairing the propagation queue did not repair the database or the ordering of noncommutative materializers.

The fix keeps synchronous `Store.commit` and the existing mailbox. It changes where reconciliation may pause:

```text
wait for old push cancellation, keeping the old coherent state
  → merge again from live pending events
  → savepoint: rollback + materialize incoming + replay pending + write head
  → install matching model and notify subscribers
  → yield, then repeat from live state
```

Ordinary steps contain at most 32 incoming events. An explicit leader rebase's first step may be larger so it reaches
the previously observed upstream head. Only the final step discards the journal through the confirmed global head.
There is no new orchestration module or framework. The production processor remains approximately its original size;
most added lines are regression tests and generated tracing snapshots.

| Invariant                                                                              | Evidence or limit                                                                                             |
| -------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------- |
| SQLite rows/head and the published model agree at yield points                         | Full-Store tests inspect both during cancellation, between prefixes, and after failure.                       |
| Local commits remain immediately readable                                              | Existing Store behavior plus subscriber-initiated commits and browser checks.                                 |
| Incoming work cannot overwrite an intervening local commit                             | Each step merges live pending events; tests use same-row updates and noncommutative arithmetic.               |
| A failing step is not partly published                                                 | Real SQLite failures in the first and second steps roll back that step. Earlier completed prefixes remain.    |
| Rebase invalidates stale cached reads                                                  | Store role services use the cache-aware SQLite wrapper; changeset and savepoint rollback invalidate caches.   |
| Delayed admissions do not resend obsolete event encodings                              | Mailbox admissions are filtered against canonical live pending events.                                        |
| Terminal cleanup cannot admit a new Store materialization                              | Admission is checked before encoding/materialization, not only in `push`.                                     |
| Whole-payload atomicity                                                                | **Not guaranteed.** Subscribers may observe coherent intermediate prefixes.                                   |
| Hard frame-time bound                                                                  | **Not guaranteed.** Pending replay, large explicit rebases, materializers and subscribers can exceed a frame. |
| Crash durability of an unacknowledged session commit or cross-database crash atomicity | **Not added.** Existing leader acknowledgement/storage limitations remain.                                    |

The savepoint/model step assumes synchronous materializers and SQLite services, as `Store.commit` already does.
Suppressing Effect scheduler yields is not a substitute for that contract: genuinely asynchronous implementations
would require another design. Cancellation, network waits and test barriers remain outside the step.

## Correctness tests and review

- [Full-Store regressions](../../tests/package-common/src/client-session/ClientSessionReconciliation.test.ts): ten tests,
  including local commits during held cancellation, noncommutative replay with aggressive scheduler yielding,
  subscriber commits, first/later-step materialization failures, admission during failure cleanup, an explicit
  64-to-96-event rollback, future materializer-hash collisions, and pending confirmation in a later prefix.
- [Existing processor tests](../../tests/package-common/src/client-session/ClientSessionSyncProcessor.test.ts): retained
  cancellation, rejection, shutdown and admission coverage. The unknown-event case now uses real SQLite.
- [SQLite wrapper tests](../../packages/@livestore/livestore/src/SqliteDbWrapper.test.ts) and
  [query-cache tests](../../packages/@livestore/livestore/src/QueryCache.test.ts): rollback invalidation and transaction SQL.

Read-only review and follow-up inspection found and fixed stale admission encodings, hashes incorrectly borrowed from
future incoming events, missing rollback-only table refreshes, cache invalidation bypasses, late failure admission,
and the minimum first-prefix size for an explicit upstream rebase. Review did not add another ownership abstraction.

One proposed extension to guard the first prefix by the local head's rebase generation was rejected after inspecting
the sequence API and testing its premise: the head ordering helpers deliberately ignore generation. This change keeps
the existing upstream-head invariant rather than introducing an unsupported generation-monotonicity contract.

The 18 updated query tracing snapshots include additional journal/head/savepoint SQL through the cache-aware adapter.
Generated savepoint IDs are normalized. A read-only run on the original experiment also reproduced nine already-stale
trace snapshots; functional assertions were not weakened to accommodate either source of trace changes.

Validation on the implementation worktree:

| Command                                                                                                                   | Result                          |
| ------------------------------------------------------------------------------------------------------------------------- | ------------------------------- |
| `pnpm exec vitest run tests/package-common/src/client-session/ClientSessionReconciliation.test.ts --testNamePattern '.*'` | 10 passed                       |
| `pnpm run test:unit`                                                                                                      | 125 passed, 1 skipped, 16 files |
| `pnpm exec vitest run --project @livestore/common`                                                                        | 271 passed, 20 files            |
| `pnpm exec vitest run --project @livestore/livestore`                                                                     | 81 passed, 1 skipped, 10 files  |
| `pnpm run ts:build`                                                                                                       | Passed                          |
| `pnpm run lint:full:fix`                                                                                                  | Passed                          |
| `git diff --check`                                                                                                        | Passed                          |

The root unit script selected package-common files in this environment, so the two affected package projects were
also run explicitly. An earlier unit run timed out in an unrelated stream-events property test during concurrent
validation load; its rerun without that contention passed. No unrelated production fix was made.

## Browser measurements

The alternative remains isolated. The original failing baseline is preserved on `experiment/session-sync-latency`
at `e2edfa693`. A separate `experiment/session-sync-fix` branch measures the fixed processor against the unchanged
synchronous alternative. The final measured source is `0cd5b18252fb668eeea05df68fe431c2f16ced89`; its production processor
matches this implementation. Raw samples and the generated table live in that branch's `tests/perf/session-sync/`.

Commands: `pnpm --dir tests/perf exec playwright test --config session-sync/playwright.config.ts`, then
`node tests/perf/session-sync/report.mjs`. The run completed successfully with 130 measured samples after 26 warmups,
five alternating A/B pairs for each of 13 workloads. Chromium 147, Apple M1 Pro, September 6, 2026.

**Both variants had zero row, pending, durable-head, immediate-read, propagation, or partial-state observation failures.**
There were no runtime/time-out failures. Input ran during reconciliation in all 60 non-idle fixed-mailbox samples and
none of the 60 synchronous samples. The original unsafe mailbox baseline had 60 durable-head failures; its timings
are not an equally correct alternative and were collected in a different run.

Representative workload medians, milliseconds:

| Workload                                  | Fixed mailbox input delay | Synchronous input delay | Fixed mailbox catch-up | Synchronous catch-up |
| ----------------------------------------- | ------------------------: | ----------------------: | ---------------------: | -------------------: |
| Advance 100, 1 write/event                |                       7.8 |                    35.8 |                   51.3 |                 40.5 |
| Rebase 100, 5 writes/event, 10 pending    |                      15.5 |                    46.3 |                   81.6 |                 50.7 |
| Advance 1,000, 5 writes/event             |                      16.1 |                   433.1 |                  718.7 |                437.7 |
| Rebase 1,000, 5 writes/event, 100 pending |                      63.4 |                   482.2 |                2,101.6 |                486.7 |

This preserves opportunities to interact without exposing unfinished state, **not the exact latency or throughput of
the unsafe baseline**. Replaying a large pending suffix every step is expensive: heavy rebases took 1.7–2.2 seconds
and had maximum-frame-gap medians of roughly 53–72 ms. The fix is a correctness-first boundary, not a completed
frame-budget optimization. The explicit upstream-rebase protocol has a correctness regression but is not a separate
browser workload here; the benchmark's rebase workload is a conflicting upstream advance.

These are synthetic timer-driven DOM inputs with real Store commits, subscriptions and browser SQLite. Deadline-to-RAF
is not INP or display scanout. Leader transport is controlled; OPFS, workers, multi-tab contention, remote networks and
telemetry exporters are not measured. Five samples per workload on one machine do not establish tail-latency bounds.

## Local checklist

- [x] Fix the current implementation without adopting the synchronous alternative.
- [x] Test head safety and row ordering at actual full-Store interleavings.
- [x] Preserve the original benchmark and rerun the same harness against the fix.
- [x] Record scheduling, failure and performance trade-offs in the draft RFC.
- [ ] Assess a follow-up optimization for the measured large-pending cost. It remains a limitation of preferred C too,
      not an undecided fork-adoption gate.

Related existing issue: [#1465](https://github.com/livestorejs/livestore/issues/1465). No push or PR is part of this work.
