# Yielding single owner: measured comparison

Status: refactored C is the preferred session-sync design for the fork as of September 17, 2026. This is the user's
architecture choice, not upstream acceptance or a release decision. A and the whole-batch alternative remain preserved
on branches; their worktrees need not remain. This document retains the original measurement evidence.

## Outcome

**Both implementations passed all checked invariants in 130 measured browser samples.** Input ran during reconciliation
in all 60 non-idle samples for each implementation. Neither exposed partially reconciled rows/model. There were no
runtime or timeout failures. The Playwright command exited successfully.

Input delay and catch-up time were closely comparable in this run. This supports the hypothesis that single ownership
does not require whole-batch blocking. It does not establish statistical equivalence, a latency bound, or that the new
implementation is easier for a human to read.

Five alternating pairs per workload after one warmup per build, Chromium 147.0.7727.15, Apple M1 Pro, September 7, 2026.
The same fixture was built twice with all LiveStore package exports resolved from the intended checkout; source maps
were checked to confirm both the processor and Store came from the expected source. Each build retains its own page,
brought to the foreground before its turn outside the timed interval. No other validation job ran alongside the final
measurement.

| Source                           | Commit                                     |
| -------------------------------- | ------------------------------------------ |
| Fixed split-owner baseline       | `4ead601cdc81e1d594fc56cfc97839cb39f9e75b` |
| Yielding single-owner experiment | `159b64689e65a488bd89804a599fc39984c16012` |

The subsequent results commit changes only documentation and measurement artifacts.
[RESULTS.md](./RESULTS.md) is generated from [results.json](./results.json). These saved measurements describe the
original implementation. A later structural refactor names the owner workflows, extracts pull persistence and
separates asynchronous execution from boot; the original measurement artifacts are retained for comparison.

That refactor was checkpointed at `66ca5d3e09ddd16d59c3e12c71f33dd64734734e`. Its fresh browser rerun also passed 130
samples with zero correctness or trial failures. Neither run compares C to main: both compare C against fixed A.

Representative workload medians, milliseconds:

| Workload                                  | Baseline input delay | Single owner input delay | Baseline catch-up | Single owner catch-up |
| ----------------------------------------- | -------------------: | -----------------------: | ----------------: | --------------------: |
| Advance 100, 1 write/event                |                  7.8 |                      8.2 |              50.1 |                  50.6 |
| Rebase 100, 5 writes/event, 10 pending    |                 16.8 |                     16.1 |              84.7 |                  84.9 |
| Advance 1,000, 5 writes/event             |                 13.8 |                     14.3 |             720.4 |                 697.9 |
| Rebase 1,000, 5 writes/event, 100 pending |                 61.1 |                     60.2 |           2,060.5 |               2,048.7 |

Do not read small median differences as a performance win. Heavy pending replay still causes long tasks: maximum RAF
gap medians in the largest rebase were 67.2 ms and 68.5 ms. Neither approach solves that shared cost.

## What the experiment changed

The synchronous dispatcher owns local materialization and model changes, upstream steps, lifecycle and propagation
decisions. Store calls one commit method and retains its own local reactive refresh. The asynchronous command runner
keeps the existing stepping loop, waits/cancels outside dispatch, and calls the owner again after each yield. No
continuation framework, runtime variant selector, extra production module or copied baseline processor was added.

The implementation removes LocalPushAdmitted and its stale-encoding repair. It adds explicit commands, one current
reconciliation identity and a notification stage. That stage is necessary because an Effect Queue/Deferred may resume
another caller inline. Notifications and subscriber callbacks run after the owner has completed its state changes.

At the measured checkpoint, the processor grew from 600 to 640 lines, while Store commit plumbing shrank by 15 lines.
The later readability refactor adds private function boundaries and types. The small interface is deeper, but
centralization is not automatically simpler: navigating owner workflows and understanding staged notifications is a
different mental cost from understanding two explicit paths.

## Correctness and review

All ten existing real-Store safety regressions remain. Three additional real-Store tests verify local-batch rollback,
fatal failure between completed pull steps, and propagation of only final rebased encodings. One additional processor
test covers rejection between acknowledgment prefixes. Existing rejection, cancellation, graceful shutdown, hash
mismatch, boot, debug barriers and subscriber behavior tests pass.

Read-only exploration and two review passes examined Store integration, lifecycle, resource ownership, identity
checks and reentrancy. The first review found a real new race: taking rejection state at pull start misses a rejection
that arrives between steps. Finalization now consults current rejection state; the redundant snapshot was removed.
The regression fails when that recovery is disabled, and passes with it restored. The second pass found no remaining
high/medium correctness issue and called the notification-stage readability cost an explicit trade-off.

Other implementation checks fixed immediate Queue/Deferred reentrancy. The old local push trace is now a commit trace
covering the owned transaction, retaining event-count metadata and encode/materialize spans. Eighteen tracing snapshots
were updated for this change and the outer local savepoint; functional query checks were retained.

## Validation

| Command                                                                                                                                                                                              | Result                                                                            |
| ---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | --------------------------------------------------------------------------------- |
| `pnpm install`                                                                                                                                                                                       | Passed; existing examples warn about Node >=24 while the environment uses Node 22 |
| `pnpm run test:unit`                                                                                                                                                                                 | 129 passed, 1 skipped, 16 files                                                   |
| `pnpm exec vitest run tests/package-common/src/client-session/ClientSessionReconciliation.test.ts tests/package-common/src/client-session/ClientSessionSyncProcessor.test.ts --testNamePattern '.*'` | 45 passed                                                                         |
| `pnpm exec vitest run --project @livestore/common --project @livestore/livestore`                                                                                                                    | 352 passed, 1 skipped, 30 files                                                   |
| `pnpm exec vitest run --project @livestore/livestore` after final trace changes                                                                                                                      | 81 passed, 1 skipped                                                              |
| `pnpm run ts:build`                                                                                                                                                                                  | Passed                                                                            |
| `pnpm exec tsc --project tests/perf/session-sync/tsconfig.json --noEmit`                                                                                                                             | Passed                                                                            |
| `pnpm run lint:full:fix`                                                                                                                                                                             | Passed                                                                            |
| `git diff --check`                                                                                                                                                                                   | Passed                                                                            |
| `SESSION_SYNC_BASELINE_WORKTREE=/absolute/path/to/baseline pnpm --dir tests/perf exec playwright test --config session-sync/playwright.config.ts`                                                    | Passed, 130 samples, zero invariant/runtime failures                              |
| `node tests/perf/session-sync/report.mjs`                                                                                                                                                            | Generated RESULTS.md                                                              |

The root unit script selects package-common files here; affected package projects were also run explicitly. Intermediate
development failures were resolved before measurement. A one-pair smoke run also passed but is not the final artifact.

## Limits and fork decision

Same limits as the fixed baseline: steps are atomic, whole pull payloads are not; storage/materializers must be
synchronous; pending replay and large explicit rebases are not bounded by wall-clock time. No new guarantee covers
unacknowledged-session crash durability or cross-database crash atomicity.

These workloads use actual Store, browser SQLite, journal and subscriptions, with controlled leader transport.
Synthetic timer input and RAF are not trusted input/INP or display scanout. OPFS, workers, multi-tab contention,
network and telemetry exporters are not measured. Explicit upstream-rebase has a regression test but is not a distinct
browser workload. Five samples per workload on one machine cannot establish tail-latency bounds.

The user has chosen the refactored single-owner implementation for the fork, with A and the other alternatives retained
on branches. The choice favors one state-changing owner and named workflows, accepting the command and notification
stage as a readability trade-off. The measurements provide no strong performance reason to prefer either yielding
implementation, and no evidence of a performance win over main. Future changes must preserve the coherent-step and
synchronous-commit contracts regardless of that architectural preference.

Start with [the RFC's alternatives section](../../../contributor-docs/rfcs/0004-serialized-sync-processors.md#choice-alternatives-and-evidence).
