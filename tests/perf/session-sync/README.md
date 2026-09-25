# Fixed mailbox versus yielding single owner

This fixture compares the clean fixed baseline A at `4ead601cd` with preferred fork design C on this branch.
It retains the workloads and invariants from the earlier full-Store browser experiment. It compares neither main
nor the whole-batch synchronous prototype.

Read [the findings](./DECISION.md), [measurement table](./RESULTS.md), and [raw samples](./results.json).

Both production builds use this same fixture. Vite resolves all LiveStore package exports from the selected checkout;
mailbox runs on port 4178, owner on 4179. Store has no experiment selector or compatibility bridge. Source maps can
be inspected to confirm each build uses its intended processor and Store implementation.

```sh
SESSION_SYNC_BASELINE_WORKTREE=/absolute/path/to/clean/baseline pnpm --dir tests/perf exec playwright test --config session-sync/playwright.config.ts
node tests/perf/session-sync/report.mjs
pnpm exec tsc --project tests/perf/session-sync/tsconfig.json --noEmit
```

Install dependencies in both checkouts first with `pnpm install`. Keep the baseline at `4ead601cd`; the original task
worktree is suitable while it remains clean. Stop other build/test jobs before collecting final measurements.
`SESSION_SYNC_RUNS=1` is a smoke run; default five measured alternating fixed-A/C pairs follow one warmup per variant/workload.
Each build keeps its own page for warmup and measured runs. The intended page is brought to the foreground outside
the timed interval. Source commits are recorded in results.json.

Workloads: idle; 100/1,000 incoming events; one/five writes per event; advance, conflicting advance requiring rebase,
and the same rebase with a controlled 20 ms cancellation finalizer. Pending suffixes are 10% of incoming count (minimum
ten). Explicit upstream-rebase is covered by correctness tests but is not a separate browser workload.

Input is a synthetic DOM event scheduled five milliseconds after pull delivery is scheduled. Its handler performs
a real Store.commit; a Store subscription updates the DOM. Measure input scheduling delay, commit duration, the next
RAF opportunity and total catch-up time. Completion means next pull demand, not an intermediate state publication.

Check rows, pending identities, durable/model head equality, immediate reads, final propagation and intermediate
row/model consistency. Failed trials retain raw samples and fail the command. This is actual browser SQLite and Store
integration with controlled leader transport, not mocked materialization.

Five samples per workload on one machine are not a tail-latency study. Synthetic input/RAF is not INP or display scanout.
OPFS, workers, network, multiple tabs and telemetry exporters are outside the experiment. Neither version has a hard
frame bound or a new cross-database crash-atomicity guarantee.

See [the design comparison](../../../contributor-docs/rfcs/0004-session-single-owner-experiment.md).
