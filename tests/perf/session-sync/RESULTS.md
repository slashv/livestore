# Session sync browser measurements

Generated from results.json at 2026-09-07T19:25:43.224Z. 147.0.7727.15; darwin arm64; Apple M1 Pro.

5 measured runs per variant/workload, after one warmup each. Orders alternate. Values below are medians in milliseconds. The sample is too small for confident tail-latency claims.

Input is a synthetic DOM input event scheduled by a timer before batch delivery. Deadline-to-frame includes scheduling delay, a real Store.commit, reactive DOM updates, and the next RAF callback. It is not trusted input, INP, or a display scanout measurement.

| Workload                              | Variant |   Sync | Input delay | Commit | Deadline → RAF | Max RAF gap | Input during sync | Final head mismatch |
| ------------------------------------- | ------- | -----: | ----------: | -----: | -------------: | ----------: | ----------------: | ------------------: |
| idle / 0 events / 1 writes            | mailbox |    0.0 |         0.9 |    1.9 |            3.4 |         8.4 |               0/5 |                 0/5 |
| idle / 0 events / 1 writes            | owner   |    0.0 |         0.8 |    2.0 |            3.4 |         8.4 |               0/5 |                 0/5 |
| advance / 100 events / 1 writes       | mailbox |   50.1 |         7.8 |    0.9 |           21.7 |        13.8 |               5/5 |                 0/5 |
| advance / 100 events / 1 writes       | owner   |   50.6 |         8.2 |    0.7 |           21.3 |        13.5 |               5/5 |                 0/5 |
| rebase / 100 events / 1 writes        | mailbox |   69.1 |        12.2 |    0.9 |           27.4 |        17.0 |               5/5 |                 0/5 |
| rebase / 100 events / 1 writes        | owner   |   70.7 |        12.2 |    0.8 |           14.4 |        17.0 |               5/5 |                 0/5 |
| cancellation / 100 events / 1 writes  | mailbox |   94.0 |         0.9 |    1.1 |            3.3 |        22.1 |               5/5 |                 0/5 |
| cancellation / 100 events / 1 writes  | owner   |   93.2 |         1.0 |    1.1 |            3.3 |        21.8 |               5/5 |                 0/5 |
| advance / 100 events / 5 writes       | mailbox |   58.2 |         9.9 |    0.8 |           25.8 |        16.6 |               5/5 |                 0/5 |
| advance / 100 events / 5 writes       | owner   |   60.8 |        10.3 |    0.7 |           23.2 |        17.2 |               5/5 |                 0/5 |
| rebase / 100 events / 5 writes        | mailbox |   84.7 |        16.8 |    0.9 |           18.2 |        21.7 |               5/5 |                 0/5 |
| rebase / 100 events / 5 writes        | owner   |   84.9 |        16.1 |    0.9 |           16.9 |        20.8 |               5/5 |                 0/5 |
| cancellation / 100 events / 5 writes  | mailbox |  104.9 |         1.0 |    1.2 |            3.4 |        25.7 |               5/5 |                 0/5 |
| cancellation / 100 events / 5 writes  | owner   |  105.1 |         1.0 |    1.2 |            3.4 |        25.0 |               5/5 |                 0/5 |
| advance / 1000 events / 1 writes      | mailbox |  622.8 |        11.3 |    0.8 |           23.2 |        19.5 |               5/5 |                 0/5 |
| advance / 1000 events / 1 writes      | owner   |  627.0 |        12.2 |    0.7 |           13.1 |        19.2 |               5/5 |                 0/5 |
| rebase / 1000 events / 1 writes       | mailbox | 1661.8 |        49.8 |    2.0 |           53.1 |        54.5 |               5/5 |                 0/5 |
| rebase / 1000 events / 1 writes       | owner   | 1672.3 |        49.2 |    2.2 |           51.6 |        56.5 |               5/5 |                 0/5 |
| cancellation / 1000 events / 1 writes | mailbox | 1674.0 |         5.5 |    2.1 |           11.7 |        53.3 |               5/5 |                 0/5 |
| cancellation / 1000 events / 1 writes | owner   | 1671.6 |         6.2 |    2.1 |           11.2 |        53.0 |               5/5 |                 0/5 |
| advance / 1000 events / 5 writes      | mailbox |  720.4 |        13.8 |    0.8 |           16.3 |        22.8 |               5/5 |                 0/5 |
| advance / 1000 events / 5 writes      | owner   |  697.9 |        14.3 |    0.8 |           15.2 |        21.1 |               5/5 |                 0/5 |
| rebase / 1000 events / 5 writes       | mailbox | 2060.5 |        61.1 |    2.1 |           64.4 |        67.2 |               5/5 |                 0/5 |
| rebase / 1000 events / 5 writes       | owner   | 2048.7 |        60.2 |    2.2 |           62.5 |        68.5 |               5/5 |                 0/5 |
| cancellation / 1000 events / 5 writes | mailbox | 2080.1 |         4.7 |    3.5 |           11.7 |        63.4 |               5/5 |                 0/5 |
| cancellation / 1000 events / 5 writes | owner   | 2090.1 |         4.9 |    3.6 |           10.6 |        64.7 |               5/5 |                 0/5 |

## Correctness and interpretation

- mailbox: 65 samples; row failures 0, pending failures 0, durable-head failures 0, immediate-read failures 0, propagation failures 0. Input observed partially reconciled rows/state in 0 samples.
- owner: 65 samples; row failures 0, pending failures 0, durable-head failures 0, immediate-read failures 0, propagation failures 0. Input observed partially reconciled rows/state in 0 samples.

Runtime/time-out failures: 0. See the raw artifact for per-trial details.

A low input delay does not establish correctness. Samples with a durable-head mismatch must not be treated as semantically equivalent successful runs. The cancellation workload deliberately injects 20ms into an old push finalizer; it is a controlled stress case, not a measured production cancellation cost.

The in-memory adapter keeps actual browser SQLite, Store materializers, journals, and query subscriptions. Leader transport is controlled. OPFS, worker messaging, remote network latency, and multi-tab behavior are outside this experiment.
