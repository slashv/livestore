# Snapshot Boot Benchmarks

Benchmarks for experimental snapshot boot
([spec](../../context/02-system/03-sync/04-snapshots/spec.md)): how long a fresh
client takes to reach the backend head with a full replay vs. a snapshot plus
tail, where the full-replay time goes, what the checkpointer costs, and which
trigger constants follow from the measurements.

Code, raw results (`results/*.jsonl`) and the runner live in
[`tests/package-common/src/snapshot/bench/`](../../tests/package-common/src/snapshot/bench/).
Measured at commit `328321b99` (branch `claude/snapshot-boot-6d51c8`).

## Summary

Measured on a heavily loaded laptop (see [Setup](#setup)); CPU times are the
reliable figures, wall-clock times are inflated.

- **Snapshot boot works and scales with state, not history.** A fresh client
  at 1M events boots from a snapshot in 2.4 s CPU (`append`, 211 MB state) or
  0.4 s (`update`, 32 MB), vs. ~500 s CPU for a full replay (0.5 ms/event from
  the 900k runs): 200–1,300× less work. Each tail event costs about as much
  as a replayed event (0.5–1 ms), so the tail, not the snapshot, dominates once
  it exceeds a few thousand events.
- **Full replay costs ~0.5 ms CPU per event** (0.44–0.62 per 250k segment, flat
  from 0 to 3M events and from 8 MB to 190 MB of state; 0.41–0.84 in the
  noisier 100k runs): 10× the trigger's default
  `replayCostPerEventMs = 0.05`. About 56% of it is the sync
  processor, Effect runtime and GC; changeset recording for the journal alone
  is ~34%; materializer JS is ~2%.
- **Bytes:** a full replay pulls ~400 B/event of JSON (410 MB at 1M). The
  snapshot is 2× smaller for `append` (211 MB) and 13× smaller for `update`
  (32 MB). At 100 Mbit/s the `append` 1M snapshot download (17 s) dominates
  its boot; at 1 Gbit/s boot is 4.1 s vs ~500 s.
- **Blockers found in the core** (details in [Findings](#findings)): a leader
  retains ~6 KB of JS heap and ~1.4 KB of wasm heap per applied event (three
  leaks), so a single leader cannot replay 1M events, and a long-running
  checkpointer grows without bound (~25 GB/h at 1k events/s). State export and
  import need the whole database in the 2 GiB wasm heap: 1.6 GB exports, 2.16
  GB fails, so the `append` mix cannot be snapshotted at 10M events (~2.1 GB
  state). The manifest hash (pure-JS SHA-256, ~140 MB/s) is the largest part
  of install and checkpointer publish cost; `node:crypto` is 13× faster.
- **Trigger:** recommended `replayCostPerEventMs: 0.5` (measured) and keep
  `downloadCostPerByteMs: 0.00004`, `alpha: 0.5`, `minIntervalMs: 10_000`,
  `minTailEvents: 100` ([Calibration](#trigger-calibration)).
- **10M:** the full 10M replay was stopped at 3M after 63 minutes of wall time
  under load; 10M figures below are explicit extrapolations from the measured
  per-event and per-byte costs.

## Scenarios and Hypotheses

Written before the measurements; the verdict column was filled in afterwards.

| ID  | Scenario                                                                                                                                                     | Hypothesis                                                                                                                                                                                                                           | Verdict                                                                                    |
| --- | ------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ------------------------------------------------------------------------------------------ |
| S1  | Full replay of 100k, 1M and 10M events, two event mixes (`append`, `update`), fresh client                                                                   | Time and bytes grow linearly with history length; per-event cost is constant. State size depends on the mix, bytes pulled do not.                                                                                                    | Confirmed: 0.44–0.62 ms CPU/event, flat over 3M events                                     |
| S2  | Snapshot boot at the same sizes with tails 0, 1k, 10k, 100k                                                                                                  | Time ≈ install(state bytes) + tail × per-event replay cost, independent of history length for a fixed state size; the `update` mix (state ≪ history) gains far more than `append`. A 100k tail costs ≈ 100k × the S1 per-event cost. | Confirmed: 200–1,300× less CPU at tail 0 (1M); tail dominates beyond a few thousand events |
| S3  | Bytes on the wire: S1 event pages vs S2 snapshot + tail, with transfer times at 100 Mbit/s and 1 Gbit/s                                                      | Snapshot bytes are smaller than event bytes; for `update` by roughly the update/insert ratio.                                                                                                                                        | Confirmed: 2× (`append`) and 13× (`update`) fewer bytes                                    |
| S4  | Breakdown of full-replay time: wire decode, arg decoding, materializer JS, state writes, journal (changeset) recording, eventlog bookkeeping, sync processor | SQLite (state + eventlog + journal) dominates; JS-side sync/Effect overhead is a minor share.                                                                                                                                        | Refuted: ~56% is sync processor / Effect / GC; SQLite ~42%                                 |
| S5  | Snapshot boot breakdown: manifest, download, verify (sha256), install, tail                                                                                  | Install and verify are linear in bytes and fast (≥ 100 MB/s); with tail 0 the boot is dominated by them.                                                                                                                             | Confirmed; SHA-256 is the largest step                                                     |
| S6  | Checkpointer under sustained load (100, 1k, 10k events/s) with the default trigger                                                                           | Export time and snapshot size grow linearly with state; publications are spaced by a tail proportional to the state size; snapshot lag stays bounded; CPU and memory stay flat.                                                      | Partly refuted: ~0.9 ms CPU/event live, memory grows ~6 KB/event                           |
| S7  | Export/import ceiling vs state size                                                                                                                          | Export and import are linear in bytes up to the wasm heap limit.                                                                                                                                                                     | Confirmed: ceiling between 1.6 and 2.16 GB                                                 |

## Setup

### Hardware and environment

| Item    | Value                                                                                       |
| ------- | ------------------------------------------------------------------------------------------- |
| CPU     | Apple M1 Pro, 10 cores (8 performance + 2 efficiency)                                       |
| RAM     | 32 GiB                                                                                      |
| OS      | macOS 27.0.1 (Darwin 27.0.0)                                                                |
| Node    | v24.13.1, `NODE_ENV=production` (no dev-only materializer hashing), `LS_DEV` unset          |
| SQLite  | `@livestore/wa-sqlite` Node wasm build (2 GiB heap cap), `NodeFS` VFS like the Node adapter |
| Storage | APFS SSD; client databases are files (`fs`), except the breakdown runs (`:memory:`)         |

**The machine was shared and heavily loaded** while the benchmarks ran: other
agent sessions, a CAD application and a VM kept the 1-minute load average
between 40 and 370 on 10 cores. Each benchmark process got roughly 0.5–0.7 of a
core. Wall-clock times are therefore inflated (typically 1.3–2×, up to 3× for
I/O-heavy runs) and noisy. Every result also records the process CPU time
(user + system), which is much less sensitive to that load; the calibration
below uses CPU time. Ratios between snapshot boot and full replay are robust
because both ran under the same conditions. Expect an idle M1 Pro to be faster
than the wall-clock columns.

### Workload

`workload.ts` defines an agent store: `sessions`, `messages` (indexed by
session) and `tasks` (indexed by session), and six synced events. Every event
is a pure function of its sequence number, so any position of a 10M-event log
can be served without storing it. Message text is 100–500 bytes (uniform),
tool calls and task results carry JSON objects, 8 writer clients interleave.

| Event                | `append` mix | `update` mix | Effect                                              |
| -------------------- | -----------: | -----------: | --------------------------------------------------- |
| `v1.SessionCreated`  |           1% |           1% | insert session with JSON metadata                   |
| `v1.SessionUpdated`  |           9% |           4% | update status + JSON metadata                       |
| `v1.MessageAppended` |          45% |           5% | insert message (100–500 B text, 30% tool-call JSON) |
| `v1.MessageStreamed` |           0% |          50% | rewrite the text of one of the last 4 messages      |
| `v1.TaskCreated`     |           5% |           2% | insert task                                         |
| `v1.TaskUpdated`     |          40% |          38% | update a recent task (20% with result JSON)         |

`append` models a transcript store: about half the events add a row, so state
grows with history (~210 B of state per event). `update` models a streaming
agent: most events rewrite existing rows, so state is far smaller than history
(~30 B of state per event). Wire size is ~400 B/event (`append`) and ~375
B/event (`update`) of JSON.

### Method

- **Backend.** `generator-backend.ts` implements `SyncBackend` over the
  generated log with live pull, 256 events per page (the Cloudflare D1 page
  size). Pages are serialized to JSON and parsed again, so the client pays the
  wire decode. Generation and serialization time is measured and subtracted:
  reported times are client time. "Event bytes" are the JSON page bytes
  (uncompressed).
- **Snapshots.** `boot.ts prepare` runs a checkpointer leader (the real
  `makeCheckpointer`, `publishNow`) to `events − tail` for every tail and
  caches blob + manifest on disk. `boot.ts snapshot` serves them from
  `makeInMemorySnapshotStore`, so "download" is a memory copy; the network is
  modelled from bytes (below).
- **One measurement per process.** Each run warms up with a 3k-event replay on
  in-memory databases, then times from leader layer construction until the
  leader's upstream head reaches the backend head (polled every 2 ms).
  Repetitions: 5 at 100k, 2–3 at 1M, 1 at 10M. Tables show the median and the
  nearest-rank p95 (for n ≤ 5 that is the maximum).
- **Network model.** Transfer time = bytes × 8 / bandwidth. A full replay
  streams pages while it applies them, so its total is `max(local, transfer)`;
  a snapshot boot downloads the blob before installing it and then streams the
  tail: `transfer(snapshot) + max(local tail, transfer(tail))`. Local time
  includes verify and install.
- **Segmented full replays.** A single leader cannot replay 1M+ events (see
  [memory findings](#finding-1-leaders-retain-memory-per-event)). Full replays
  of 1M and 10M events therefore restart the client process every 250k events
  on its persisted databases, exactly like a real client restart (the next
  process resumes from the persisted cursor). Each restart costs a leader boot
  of ~30 ms plus process start, which is excluded; reported time is the sum of
  the segments.
- **Breakdown (S4).** Three complementary methods: (1) a `SqliteDb` wrapper
  (`timeDb` in `lib.ts`) times every SQLite call of the state and eventlog
  databases and classifies it by SQL (user tables, journal, state head,
  savepoints, eventlog), plus `session()`/`changeset()`/`finish()`; (2) variant
  runs: no-op materializers (no state SQL, same decode/sync/eventlog path) and a
  no-op session (no changeset recording); (3) micro-benchmarks for wire JSON
  parse, arg decoding and statement building (`micro.ts`). The remainder of
  client time after (1) and (3) is attributed to the sync processor, the
  Effect runtime and GC. A CPU profile (`node --cpu-prof`) cross-checks it.

## Results

The tables in this section are generated by `node summarize.ts` from
`results/*.jsonl`; numbers quoted in the text come from them. Rows with `n=1`
are single runs.

### Tables

#### Boot time and bytes (S1, S2, S3)

Local time on an idle machine is approximated by the CPU median. Network columns: local CPU time plus modelled transfer (replay: `max(local, transfer)` since pages stream; snapshot: `transfer(blob) + max(local, transfer(tail))`).

| mix    | events | boot                 | n   | wall median | wall p95 | CPU median | speedup (CPU) | event bytes | snapshot bytes | @100 Mbit/s | @1 Gbit/s |
| ------ | ------ | -------------------- | --- | ----------- | -------- | ---------- | ------------- | ----------- | -------------- | ----------- | --------- |
| append | 100k   | full replay          | 3   | 295 s       | 474 s    | 84 s       | –             | 43.5 MB     | –              | 84 s        | 84 s      |
| append | 100k   | snapshot + 10k tail  | 5   | 9.7 s       | 18 s     | 6.1 s      | 14×           | 4.4 MB      | 19.6 MB        | 7.6 s       | 6.2 s     |
| append | 100k   | snapshot + 1k tail   | 5   | 1.3 s       | 1.6 s    | 994 ms     | 84×           | 0.4 MB      | 21.4 MB        | 2.7 s       | 1.2 s     |
| append | 100k   | snapshot + 0 tail    | 5   | 416 ms      | 910 ms   | 291 ms     | 289×          | 0.0 MB      | 21.4 MB        | 2.0 s       | 462 ms    |
| append | 900k   | full replay          | 1   | 649 s       | 649 s    | 448 s      | –             | 393.6 MB    | –              | 448 s       | 448 s     |
| append | 1M     | snapshot + 100k tail | 3   | 249 s       | 598 s    | 80 s       | ~6×           | 43.7 MB     | 189.8 MB       | 95 s        | 81 s      |
| append | 1M     | snapshot + 10k tail  | 3   | 8.9 s       | 9.4 s    | 7.4 s      | ~67×          | 4.4 MB      | 208.7 MB       | 24 s        | 9.1 s     |
| append | 1M     | snapshot + 1k tail   | 3   | 3.3 s       | 3.9 s    | 2.9 s      | ~171×         | 0.4 MB      | 210.6 MB       | 20 s        | 4.6 s     |
| append | 1M     | snapshot + 0 tail    | 3   | 2.4 s       | 2.5 s    | 2.4 s      | ~206×         | 0.0 MB      | 210.6 MB       | 19 s        | 4.1 s     |
| update | 100k   | full replay          | 3   | 44 s        | 132 s    | 41 s       | –             | 40.8 MB     | –              | 41 s        | 41 s      |
| update | 100k   | snapshot + 10k tail  | 5   | 79 s        | 135 s    | 11 s       | 4×            | 4.1 MB      | 3.8 MB         | 12 s        | 11 s      |
| update | 100k   | snapshot + 1k tail   | 5   | 15 s        | 21 s     | 1.3 s      | 32×           | 0.4 MB      | 4.2 MB         | 1.6 s       | 1.3 s     |
| update | 100k   | snapshot + 0 tail    | 5   | 1.1 s       | 1.6 s    | 114 ms     | 361×          | 0.0 MB      | 4.2 MB         | 449 ms      | 147 ms    |
| update | 900k   | full replay          | 1   | 759 s       | 759 s    | 459 s      | –             | 368.6 MB    | –              | 459 s       | 459 s     |
| update | 1M     | snapshot + 100k tail | 1   | 348 s       | 348 s    | 80 s       | ~6×           | 41.0 MB     | 28.8 MB        | 82 s        | 80 s      |
| update | 1M     | snapshot + 10k tail  | 3   | 53 s        | 63 s     | 9.9 s      | ~52×          | 4.1 MB      | 31.5 MB        | 12 s        | 10 s      |
| update | 1M     | snapshot + 1k tail   | 3   | 8.3 s       | 9.2 s    | 1.4 s      | ~353×         | 0.4 MB      | 31.9 MB        | 4.0 s       | 1.7 s     |
| update | 1M     | snapshot + 0 tail    | 3   | 1.5 s       | 2.6 s    | 400 ms     | ~1276×        | 0.0 MB      | 31.9 MB        | 2.9 s       | 655 ms    |
| update | 3M     | full replay          | 1   | 3771 s      | 3771 s   | 1676 s     | –             | 1.23 GB     | –              | 1676 s      | 1676 s    |
| update | 3.3M   | snapshot + 100k tail | 1   | 295 s       | 295 s    | 80 s       | ~23×          | 41.2 MB     | 100.1 MB       | 88 s        | 81 s      |
| update | 3.3M   | snapshot + 10k tail  | 1   | 75 s        | 75 s     | 14 s       | ~132×         | 4.1 MB      | 102.8 MB       | 22 s        | 15 s      |
| update | 3.3M   | snapshot + 1k tail   | 1   | 12 s        | 12 s     | 2.7 s      | ~681×         | 0.4 MB      | 103.3 MB       | 11 s        | 3.5 s     |
| update | 3.3M   | snapshot + 0 tail    | 1   | 10 s        | 10 s     | 1.9 s      | ~989×         | 0.0 MB      | 103.3 MB       | 10 s        | 2.7 s     |

Full replays measured while preparing snapshots (single run, the first position of `prepare`):

| mix    | events | segments | wall   | CPU    | CPU ms/event | event bytes | state bytes | eventlog bytes |
| ------ | ------ | -------- | ------ | ------ | ------------ | ----------- | ----------- | -------------- |
| append | 900k   | 4        | 649 s  | 448 s  | 0.498        | 393.6 MB    | 189.8 MB    | 388.4 MB       |
| update | 900k   | 4        | 759 s  | 459 s  | 0.510        | 368.6 MB    | 28.8 MB     | 359.9 MB       |
| update | 3M     | 12       | 3771 s | 1676 s | 0.559        | 1.23 GB     | 93.8 MB     | 1.20 GB        |

Per-event full-replay cost (client wall and CPU per event):

| mix    | events | n   | wall ms/event | CPU ms/event | wire bytes/event | state bytes/event |
| ------ | ------ | --- | ------------- | ------------ | ---------------- | ----------------- |
| append | 100k   | 3   | 2.954         | 0.840        | 435              | 216               |
| update | 100k   | 3   | 0.437         | 0.411        | 408              | 41                |

#### Snapshot boot breakdown (S5)

| mix    | events | tail | snapshot | manifest | download | verify (sha256) | install | after install (tail) | verify MB/s | install MB/s | tail ms/event |
| ------ | ------ | ---- | -------- | -------- | -------- | --------------- | ------- | -------------------- | ----------- | ------------ | ------------- |
| append | 100k   | 0    | 21.4 MB  | 0 ms     | 9 ms     | 275 ms          | 164 ms  | 24 ms                | 78          | 131          | –             |
| append | 100k   | 1k   | 21.4 MB  | 0 ms     | 8 ms     | 181 ms          | 143 ms  | 928 ms               | 119         | 150          | 0.928         |
| append | 100k   | 10k  | 19.6 MB  | 0 ms     | 5 ms     | 252 ms          | 78 ms   | 9.1 s                | 78          | 251          | 0.914         |
| append | 1M     | 0    | 210.6 MB | 0 ms     | 47 ms    | 1.5 s           | 822 ms  | 19 ms                | 141         | 256          | –             |
| append | 1M     | 1k   | 210.6 MB | 0 ms     | 42 ms    | 1.3 s           | 737 ms  | 1.1 s                | 160         | 286          | 1.125         |
| append | 1M     | 10k  | 208.7 MB | 0 ms     | 32 ms    | 1.9 s           | 746 ms  | 6.0 s                | 112         | 280          | 0.601         |
| append | 1M     | 100k | 189.8 MB | 0 ms     | 34 ms    | 1.5 s           | 1.1 s   | 249 s                | 123         | 175          | 2.488         |
| update | 100k   | 0    | 4.2 MB   | 0 ms     | 6 ms     | 283 ms          | 429 ms  | 201 ms               | 15          | 10           | –             |
| update | 100k   | 1k   | 4.2 MB   | 0 ms     | 29 ms    | 541 ms          | 264 ms  | 14 s                 | 8           | 16           | 14.417        |
| update | 100k   | 10k  | 3.8 MB   | 0 ms     | 10 ms    | 182 ms          | 174 ms  | 79 s                 | 21          | 22           | 7.876         |
| update | 1M     | 0    | 31.9 MB  | 0 ms     | 9 ms     | 1.1 s           | 470 ms  | 39 ms                | 29          | 68           | –             |
| update | 1M     | 1k   | 31.9 MB  | 0 ms     | 14 ms    | 1.8 s           | 891 ms  | 5.5 s                | 18          | 36           | 5.521         |
| update | 1M     | 10k  | 31.5 MB  | 0 ms     | 21 ms    | 2.4 s           | 742 ms  | 50 s                 | 13          | 42           | 5.041         |
| update | 1M     | 100k | 28.8 MB  | 0 ms     | 12 ms    | 1.7 s           | 248 ms  | 347 s                | 17          | 116          | 3.474         |
| update | 3.3M   | 0    | 103.3 MB | 0 ms     | 590 ms   | 5.3 s           | 4.4 s   | 92 ms                | 19          | 24           | –             |
| update | 3.3M   | 1k   | 103.3 MB | 0 ms     | 590 ms   | 5.4 s           | 3.0 s   | 3.4 s                | 19          | 34           | 3.432         |
| update | 3.3M   | 10k  | 102.8 MB | 0 ms     | 481 ms   | 5.6 s           | 5.8 s   | 63 s                 | 18          | 18           | 6.329         |
| update | 3.3M   | 100k | 100.1 MB | 0 ms     | 21 ms    | 2.5 s           | 1.2 s   | 293 s                | 39          | 83           | 2.927         |

#### Checkpointer export cost vs state size (S6, from `prepare`)

| mix    | seqNum | snapshot | export | hash   | upload (in-memory) | export MB/s | hash MB/s | error |
| ------ | ------ | -------- | ------ | ------ | ------------------ | ----------- | --------- | ----- |
| append | 90k    | 19.6 MB  | 629 ms | 393 ms | 22 ms              | 31          | 50        |       |
| append | 99k    | 21.4 MB  | 564 ms | 978 ms | 41 ms              | 38          | 22        |       |
| append | 100k   | 21.4 MB  | 39 ms  | 456 ms | 37 ms              | 543         | 47        |       |
| append | 900k   | 189.8 MB | 355 ms | 1.5 s  | 201 ms             | 535         | 130       |       |
| append | 990k   | 208.7 MB | 3.0 s  | 7.1 s  | 255 ms             | 70          | 29        |       |
| append | 999k   | 210.6 MB | 1.3 s  | 8.1 s  | 712 ms             | 167         | 26        |       |
| append | 1M     | 210.6 MB | 718 ms | 8.0 s  | 245 ms             | 293         | 26        |       |
| update | 90k    | 3.8 MB   | 8 ms   | 26 ms  | 6 ms               | 468         | 146       |       |
| update | 99k    | 4.2 MB   | 32 ms  | 190 ms | 3 ms               | 130         | 22        |       |
| update | 100k   | 4.2 MB   | 28 ms  | 93 ms  | 6 ms               | 149         | 45        |       |
| update | 900k   | 28.8 MB  | 190 ms | 1.5 s  | 47 ms              | 152         | 19        |       |
| update | 990k   | 31.5 MB  | 794 ms | 826 ms | 59 ms              | 40          | 38        |       |
| update | 999k   | 31.9 MB  | 90 ms  | 300 ms | 10 ms              | 355         | 106       |       |
| update | 1M     | 31.9 MB  | 27 ms  | 214 ms | 10 ms              | 1186        | 149       |       |
| update | 3.2M   | 100.1 MB | 3.3 s  | 5.9 s  | 292 ms             | 31          | 17        |       |
| update | 3.29M  | 102.8 MB | 7.0 s  | 5.9 s  | 208 ms             | 15          | 17        |       |
| update | 3.299M | 103.3 MB | 5.5 s  | 6.9 s  | 453 ms             | 19          | 15        |       |
| update | 3.3M   | 103.3 MB | 3.9 s  | 5.4 s  | 437 ms             | 27          | 19        |       |

#### Export/import vs database size (S7)

| database | export | sha256 | import (fs target) | export MB/s | sha256 MB/s | import MB/s | wasm heap after export | result                                        |
| -------- | ------ | ------ | ------------------ | ----------- | ----------- | ----------- | ---------------------- | --------------------------------------------- |
| 83.8 MB  | 138 ms | 1.3 s  | 531 ms             | 608         | 66          | 158         | 82.7 MiB               | ok                                            |
| 272.4 MB | 1.1 s  | 3.7 s  | 2.8 s              | 246         | 74          | 98          | 262.6 MiB              | ok                                            |
| 544.9 MB | 2.5 s  | 8.1 s  | 4.9 s              | 222         | 67          | 110         | 522.4 MiB              | ok                                            |
| 1.09 GB  | 3.2 s  | 18 s   | 12 s               | 343         | 59          | 91          | 1042.1 MiB             | ok                                            |
| 1.61 GB  | 12 s   | 32 s   | 20 s               | 137         | 51          | 80          | 1541.8 MiB             | ok                                            |
| 2.16 GB  | –      | –      | –                  | –           | –           | –           | 16.6 MiB               | export failed: ~@livestore/common/SqliteError |

#### Full-replay breakdown (S4)

memory storage, append mix, 100k events:

| component                                       | µs/event | share | method                             |
| ----------------------------------------------- | -------- | ----- | ---------------------------------- |
| wire JSON parse                                 | 1.1      | 0%    | micro-benchmark (CPU)              |
| arg decoding (Schema)                           | 0.6      | 0%    | micro-benchmark (CPU)              |
| materializer JS: building statements            | 14.9     | 2%    | micro-benchmark (CPU)              |
| state writes (SQLite, user tables)              | 61.7     | 8%    | timed SqliteDb wrapper             |
| journal: session create / changeset / finish    | 38.5     | 5%    | timed SqliteDb wrapper             |
| journal: changeset row writes                   | 63.8     | 9%    | timed SqliteDb wrapper             |
| state head row + savepoints                     | 75.7     | 10%   | timed SqliteDb wrapper             |
| eventlog bookkeeping (inserts, sync status, tx) | 63.7     | 9%    | timed SqliteDb wrapper             |
| sync processor, Effect runtime, GC (remainder)  | 411.1    | 56%   | CPU per event minus the rows above |
| **total (CPU per event)**                       | 731.0    | 100%  | n=1; wall 5393.1 µs/event          |

Variant runs (whole client, per event):

| variant                                | wall µs/event | CPU µs/event | CPU delta vs full |
| -------------------------------------- | ------------- | ------------ | ----------------- |
| full                                   | 5393.1        | 731          |                   |
| no changeset recording (no-op session) | 2432.2        | 480          | -251              |
| no-op materializers (no state SQL)     | 1989.4        | 357          | -374              |

#### Micro-benchmarks

| mix    | wire bytes/event | JSON parse µs/event | arg decode µs/event | build statements µs/event | sha256 noble MB/s | sha256 node:crypto MB/s |
| ------ | ---------------- | ------------------- | ------------------- | ------------------------- | ----------------- | ----------------------- |
| append | 403              | 1.07                | 0.60                | 15.5                      | 137               | 1873                    |
| update | 375              | 0.85                | 0.32                | 19.3                      | 106               | 2133                    |

#### Checkpointer under sustained load, wall clock (S6)

| label   | mix    | base | rate ev/s | duration | published | median interval | median tail/publish | median snapshot | export+hash median | max snapshot lag | final leader behind | CPU share | heap growth |
| ------- | ------ | ---- | --------- | -------- | --------- | --------------- | ------------------- | --------------- | ------------------ | ---------------- | ------------------- | --------- | ----------- |
| default | append | 100k | 100       | 120 s    | 1         | –               | 9.269k              | 23.0 MB         | 1.1 s              | 9.195k           | 627                 | 14%       | 86 MiB      |
| default | append | 100k | 1000      | 120 s    | 0         | –               | –                   | –               | –                  | 119.932k         | 111.655k            | 13%       | 74 MiB      |
| default | append | 100k | 10000     | 120 s    | 0         | –               | –                   | –               | –                  | 1.199581M        | 1.183197M           | 18%       | 130 MiB     |
| default | append | 1M   | 100       | 120 s    | 0         | –               | –                   | –               | –                  | 11.913k          | 3.903k              | 9%        | 99 MiB      |
| default | append | 1M   | 1000      | 120 s    | 0         | –               | –                   | –               | –                  | 119.74k          | 113.703k            | 10%       | 117 MiB     |
| default | append | 1M   | 10000     | 120 s    | 0         | –               | –                   | –               | –                  | 1.195658M        | 1.186442M           | 11%       | 149 MiB     |

#### Checkpointer under sustained load, event time (S6)

| trigger                               | base | rate ev/s | duration | published | median tail/publish | median snapshot | export / hash / upload (median) | max snapshot lag | CPU ms/event | core share at rate | heap growth |
| ------------------------------------- | ---- | --------- | -------- | --------- | ------------------- | --------------- | ------------------------------- | ---------------- | ------------ | ------------------ | ----------- |
| default                               | 100k | 100       | 120 s    | 1         | 9.3k                | 23.0 MB         | 626 ms / 523 ms / 40 ms         | 9.2k (92 s)      | 0.85         | 8%                 | 76 MiB      |
| default                               | 100k | 1k        | 60 s     | 5         | 12k                 | 28.2 MB         | 964 ms / 1.6 s / 21 ms          | 13k (13 s)       | 0.91         | 91%                | 269 MiB     |
| calibrated (replayCostPerEventMs 0.5) | 100k | 1k        | 60 s     | 6         | 10k                 | 26.7 MB         | 809 ms / 1.2 s / 50 ms          | 9k (9 s)         | 0.95         | 95%                | 351 MiB     |
| default                               | 100k | 10k       | 10 s     | 1         | 10k                 | 23.8 MB         | 259 ms / 725 ms / 6 ms          | 90k (9 s)        | 1.06         | 1058%              | 482 MiB     |
| calibrated (replayCostPerEventMs 0.5) | 1M   | 1k        | 30 s     | 3         | 10k                 | 214.5 MB        | 2.1 s / 5.4 s / 726 ms          | 9k (9 s)         | 1.09         | 109%               | 148 MiB     |

### S1: full replay scales linearly

CPU per event is flat: 0.44–0.62 ms for every 250k-event segment of the 3M
`update` run, while the state grew from 8 MB to 90 MB and the eventlog to 1.2
GB; the 900k runs of both mixes average 0.50 ms. Bytes pulled are ~400 B/event
(`append`) and ~370 B/event (`update`) regardless of state size. State is ~210
B/event (`append`) and ~32 B/event (`update`); the eventlog ~400–430 B/event.
**Verdict: confirmed.** Wall time per event varied from 0.6 to 4.7 ms with the
machine load; it is not a property of the code.

10M (extrapolated, not measured): ~5,000 s of CPU (~1.4 h on one core) and ~4
GB of JSON for either mix; the eventlog would be ~4.3 GB. A single process
cannot do it today (Finding 1); restarting every 250k events works.

### S2/S3: snapshot boot

With tail 0, boot is verify + install + leader boot: 0.29 s CPU for 21 MB, 2.4 s
for 211 MB (`append`), 0.11 s for 4 MB and 0.40 s for 32 MB (`update`). At
3.3M `update` events (103 MB state) it is 1.9 s CPU, ~990× less than the
equivalent full replay. Each tail event adds
0.5–1.1 ms CPU, so a 10k tail costs 6–11 s and a 100k tail 80 s, i.e. the tail
quickly outweighs the snapshot. A 100k tail is still ~6× cheaper than a 1M full
replay; at 1M a 1k tail is 170–350× cheaper. **Verdict: confirmed**: boot time is a
function of state bytes and tail length, independent of history length (the
1M and 3.3M `update` boots differ by state size only).

Bytes: the `append` snapshot is half the size of the event history it replaces
(211 MB vs 410 MB at 1M), the `update` snapshot 13× smaller (32 MB vs 410 MB).
Both compress: gzip -6 shrinks the 1M `append` snapshot 4.25× (211 → 49.6 MB)
and event pages 7.3× (the generated text has a small vocabulary, so the JSON
ratio is optimistic). Neither the benchmark nor the Cloudflare routes compress
today. **Verdict: confirmed**, with the network dominating `append` boots at
100 Mbit/s (17 s of 19 s at 1M). Compressing blobs (e.g. gzip content-encoding
on the R2 route) would cut that ~4×.

10M (extrapolated): `update` state would be ~320 MB → ~4 s CPU to verify and
install plus the tail, ~26 s at 100 Mbit/s. `append` state would be ~2.1 GB,
above the export ceiling (S7): no snapshot can be produced with the current
export path.

### S4: where full-replay time goes

Breakdown at 100k `append` events on in-memory databases (single runs, see the
breakdown table): materializer JS (building SQL from the query builder) is 15
µs/event (2%), arg decoding 0.6 µs and wire JSON parse 1 µs. SQLite work is
~42%: user-table writes 8%, changeset session operations 5%, journal row
writes 9%, state head + savepoints 10%, eventlog bookkeeping 9%. The remaining
**~56% is the sync processor, the Effect runtime and GC**. The variant runs
agree: dropping changeset recording saves ~34% of CPU, dropping all state SQL
(no-op materializers) ~51%. A CPU profile of the same run attributes 27% of
self time to Effect internals, 22% to SQLite wasm, 5% to span stack-trace
capture (`addSpanStackTrace`, one span per materialized event and per SQL
call) and 5% to the quadratic metadata lookup in
`LeaderPersistence.materializeEvents` (Finding 3). **Verdict: refuted**: JS-side
overhead, not SQLite, dominates.

### S5: snapshot install

Manifest fetch and download (memory copy) are negligible here. Verify
(pure-JS SHA-256) runs at 80–160 MB/s in the `append` runs and is the largest
local step: 1.5 s of 2.4 s at 211 MB (the `update` runs hit the worst load and
show 8–40 MB/s wall-clock; the micro-benchmark gives 137 MB/s). Install
(`dbState.import`: deserialize into the wasm heap + backup into the fs
database) runs at 130–290 MB/s in the `append` runs.
**Verdict: confirmed** (linear in bytes, ≥ 100 MB/s), but hashing should move to
native code (13× faster).

### S6: checkpointer

Wall-clock runs (100/1k/10k events/s): on this machine the checkpointer process
got 9–18% of a core, so it fell behind at 1k events/s and above; those rows
show the effect of CPU starvation, not of the code. The event-time runs remove
that: every virtual second appends `rate` events, waits until they are
applied, and advances the trigger's clock. Their CPU per virtual second is the
core share a dedicated machine needs:

- Following the log live costs ~0.85–0.95 ms CPU per event including
  publications, ~1.7× the bulk replay cost (smaller batches, plus the
  publications). One core therefore follows ~1k events/s; **10k events/s
  needs ~10.6 cores (measured 1,058%) and is not sustainable** by one
  checkpointer leader, which is single-threaded.
- Publication cost is export + hash: for 23–34 MB states 0.2–3.7 s export and
  0.8–2 s hash under load; for 214 MB 2.1 s + 5.4 s, so with a 10 s
  publication interval that checkpointer spent most of its time publishing (micro-benchmark: 140 MB/s hash, 250–550 MB/s
  export). Snapshot size grows linearly with state (S7 confirms up to 1.6 GB).
- With the default trigger (`replayCostPerEventMs 0.05`) a 23 MB state publishes
  every ~9.3k events (93 s at 100 events/s) and a 211 MB state every ~84k
  events. With the calibrated 0.5 the threshold is 10× lower (920 events at
  23 MB); `minIntervalMs` (10 s) then bounds the lag to ~10 s of events.
- Memory is not flat: JS heap grows ~6 KB per applied event (Finding 1), e.g.
  +270 MiB over 60k events.

**Verdict: partly refuted**: spacing and export cost behave as modelled, but CPU
per event is high and memory grows without bound.

### S7: export/import ceiling

Export (`sqlite3_serialize`) needs the whole database in the wasm heap, and
import deserializes it there too. 1.61 GB exports and imports; 2.16 GB fails
with `SqliteError` (no crash). Throughput under load: export 140–600 MB/s,
SHA-256 50–75 MB/s, import 80–160 MB/s. **Verdict: confirmed, with a hard
ceiling at ~1.6–2 GB per snapshot** (lower in a Durable Object's 128 MB).

## Findings

### Finding 1: leaders retain memory per event

A leader's JS heap grows ~6 KB and its wasm heap ~1.4 KB per applied event,
after GC, in every mode (bulk replay, tail, live). Three sources:

1. **`makeSqliteDb` keeps every prepared statement until close**
   (`packages/@livestore/sqlite-wasm/src/make-sqlite-db.ts:126`,
   `preparedStmts.push`; `finalize` never removes it). `execute`/`select`
   prepare and finalize a statement per call, so each call leaks its statement
   object, closures and SQL string: 506 B of JS heap per call
   (`leak-probe.ts`), and replay makes ~10 SQL calls per event.
2. **wa-sqlite never frees the SQL text it copies into the wasm heap**
   (`packages/@livestore/wa-sqlite/src/sqlite-api.js:829`: the `finally` that
   runs `onFinally` and frees `pzHead` is commented out). 227 B of wasm heap per
   call for a 248-byte statement; the wasm heap is capped at 2 GiB, which a
   single process reaches after roughly 1.4M applied events.
3. **`makePullQueueSet` caches every pulled batch for late-joining sessions**
   (`LeaderSyncProcessor.ts:1248`, `cachedPullItems`, already marked "could
   grow unbounded"). Heap snapshots show event args, session ids and payload
   arrays retained through it.

Effects: V8's default 4 GiB heap fills at ~700k events, so a single client
session cannot replay 1M events (this is why the benchmarks restart every
250k events); a checkpointer at 1k events/s grows by ~25 GB/h and would exhaust
a 128 MB Durable Object after ~15–20k events. Fixes: drop finalized statements
from `preparedStmts` (or keep a `Set` and delete on finalize), restore the
`pzHead` free in `sqlite3.statements`, cache prepared statements for hot SQL
(the `execSqlPrepared` TODO), and bound `cachedPullItems` by the lowest cursor
of connected sessions.

### Finding 2: export/import ceiling

`export()`/`import()` materialize the whole database in the wasm heap (S7).
Snapshots above ~1.6 GB cannot be produced or installed in the Node build; the
Cloudflare checkpointer is limited far lower. A streaming export (page-wise
VFS read into the blob store) would remove the ceiling. The in-progress
`exportSnapshotWith` / VFS staging work in the working tree may address this;
re-run `export-limits.ts` once it lands.

### Finding 3: replay hot spots

- `LeaderPersistence.materializeEvents` looks up each event's sync metadata
  with `pulledEvents.find(...)` (`LeaderPersistence.ts:103`): O(batch²) for
  coalesced batches of up to 16 × 256 events; ~5% of replay CPU. A `Map` keyed
  by sequence number makes it O(n).
- Per-event `Effect.withSpan` (materialize, every SQL call) captures stack
  traces: ~5% of CPU.
- Changeset recording (session + journal rows) is ~34% of replay CPU. During a
  pull with no pending local events nothing can be rolled back; skipping the
  session for such batches would save a third of replay time.
- `sha256Hex` uses `@noble/hashes` (~140 MB/s); `node:crypto` does ~1.9 GB/s
  and WebCrypto `crypto.subtle.digest` is native in browsers, workers and Node.

### Finding 4: trigger and checkpointer details

- The first snapshot always passes `minIntervalMs` (`msSinceLastSnapshot =
Infinity`), so `minIntervalMs: Infinity` does not stop the loop from
  publishing; only `minTailEvents` does. Intended, but worth a doc line.
- `PublishTimings.totalMs` includes waiting for the persistence lock; a
  separate `lockWaitMs` would make export cost readable.

### Core hooks that would help measurement

- `snapshotBoot` timings end at install; a timestamp for "upstream head reached
  the backend head seen at boot" would give boot-to-ready without polling.
- An option to disable per-event spans in the leader hot path (or sample them),
  to measure without tracing overhead.
- `lockWaitMs` in `PublishTimings` (above).

## Trigger Calibration

The trigger publishes when `tail × replayCostPerEventMs > alpha × bytes ×
downloadCostPerByteMs`.

- `replayCostPerEventMs`: measured client CPU per replayed event is 0.50 ms
  (bulk, fs storage, M1 Pro performance core) and 0.6–1.1 ms for short tails
  and live batches, where per-batch overhead weighs more. Browsers (OPFS) and
  mobile devices are slower. **Use `0.5`.**
- `downloadCostPerByteMs` is network + verify + install per byte. Local part:
  verify 1/140 MB/s = 7.1e-6 ms/B, install 1/150–290 MB/s ≈ 4–7e-6 ms/B, so
  ~1.2e-5 ms/B (≈ 0.5e-5 with native hashing). Network: 8e-5 ms/B at 100
  Mbit/s, 8e-6 at 1 Gbit/s. The default `0.00004` (≈ 25 MB/s end to end)
  sits between a consumer link and a datacenter link. **Keep `0.00004`.**
- `alpha 0.5`, `minIntervalMs 10_000`, `minTailEvents 100`: keep. With
  `replayCostPerEventMs 0.5`, the publish threshold is `tail > 4e-5 × bytes`:
  ~850 events for a 21 MB state, ~8.4k for 211 MB, ~40k for 1 GB. Boots then
  stay within ~1.5× of the snapshot download; e.g. 211 MB at 100 Mbit/s: 17 s
  download + ≤ 4.2 s tail.

Exact change for the lead (`packages/@livestore/common/src/snapshot/trigger.ts`):

```ts
export const defaultTriggerParams: TriggerParams = {
  alpha: 0.5,
  replayCostPerEventMs: 0.5,
  downloadCostPerByteMs: 0.00004,
  minIntervalMs: 10_000,
  minTailEvents: 100,
}
```

Also update the `@default 0.05` JSDoc on `replayCostPerEventMs`, its
`tests/perf-snapshot` reference (the benchmarks live in
`tests/package-common/src/snapshot/bench/`), and the parameter table in the
spec.

Checkpointer cost with these values: each publication costs export + hash ≈
(1/250 MB/s + 1/140 MB/s) × bytes, and publications are spaced
`4e-5 × bytes` events apart, so the amortized cost is ~0.28 ms per appended
event, independent of state size: about half of the checkpointer's own replay
cost. With native hashing it drops to ~0.1 ms. `minIntervalMs` caps it further
at low rates.
If checkpointer CPU matters more than boot latency, `alpha: 1` halves this at
the price of boots up to 2× the snapshot download; the 214 MB event-time run
shows the cost is real on a loaded host (7.5 s of export + hash per 10 s).

## Caveats

- Shared, overloaded machine (load average 40–370): wall times are inflated
  and noisy; CPU time is less affected but still varies 0.5–1.1 ms/event when
  the scheduler moves the process to efficiency cores. Re-run on an idle
  machine before quoting absolute wall times.
- Repetitions are small (5 at 100k, 3 at 1M, 1 for 3M/3.3M, breakdown and
  checkpointer runs); p95 over n ≤ 5 is the maximum.
- Node + wa-sqlite + `NodeFS` only. Browsers (OPFS), Expo and Durable Objects
  have different storage costs and memory limits.
- Download is a memory copy; the network is modelled, not simulated, and there
  is no request latency (a manifest round trip adds one RTT).
- The 10M runs were not completed: the `append` 10M run was stopped early in
  favour of the `update` run, which was stopped at 3M (63 min of segment wall
  time) when the machine load made the remaining 7M a multi-hour run. The 3.3M
  `update` snapshot boots come from that run's databases. 10M numbers are
  extrapolations, marked as such.
- Event text is generated from a 40-word vocabulary; compression ratios are
  optimistic. Sync metadata is `None` (the mock provider shape); Cloudflare
  pages carry a little more per event.
- Full replays of 1M+ events restart the client every 250k events
  (Finding 1); a leader restart costs ~30 ms of boot plus process start, which
  is excluded.

## Reproduce

```bash
cd tests/package-common/src/snapshot/bench
export NODE_ENV=production SNAPSHOT_BENCH_DIR=/path/with/20GB/free
./run-all.sh micro prepare snapshot replay breakdown checkpointer export-limits leak-probe
./run-all.sh prepare10m snapshot10m   # hours; update mix first
node summarize.ts                     # markdown tables from results/*.jsonl
```

Single measurements, e.g.:

```bash
node boot.ts replay   --mix append --events 100000
node segmented.ts replay --mix update --events 1000000 --segment 250000
node boot.ts prepare  --mix append --events 100000 --tails 0,1000,10000
node boot.ts snapshot --mix append --events 100000 --tail 1000
node boot.ts replay   --mix append --events 100000 --storage memory --breakdown full   # or noop, nosession
node checkpointer.ts  --mix append --base 100000 --rate 1000 --seconds 120 [--trigger '{"replayCostPerEventMs":0.5}']
node export-limits.ts --mib 1024
node --expose-gc leak-probe.ts --calls 200000
```

The scripts run directly with Node 24 (type stripping); they are not part of
the default Vitest run.
