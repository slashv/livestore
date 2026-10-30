# Snapshot Boot

> **Status: accepted as an experiment (2026-10-29).** Opt-in and experimental. The detailed contract lives in
> [`context/02-system/03-sync/04-snapshots/`](../../context/02-system/03-sync/04-snapshots/spec.md), and the full
> measurements in the [benchmark report](../perf/snapshot-boot-benchmarks.md).

## Problem

A LiveStore client builds its local database by replaying every event in the store's history. A new client, or one
that starts with empty storage, downloads the entire history and replays it before it is ready. Startup time and
download size therefore grow with the length of the history, even when the data the client actually needs stays small.

This hurts most in the workload that motivated this RFC: around 100 AI agents per system, each a LiveStore client,
writing events at high rates and running in fresh sandboxes that restart often. Every restart starts from zero.

| History    | Download (event log) | Time to replay |
| ---------- | -------------------- | -------------- |
| 1M events  | ~400 MB              | ~5 minutes     |
| 10M events | ~4 GB                | ~44 minutes    |

We want startup to depend on how much data there is now, not on how much history led to it, without losing any history.

## Solution

**Download the result instead of replaying the history.**

1. **A checkpointer keeps a recent copy of the database.** It is an ordinary LiveStore client running next to the sync
   backend. It follows the event log, never writes to it, and every so often uploads a copy of its database, a
   _snapshot_, together with the position in the log that the snapshot reflects.
2. **A new client starts from the snapshot.** It downloads the latest snapshot, checks it against a hash, installs it
   as its database, and then downloads and replays only the events written after it.
3. **Nothing is deleted.** The full event log stays on the backend. Snapshots are a shortcut, not a replacement.

```
                    2. download newer events               follows the log
+------------------+               +-------------------+             +========================+
| New client       |  <----------  | Sync backend      |  ---------> | Checkpointer (new)     |
| e.g. an agent in |               | full event log,   |             | read-only client,      |
| a fresh sandbox  |               | never shortened   |             | Durable Object or Node |
+------------------+               +-------------------+             +========================+
         ^                                                                        |
         |  1. download snapshot   +-------------------+   uploads a snapshot     |
         +------------------------ | Object storage    | <------------------------+
                                   | latest snapshot   |   every so often
                                   | per app version   |
                                   +-------------------+
```

```ts
sync: {
  backend: makeWsSync({ url }),
  snapshot: { source: makeHttpSnapshotSource({ url }) }, // opt in; without it nothing changes
}
```

A few rules keep this safe:

- **Snapshots only match the same app version's data layout.** Each snapshot is labelled with a fingerprint of the
  table layout and the event definitions. A client only uses a snapshot with a matching label, so a schema change
  simply starts a new line of snapshots. The previous snapshot stays available for a while so that clients still on
  the old version keep working during a rollout.
- **Changing what an event does means giving it a new name.** The label does not cover the code that turns events
  into table rows. LiveStore already asks for a new event name (for example `v2.TodoCreated`) when that behaviour
  changes; snapshot boot relies on that convention.
- **Any problem falls back to today's behaviour.** A missing, corrupt, outdated or slow snapshot is ignored and the
  client replays the full history as before. A bad snapshot never stops a client from starting.
- **Existing clients are untouched.** A client that already has local data keeps using it.
- **Snapshots are published only when worthwhile.** The checkpointer publishes once replaying the new events would
  cost a client more than about half of downloading a fresh snapshot, and at most every 10 seconds. A quiet store
  publishes nothing.

On Cloudflare, snapshots are stored in R2, the sync Durable Object keeps track of the latest one, and the checkpointer
runs as its own Durable Object. The building blocks also run in Node.

## Results

Measured on an M1 Pro (details in the [benchmark report](../perf/snapshot-boot-benchmarks.md)).

**Startup time for a new client**

| History    | Full replay | From snapshot | Snapshot size |
| ---------- | ----------- | ------------- | ------------- |
| 1M events  | ~290 s      | 0.1–0.8 s     | 32–211 MB     |
| 10M events | ~44 min     | 1.3 s         | 311 MB        |

Events written after the snapshot still cost about 0.3 ms each, which is why the checkpointer publishes frequently.

**AI agents restarting** (time for a restarted agent to catch up)

| Agents | Without snapshots | With snapshots |
| ------ | ----------------- | -------------- |
| 10     | 38 s              | 3 s            |
| 25     | 50 s              | 5.3 s          |
| 50     | 75 s              | 8.4 s          |

With 100 agents, a restart takes 17 s with snapshots, and the data stays correct at every scale tested.

**Cost of the checkpointer:** at 1,000 events per second it uses about half of one CPU core, publishes a new snapshot
roughly every 10 seconds and stays within about 650 events of the latest. On Cloudflare it streams the export, so its
memory stays around 17 MB for databases up to 521 MB.

**Correctness:** test suites check that a client started from a snapshot ends up with exactly the same data as one
that replayed the full history. They cover concurrent writers, unsynced local changes, schema changes, broken or
missing snapshots, and crashes and restarts at every step.

## Limitations

- **Experimental and opt-in.** Running a checkpointer and storing snapshots is an extra cost for operators.
- **The first offline start after a schema-changing app update needs the network.** A client that started from a
  snapshot does not have the older history locally, so it cannot rebuild its database on its own. It needs either a
  snapshot for the new version or the missing history. Until it is online, startup fails with a clear error. No local
  data is lost. Running the new version's checkpointer before releasing the update keeps this short.
- **Older history is not on the device.** On a client that started from a snapshot, reading the raw event history
  (for example in devtools) starts at the snapshot.
- **Snapshot size ceiling.** Installing a snapshot works up to about 1.6 GB of data and fails around 2 GB.
- **Events after the snapshot still replay one by one.** If the checkpointer falls far behind, startup slows down
  accordingly.

## Alternatives Considered

The main alternative was **compacting the event log**: removing events that later events make redundant (for example,
keeping only the last update to each record), or cutting off old history.

|                                 | Compaction                                                                                   | Snapshots                                      |
| ------------------------------- | -------------------------------------------------------------------------------------------- | ---------------------------------------------- |
| Startup cost                    | Still replays every remaining event; shrinks the log but stays proportional to it            | Download the current data, replay a short tail |
| History                         | Lost for good                                                                                | Kept in full                                   |
| Work for app authors            | Must declare, per event, which events replace which, and keep that correct                   | None                                           |
| Changes to sync and the backend | Large: the log is no longer append-only, and in-flight syncs must cope with rewritten ranges | Small, additive and opt-in                     |
| Risk if something is wrong      | Silent, permanent data loss                                                                  | Falls back to a full replay                    |

Compaction makes the history smaller, but snapshots attack startup cost directly without giving up anything that
inspection, debugging or future features may need from the history.

Other options we rejected:

- **Syncing the database page by page** (like Litestream): a second sync system tied to SQLite internals. A whole-file
  copy with one hash is simpler to get right.
- **Building snapshots inside the sync backend:** the backend would have to understand events, which it deliberately
  does not.
- **Letting any client publish snapshots:** clients hold unsynced changes and may run different app versions. A
  dedicated, read-only checkpointer only ever exports confirmed data.

## Open Questions

- What evidence should graduate snapshot boot from experimental to default?
- Should snapshots be served through a CDN or regional storage, closer to clients?
- Should a client that started from a snapshot be able to read older history from the backend on demand?
