# State Snapshots — Requirements

Role: `04-snapshots/` owns snapshot boot: how a fresh client installs a state
snapshot published next to the sync backend and pulls only the events after
it, so that startup cost depends on state size rather than history length. The
whole node is experimental (LS.SYS-R06) and changes nothing unless a client
opts in.

## Context

Builds on [../requirements.md](../requirements.md) (LS.SYS.SYNC-R01,
LS.SYS.SYNC-R06), [../03-cf/requirements.md](../03-cf/requirements.md), the
state contract ([../../02-state/requirements.md](../../02-state/requirements.md))
and its SQLite realization's schema management
([../../02-state/01-sqlite/02-schema-management/requirements.md](../../02-state/01-sqlite/02-schema-management/requirements.md)),
and root LS-R04, LS-R05, LS-R06. Code: `packages/@livestore/common/src/snapshot/`
(key, manifest, source/sink, trigger, in-memory store) and
`SyncOptions.snapshot` in `packages/@livestore/common/src/sync/sync.ts`.
Proposal history: [RFC 0005](../../../../contributor-docs/rfcs/0005-snapshot-boot.md);
decision: [.decisions/0001](./.decisions/0001-snapshot-boot-over-compaction.md).

## Assumptions

- **LS.SYS.SYNC.SNAP-A01 Cold-boot-heavy workload:** The motivating workload
  is many short-lived clients per system (on the order of 100 AI agents) that
  run in fresh sandboxes, restart often, and append at high rates. Each restart
  is a cold boot with an empty local eventlog, so without snapshots every
  restart replays the store's whole history.
- **LS.SYS.SYNC.SNAP-A02 Materializer changes ship under new event names:** A
  change to what an event does to state is shipped as a new event definition
  with a new (versioned) name, e.g. `v2.TodoCreated`, never by editing the
  materializer of an existing name (LS.SYS.EVT-R01 convention). Materializer
  changes are therefore forward-only: events already in the log keep the
  effect they had. Neither the state fingerprint nor the event-definition hash
  covers materializer code (see
  [schema management](../../02-state/01-sqlite/02-schema-management/spec.md#state-fingerprint-contract)),
  so a snapshot key cannot detect a violation. Mirrors
  LS.SYS.STATE.SQLITE-A01.

## Acceptable Tradeoffs

- **LS.SYS.SYNC.SNAP-T01 Bounded staleness:** A booting client pays at most
  about `(1 + alpha)` times the snapshot download, plus the replay of events
  appended since the latest snapshot. Snapshot freshness is traded against
  checkpointer work and object storage.
- **LS.SYS.SYNC.SNAP-T02 Partial local history:** A snapshot-booted client
  holds no local events below its eventlog base. Local history reads and a
  local full rebuild need the backend (see LS.SYS.SYNC.SNAP-R10 and the open
  questions in [spec.md](./spec.md#open-design-questions)).

## Requirements

### Opt-in and history

- **LS.SYS.SYNC.SNAP-R01 Opt-in:** Snapshot boot runs only when the client
  configures `syncOptions.snapshot`. Without it, boot, sync, rebuild and
  storage behave exactly as before. `refines: LS.SYS-R06`
- **LS.SYS.SYNC.SNAP-R02 History is kept:** Publishing or installing a
  snapshot never truncates, compacts, or rewrites the sync backend's eventlog.
  The backend log remains the complete canonical history.
  `refines: LS-R04, LS.SYS.EVT-R07`
- **LS.SYS.SYNC.SNAP-R03 Provider contract unchanged:** Snapshot reading and
  publishing go through a separate snapshot source/sink contract; the sync
  provider contract (`connect/pull/push/ping`) does not change. A provider
  realization may host snapshot manifests next to its log.
  `refines: LS.SYS.SYNC-R02`

### Client boot

- **LS.SYS.SYNC.SNAP-R04 Eligibility:** Only a client whose local eventlog is
  empty (no events, upstream head at the root, no eventlog base) attempts
  snapshot boot. A client with local history keeps booting from its own
  eventlog and state.
- **LS.SYS.SYNC.SNAP-R05 Key compatibility:** A client installs only a
  snapshot whose key equals its own: state schema fingerprint, event-definition
  hash, and storage format version. `refines: LS.SYS.STATE.SQLITE.SM-R01`
- **LS.SYS.SYNC.SNAP-R06 Backend identity:** A snapshot applies only to the
  backend identity (`backendId`) it was derived from. After a backend reset,
  earlier snapshots are not served and are never installed.
- **LS.SYS.SYNC.SNAP-R07 Verified install:** Before a snapshot replaces the
  client's state, its byte length and SHA-256 match the manifest, its state
  head equals the manifest position, and its rebuild-complete marker is
  present. `refines: LS.SYS-R02`
- **LS.SYS.SYNC.SNAP-R08 Fallback, never failure:** Any snapshot failure
  (none published, network error, timeout, integrity or validation failure)
  is logged and boot falls back to full replay from the backend. Snapshot boot
  never fails a boot that full replay would complete.
- **LS.SYS.SYNC.SNAP-R09 Eventlog base:** A snapshot-booted client records the
  snapshot position as its eventlog base. Its local eventlog continues after
  the base, its upstream and local heads start at the base, and pulling
  resumes from the base with the manifest's cursor metadata.
  `refines: LS.SYS.SYNC-R06, LS.SYS.EVT-R08`

### State equivalence and rebuild

- **LS.SYS.SYNC.SNAP-R10 Equivalent state:** Under LS.SYS.SYNC.SNAP-A02,
  installing a snapshot at position `p` and materializing the events after
  `p` produces the same state as replaying the full backend log.
  `refines: LS-R05, LS-R06, LS.SYS.STATE-R03`
- **LS.SYS.SYNC.SNAP-R11 Rebuild with a partial eventlog:** When a client
  whose eventlog starts at a base must rebuild state (state schema change or
  missing state), it either re-seeds from a compatible snapshot or backfills
  the missing history from the backend before a full replay. It never
  rebuilds from the partial local eventlog alone.
  `refines: LS.SYS.STATE.SQLITE.SM-R01`

### Production

- **LS.SYS.SYNC.SNAP-R12 Passive checkpointer:** Snapshots are produced by a
  checkpointer, an ordinary LiveStore client of the store that pulls but never
  commits or pushes. It cannot change the canonical log.
- **LS.SYS.SYNC.SNAP-R13 Consistent export:** An exported snapshot reflects
  exactly the confirmed events up to its manifest position, with no pending or
  partially materialized events.
- **LS.SYS.SYNC.SNAP-R14 Safe publication:** A sink makes a blob readable
  before it publishes the manifest that points to it, keeps a replaced blob
  readable for a grace period, and only advances a key's manifest to a higher
  position.
- **LS.SYS.SYNC.SNAP-R15 Cost-based trigger:** The checkpointer publishes a
  new snapshot only when replaying the tail would cost more than a fixed share
  of downloading the snapshot, subject to a minimum tail and a minimum
  interval. A store with no new events publishes nothing.
