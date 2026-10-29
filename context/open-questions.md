# LiveStore — Open Questions

- **LS-DQ1 Command/intent design.** RFC 0002 (command replay) is an open
  proposal: commands as replayable captures of user intent that re-validate
  against current state on rebase, spanning event model, sync, and the store
  API. Its design and coined terms live in RFC 0002, not in the tree, per
  [.decisions/0004-rfc-vrs-boundary.md](./.decisions/0004-rfc-vrs-boundary.md)
  (this resolved the earlier placement question). Unresolved: whether to accept
  the design and fold it into the owning nodes. Blocked on: a dedicated
  acceptance decision backed by design/prototype evidence (per the evidence
  conventions in [spec.md](./spec.md)). This is the single tree anchor for the
  proposal; node `DQ`s (e.g. LS.SYS.STORE-DQ1) cross-reference it.

- **LS-DQ3 Offline rebuild of a snapshot-booted client.** Experimental
  snapshot boot (`02-system/03-sync/04-snapshots/`, RFC 0005) lets a fresh
  client start its local eventlog at a snapshot base. If the app then ships a
  state schema change while that client is offline, the client can neither
  re-seed from a snapshot nor backfill history, so boot fails with a clear
  error instead of producing wrong state. This is a tension between LS-R03
  (offline by default) and LS-R06 (rebuildability) for opted-in clients.
  Options: keep the documented limit; let the client keep serving its old
  state database until it is online; or backfill history in the background
  after snapshot boot so the eventlog becomes complete. Blocked on: usage
  evidence from the experimental rollout.

All branch nodes are populated as drafts (2026-07-15); node-local design
questions live in each node's spec.
