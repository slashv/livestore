# 0014 — Add participants through explicit plan steps

Status: accepted (maintainer review and cross-profile conformance, 2026-07-26)

Refines [decision 0001](./0001-declarative-scenario-verification.md) and
[decision 0003](./0003-local-process-and-browser-realizations.md).

## Context

The runner previously created every declared Client and session before the
first phase. An initially disconnected Client can approximate a late join, but
it still constructs local State, the Store, and processors before backend
history exists. Likewise, stopping and restarting a page cannot prove that a
new session identity can first attach to an already-running browser Client and
Leader.

Every host already realizes `createClient`, so delaying that request does not
need a new sync boundary. The browser host also already owns the primitive that
opens a session page in an existing persistent context. The static assumptions
instead lived in Scenario validation, capability derivation, terminal snapshot
selection, and topology projection.

## Options

- **Predeclare dormant participants in the initial topology (rejected).** This
  preserves one static identity list but makes it ambiguous whether a declared
  participant exists before its activation step.
- **Carry participant definitions in explicit addition steps (chosen).** The
  initial topology describes startup reality and the plan records when each new
  identity begins to exist.
- **Add participant lifecycle controls to adapters or the sync engine
  (rejected).** Hosts can invoke ordinary Store and browser-page creation paths;
  no Scenario-specific product seam is needed.
- **Add removal in the same slice (rejected).** Removal introduces separate
  convergence-membership, persistent-storage, teardown, and identity-reuse
  semantics that addition does not require.

## Decision

The portable AST supports sequential `create-client` and `add-session` steps.
`create-client` carries a complete initial Client definition with at least one
session. `add-session` carries a new participant reference whose Client must
already exist. Addition operations are not valid inside `parallel` v1 groups.

Validation processes phases and steps in order. Duplicate identities, use
before creation, session addition before Client creation, and eventual
per-Client session counts fail preflight before any Client is created. Oracles
and terminal snapshots may select identities that a prior plan step creates.

Dynamic Client creation reuses the existing host `createClient` request and is
advertised by the in-process, process, and browser profiles. Dynamic session
addition uses a separate host request and is currently advertised only by the
browser profile, which opens a new page in the Client's existing persistent
browser context. The new page therefore uses the normal SharedWorker, Web Lock,
OPFS, Store, materializer, and sync paths.

Creation acknowledgements prove only that the host completed Store or page
startup. System observations establish appearance, and explicit Settlement and
oracles establish catch-up, pending resolution, Eventlog equality, or State
convergence. The operations add no adapter, transport, processor, or sync-engine
control.

## Consequences

- A Scenario can create an empty local Client after authoritative backend
  history exists and write while its background catch-up is settling.
- A browser Scenario can attach a genuinely new session to an existing Client
  and exercise it through ordinary actions and observations.
- Visual projections retain all plan-declared lanes but activate them only at
  their acknowledged creation boundaries.
- Generic removal is not included; [decision
  0015](./0015-reject-generic-participant-removal.md) separates the possible
  runtime, persistence, authorization, and Settlement semantics.
