# 0015 — Reject generic participant removal

Status: accepted (maintainer review, 2026-07-26)

Refines [decision 0003](./0003-local-process-and-browser-realizations.md) and
[decision 0014](./0014-add-participants-through-explicit-plan-steps.md).

## Context

The original lifecycle requirement used one verb, “remove,” without specifying
which system effect it represented. For a Client it could mean losing network
connectivity, terminating the running application, retaining or deleting local
persistent data, excluding an identity from a convergence expectation, or
revoking authorization. For a session it could mean closing a browser tab,
forgetting the session identity, or preventing a later restart.

These behaviors exercise different boundaries and have different recovery and
evidence semantics. Treating them as one portable operation would make a
Scenario readable but not precise about what the selected host actually did.
The implemented browser `stop-session` already closes the page and
`restart-session` reopens that retained identity; a separate generic removal
would add no current product behavior.

## Options

- **Add `remove-client` and `remove-session` with profile-defined meaning
  (rejected).** Cross-profile traces would use the same words for materially
  different runtime, storage, and membership effects.
- **Make session removal a terminal synonym for session stop (rejected).** This
  duplicates the physical control only to change whether the controller plans
  to use the identity again.
- **Retain precise existing controls and name any future behavior directly
  (chosen).** Connectivity, runtime lifecycle, persistent data, authorization,
  and Settlement membership remain distinct concepts.

## Decision

The portable Scenario AST does not define generic Client or session removal.
`disconnect` changes only a Client's backend connectivity. `stop-session`
closes the session runtime while retaining its stable identity and Client data;
a supported `restart-session` can use that identity again. A Settlement's
participant list states the convergence group for that barrier and does not
delete or revoke excluded participants.

Client-runtime termination, persistent local-data deletion, and authorization
revocation are unsupported by the current portable surface. Add one only when
a concrete Scenario justifies its distinct operation name, capability,
terminal outcome, persistence effect, and owning subsystem boundary. If a
Scenario needs to finish while a session remains stopped, terminal active-
participant accounting should follow the stop lifecycle rather than introduce
a removal synonym.

## Consequences

- DELTA-005 no longer treats Client/session removal as missing syntax.
- Closing and reopening a browser tab remains expressed by session stop and
  restart.
- Offline, terminated, deleted, excluded, and revoked are not interchangeable
  Scenario states.
- Future lifecycle additions must state whether identity and persistent local
  data survive and how Settlement membership is affected.
