# 0011 — Model baseline backend outage at the participant route

Status: accepted (maintainer review and local hard-cut experiment, 2026-07-25)

Refines [decision 0001](./0001-declarative-scenario-verification.md) and
[decision 0010](./0010-keep-controlled-replay-out-of-sync-paths.md).

## Context

The Scenario baseline needs a real backend-availability fault without adding
test-only state or recovery controls to LiveStore's sync engine. For local
`sync-cf`, Wrangler owns a Worker and SQLite Durable Object whose persisted state
should remain real and inspectable while participant traffic is unavailable.

An experiment that destroyed active proxied TCP sockets produced the stronger
failure, but existing long-lived sync work did not fully recover after the new
WebSocket connection appeared. Making that case a passing baseline would require
changes in production sync recovery paths, which are not justified solely by
Scenario testing at this stage.

## Options

- **Stop and restart Wrangler or its Durable Object (rejected for the
  baseline).** This combines route loss with backend-process and runtime
  lifecycle behavior, makes authoritative observation unavailable during the
  fault, and adds slower state/lifecycle coordination.
- **Destroy active participant sockets (rejected for the baseline).** This is a
  useful future recovery scenario, but the experiment exposed sync recovery
  implications outside the Scenario workspace.
- **Withhold traffic at a stable participant-route proxy (chosen).** This models
  a transient network blackhole around the real backend while retaining active
  transport objects and backend state.
- **Use only the controlled in-memory backend (rejected as sole evidence).** It
  is useful for fast contract tests but does not exercise the production
  WebSocket client, local Worker, or Durable Object.

## Decision

The portable operations are `backend-unavailable` and `backend-available`.
Local `sync-cf` participants connect through a Scenario-owned TCP proxy. Fault
injection pauses reads on existing proxy sockets and rejects connections opened
while unavailable. Fault removal resumes those sockets. Wrangler, the Worker,
and Durable Object keep running throughout.

The backend observer connects directly to Wrangler, outside the affected
participant route, so the runner can retain authoritative Eventlog evidence
during the outage. The mock realization maps the same operations to its existing
availability control. Every participant profile advertises the capability only
when its selected backend supplies this control.

Injection and removal become trace facts only after the host acknowledgement and
a system observation of the requested backend availability. Recovery remains a
separate settlement observation and requires the full convergence predicate.

## Consequences

- In-process, process, and browser participants exercise the same real local
  backend route fault without sync-engine changes.
- Writes made during the blackhole exercise real queuing, push, pull, rebase,
  materialization, and eventual convergence when traffic resumes.
- This capability does not claim recovery after an established TCP/WebSocket is
  destroyed, a Worker or Durable Object restarts, or Wrangler is killed.
- Hard socket cuts and backend-process restart remain possible future profiles
  when their sync-runtime implications are independently justified.
