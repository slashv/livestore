# 0006 — Preserve failed runs as replayable artifacts

Status: accepted (maintainer confirmation, 2026-07-21)

## Context

A convergence timeout is often the most valuable outcome of a sync scenario.
If execution throws before encoding its trace, the runner discards the sampled
heads and pending state needed to distinguish a slow system from a stuck or
divergent one. Extending the timeout can hide the symptom and makes the runner
less useful as a focused debugging tool.

Preflight errors such as an unsupported execution profile are configuration
failures. Once a run has started, however, an operation or settlement failure
is evidence about that execution and belongs in the same artifact protocol as
a passing run.

## Options

- **Throw and discard partial execution (rejected).** This preserves a simple
  success-only result type but loses the evidence that explains the failure.
- **Keep polling with a generous recovery timeout (rejected).** This delays
  feedback and can turn a reproducible stuck state into an opaque test timeout.
- **Persist a terminal failure with the partial trace (chosen).** A failed
  operation produces structured failure records and a valid failed artifact;
  command-line execution still exits unsuccessfully.

## Decision

After `run.started`, execution failures are captured as a valid run artifact
with `status: failed`. A failed settlement records its declared timeout, stable
error classification, and the latest successfully sampled observations for
every expected participant before the run emits its terminal failure record.
The trace retains the active phase, step, and correlation identity. An
Operation outcome separately records definite failure or indefinite completion
when a request boundary is lost; neither replaces participant runtime,
settlement, or terminal run failure records.
Participant hosts also drain runtime failures exposed by their execution
boundary. For example, the browser host records a LiveStore worker error as a
participant-scoped `runtime.failure.observed` record before settlement and run
failure records terminate execution. This is diagnostic capture at the host
boundary, not new LiveStore synchronization behavior.
An observed participant runtime failure takes precedence over a later sampled
head predicate; a runtime that has shut down cannot be declared converged merely
because its last reported global head matches the backend.

Settlement deadlines remain explicit scenario inputs and should be short
enough for interactive diagnosis. The representative browser workload uses a
15-second convergence budget rather than extending a suspected failure to two
minutes.

The CLI writes and catalogs the failed artifact before returning a non-zero
exit status. The visualizer treats settlement and run failures as system
moments and renders a prominent failure boundary in both the main timeline and
range overview.

## Consequences

- A sync failure can be opened and scrubbed without rerunning the scenario.
- The final sampled heads and pending counts remain available even when final
  oracle snapshots could not be captured.
- `snapshots` and `verdicts` may be empty on an execution-failed artifact; the
  structured failure records explain why evaluation did not complete.
- Preflight validation may still fail without an artifact because no run or
  participant execution began.
- Capturing the failure does not convert it into success: automation receives
  a non-zero CLI result and the artifact remains visibly failed.
