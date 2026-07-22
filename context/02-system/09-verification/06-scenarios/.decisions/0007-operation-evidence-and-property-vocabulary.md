# 0007 — Separate operation evidence, convergence, and property verdicts

Status: accepted (maintainer confirmation, 2026-07-22)

## Context

The scenario trace already retains more than a request/response history:
instructions, participant-host acknowledgements, sampled observations, runtime
diagnostics, settlement records, oracle verdicts, and terminal run state. The
previous vocabulary overloaded acknowledgement as success, correlation as
causation, fault removal as recovery, and settlement as both convergence and
correctness evaluation. Process timeouts and browser evaluation failures also
show why absence of a response cannot prove that an operation did not occur.

## Options

- **Treat acknowledgement as distributed completion and settlement as the
  final verdict (rejected).** A Participant host can confirm only its own
  handling boundary, and a property can fail after participants converge.
- **Call the entire Scenario trace an operation history (rejected).** The trace
  is a richer evidence envelope, while complete history-based checking needs
  explicit invocation/outcome boundaries for failed, indefinite, and
  overlapping operations.
- **Separate operation evidence, fault lifecycle, convergence, and property
  evaluation (chosen).** Preserve their relationships without collapsing one
  boundary into another.

## Decision

Each runner-invoked Scenario operation has a stable identity spanning its
instruction, Participant-host response, related observations, and Operation
outcome. A Control acknowledgement proves only that the host completed handling
the request at its advertised boundary. Outcomes are successful, definite
failure, or indefinite; loss of a completion response never proves the
operation did not occur.

Correlation groups related evidence and creates no ordering edge. Scenario
causal order consists only of participant-local sequence and explicit
dependency or causation edges. A Scenario operation history is a derived
projection over retained invocation/outcome boundaries, not a synonym for the
full Scenario trace and not complete until required failure and concurrency
boundaries exist.

The adverse-condition lifecycle is Fault injection, Fault removal, separately
observed Recovery, Quiescence, and Convergence. A Settlement barrier is the
bounded profile-specific confirmation of stable Convergence. Scenario oracles
then evaluate declared Scenario properties and emit Scenario verdicts. A
property violation can fail the run after successful settlement without
retroactively changing the settlement result. Participant runtime failure,
Operation outcome, settlement failure, Scenario verdict, and terminal run
status remain distinct evidence families.

Evidence: maintainer-approved vocabulary and first implementation slice on
2026-07-22, informed by comparison with the Antithesis reliability glossary.

## Consequences

- Existing serialized acknowledgement payload names may remain for artifact
  compatibility, but UI and documentation call their boundary a Control
  acknowledgement.
- Participant-host failure categories describe infrastructure, request
  rejection, invalid response, response timeout, or transport failure
  independently from definite/indefinite Operation outcome certainty.
- Failure-only outcome records may be added compatibly while successful
  outcomes are projected from retained acknowledgements.
- Sampled observations may share a correlation ID with an operation without
  claiming a causal edge.
- The current stable-poll settlement implementation is correctly scoped to
  convergence; oracle evaluation remains a later stage.
- Explicit disconnect-fault, Quiescence, and Recovery evidence landed in the
  first follow-up slice; complete concurrent histories and broader fault models
  remain tracked deltas rather than being hidden by stronger wording.
