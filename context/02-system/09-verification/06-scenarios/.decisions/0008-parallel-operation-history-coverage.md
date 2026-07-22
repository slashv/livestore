# 0008 — Preserve child operations across bounded parallel scheduling

Status: accepted (maintainer direction, 2026-07-22)

## Context

The first runner executed every plan step serially. Its derived Scenario
operation history could classify retained outcomes, but it had no overlapping
invocation intervals and no checker consumed the projection. Treating a
parallel container as one operation would hide the child boundaries that a
history-based property needs.

## Options

- **Keep serial execution and synthesize overlap from logical time
  (rejected).** Equal logical time does not prove that two operations were in
  flight together.
- **Give only the parallel container an invocation/outcome boundary
  (rejected).** This erases the identities and individual outcomes of the
  operations whose interaction is under test.
- **Treat parallelism as orchestration over retained child operations
  (chosen).** Each child keeps its own instruction, Control acknowledgement or
  failure outcome, and invocation/outcome interval.

## Decision

A `parallel` plan step contains two or more ordinary non-settlement Scenario
operations. The runner records every child instruction, releases the child
host requests only after every sibling has reached that invocation boundary,
and then executes them concurrently. It waits for every child outcome so one
failure does not erase a sibling's evidence. A failing group terminates the run
after retaining each available child outcome and identifies a failing child as
the terminal step.

The derived Scenario operation history declares its included operation
families, excluded evidence-gathering interactions, and its
instruction-to-Control-outcome concurrency boundary. Completeness is always
relative to that declaration. The first history oracle checks named operations
for terminal outcomes, optional rejection of indefinite outcomes, and optional
observed interval overlap. It does not claim linearizability, serializability,
or a general consistency model.

## Consequences

- The parallel container is a scheduling construct, not an additional
  operation-history entry.
- Internal system/sync observation and State-inspection requests remain
  outside the declared application/control history coverage.
- Nested settlement fault-removal controls retain their own failure outcomes
  in addition to the enclosing settlement failure.
- The new AST and oracle variants are additive. Trace payloads and artifact
  envelopes remain version 3 and version 4, and existing saved/reference
  artifacts remain loadable.
