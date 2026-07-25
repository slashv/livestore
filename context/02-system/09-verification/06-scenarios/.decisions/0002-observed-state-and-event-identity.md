# 0002 — Project observed state with run-local event identity

Status: accepted (maintainer confirmation, 2026-07-20)

Trace ordering and timeline projection are refined by
[decision 0004](./0004-causal-order-and-calibrated-time.md).
The scope of product instrumentation for Event lineage is refined by
[decision 0009](./0009-keep-scenario-lineage-out-of-sync-engine.md).

## Context

A replay visualizer must let a contributor scrub a scenario trace and inspect
how backend, Client, Leader-role, and session state appeared at that point. A
distributed run has no atomically observable global state at a wall-clock
instant, and LiveStore event sequence numbers identify eventlog positions that
can change through rebasing. Event name, arguments, origin, and current position
therefore cannot always correlate one event across components, especially when
the same session emits equivalent events more than once.

The trace must remain grounded in actual LiveStore behavior. A scenario-side
model inferred from runner instructions could diverge from the Stores,
processors, eventlogs, and backend that the scenario is intended to verify.

## Options

- **Project the trace prefix at an observation cursor (chosen).** Scrubbing
  selects a monotonic runner-observation index and reconstructs the runner's
  accumulated observed state after that record. Claiming an atomic global
  snapshot was rejected because participant observations arrive separately.
  Requiring logical or wall-clock time as the timeline coordinate was rejected
  because presentation spacing does not define cursor semantics.
- **Add a minimal run-local event reference over real event facts (chosen).**
  The trace embeds actual encoded LiveStore event data and positions, assigns an
  opaque reference at first observation, and carries it through explicit
  rebase, confirmation, and propagation mappings emitted by real sync
  transitions. Correlation solely by event fields was rejected because
  positions change and equivalent events can repeat. Adding an immutable field
  to the product event schema was rejected because scenario visualization does
  not justify redesigning LiveStore events.
- **Observe product transitions rather than simulate them (chosen).** Existing
  sync-state, eventlog, backend, DevTools, and internal observation surfaces are
  reused where sufficient. When they cannot expose a portable fact, a profile
  advertises that capability limit unless the owning subsystem independently
  justifies a new observation seam. Deriving state changes from scenario
  instructions was rejected because an instruction is not proof that product
  state changed.

## Decision

Adopt all three chosen options. Observation index defines scrub boundaries;
uniform observation-order and relative-wall-time layouts are interchangeable
views of the same trace. Logical time remains a scheduling and reproduction
concept and is not required as the visual timeline axis.

Evidence: maintainer confirmation during the first scenario-runner
implementation review on 2026-07-20, using the initial replay-visualizer design
as the concrete target.

## Consequences

- The portable trace needs component-scoped observations for the backend,
  Client Leader roles, and Client sessions rather than one conflated sync row.
- A profile claiming exact Event lineage must expose explicit old/new position
  mappings at rebase and confirmation points. Current profiles omit that
  capability rather than adding Scenario-only sync-engine instrumentation.
- Timeline causality is not inferred from temporal proximity; arrows use event
  references plus correlation and causation identifiers.
- A pure trace-prefix projector becomes a testable contract shared by replay
  visualization and artifact inspection.
- Full projection checkpoints may be stored for seek performance, but they are
  derived caches rather than authoritative evidence.
- The first replay projection is limited to sync and eventlog state.
  Materialized application State remains explicit inspector-snapshot evidence
  rather than a value captured at every cursor.
