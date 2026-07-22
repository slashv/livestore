# 0004 — Separate causal order from calibrated elapsed time

Status: accepted (maintainer confirmation, 2026-07-21)

## Context

The first replay visualizer lays component observations out by runner trace
order. One observation pass can first see an event in the backend, several
Client Leader roles, and their sessions after the actual propagation has
already completed. Flattening that aggregate observation creates a convenient
sequence but no evidence that those component transitions happened in that
order. Treating the capture as one atomic moment would instead hide meaningful
propagation stages.

A causal-stage view alone can also hide operational delay. Two Leaders may be
sibling recipients of the same backend event while one is scheduled or applies
the event hundreds of milliseconds later. Contributors need to see both the
dependency structure and such elapsed-time offsets without introducing
timestamps into LiveStore's synchronization semantics.

The delayed-Client-B example used during review was the decisive scenario:
Leader A and Leader B remain sibling backend-propagation branches, while the
trace must also reveal that B received or applied the event materially later.

## Options

- **Use runner record or observation-capture order as the timeline (rejected).**
  This is reproducible and scrub-friendly but turns collection order into a
  false product execution order and can either serialize concurrent branches
  or collapse intermediate transitions missed between samples.
- **Use synchronized timestamps as the canonical order (rejected).** Clock
  calibration can expose latency, but timestamps do not prove why one event
  exists at another component. Skew, transport delay, and overlapping
  uncertainty can also produce false before/after claims.
- **Use only a causal-stage layout (rejected).** It explains propagation but
  can visually conceal a slow participant when independent branches occupy the
  same structural stage.
- **Preserve causal, local-order, and elapsed-time evidence separately and
  project them together (chosen).** Explicit boundary/control causation and
  participant-local sequence define supported partial-order facts. Calibrated
  monotonic time annotates those facts with an uncertainty interval. Flow and
  elapsed-time layouts remain interchangeable projections of the same trace.

## Decision

The canonical scenario trace represents a causal partial order, not a global
total order. Participant-local sequence and explicit instruction,
acknowledgement, boundary-transition, dependency, and causation records are the
ordering evidence. Correlation associates related records but creates no edge.
Independent records remain unordered even when one has a later timestamp.

Profiles that provide cross-process timing record participant-local monotonic
time and clock calibration sufficient to estimate a shared scenario-time
interval with explicit uncertainty. Coordinator receipt time and observation
capture membership remain distinct collection facts. Timestamp proximity,
event-field equality, and capture membership never create causal edges, and no
timestamp affects LiveStore synchronization.

The visualizer provides causal-flow and elapsed-time projections over the same
immutable records. Flow stages may align sibling propagation branches while
retaining visible latency evidence. Elapsed-time layout positions records by
their calibrated interval and does not force overlapping intervals into a
total order. Both retain the observation-index cursor, a raw trace carpet, and
drill-down to the evidence behind every aggregate.

Transition timestamps name what they measured. A boundary receive or apply
hook may claim that transition; a later sampled observation claims only
`firstObserved`. Scenario-side wrappers instrument existing boundaries first.
Any product change needed to expose an exact application transition is an
optional internal/dev observation seam owned by that subsystem, not a new
product event field or synchronization dependency.

## Consequences

- A Scenario observation capture is useful for scrubbing and compact trace
  grouping but is never presented as an atomic distributed moment.
- The same causal stage does not mean simultaneous; it means the displayed
  records occupy the same structural propagation role.
- A delayed sibling remains visible through elapsed-time position, wait/latency
  annotation, or both.
- Timestamp evidence that contradicts a known causal edge outside its stated
  uncertainty is surfaced as a calibration or instrumentation defect rather
  than used to reorder the causal graph.
- Exact transport-receive versus eventlog-apply latency is available only when
  both transitions are instrumented; snapshot polling cannot reconstruct it.
- The trace protocol and artifacts must retain raw local sequence, calibration,
  uncertainty, capture, and evidence-semantics fields even when a visual
  projection aggregates their records.
