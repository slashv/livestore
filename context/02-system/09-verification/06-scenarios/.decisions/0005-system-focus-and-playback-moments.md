# 0005 — Separate trace visibility from playback navigation

Status: accepted (maintainer confirmation, 2026-07-21)

## Context

Raw scenario traces contain runner instructions, acknowledgements, repeated
sampling, semantic system observations, settlement polling, and verdicts. This
detail is necessary for forensic inspection, but record-by-record playback can
spend most of its time on records that do not change the system picture. Hiding
raw records entirely would remove the evidence needed to explain failures.

Visibility and playback answer different questions. Visibility controls which
evidence receives visual prominence. Playback controls which authoritative
cursor boundaries are visited over time. Coupling them would prevent useful
combinations such as showing all evidence while playing only material changes.

## Options

- **Always show and play every trace record (rejected).** This preserves raw
  evidence but makes ordinary scenario playback noisy and slow.
- **Filter records before projection (rejected).** Intermediate records may be
  necessary to reconstruct state at a later cursor, so this creates a second,
  misleading trace semantics.
- **Classify solely by record origin (rejected).** Some acknowledgements, such
  as Client creation or connectivity application, are material system
  transitions, while some observations merely repeat unchanged state.
- **Derive visibility and navigation independently over the immutable trace
  (chosen).** System focus uses payload semantics and projected state changes;
  moment playback visits selected raw boundaries while state reduction still
  consumes the complete prefix.

## Decision

The visualizer offers system-focused and all-records visibility. System focus
retains application actions, eventlog changes, topology, connectivity,
lifecycle, settlement, failures, and other material state changes. All-records
visibility exposes the complete trace and its evidence semantics.

The visualizer separately offers moment and record playback. Record playback
visits every observation-index boundary. Moment playback visits semantic
system-transition records plus the final record boundary of each observation
capture that materially changes projected system state. A moment may reference
multiple raw records, but it has exactly one authoritative cursor index.

Filtering never changes prefix reduction. At every selected cursor the viewer
reduces all preceding raw records, including records hidden from the current
visibility mode or skipped by the current playback mode.

## Consequences

- The default user-facing combination can be system focus plus moment
  playback, while all-records plus record playback remains the forensic view.
- The UI reports moment and raw-record positions together when moment playback
  is active.
- Observation captures remain non-atomic sampling passes; choosing their final
  boundary for navigation does not change that evidence claim.
- The classifier and material-state comparison are centralized and testable
  viewer projections, not LiveStore core instrumentation.
- Persistent material state such as Client disconnection can be projected as
  an interval across that Client's Leader-role and session lanes. Explicit
  transition boundaries are preferred; sampled boundaries remain visibly
  uncertain.
- New trace payload families require an explicit classification decision when
  they should become system-focused navigation points.
