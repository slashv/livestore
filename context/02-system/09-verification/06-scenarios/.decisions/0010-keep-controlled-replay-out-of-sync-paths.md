# 0010 — Keep controlled replay out of sync paths for now

Status: accepted (maintainer review, 2026-07-25)

Refines [decision 0001](./0001-declarative-scenario-verification.md) and
[decision 0008](./0008-parallel-operation-history-coverage.md).

## Context

The Scenario runner reproduces a normalized plan, seeded workload choices, and
requested operations, but it does not reproduce the internal delivery order
chosen by Effect scheduling, session↔Leader transport, Leader↔backend transport,
or a concrete backend. Bounded `parallel` steps prove that child operations
overlapped at the host request boundary; they do not fix the order of the sync
work triggered by those operations.

Faithful interleaving replay would require the runner to hold and release
individual deliveries and responses at synchronization boundaries. The current
Scenario connectivity wrapper can disconnect a Client, but it cannot gate
individual session↔Leader or Leader↔backend messages. Adding those gates solely
for Scenario replay would introduce control state and test-oriented seams into
production-adjacent adapters, transports, or sync execution paths.

## Options

- **Add delivery gates throughout the sync paths now (rejected).** This could
  reproduce selected races, but adds synchronization-path complexity before an
  independent product or debugging need justifies the same controls.
- **Call a seeded rerun controlled replay (rejected).** Repeating inputs and
  requested operations does not reproduce internal interleaving and must not be
  presented as if it does.
- **Retain seeded reproduction and advertise the limit (chosen).** Preserve the
  normalized plan, seed, explicit operation boundaries, and trace evidence
  while making internal delivery ordering an unsupported capability.

## Decision

Do not add session↔Leader, Leader↔backend, adapter, transport, processor, or sync
engine gates solely to support Scenario interleaving replay. Current artifacts
use seeded reproduction: they reproduce declared inputs and requested choices,
not internal delivery order or a byte-identical trace.

Recorded boundary replay remains a possible future capability when an owning
runtime, transport, sync, or observability subsystem independently introduces a
suitable controllable seam. A future profile must name the boundaries it
controls, record each release decision, and report the first replay divergence;
seed equality alone is never sufficient.

## Consequences

- A timing-sensitive failure may not recur when the same Scenario and seed are
  run again.
- Bounded parallel execution retains real overlapping host-operation evidence,
  but makes no claim about the resulting internal message order.
- Scenario artifacts and documentation continue to label reproduction as
  `seeded`; they do not advertise controlled interleaving replay.
- DELTA-006 closes as an accepted capability limit. It may be reopened only
  when independently justified control seams change the implementation cost.
