# DELTA-002 — Scenario operation history is incomplete

Status: closed (2026-07-22) — resolved by bounded parallel scheduling,
declared history coverage, and a history-based oracle.

## Resolution

The runner now supports a `parallel` orchestration step whose child operations
retain independent identities and instruction/outcome intervals. Every child
instruction reaches the trace before the group releases host requests, and the
runner awaits all child exits so successful, definite-failure, and indefinite
outcomes remain visible even when one sibling fails.

The derived projection declares coverage for Client creation, application
actions, connectivity, session and Client lifecycle, and settlement across the
instruction-to-Control-outcome boundary. It explicitly excludes system/sync
sampling and State inspection from that history rather than silently claiming
they are covered. Nested settlement fault-removal controls now retain their own
failure outcomes as well as the enclosing settlement result.

An `operation-history` oracle checks named operations for terminal outcomes,
can reject indefinite outcomes, and can require evidence that invocation
intervals overlapped. The portable offline-writer corpus uses this checker for
its concurrent writes. This is a complete concurrent history only for its
declared coverage and makes no linearizability, serializability, or general
consistency-model claim.

Existing trace payloads and artifact envelopes remain version 3/version 4;
the scheduling and oracle AST variants are additive and existing saved and
reference artifacts remain loadable.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R21` and
[decision 0008](../.decisions/0008-parallel-operation-history-coverage.md).
