# 0001 — Declarative, production-shaped scenario verification

Status: draft (awaiting acceptance of RFC 0003 in livestorejs/livestore#1442)

## Context

Focused unit, integration, conformance, determinism, and performance tests do
not provide one reproducible way to exercise the complete sync system across
multiple clients and sessions, changing topology, faults, recovery,
materialization, and runtime boundaries. A scenario architecture must produce
portable correctness evidence without replacing the LiveStore components it
is intended to verify.

## Options

- **Declarative typed scenarios with a versioned serializable AST (chosen).**
  Contributors author TypeScript through Effect Schema-backed constructors;
  the normalized AST makes control flow, time, randomness, faults, and oracles
  inspectable and replayable. Arbitrary TypeScript or Effect orchestration was
  rejected because hidden executable behavior cannot be validated, migrated,
  replayed, or visualized uniformly. YAML or JSON as the primary authoring
  format was rejected because it duplicates application types and weakens
  inference; JSON remains an encoding for the normalized AST and artifacts.
- **Production-shaped in-process execution with optional cross-profile
  evidence (chosen).** The primary correctness profile runs real Stores,
  sync processors, materializers, and SQLite behind controlled in-memory
  boundaries. Pure `SyncState` model testing alone was rejected because it
  omits queues, retries, cursors, lifecycle, and materialization. Requiring
  every participant to run in a browser or container was rejected because it
  makes dense deterministic exploration too expensive. Participant execution
  profiles and sync-backend realizations remain orthogonal without requiring
  their full Cartesian product or one-to-one equivalent outcomes.
- **Read-model-independent scenario semantics with SQLite in the initial full
  stack (chosen).** SQLite participates in the first production-shaped profile
  because current rebases, transactions, and failure behavior depend on it,
  but it is not part of scenario-level sync semantics. Making SQLite permanent
  in the language and omitting materialization from the initial runner were
  both rejected.
- **Headless authority behind a normalized trace boundary (chosen).** Oracles,
  run artifacts, live visualization, and replay consume the same versioned
  semantic trace. Coupling execution to a dashboard was rejected because it
  prevents cheap CI runs and makes reproduction dependent on the UI.

## Decision

Adopt the four chosen architectural constraints above. The runner, participant
hosts, profiles, backend realizations, trace consumers, and visualizer may
evolve independently only while preserving the scenario semantics and the
serializable control, trace, and artifact boundaries.

Evidence: [RFC 0003: Scenario-Based Sync
Verification](../../../../../contributor-docs/rfcs/0003-scenario-based-sync-verification.md),
pending acceptance in livestorejs/livestore#1442. Replace this draft status
with the merge date when the RFC is accepted.

## Consequences

- The subsystem is privately owned under `tests/scenarios/`; product packages
  must not depend on it.
- Exact constructor ergonomics, action/inspector APIs, fault-injection seams,
  trace retention, adversarial modes, failure minimization, and performance
  reuse remain design questions rather than hidden choices in this record.
- RFC 0003's delivery sequence remains historical implementation guidance, not
  a normative ordering constraint in the intent layer.
- The absence of the accepted subsystem is recorded by the node's initial
  umbrella delta; later partial implementation is represented by narrower
  deltas for the remaining contract gaps.
