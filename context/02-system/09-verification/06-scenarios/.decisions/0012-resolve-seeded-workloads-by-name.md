# 0012 — Resolve seeded workloads by name outside the Scenario AST

Status: accepted (maintainer review and cross-profile conformance, 2026-07-26)

Refines [decision 0001](./0001-declarative-scenario-verification.md) and
[decision 0010](./0010-keep-controlled-replay-out-of-sync-paths.md).

## Context

Corpus helpers can generate many ordinary action steps while a module loads,
but that erases the compact pattern from the serialized Scenario and often
hard-codes a seed inside the helper. Embedding a generator callback in the AST
would make the plan non-portable. Sending workload definitions to participant
hosts would also enlarge a boundary that already supports ordinary named
application actions.

## Options

- **Commit only eagerly expanded action lists (rejected).** The result is
  executable but does not retain the reusable pattern or its seed boundary.
- **Embed generator callbacks in TypeScript Scenario nodes (rejected).** This
  breaks canonical serialization and transport-neutral validation.
- **Resolve a stable workload name from the application definition (chosen).**
  The AST retains only data, while the controller expands the trusted local
  definition into ordinary host actions.
- **Teach every participant host about workloads (rejected).** Hosts need no new
  behavior because generated work already crosses their named-action contract.

## Decision

A portable `workload` step contains its ID, stable workload name, JSON input,
declared participant targets, and an integer action count between 1 and 10,000.
The application definition owns a workload library keyed by stable name. A
workload definition schema-decodes its input and emits exactly one serializable
named action for each zero-based iteration.

Before any Client is created, the runner resolves and expands every workload
once. It derives a workload-specific seed from the Scenario seed, phase ID, step
ID, and workload name. The definition receives only its decoded input, declared
targets, iteration, and a deterministic random source. An unknown workload,
invalid input, unserializable output, undeclared target, or generated ID
collision fails preflight.

Generated action IDs use the enclosing workload ID plus a stable ordinal. Each
action produces the ordinary action instruction and Control outcome, linked to
the workload by causation. The workload itself retains a separate
instruction/outcome boundary containing its derived seed and generated action
IDs. Operation-history projection declares both workload and application-action
families.

Workload v1 dispatches generated actions sequentially. It does not claim
controlled sync interleaving, and it introduces no adapter, transport,
processor, or sync-engine control.

## Consequences

- The same compact Scenario and application workload library reproduce the same
  generated action sequence for the same source revision and seed.
- Artifacts retain the compact authored workload and the fully emitted action
  evidence without serializing callbacks.
- In-process, process, and browser profiles share the feature because hosts see
  only their existing named-action requests.
- Rate-based generation, stop conditions, generated parallelism, and workload
  nesting require separate future contracts.
