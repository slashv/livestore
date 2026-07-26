# 0013 — Keep application mutations behind named actions

Status: accepted (maintainer review, 2026-07-26)

Refines [decision 0001](./0001-declarative-scenario-verification.md) and
[decision 0012](./0012-resolve-seeded-workloads-by-name.md).

## Context

The original Scenario surface called for both direct schema-event commits and
named application actions. A direct-event step would still need a schema-known
event name and valid encoded payload; it would bypass only the application
action registry. Supporting that second mutation path would require its own AST
variant, validation, capability, host request, trace instruction/outcome,
projection, visualization, and cross-profile conformance coverage.

Current Scenarios do not need that duplicate path. When event-level control is
useful, an application fixture can expose a low-level or test-only named action
that validates its input and commits exactly one schema event through the real
typed Store. That keeps event construction with the application definition and
uses the existing portable host boundary.

Direct-event syntax would not by itself test incompatible application versions
or unknown events. True version-skew testing requires different application or
schema definitions to be loaded for different Clients, while malformed-event
testing belongs at a deliberately adversarial boundary rather than a normal
schema-valid commit path.

## Options

- **Add direct schema-event Scenario steps now (rejected).** This duplicates the
  named-action execution and evidence path without a concrete Scenario that
  needs generic event dispatch.
- **Treat arbitrary encoded or unknown events as direct commits (rejected).** A
  normal Store commit remains schema-valid and cannot represent corruption or
  an event unknown to the loaded application.
- **Use named actions for all portable application mutations (chosen).** A
  fixture may make an action intentionally thin or test-only when a Scenario
  needs direct control over one schema event.

## Decision

Named actions are the sole application-mutation operation in the portable
Scenario AST and participant-host contract. Do not add a direct schema-event
step, capability, host request, or trace family for the current Scenario
baseline.

Reconsider a generic direct-event surface only when a concrete need such as
schema-driven event fuzzing or migration coverage cannot be expressed
reasonably through application-owned actions. Multi-version Client execution
and malformed transport or storage events remain separate capabilities and
must not be implied by direct schema-event syntax.

## Consequences

- Application fixtures retain responsibility for constructing and committing
  valid application events.
- Event-level Scenarios can use explicit low-level named actions without
  changing the runner or participant-host protocols.
- DELTA-005 no longer counts direct schema-event steps as implementation drift.
- Generic schema fuzzing, cross-version execution, or adversarial invalid-event
  injection would require separately justified contracts.
