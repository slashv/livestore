# DELTA-010 — Host conformance is not fully shared and capability-driven

Status: closed (2026-07-25) — resolved by derived preflight requirements and a
shared capability-parameterized suite for every implemented host profile.

## Resolution

Host capabilities now use a closed typed vocabulary. Runner preflight derives
requirements from Client/session topology, action and connectivity operations,
session and Client lifecycle operations, mandatory system/sync observations,
and state-inspecting oracles. It unions those facts with explicit
platform-specific requirements and rejects missing capabilities or excessive
session counts before emitting run evidence or creating a Client.

`host-conformance.test.ts` runs one shared harness against the in-process,
process, and browser factories using each factory's advertised capability set.
It verifies stable instruction/outcome identity, Control acknowledgements,
action and connectivity controls, observation and Settlement evidence,
state inspection, capability-gated session and Client lifecycle isolation,
known definite rejection, synthetic lost-response indefiniteness, preflight
rejection, valid artifacts, and scoped cleanup. Process IDs and browser profile
directories are observed while live and verified absent after the host scope
closes.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R04,
LS.SYS.VER.SCEN-R05`.

## Accepted Contract

Future host profiles join the same factory table and must pass the shared
contract for every capability they advertise. Structural requirements remain
runner-derived; explicit `requires` entries are reserved for additional
platform or evidence guarantees that cannot be inferred from the Scenario AST.
