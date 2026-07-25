# DELTA-012 — Snapshot oracles can run without terminal Settlement evidence

Status: closed (2026-07-25) — resolved by construction-time and runner
preflight validation of the terminal Settlement boundary.

## Resolution

Scenario validation now derives the union of participants selected by
snapshot-based oracles and requires the final plan step to be a Settlement
covering that union. This rejects missing evidence boundaries, modifying
operations after an earlier Settlement, and incomplete convergence groups.
Operation-history-only Scenarios remain valid without Settlement.

Runner preflight repeats the same normalized-AST validation before emitting
run evidence or creating participants, so manually constructed typed values
cannot bypass the contract.

## VRS

[spec.md](../spec.md) `LS.SYS.VER.SCEN-R14, LS.SYS.VER.SCEN-R15`.
