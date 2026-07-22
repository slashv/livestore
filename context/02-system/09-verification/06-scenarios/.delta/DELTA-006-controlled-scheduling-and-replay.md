# DELTA-006 — Controlled scheduling and replay are incomplete

Status: open

## Divergence

Phase order provides sequence, and `parallel` joins a bounded set of retained
child operations after releasing their host requests from a shared invocation
barrier. The runner does not yet implement logical `at`/`after`, repetition,
condition waits, runner-owned delivery gates, recorded scheduling decisions, or
replay that reports its first divergent decision.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R08,
LS.SYS.VER.SCEN-R10, LS.SYS.VER.SCEN-R18`.

## Implementation Contract

Introduce explicit serializable schedule nodes and runner-owned logical time.
The controlled in-process profile must record decisions at the declared action,
fault, mock-backend response, delivery, and clock boundaries. Replay must gate
those boundaries according to the recording and fail explicitly at the first
unavailable or incompatible decision. Seed reproduction alone must not be
reported as interleaving replay.
