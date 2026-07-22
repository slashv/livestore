# DELTA-007 — Scenario fault-model breadth is incomplete

Status: open

## Divergence

The implemented fault model covers Client connectivity disconnect/reconnect,
including separately observed injection, removal, recovery, and convergence.
It does not provide portable backend-availability, latency, constrained
throughput, controlled delivery, or participant-process death faults with
declared assumptions and activation evidence.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R11`.

## Implementation Contract

Add first-class fault definitions and operations that distinguish a requested
injection, observed activation, requested removal, observed removal, and later
Recovery. At minimum, the baseline must exercise backend unavailability through
the controlled in-process backend. Each profile must advertise unsupported
faults rather than silently approximating them.
