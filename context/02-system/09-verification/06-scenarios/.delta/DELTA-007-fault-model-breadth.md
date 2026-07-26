# DELTA-007 — Scenario fault-model breadth is incomplete

Status: closed (2026-07-25) — resolved by adding a portable backend-
availability fault backed by a controlled mock and a real local `sync-cf`
participant-route proxy.

## Resolution

The Scenario AST now has first-class `backend-unavailable` and
`backend-available` operations, and capability derivation rejects a profile
whose selected host/backend composition cannot realize them. Requested
availability, host acknowledgement, first-observed injection/removal, later
Recovery, and stable convergence remain distinct trace facts.

The controlled mock maps the operations to its availability state. Local
`sync-cf` places a Scenario-owned TCP proxy on the participant route: an outage
withholds traffic on existing sockets and rejects new connections while
Wrangler, the Worker, and Durable Object remain live. The direct authoritative
observer remains available. The same portable outage-and-recovery scenario
passes through in-process, process, and browser placements.

[Decision 0011](../.decisions/0011-model-backend-outage-at-participant-route.md)
records why the baseline does not destroy established sockets or restart
Wrangler. Latency, throughput, controlled-delivery, hard-socket-cut, and
participant/backend-process-death faults remain unavailable capabilities rather
than silent approximations.

## VRS

[requirements.md](../requirements.md) `LS.SYS.VER.SCEN-R11`.

## Accepted Contract

Backend availability must remain a backend/host composition capability, not a
sync-engine control seam. A realization must distinguish request,
acknowledgement, observed activation/removal, and later Recovery; it must state
which route and lifecycle layers are affected. Unsupported stronger faults are
advertised as unavailable rather than approximated by this baseline.
