# Scenario-Based Sync Verification — Intuition

_For: contributors investigating sync correctness · Assumes:
[../intuition.md](../intuition.md) · Covers: why reproducible scenarios are a
separate verification evidence shape_

## Focused tests prove parts; scenarios prove compositions

Unit and conformance tests remain the fastest way to prove a local invariant.
They do not show how Stores, session and leader processors, materialization,
queues, retries, topology changes, faults, and recovery behave together over a
long run. A scenario makes that composition reviewable and reproducible without
reimplementing LiveStore as a second behavioral model.

The central loop is:

```text
typed scenario source → serializable scenario AST → runner
                                                   │
                                                   ▼
scenario oracle ← scenario trace ← real LiveStore components
       │
       └── verdict + reproducible run artifact
```

## Control is not observation

“Disconnect Client A” is an instruction. “Client A reported offline” is an
observation. Treating the request as proof would hide failures in the control
surface itself. Scenario traces keep instructions, Control acknowledgements,
Operation outcomes, observations, and Scenario verdicts distinct. Correlation
groups evidence about the same operation; only explicit dependency/causation
edges and participant-local sequence order it. A Control acknowledgement says
the host finished handling a request, not that a Sync backend accepted it or
that other participants observed it.

Timeouts deserve particular care. A child process or browser may apply a
request and then lose its response. That is an indefinite Operation outcome,
not evidence that the operation did not happen.

## Fast evidence and faithful evidence answer different questions

The controlled in-process profile asks whether the composed sync system is
correct under a particular workload, schedule, topology, and fault sequence.
Platform-realized profiles add evidence about real persistence, transport,
leadership, and lifecycle boundaries. Results are scoped to their profile;
cross-profile comparison is useful when explicitly requested, not an implied
one-to-one equivalence guarantee.

## Convergence is an explicit claim

Open streams, future polling, and telemetry mean “nothing is running” is not a
useful definition of completion. A settle phase instead names the participants
expected to converge, the faults whose injection must stop, the work that must
become quiescent, the barrier that confirms stable Convergence, and the timeout
that bounds the claim. Fault removal is followed by separately observed
Recovery; it does not prove it.

Settlement answers whether the declared convergence barrier completed. After
that, Scenario oracles evaluate Scenario properties and emit verdicts. A
property can fail a settled run without rewriting the convergence evidence.

Eventlog convergence and State convergence are different evidence. The first
profile exercises SQLite because current production processors depend on its
materialization and rollback behavior, while the scenario language keeps sync
semantics independent of that realization.
