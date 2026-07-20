# 0003 — Local process and browser profile realizations

Status: accepted (maintainer direction, 2026-07-20)

## Context

The first platform-realized scenario slice must test boundaries that the
in-process host cannot provide, while preserving the existing Client and
Client-session ontology. It also needs a real local sync backend without making
tests depend on a deployed service.

A browser tab is a Client session, not the top-level Client. Treating every tab
as a Client would avoid multi-session coordination but would fail to exercise
the production SharedWorker leader, Web Locks, and shared OPFS state.

## Options

- **One Node child process per Client (chosen).** Serialized commands and
  observations cross Node IPC, while the child owns its Store and SQLite state.
  A worker thread was rejected for this slice because it does not establish an
  OS-process boundary. One process per session was rejected because it would
  misrepresent the top-level Client.
- **One persistent browser context per Client and one page per Client session
  (chosen).** Pages in a Client share one origin, SharedWorker, Web Locks, and
  OPFS. Separate persistent contexts isolate Clients. Closing and reopening a
  page exercises session lifecycle; closing and reopening a context with the
  same profile directory exercises persistent Client lifecycle. Treating each
  page or context as a complete independent scenario Client was rejected
  because it cannot represent both isolation and multi-session leadership.
- **Repository sync-cf under local workerd (chosen).** The realization launches
  the actual sync-cf Worker and SQLite Durable Object through the repository's
  Wrangler development-server support and connects through the production
  WebSocket client. Replacing the backend with an HTTP/WebSocket-shaped fake
  was rejected because it would not provide backend serialization,
  persistence, or reconnection evidence.

## Decision

Adopt all three mappings for the initial local fidelity profiles. Browser
connectivity faults use the browser-context network boundary. Browser startup
disables the adapter's unlocked OPFS fast-path read and waits for SharedWorker
termination so tightly sequenced scenario lifecycle remains deterministic
without replacing the production worker topology.

Evidence: maintainer direction to implement both the real-process and full
browser profiles on 2026-07-20; implementation checkpoints `46051d1be`,
`68e38fec2`, and `2e1a23ae0`.

## Consequences

- Profile capability claims stay asymmetric. The process profile currently
  supports one session per Client and no lifecycle restart; the browser profile
  supports multiple sessions plus page and persistent Client restart.
- The portable offline-writer scenario runs unchanged across in-process,
  process, and browser hosts. The multi-session lifecycle scenario explicitly
  requires browser capabilities.
- A browser Client restart preserves OPFS by reusing its profile directory;
  final run cleanup removes the temporary profile.
- The local backend is development evidence, not deployed-backend evidence.
- Additional process sessions, process termination controls, browser/backend
  combinations, and deployed backends remain incremental capabilities rather
  than implied parity.
