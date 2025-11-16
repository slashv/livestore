# Implementation Plan - Todo-Centric Streaming Harness

1. **Schema & Queries**
   - Replace the bespoke streaming schema/events with the TodoMVC schema from `examples/web-todomvc-sync-cf/src/livestore/schema.ts` (todos table + uiState document).
   - Update `queries.ts` to expose the todo list and any supplemental counts needed by the streaming UI.

2. **Event Modeling**
   - Remove streaming-specific events; ensure the app only emits standard todo events (`TodoCreated`, `TodoCompleted`, `TodoUncompleted`, `TodoDeleted`, `TodoClearedCompleted`, `uiStateSet`).
   - Provide helpers for generating realistic todo payloads (stable ids/text) and optionally toggling/completing for variety.

3. **Streaming Engine Refactor**
   - Rework `StreamControls` so that:
     - “Start streaming”/“Stop streaming” simply control pulling events from the local queue and committing them immediately (no synthetic update events).
     - “Start generate”/“Stop generate” create todo events at the configured rate and append them to the queue; seeded events are just todo creations committed up front.
   - Track queue depth, generated ids, and emitted metrics using the todo events.

4. **UI Updates**
   - Adjust `EventsList` (or introduce a todo list view) to render actual todo state, highlighting recently streamed items (newest-first or last-updated indicator).
   - Surface DOM attributes for dataset id, queue depth, generated vs streamed counts, and active rate so Playwright can assert both scenarios.

5. **Playwright Utilities**
   - Refresh helpers in `tests/perf-streaming/tests/utils.ts` for the renamed controls (`start-generate`, `stop-generate`, etc.) and new data attributes.
   - Add seed/generate/stream waiters that understand todo semantics (e.g., wait until todo count reaches expected value).

6. **Test Suites**
   - **Latency**: create separate cases for (a) pre-seeded todos streamed at full speed and (b) live todo generation while streaming.
   - **Memory**: update to use the new helpers and measure after a representative live generation run.

7. **Validation**
   - Run `pnpm exec tsc --noEmit -p tests/perf-streaming/tsconfig.json` and ensure the app builds.

## Risks / Considerations
- Need deterministic id/text generation so repeated runs remain stable.
- Streaming and generation loops must coordinate without re-introducing synthetic events or double-commits.
