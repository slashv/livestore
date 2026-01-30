# Changeset Apply Bug Fix Options

Summary:
- Problem: sqlite3.changeset_apply in wa-sqlite passes a JS callback directly to WASM. During rebase rollback (cross-context sync), SQLite invokes the callback and WASM throws `RuntimeError: function signature mismatch`, failing `repro/tests/repro.spec.ts` in headed mode.
- Option A (recommended): Add a C/JS adapter wrapper (libsession) so SQLite calls a real C callback that dispatches to JS via Module.setCallback. Requires emcc rebuild.
- Option B (fallback): JS-only change to use Module.addFunction to pass a real function pointer to sqlite3changeset_apply. No rebuild, but weaker callback handling and pointer lifecycle risks.

## Context
- Rollback applies inverted changesets during rebase:
  - packages/@livestore/common/src/leader-thread/materialize-event.ts
  - packages/@livestore/common/src/sync/ClientSessionSyncProcessor.ts
- The current binding lives in packages/@livestore/wa-sqlite/src/sqlite-api.js and uses cwrap with `nnnnnn:n`.

## Scope note (important)

- The failing `changeset_apply` path is not unique to `clientDocument`. It is the same session changeset apply used for any SQLite-backed state (including `State.SQLite.table`) when a rebase rollback applies inverted changesets.
- `Schema.Record`/hash-map payloads are not the root cause. They make conflicts more likely by repeatedly updating the same JSON row, which increases the chance that rebase will invoke `changeset_apply` with a conflict callback.
- The trigger is a conflict during rebase rollback, not the table type or schema shape.

## Option A: C/JS adapter wrapper (robust)
Use the same adapter pattern as libhook/libfunction:
1. Add a C wrapper (libsession.c) with static xFilter/xConflict callbacks that match SQLite's signature.
2. Add a JS registration layer (libsession.js) that stores JS callbacks via Module.setCallback and exposes Module.changeset_apply.
3. Extend adapter signatures and build wiring.

File-level changes:
- New: packages/@livestore/wa-sqlite/src/libsession.c
- New: packages/@livestore/wa-sqlite/src/libsession.js
- Update: packages/@livestore/wa-sqlite/src/libadapters.js (add signature, e.g. ippip)
- Update: packages/@livestore/wa-sqlite/src/libadapters.h
- Update: packages/@livestore/wa-sqlite/src/asyncify_imports.json
- Update: packages/@livestore/wa-sqlite/src/exported_functions.json
- Update: packages/@livestore/wa-sqlite/Makefile (include libsession)
- Update: packages/@livestore/wa-sqlite/src/sqlite-api.js (call Module.changeset_apply)

Pros:
- Correct ABI handling for callbacks.
- Matches existing wa-sqlite patterns and supports per-call handlers cleanly.
- Lowest long-term risk.

Cons:
- Requires emcc and rebuilding wa-sqlite outputs (dist/).
- Touches multiple files.

When to choose:
- Production fix, long-lived change, or any scenario needing real conflict handlers.

## Option B: JS-only addFunction pointer (fallback)
Use Module.addFunction to register a WASM function pointer and pass it to sqlite3changeset_apply.

Example shape:
```js
const SQLITE_CHANGESET_REPLACE = 1;
const onConflictPtr = Module.addFunction(
  (_pCtx, _eConflict, _pIter) => SQLITE_CHANGESET_REPLACE,
  'iiii'
);
const result = f(db, changesetData.length, inPtr, 0, onConflictPtr, 0);
```

Notes and risks:
- Must keep a single static pointer to avoid leaks (removeFunction is not exported).
- options.onConflict can be supported via a mutable closure, but reentrancy would be unsafe.
- Not aligned with the adapter pattern used elsewhere.

Pros:
- Minimal change (single file).
- No emcc rebuild needed.

Cons:
- More brittle and less extensible for advanced conflict handling.

When to choose:
- Quick mitigation or when emcc rebuild is not available.

## Validation
- `cd repro && npx playwright test --headed`
- `cd tests/wa-sqlite && pnpm test` (especially for Option A)
