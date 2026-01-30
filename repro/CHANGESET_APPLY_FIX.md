# wa-sqlite `changeset_apply` Fix Summary

## Problem

The `sqlite3.changeset_apply()` function was causing a `RuntimeError: function signature mismatch` when called during concurrent multi-client sync scenarios. This occurred when:

1. Multiple browser tabs share a SharedWorker
2. One context is actively committing events
3. Another context starts and tries to sync
4. During rebase, `rollback()` calls `changeset.invert().apply()`

## Root Cause

In `packages/@livestore/wa-sqlite/src/sqlite-api.js`, the original implementation passed a JavaScript function directly to WASM:

```javascript
const onConflict = () => { return SQLITE_CHANGESET_REPLACE; }
const result = f(db, changesetData.length, inPtr, null, onConflict, null);
```

WASM's `cwrap` with signature `'nnnnnn:n'` expects 6 numbers (pointers), but `onConflict` is a JS function object, not a valid function pointer. When SQLite tried to invoke the callback, it caused a function signature mismatch.

## Solution

Implemented proper WASM callback handling using wa-sqlite's established adapter pattern:

1. **C-side wrapper** (`libsession.c`): Static callback functions that match SQLite's expected signatures
2. **JS-side registration** (`libsession.js`): Registers callbacks using `Module['setCallback']` with a unique key
3. **Adapter dispatch**: When SQLite calls the callback, the C code uses the key to look up and invoke the JavaScript function

## Files Changed in `packages/@livestore/wa-sqlite/`

| File | Change |
|------|--------|
| `src/libsession.c` | **NEW** - C wrapper for `xFilter` and `xConflict` callbacks |
| `src/libsession.js` | **NEW** - JS callback registration and `Module['changeset_apply']` |
| `src/libadapters.js` | Added `'ippip'` signature for `xConflict` callback |
| `src/libadapters.h` | Added `DECLARE(I, ippip, P, P, I, P)` |
| `src/asyncify_imports.json` | Added `"ippip"` and `"ippip_async"` |
| `src/exported_functions.json` | Added `"_libsession_changeset_apply"` |
| `src/sqlite-api.js` | Changed to use `Module.changeset_apply` |
| `Makefile` | Added libsession files, SESSION defines, node build target |

## Verification

All wa-sqlite tests pass (21/21), including the session extension test `"should apply changeset to revert changes"` which directly exercises `changeset_apply` with callbacks.

```bash
cd tests/wa-sqlite && pnpm test
```

## Building

To rebuild wa-sqlite with the fix:

```bash
cd packages/@livestore/wa-sqlite
make clean && make dist
```

Requires Emscripten (`emcc`) to be installed.
