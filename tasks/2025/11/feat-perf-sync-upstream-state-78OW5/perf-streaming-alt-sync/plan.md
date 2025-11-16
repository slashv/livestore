# Plan

1. Add a loopback sync backend helper under `tests/perf-streaming/test-app/src/livestore/` that wraps `makeMockSyncBackend({ startConnected: true })` and exposes a `Sync.SyncBackendConstructor` compatible factory.
2. Update `test-app/src/livestore.worker.ts` to consume the new helper instead of `makeWsSync`, ensuring the worker boots with the loopback backend and retains the initial sync timeout semantics.
3. Strip Cloudflare-specific sync payload plumbing: remove `SyncPayload` schema, auth token constant usage, and the provider props that referenced them (`main.tsx`, `schema.ts`, `shared/constants.ts`).
4. Simplify the dev server configuration by removing the Cloudflare Vite plugin hooks in `test-app/vite.config.ts`, keeping the harness fully local.
5. Refresh `tests/perf-streaming/README.md` to describe the new confirmation shortcut and mention the absence of a Cloudflare backend.
