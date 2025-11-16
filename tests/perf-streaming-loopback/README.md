# LiveStore Streaming Perf Harness

The perf-streaming package hosts a minimal React app that drives the LiveStore event stream while skipping the Cloudflare sync backend. It now provisions an in-process loopback sync layer powered by `makeMockSyncBackend`, which immediately replays pushed events back to the leader thread so the upstream head advances and the streaming pagination paths can be profiled without any external services.

## Commands

- `pnpm dev` – run the Vite dev server.
- `pnpm build` – build the React app for production.
- `pnpm preview` – serve the built app locally.

The previous Cloudflare worker remains available under `src/cf-worker/` for reference, but the default flow no longer requires Wrangler or a remote Durable Object deployment.
