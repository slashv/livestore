# `@livestore/sync-cf`

## Goals

- Flexible: Leverage the strenghts of Cloudflare abstractions and let app developers choose right setup for their use case (configurable transports/storage)
- Efficient: Let's Durable Objects hibernate when not possible to avoid CPU billing while still staying fully reactive
- Cloudflare idiomatic: Embrace Cloudflare's APIs and abstractions so using LiveStore on Cloudflare feels right at home

## Transports

| Transport          | Connection | Encoding     | Runtime Env                                  |
| ------------------ | ---------- | ------------ | -------------------------------------------- |
| WebSocket          | Stateful   | JSON         | Anywhere                                     |
| HTTP               | Stateless  | JSON         | Anywhere                                     |
| Durable Object RPC | Stateful   | SchemaBinary | Only available between Durable Objects (DOs) |

### WebSocket

```
┌─────────────┐  WS (hibernated) ┌─────────────┐
│   Client    │ ──────────────── │  Sync DO    │
└─────────────┘                  └─────────────┘
```

- Support hibernated WebSockets (i.e. client DO doesn't need to stay alive during pull streaming)

### HTTP

```
┌─────────────┐      HTTP       ┌─────────────┐
│   Client    │ ─────────────── │  Sync DO    │
└─────────────┘                 └─────────────┘
```

- Request-response model for each RPC call
- Streaming responses are implemented via HTTP streaming
  - Requires `enable_request_signal` compatibility flag to properly support `pull` streaming responses
  - Keeps DO alive for the duration of the stream (which causes CPU billing)

### Durable Object RPC

```
┌─────────────┐     DO RPC      ┌─────────────┐
│ DO Client   │ ─────────────── │  Sync DO    │
└─────────────┘                 └─────────────┘
```

- Only supported in combination with `@livestore/adapter-cloudflare`
- RPC streams are implement via `ReadableStream` which is billed for the entire duration of the stream, not just the time it takes to send the data

## Storage

- D1
- Cloudflare Durable Objects SQLite

## Snapshots (experimental)

Opt-in realization of snapshot boot: fresh clients install a recent state snapshot and only pull the events after it.
The sync backend stays schema-blind and only stores the small manifest pointer.

```
┌──────────────┐ blob (R2 put or multipart) ┌───────────┐            ┌──────────┐ GET /snapshot/blob     ┌──────────┐
│ Checkpointer │ ─────────────────────────► │    R2     │ ◄───────── │  Worker  │ ◄───────────────────── │  Client  │
│ (R2 sink)    │ ─────────────────────────► │  Sync DO  │ ◄───────── │          │ ◄───────────────────── │          │
└──────────────┘ manifest (RPC)             └───────────┘   RPC      └──────────┘ GET /snapshot/manifest └──────────┘
```

- Sync DO: `putSnapshotManifest` / `getSnapshotManifest` / `isSnapshotObjectServable` RPC methods, backed by the
  `snapshot_manifests_<V>` table (current and previous manifest per `(storeId, key)`). A manifest is accepted only for
  a store with backend history in that DO, the current `backendId`, a `seqNum` at or below the head, and not older than
  the current one of the same `backendId` for the key. Manifests of an earlier backend identity are never served.
- Worker: `makeWorker({ snapshots: { bucketBinding } })` (or `matchSnapshotRequest` + `handleSnapshotRequest` in a
  custom `fetch`) serves `GET …/snapshot/manifest?storeId&key&payload` and `GET …/snapshot/blob?storeId&objectKey&payload`.
  Both run `validatePayload`; the blob route serves only the current or previous manifest's blob of that store
  (`403` outside the store prefix, `404` otherwise). Storage failures answer `503`, and clients fall back to full replay.
- Checkpointer: `makeCheckpointerDurableObject` from `@livestore/adapter-cloudflare` runs a headless leader per
  `${storeId}/${snapshotKey}` that follows the log via DO-RPC live pull. Pass
  `makeDurableObject({ snapshots: { checkpointerBinding, snapshotKeys, wakeIntervalMs? } })` (or
  `{ checkpointers: [{ binding, snapshotKey }] }` for one checkpointer class per schema) so pushes wake it (at most
  once per `wakeIntervalMs`, default 10 s, with the current `backendId` and head, so a reset backend restarts it).
  Live-pull callbacks to subscriber DOs time out after 5 s, so a slow checkpointer never holds up pushes.
- Sink: `makeR2SnapshotSink({ bucket, storeId, getSyncBackendStub, graceMs?, stage?, partSizeBytes? })` uploads
  `snapshots/<encodeURIComponent(storeId)>/<encodeURIComponent(key)>/<seqNum>-<id>.sqlite`, then publishes the
  manifest (deleting the blob if the manifest is rejected), then deletes blobs of the key that were replaced more than
  `graceMs` (default 5 minutes) ago; the current and previous manifests' blobs are always kept. With `stage` (used by
  the checkpointer DO's default `exportMode: 'streamed'`) the state file's pages are copied inside DO storage under the
  leader's persistence lock and streamed to R2 as a multipart upload in `partSizeBytes` parts (default 5 MiB), so
  memory stays near one part at any state size. R2 does not verify SHA-256 for multipart uploads; clients verify it.
- Clients: `makeHttpSnapshotSource({ url })` (same URL as the sync backend) or, inside Durable Objects,
  `makeDoRpcSnapshotSource({ getSyncBackendStub, bucket })`.

## TODO (in readme)

- possibly add a CF Queue based transport mechanism for pull streaming responses
- alternative for DO RPC streams: poke to pull (let server callback client DO via client-side RPC endpoint)
