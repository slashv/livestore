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
┌──────────────┐ blob (R2 put)  ┌───────────┐            ┌──────────┐ GET /snapshot/blob     ┌──────────┐
│ Checkpointer │ ─────────────► │    R2     │ ◄───────── │  Worker  │ ◄───────────────────── │  Client  │
│ (R2 sink)    │ ─────────────► │  Sync DO  │ ◄───────── │          │ ◄───────────────────── │          │
└──────────────┘ manifest (RPC) └───────────┘   RPC      └──────────┘ GET /snapshot/manifest └──────────┘
```

- Sync DO: `putSnapshotManifest` / `getSnapshotManifest` RPC methods. A manifest is accepted only for the DO's store and
  current `backendId`, with `seqNum` at or below the head and not older than the current one for the key. Manifests of
  an earlier backend identity are never served.
- Worker: `makeWorker({ snapshots: { bucketBinding } })` (or `matchSnapshotRequest` + `handleSnapshotRequest` in a
  custom `fetch`) serves `GET …/snapshot/manifest?storeId&key&payload` and `GET …/snapshot/blob?storeId&objectKey&payload`.
  Both run `validatePayload`; blob keys must start with `snapshots/<storeId>/`.
- Checkpointer: `makeR2SnapshotSink({ bucket, storeId, getSyncBackendStub, graceMs })` uploads
  `snapshots/<storeId>/<key>/<seqNum>-<id>.sqlite`, then publishes the manifest, then deletes blobs of the key that were
  replaced more than `graceMs` ago (the previous manifest's blob is always kept).
- Clients: `makeHttpSnapshotSource({ url })` (same URL as the sync backend) or, inside Durable Objects,
  `makeDoRpcSnapshotSource({ getSyncBackendStub, bucket })`.

## TODO (in readme)

- possibly add a CF Queue based transport mechanism for pull streaming responses
- alternative for DO RPC streams: poke to pull (let server callback client DO via client-side RPC endpoint)
