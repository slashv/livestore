# Research: OTEL propagation across leader-thread boundaries

## Goal
- Confirm where the `ClientSessionLeaderThreadProxy` methods add spans.
- Assess whether spans created inside leader-thread helpers (e.g. `stream-events.ts`) flow back into store-level traces when the leader runs in a worker.

## Observations

1. **Client session wrappers add host-side spans** – Both the web worker adapter and the Node adapter wrap every RPC to the leader thread with `Effect.withSpan` / `Stream.withSpan` before delegating to `worker.executeEffect`. That means any store call (e.g. `events.stream`) already runs inside a named span on the host runtime.

```319:362:packages/@livestore/adapter-web/src/web-worker/client-session/persisted-adapter.ts
      const runInWorker = <TReq extends typeof WorkerSchema.SharedWorkerRequest.Type>(
        req: TReq,
      ): TReq extends Schema.WithResult<infer A, infer _I, infer E, infer _EI, infer R>
        ? Effect.Effect<A, UnexpectedError | E, R>
        : never =>
        Fiber.join(sharedWorkerFiber).pipe(
          Effect.tap(() => waitForSharedWorkerInitialized),
          Effect.flatMap((worker) => worker.executeEffect(req) as any),
          Effect.logWarnIfTakesLongerThan({
            label: `@livestore/adapter-web:client-session:runInWorker:${req._tag}`,
            duration: 2000,
          }),
          Effect.withSpan(`@livestore/adapter-web:client-session:runInWorker:${req._tag}`),
          Effect.mapError((cause) =>
            Schema.is(UnexpectedError)(cause)
              ? cause
              : ParseResult.isParseError(cause) || Schema.is(WorkerError.WorkerError)(cause)
                ? new UnexpectedError({ cause })
                : cause,
          ),
          Effect.catchAllDefect((cause) => new UnexpectedError({ cause })),
        ) as any
```

```453:499:packages/@livestore/adapter-web/src/web-worker/client-session/persisted-adapter.ts
      const leaderThread: ClientSession['leaderThread'] = {
        export: runInWorker(new WorkerSchema.LeaderWorkerInnerExport()).pipe(
          Effect.timeout(10_000),
          UnexpectedError.mapToUnexpectedError,
          Effect.withSpan('@livestore/adapter-web:client-session:export'),
        ),
        events: {
          pull: ({ cursor }) =>
            runInWorkerStream(new WorkerSchema.LeaderWorkerInnerPullStream({ cursor })).pipe(Stream.orDie),
          push: (batch) =>
            runInWorker(new WorkerSchema.LeaderWorkerInnerPushToLeader({ batch })).pipe(
              Effect.withSpan('@livestore/adapter-web:client-session:pushToLeader', {
                attributes: { batchSize: batch.length },
              }),
            ),
          stream: (options) =>
            runInWorkerStream(new WorkerSchema.LeaderWorkerInnerStreamEvents(options)).pipe(
              Stream.withSpan('@livestore/adapter-web:client-session:streamEvents'),
              Stream.orDie,
            ),
        },
        ...
      }
```

2. **Leader worker adds spans around most RPC handlers** – When the leader thread runs in a dedicated worker, the worker-side handlers wrap heavy operations with their own spans. `StreamEvents` is decorated via `Stream.withSpan`, and `PushToLeader` uses `Effect.withSpan`. The only missing coverage is the `PullStream` handler (no span today).

```190:220:packages/@livestore/adapter-web/src/web-worker/leader-worker/make-leader-worker.ts
    PullStream: ({ cursor }) =>
      Effect.gen(function* () {
        const { syncProcessor } = yield* LeaderThreadCtx
        return syncProcessor.pull({ cursor })
      }).pipe(Stream.unwrapScoped),
    PushToLeader: ({ batch }) =>
      Effect.andThen(LeaderThreadCtx, ({ syncProcessor }) =>
        syncProcessor.push(
          batch.map((event) => new LiveStoreEvent.EncodedWithMeta(event)),
          { waitForProcessing: true },
        ),
      ).pipe(Effect.uninterruptible, Effect.withSpan('@livestore/adapter-web:worker:PushToLeader')),
    StreamEvents: (options) =>
      LeaderThreadCtx.pipe(
        Effect.map(({ dbEventlog, syncProcessor }) => {
          const { _tag: _ignored, ...payload } = options as any
          const streamOptions = payload as StreamEventsOptions
          return streamEventsWithSyncState({
            dbEventlog,
            syncState: syncProcessor.syncState,
            options: streamOptions,
          })
        }),
        Stream.unwrapScoped,
        Stream.withSpan('@livestore/adapter-web:worker:StreamEvents'),
      ),
```

3. **In-memory adapter exposes the raw helpers with no extra spans** – When the leader thread runs inline (e.g. tests, dev tooling), the proxy just forwards to in-process helpers without any wrapper spans.

```240:260:packages/@livestore/adapter-web/src/in-memory/in-memory-adapter.ts
      const leaderThread = ClientSessionLeaderThreadProxy.of({
        events: {
          pull: ({ cursor }) => syncProcessor.pull({ cursor }),
          push: (batch) =>
            syncProcessor.push(
              batch.map((item) => new LiveStoreEvent.EncodedWithMeta(item)),
              { waitForProcessing: true },
            ),
          stream: (options) =>
            streamEventsWithSyncState({
              dbEventlog,
              syncState: syncProcessor.syncState,
              options,
            }),
        },
        ...
      })
```

4. **`stream-events.ts` currently returns chunks without spans** – The pagination loop is entirely uninstrumented, so batch counts, cursor changes, and queue waits are invisible to telemetry today.

```43:85:packages/@livestore/common/src/leader-thread/stream-events.ts
  return Stream.unwrapScoped(
    Effect.gen(function* () {
      const headQueue = yield* Queue.sliding<EventSequenceNumber.EventSequenceNumber>(1)
      yield* syncState.changes.pipe(
        Stream.map((state) => state.upstreamHead),
        Stream.runForEach((head) => Queue.offer(headQueue, head)),
        Effect.forkScoped,
      )
      return Stream.paginateChunkEffect({ cursor: initialCursor, head: EventSequenceNumber.ROOT }, ({ cursor, head }) =>
        Effect.gen(function* () {
          if (options.until && EventSequenceNumber.isGreaterThanOrEqual(cursor, options.until)) {
            return [Chunk.empty(), Option.none()]
          }
          const nextHead =
            EventSequenceNumber.isGreaterThanOrEqual(cursor, head) ? yield* Queue.take(headQueue) : head
          const target = EventSequenceNumber.make({
            global: Math.min(cursor.global + batchSize, nextHead.global),
            client: EventSequenceNumber.clientDefault,
          })
          const chunk = Eventlog.getEventsFromEventlog({
            dbEventlog,
            options: {
              ...options,
              since: cursor,
              until: target,
            },
          })
          const nextState =
            options.until && EventSequenceNumber.isGreaterThanOrEqual(target, options.until)
              ? Option.none()
              : Option.some({ cursor: target, head: nextHead })
          return [chunk, nextState]
        }),
      )
    }),
  )
```

5. **`getEventsFromEventlog` is also synchronous** – There are no spans around the SQLite read or encoding loop, so we cannot attribute latency per batch.

```103:169:packages/@livestore/common/src/leader-thread/eventlog.ts
export const getEventsFromEventlog = ({
  dbEventlog,
  options,
}: {
  dbEventlog: SqliteDb
  options: StreamEventsOptions
}): Chunk.Chunk<LiveStoreEvent.AnyEncoded> => {
  const since = options.since ?? EventSequenceNumber.ROOT
  const batchSize = options.batchSize ?? STREAM_EVENTS_BATCH_SIZE_MAX
  const makeQuery = () => {
    let query = eventlogMetaTable.where('seqNumGlobal', '>', since.global)
    if (options.until) {
      query = query.where('seqNumGlobal', '<=', options.until.global)
    }
    ...
  }
  const eventlogEvents = dbEventlog.select(makeQuery())
  if (eventlogEvents.length === 0) {
    return Chunk.empty<LiveStoreEvent.AnyEncoded>()
  }
  const encodedEvents = eventlogEvents.map((eventlogEvent) => {
    return LiveStoreEvent.AnyEncoded.make({
      name: eventlogEvent.name,
      args: eventlogEvent.argsJson,
      seqNum: {
        global: eventlogEvent.seqNumGlobal,
        client: eventlogEvent.seqNumClient,
        rebaseGeneration: eventlogEvent.seqNumRebaseGeneration,
      },
      ...
    })
  })
  return Chunk.fromIterable(encodedEvents)
}
```

## Propagation model
- Store code consumes `leaderThread.events.stream` directly. The proxy stream is already wrapped in a host span, so any `Stream.mapChunksEffect` handling inside `store.ts` sits underneath that host span.
- When the leader runs in a worker, `worker.executeEffect` is called while the host span is active. The Effect runtime serializes fiber refs (including the tracer span) by default when using `WorkerRunner`. Because the worker handler creates its own spans without setting `root: true`, those spans become children of the sent tracer context. In practice this links worker spans (`@livestore/adapter-web:worker:*`) under the immediate host span (`@livestore/adapter-web:client-session:*`), which is itself a child of the caller span in the store.
- In-memory adapters bypass worker RPC. Spans created inside `stream-events.ts` would attach directly to whatever span is active when the store calls `leaderThread.events.stream`. Right now there is no span inside the helper, so all work sits inside the host wrapper span when a worker is involved, and becomes invisible when running in-process.
- Because `stream-events.ts` and `eventlog.ts` are common code that executes in every environment (worker or direct), adding spans there would give consistent visibility. They would nest under the ambient tracer context regardless of transport: under the host span when remote, or directly under the store span when running inline.

## Unknowns / follow-ups
- `WorkerRunner` context propagation is implicit; there is no repository code explicitly forwarding the OTEL context. Confirm at runtime (e.g. by inspecting a trace) that worker spans link to the host span as expected.
- `PullStream` lacks instrumentation; if we care about pull latency, consider adding a span parallel to `PushToLeader` / `StreamEvents`.
- Instrumentation in `eventlog.ts` will need to move the synchronous query into an `Effect` (`Effect.suspend`) before wrapping it with `Effect.withSpan`, otherwise we cannot attach tracing metadata.

## Next steps
- Instrument the pagination loop (`stream-events.ts`) with a batch-level span recording cursor bounds, queue wait, batch size, hit count, etc.
- Wrap the SQLite fetch in `eventlog.ts` with a dedicated span to measure query latency and row counts.
- Verify via OTEL backend that worker spans appear under the store call once instrumentation is in place; if not, investigate explicit context propagation (e.g. pass `otel.Context` through `StreamEventsOptions`).

