import { expect } from 'vitest'

import type { CheckpointerDoStats } from '@livestore/adapter-cloudflare'
import type { Snapshot } from '@livestore/common'
import { nanoid } from '@livestore/livestore'
import { makeHttpSync } from '@livestore/sync-cf/client'
import { objectToString } from '@livestore/utils'
import { Vitest } from '@livestore/utils-dev/node-vitest'
import { Effect, Schedule, Schema } from '@livestore/utils/effect'

import { makeEventFactory, setupProviderRuntime, SyncDoProbeError } from './do-idle-helpers.ts'
import * as CloudflareHttpProvider from './providers/cloudflare-http-rpc.ts'
import { SyncProviderImpl } from './types.ts'

/**
 * Measures the checkpointer Durable Object at growing state sizes on local workerd: catch-up time, export, hash and R2
 * upload time, snapshot size and the leader's wasm heap. Opt-in and slow:
 *
 * ```sh
 * SNAPSHOT_DO_LIMITS_MB=10,50,100,200 [SNAPSHOT_DO_LIMITS_MODE=buffered] TEST_SYNC_PROVIDER=cf-http-do pnpm exec vitest run src/snapshot-do-limits.test.ts
 * ```
 *
 * Local workerd does not enforce the 128 MB isolate limit, so the numbers show what production would need. Workers
 * timers only advance on I/O, so the in-DO phase timings under-report CPU-bound work; `publishWallMs` is measured from
 * the test process.
 */

/** `streamed` (default) or `buffered` export, see `makeCheckpointerDurableObject({ exportMode })`. */
const exportMode = process.env.SNAPSHOT_DO_LIMITS_MODE === 'buffered' ? 'buffered' : 'streamed'

const sizesMb = (process.env.SNAPSHOT_DO_LIMITS_MB ?? '')
  .split(',')
  .map((size) => Number(size.trim()))
  .filter((size) => Number.isFinite(size) && size > 0)

/** Each event materializes one todo row with a text of this size, so state grows by roughly this per event. */
const TEXT_BYTES = 32 * 1024
const EVENTS_PER_PUSH = 25

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown))

Vitest.describe.skipIf(sizesMb.length === 0)('checkpointer Durable Object limits', { timeout: 60 * 60_000 }, () => {
  const getContext = setupProviderRuntime(CloudflareHttpProvider.doSqlite.layer)

  Vitest.live(`grows state to ${sizesMb.join(', ')} MB and publishes a snapshot at each size`, () =>
    Effect.gen(function* () {
      const { makeProvider } = yield* SyncProviderImpl
      const rows: Record<string, unknown>[] = []

      for (const sizeMb of sizesMb) {
        const storeId = `do-limits-${sizeMb}mb-${nanoid()}`
        const writer = yield* makeProvider({ storeId, clientId: 'writer', payload: undefined })
        const url = writer.metadata.url
        const baseUrl = typeof url === 'string' ? url : objectToString(url)
        const measureUrl = (path: string) =>
          `${baseUrl}/snapshot-e2e/${exportMode === 'buffered' ? 'measure-buffered' : 'measure'}/checkpointer/${path}?storeId=${storeId}`

        const eventCount = Math.ceil((sizeMb * 1024 * 1024) / TEXT_BYTES)
        const factory = makeEventFactory({
          client: { clientId: 'writer', sessionId: 'writer-session' },
          startSeq: 1,
          initialParent: 'root',
        })
        const filler = 'x'.repeat(TEXT_BYTES)
        const pushStart = performance.now()
        for (let i = 0; i < eventCount; i += EVENTS_PER_PUSH) {
          const batch = Array.from({ length: Math.min(EVENTS_PER_PUSH, eventCount - i) }, (_, j) =>
            factory.todoCreated.next({ id: `todo-${i + j}`, text: `${i + j}-${filler}`, completed: false }),
          )
          yield* writer.push(batch)
        }
        const pushMs = performance.now() - pushStart

        // Boot the checkpointer on the full log, then wait until it has applied every event.
        const catchUpStart = performance.now()
        yield* getJson(measureUrl('ensure-running'))
        const caughtUp = yield* getJson<CheckpointerDoStats>(measureUrl('stats')).pipe(
          Effect.filterOrFail(
            (stats) => stats.head === eventCount,
            (stats) => new SyncDoProbeError({ message: encodeJson(stats) }),
          ),
          Effect.retry(Schedule.spaced('1 second')),
          Effect.timeout('30 minutes'),
          Effect.orDie,
        )
        const catchUpMs = performance.now() - catchUpStart

        // Wall clock from the test process: inside workerd, timers only advance on I/O, so CPU-bound phases (export,
        // hashing) under-report in the checkpointer's own timings.
        const publishStart = performance.now()
        const manifest = yield* getJson<Snapshot.SnapshotManifest | null>(measureUrl('publish-now'))
        const publishWallMs = performance.now() - publishStart
        expect(manifest?.seqNum).toBe(eventCount)
        const published = yield* getJson<CheckpointerDoStats>(measureUrl('stats'))
        const timings = published.lastPublish!

        const row = {
          exportMode,
          targetMb: sizeMb,
          events: eventCount,
          stateDbMb: mb(published.runtime.stateDbBytes),
          snapshotMb: mb(timings.byteLength),
          pushS: seconds(pushMs),
          catchUpS: seconds(catchUpMs),
          exportMs: Math.round(timings.exportMs),
          hashMs: Math.round(timings.hashMs),
          uploadMs: Math.round(timings.uploadMs),
          publishTotalMs: Math.round(timings.totalMs),
          publishWallMs: Math.round(publishWallMs),
          wasmHeapBeforeExportMb: mb(caughtUp.runtime.wasmHeapBytes),
          wasmHeapAfterExportMb: mb(published.runtime.wasmHeapBytes),
        }
        rows.push(row)
        console.log(`[snapshot-do-limits] ${encodeJson(row)}`)
      }

      console.table(rows)
    }).pipe(Effect.provide(getContext())),
  )
})

const mb = (bytes: number) => Math.round((bytes / (1024 * 1024)) * 10) / 10
const seconds = (ms: number) => Math.round(ms / 100) / 10

const getJson = <T>(url: string) =>
  Effect.promise(async () => {
    const res = await fetch(url)
    if (res.ok === false) throw new Error(`${url} failed with ${res.status}: ${await res.text()}`)
    return (await res.json()) as T
  })
