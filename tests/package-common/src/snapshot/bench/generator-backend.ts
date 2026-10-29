import { SyncBackend, UnknownError } from '@livestore/common'
import type { LiveStoreEvent } from '@livestore/common/schema'
import { Effect, KeyValueStore, Option, Stream, SubscriptionRef } from '@livestore/utils/effect'

import { eventAt, type MixName } from './workload.ts'

/**
 * Sync backend over a generated log: event `n` is `eventAt(mix, n)`, so a 10M-event history costs no memory.
 *
 * Pages are serialized to JSON and parsed again, as on the wire. Generation + serialization is the server's work and
 * is accounted in `stats.serverMs`; parsing is the client's work and stays in the client's wall-clock time.
 */
export interface GeneratorBackend {
  readonly makeSyncBackend: SyncBackend.SyncBackendConstructor
  /** Current backend head (last appended position). */
  readonly head: SubscriptionRef.SubscriptionRef<number>
  /** Appends `count` events to the log and wakes live pulls. */
  readonly advance: (count: number) => Effect.Effect<void>
  readonly stats: {
    /** Events served across all pulls. */
    eventsServed: number
    /** JSON bytes of the served pages (the payload a provider would put on the wire, before compression). */
    bytesServed: number
    pages: number
    /** Time spent generating and serializing pages, i.e. backend-side work inside this process. */
    serverMs: number
  }
  readonly resetStats: () => void
}

export const makeGeneratorBackend = ({
  mix,
  initialHead,
  pageSize = 256,
  backendId = 'bench-backend',
}: {
  mix: MixName
  initialHead: number
  /** Events per pull page. The Cloudflare D1 storage starts at 256 per page. */
  pageSize?: number
  backendId?: string
}): Effect.Effect<GeneratorBackend> =>
  Effect.gen(function* () {
    const head = yield* SubscriptionRef.make(initialHead)
    const stats = { eventsServed: 0, bytesServed: 0, pages: 0, serverMs: 0 }

    /** Builds the wire page for (from, to] and parses it back as a client would. */
    const page = (from: number, to: number, remaining: number): SyncBackend.PullResItem => {
      const serverStart = performance.now()
      const batch: Array<{ eventEncoded: LiveStoreEvent.Global.Encoded; metadata: null }> = []
      for (let n = from + 1; n <= to; n++) batch.push({ eventEncoded: eventAt(mix, n), metadata: null })
      const wire = JSON.stringify({ batch, remaining })
      stats.serverMs += performance.now() - serverStart
      stats.bytesServed += wire.length
      stats.eventsServed += to - from
      stats.pages++

      const parsed = JSON.parse(wire) as { batch: typeof batch; remaining: number }
      return {
        batch: parsed.batch.map((item) => ({ eventEncoded: item.eventEncoded, metadata: Option.none() })),
        pageInfo: parsed.remaining > 0 ? SyncBackend.pageInfoMoreKnown(parsed.remaining) : SyncBackend.pageInfoNoMore,
      }
    }

    const makeSyncBackend: SyncBackend.SyncBackendConstructor = () =>
      Effect.gen(function* () {
        const isConnected = yield* SubscriptionRef.make(true)
        // Persist the identity on the first response, as real providers do, so snapshot exports know the backend.
        const kv = yield* Effect.serviceOption(KeyValueStore.KeyValueStore)
        const recordBackendId = Option.match(kv, {
          onNone: () => Effect.void,
          onSome: (store) => store.set('backendId', backendId).pipe(Effect.ignore),
        })

        const pull: SyncBackend.SyncBackend['pull'] = (cursor, options) => {
          const start = Option.match(cursor, { onNone: () => 0, onSome: (c) => c.eventSequenceNumber as number })
          const live = options?.live === true
          return Stream.fromEffect(recordBackendId).pipe(
            Stream.flatMap(() =>
              Stream.paginate(start, (last: number) =>
                Effect.gen(function* () {
                  let current = yield* SubscriptionRef.get(head)
                  if (current <= last) {
                    if (live === false) {
                      const empty: readonly [ReadonlyArray<SyncBackend.PullResItem>, Option.Option<number>] = [
                        [SyncBackend.pullResItemEmpty()],
                        Option.none(),
                      ]
                      return empty
                    }
                    current = yield* SubscriptionRef.waitUntil(head, (h) => h > last)
                  }
                  const to = Math.min(current, last + pageSize)
                  const item = page(last, to, current - to)
                  const next: readonly [ReadonlyArray<SyncBackend.PullResItem>, Option.Option<number>] = [
                    [item],
                    live === true || to < current ? Option.some(to) : Option.none(),
                  ]
                  return next
                }),
              ),
            ),
          )
        }

        return SyncBackend.of({
          isConnected,
          connect: Effect.void,
          ping: Effect.void,
          pull,
          push: () => Effect.fail(new UnknownError({ cause: 'The benchmark backend is read-only' })),
          metadata: { name: 'bench-generator', description: 'Generated agent workload log' },
          supports: { pullPageInfoKnown: true, pullLive: true },
        })
      })

    return {
      makeSyncBackend,
      head,
      advance: (count) => SubscriptionRef.update(head, (h) => h + count),
      stats,
      resetStats: () => {
        stats.eventsServed = 0
        stats.bytesServed = 0
        stats.pages = 0
        stats.serverMs = 0
      },
    }
  })
