import { makeInMemoryAdapter } from '@livestore/adapter-web'
import {
  type Adapter,
  createStorePromise,
  EventSequenceNumber,
  Events,
  LiveStoreEvent,
  makeSchema,
  Schema,
  State,
  StoreInternalsSymbol,
} from '@livestore/livestore'
import { Deferred, Effect, Exit, Queue, Stream } from '@livestore/utils/effect'

import type { PullItem } from '../../../packages/@livestore/common/src/ClientSessionLeaderThreadProxy.js'

export interface Trial {
  implementation: 'mailbox' | 'owner'
  scenario: 'idle' | 'advance' | 'rebase' | 'cancellation'
  eventCount: number
  writesPerEvent: number
}

export interface Sample extends Trial {
  inputError: string | null
  syncMs: number
  inputDelayMs: number
  commitMs: number
  inputToFrameMs: number
  deadlineToFrameMs: number
  maxFrameGapMs: number
  maxTimerDelayMs: number
  deadlineDuringSync: boolean
  inputDuringSync: boolean
  pendingCount: number
  rowsCorrect: boolean
  pendingCorrect: boolean
  stateHeadCorrect: boolean
  immediateReadCorrect: boolean
  propagationCorrect: boolean
  transientRowsMatchState: boolean
  cancellationStarted: boolean
  cancellationObservedCorrect: boolean
}

declare global {
  interface Window {
    sessionSyncBenchmark: { run: (trial: Trial) => Promise<Sample> }
  }
}

const rows = State.SQLite.table({
  name: 'benchmark_rows',
  columns: {
    id: State.SQLite.text({ primaryKey: true }),
    value: State.SQLite.integer({ default: 0 }),
  },
})
const rowCreated = Events.synced({
  name: 'benchmark.RowCreated',
  schema: Schema.Struct({ id: Schema.String, writes: Schema.Finite }),
})
const schema = makeSchema({
  events: { rowCreated },
  state: State.SQLite.makeState({
    tables: { rows },
    materializers: State.SQLite.materializers(
      { rowCreated },
      {
        'benchmark.RowCreated': ({ id, writes }) => [
          rows.insert({ id, value: 0 }),
          ...Array.from({ length: writes - 1 }, (_, index) => rows.update({ value: index + 1 }).where({ id })),
        ],
      },
    ),
  }),
})

const frame = () => new Promise<number>((resolve) => requestAnimationFrame(() => resolve(performance.now())))
const pause = (ms: number) => new Promise<void>((resolve) => setTimeout(resolve, ms))

declare const __SESSION_SYNC_VARIANT__: Trial['implementation']

const run = async (trial: Trial): Promise<Sample> => {
  if (trial.implementation !== __SESSION_SYNC_VARIANT__) throw new Error('Wrong benchmark build for variant')
  const pulls = Effect.runSync(Queue.unbounded<typeof PullItem.Type>())
  const pushStarted = Effect.runSync(Deferred.make<void>())
  const pendingCount =
    trial.scenario === 'rebase' || trial.scenario === 'cancellation' ? Math.max(10, trial.eventCount / 10) : 0
  let pushCalls = 0
  const cancellation = { started: false }
  let pullDelivered = false
  let notePullCompleted = () => {}
  const propagated: LiveStoreEvent.Client.Encoded[] = []
  const base = makeInMemoryAdapter({ clientId: 'local-client', sessionId: 'local-session' })
  // Keep the real browser DB/bootstrap and replace only the transport observed by the session processor.
  const adapter: Adapter = (args) =>
    base(args).pipe(
      Effect.map((session) => ({
        ...session,
        leaderThread: {
          ...session.leaderThread,
          events: {
            ...session.leaderThread.events,
            // Both processors request the next stream element only after handling the previous one. This observes
            // completion even when an intervening local commit publishes the already-installed upstream head early.
            pull: () =>
              Stream.fromEffect(
                Effect.suspend(() => {
                  if (pullDelivered === true) {
                    pullDelivered = false
                    notePullCompleted()
                  }
                  return Queue.take(pulls).pipe(
                    Effect.tap(() =>
                      Effect.sync(() => {
                        pullDelivered = true
                      }),
                    ),
                  )
                }),
              ).pipe(Stream.forever),
            push: (batch) =>
              Effect.suspend(() => {
                pushCalls++
                if (pendingCount > 0 && pushCalls === 1) {
                  return Deferred.succeed(pushStarted, undefined).pipe(
                    Effect.andThen(Effect.never),
                    Effect.ensuring(
                      Effect.sync(() => {
                        cancellation.started = true
                      }).pipe(Effect.andThen(trial.scenario === 'cancellation' ? Effect.sleep(20) : Effect.void)),
                    ),
                  )
                }
                return Effect.sync(() => {
                  propagated.push(...batch)
                })
              }),
          },
        },
      })),
    )
  const abort = new AbortController()
  const store = await createStorePromise({
    schema,
    adapter,
    storeId: `session-bench-${crypto.randomUUID()}`,
    disableDevtools: true,
    signal: abort.signal,
    logLevel: 'Error',
    params: { leaderPushBatchSize: 100 },
  })
  const internals = store[StoreInternalsSymbol]
  const db = internals.clientSession.sqliteDb
  const readState = () => Effect.runSync(internals.syncProcessor.syncState)
  const unsubscribeRows = store.subscribe(rows.select(), (value) => {
    document.querySelector('#row-count')!.textContent = String(value.length)
  })
  const localIds = Array.from({ length: pendingCount }, (_, index) => `local-${index}`)
  if (localIds.length > 0) {
    store.commit(...localIds.map((id) => rowCreated({ id, writes: trial.writesPerEvent })))
    await Effect.runPromise(Deferred.await(pushStarted))
  }
  const remoteIds =
    trial.scenario === 'idle' ? [] : Array.from({ length: trial.eventCount }, (_, index) => `remote-${index}`)
  let parentSeqNum = EventSequenceNumber.Client.ROOT
  const upstreamEvents = remoteIds.map((id, index) => {
    const seqNum = EventSequenceNumber.Client.Composite.make({ global: index + 1, client: 0, rebaseGeneration: 0 })
    const event = LiveStoreEvent.Client.Encoded.make({
      name: rowCreated.name,
      args: { id, writes: trial.writesPerEvent },
      seqNum,
      parentSeqNum,
      clientId: 'remote-client',
      sessionId: 'remote-session',
    })
    parentSeqNum = seqNum
    return event
  })
  const input = document.querySelector<HTMLInputElement>('#interaction')!
  let inputRun = 0
  let commitMs = 0
  let inputError: string | null = null
  let immediateReadCorrect = false
  let transientRowsMatchState = false
  let inputResolve = () => {}
  const inputDone = new Promise<void>((resolve) => {
    inputResolve = resolve
  })
  input.oninput = () => {
    inputRun = performance.now()
    try {
      const before = readState()
      transientRowsMatchState =
        db.select<{ count: number }>('SELECT COUNT(*) AS count FROM benchmark_rows')[0]!.count ===
        before.upstreamHead.global + before.pending.length
      const commitStarted = performance.now()
      store.commit(rowCreated({ id: 'interaction', writes: trial.writesPerEvent }))
      commitMs = performance.now() - commitStarted
      immediateReadCorrect = store.query(rows.where({ id: 'interaction' })).length === 1
    } catch (error) {
      inputError = String(error)
    } finally {
      inputResolve()
    }
  }
  await frame()
  let previousFrame = await frame()
  let maxFrameGapMs = 0
  let active = true
  const tickFrame = () => {
    const now = performance.now()
    maxFrameGapMs = Math.max(maxFrameGapMs, now - previousFrame)
    previousFrame = now
    if (active === true) requestAnimationFrame(tickFrame)
  }
  requestAnimationFrame(tickFrame)
  let lastTick = performance.now()
  let maxTimerDelayMs = 0
  const heartbeat = setInterval(() => {
    const now = performance.now()
    maxTimerDelayMs = Math.max(maxTimerDelayMs, now - lastTick - 1)
    lastTick = now
  }, 1)
  let syncFinished = 0
  let resolveSync = () => {}
  const syncDone = new Promise<void>((resolve) => {
    resolveSync = resolve
  })
  notePullCompleted = () => {
    if (syncFinished === 0) {
      syncFinished = performance.now()
      resolveSync()
    }
  }
  const started = performance.now()
  const inputDeadline = started + 5
  // Schedule before enqueuing the batch, so its lateness includes time spent blocked in reconciliation.
  const inputTimer = setTimeout(() => {
    input.value = 'interaction'
    input.dispatchEvent(new InputEvent('input', { bubbles: true, data: 'interaction' }))
  }, 5)
  if (upstreamEvents.length > 0) {
    Effect.runSync(
      Queue.offer(pulls, {
        payload: { _tag: 'upstream-advance', newEvents: upstreamEvents },
        globalHead: parentSeqNum,
        materializerHashes: [],
      }),
    )
  } else {
    syncFinished = started
    resolveSync()
  }
  const nextInputFrame = inputDone.then(frame)
  let timeout: ReturnType<typeof setTimeout> | undefined
  try {
    await Promise.race([
      Promise.all([inputDone, syncDone]),
      new Promise<never>((_, reject) => {
        timeout = setTimeout(
          () => reject(new Error(`Input/reconciliation timed out; input error: ${inputError}`)),
          30000,
        )
      }),
    ])
  } catch (error) {
    active = false
    clearInterval(heartbeat)
    clearTimeout(inputTimer)
    unsubscribeRows()
    abort.abort()
    input.oninput = null
    throw error
  } finally {
    clearTimeout(timeout)
  }
  const inputFrame = await nextInputFrame
  await frame()
  active = false
  clearInterval(heartbeat)
  clearTimeout(inputTimer)
  unsubscribeRows()
  const state = readState()
  const expectedIds = [...localIds, ...remoteIds, 'interaction'].toSorted()
  const actualRows = db.select<{ id: string; value: number }>('SELECT id, value FROM benchmark_rows ORDER BY id')
  const durableHead = db.select<{ seqNumGlobal: number; seqNumClient: number; seqNumRebaseGeneration: number }>(
    'SELECT * FROM __livestore_state_head',
  )[0]!
  // Drain after timing: propagation latency is separate from synchronous reconciliation and input responsiveness.
  const propagationComplete = () =>
    state.pending.every((event) => propagated.some((sent) => LiveStoreEvent.Client.isEqualEncoded(event, sent)))
  const drainDeadline = performance.now() + 3000
  while (propagationComplete() === false && performance.now() < drainDeadline) await pause(5)
  await store.shutdownPromise()
  const result: Sample = {
    ...trial,
    inputError,
    syncMs: syncFinished - started,
    inputDelayMs: Math.max(0, inputRun - inputDeadline),
    commitMs,
    inputToFrameMs: inputFrame - inputRun,
    deadlineToFrameMs: inputFrame - inputDeadline,
    maxFrameGapMs,
    maxTimerDelayMs,
    deadlineDuringSync: inputDeadline <= syncFinished,
    inputDuringSync: inputRun < syncFinished,
    pendingCount: state.pending.length,
    rowsCorrect:
      JSON.stringify(actualRows.map(({ id }) => id)) === JSON.stringify(expectedIds) &&
      actualRows.every(({ value }) => value === trial.writesPerEvent - 1),
    pendingCorrect:
      JSON.stringify(state.pending.map((event) => String(event.args.id)).toSorted()) ===
      JSON.stringify([...localIds, 'interaction'].toSorted()),
    stateHeadCorrect:
      durableHead.seqNumGlobal === state.localHead.global &&
      durableHead.seqNumClient === state.localHead.client &&
      durableHead.seqNumRebaseGeneration === state.localHead.rebaseGeneration,
    immediateReadCorrect,
    propagationCorrect: state.pending.every((event) =>
      propagated.some((sent) => LiveStoreEvent.Client.isEqualEncoded(event, sent)),
    ),
    transientRowsMatchState,
    cancellationStarted: cancellation.started,
    cancellationObservedCorrect: pendingCount === 0 || cancellation.started === true,
  }
  abort.abort()
  input.oninput = null
  document.querySelector('#status')!.textContent = JSON.stringify(result)
  return result
}

window.sessionSyncBenchmark = { run }
document.querySelector('#status')!.textContent = 'Ready'
