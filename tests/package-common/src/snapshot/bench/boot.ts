/**
 * Snapshot boot benchmark: one measurement per process.
 *
 *   node boot.ts prepare  --mix append --events 1000000 --tails 0,1000,10000,100000
 *   node boot.ts replay   --mix append --events 1000000 [--storage fs|memory] [--breakdown full|noop|nosession]
 *   node boot.ts snapshot --mix append --events 1000000 --tail 10000 [--storage fs|memory]
 *
 * `prepare` runs a checkpointer leader and caches the snapshots at `events - tail` for every tail; `snapshot` boots a
 * fresh client from the cached snapshot and pulls the tail. Results are appended to `results/<command>.jsonl`.
 * Run with `NODE_ENV=production` (no dev-only materializer hashing).
 */
import fs from 'node:fs'
import path from 'node:path'
import v8 from 'node:v8'

import { Snapshot, StateSqliteDb } from '@livestore/common'
import { LeaderThreadCtx, makeCheckpointer } from '@livestore/common/leader-thread'
import { Effect, Option, SubscriptionRef } from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import { makeGeneratorBackend } from './generator-backend.ts'
import {
  benchDataDir,
  bootBenchLeader,
  fromJson,
  type ClientDbs,
  hardware,
  makeClientDbs,
  makeSqlite,
  makeSqlTimings,
  memorySnapshot,
  parseArgs,
  toJson,
  type Storage,
  timeDb,
  trackPeakRss,
  waitForHead,
  writeResult,
} from './lib.ts'
import { type MixName, noopSchema, schema } from './workload.ts'

const storeId = 'bench-store'
const args = parseArgs(process.argv.slice(2))
const command = process.argv[2]
const mix = (args.mix ?? 'append') as MixName
const events = Number(args.events ?? 100_000)
const storage = (args.storage ?? 'fs') as Storage
const pageSize = Number(args.pageSize ?? 256)
const rep = Number(args.rep ?? 0)
const snapshotDir = path.join(benchDataDir, 'snapshots')

const snapshotPath = (seqNum: number) => path.join(snapshotDir, `${mix}-${seqNum}`)

/** Warms the JIT and the wasm module with a short full replay, so measurements exclude one-off compilation. */
const warmup = (makeSqliteDb: Effect.Success<typeof makeSqlite>) =>
  Effect.gen(function* () {
    const backend = yield* makeGeneratorBackend({ mix, initialHead: 3_000, pageSize })
    const dbs = yield* makeClientDbs({ makeSqliteDb, storage: 'memory', name: 'warmup' })
    const leader = yield* bootBenchLeader({
      makeSqliteDb,
      dbs,
      schema,
      syncOptions: { backend: backend.makeSyncBackend, livePull: true },
      clientId: 'warmup',
      storeId,
    })
    yield* waitForHead(leader, 3_000)
    yield* leader.shutdown
  }).pipe(Effect.scoped)

/**
 * Full replay of the backend log up to `target` by a fresh client.
 *
 * With `segment > 0` (fs storage only) the client is restarted every `segment` events on its persisted databases: the
 * leader keeps every pulled event in memory (`makePullQueueSet` caches pull items for late sessions, and
 * `makeSqliteDb` keeps every prepared statement until close), about 6 KB per event, so a single leader cannot replay
 * 1M+ events within the heap limit. The backend head moves with the segments, so each restart resumes from its
 * persisted cursor exactly like a real client restart. Reported time is the sum of the segments' boot-to-head times.
 */
const fullReplay = ({
  makeSqliteDb,
  backend,
  name,
  from,
  target,
  segment,
  leaderSchema = schema,
  wrapDbs = (dbs) => dbs,
}: {
  makeSqliteDb: Effect.Success<typeof makeSqlite>
  backend: Effect.Success<ReturnType<typeof makeGeneratorBackend>>
  name: string
  /** Position the persisted databases of `name` already reflect (0: start from empty databases). */
  from: number
  target: number
  segment: number
  leaderSchema?: typeof schema
  wrapDbs?: (dbs: ClientDbs) => { dbState: ClientDbs['dbState']; dbEventlog: ClientDbs['dbEventlog'] }
}) =>
  Effect.gen(function* () {
    const step = segment > 0 && storage === 'fs' ? segment : target
    let wallMs = 0
    let leaderLayerMs = 0
    let segments = 0
    let reached = from
    while (true) {
      const segmentHead = Math.min(target, reached + step)
      yield* SubscriptionRef.set(backend.head, segmentHead)
      const dbs = yield* makeClientDbs({ makeSqliteDb, storage, name, reopen: reached > 0 })
      const start = performance.now()
      const leader = yield* bootBenchLeader({
        makeSqliteDb,
        dbs: wrapDbs(dbs),
        schema: leaderSchema,
        syncOptions: { backend: backend.makeSyncBackend, livePull: true },
        clientId: 'fresh-client',
        storeId,
        headless: args.headless === 'true',
      })
      leaderLayerMs += performance.now() - start
      yield* waitForHead(leader, segmentHead)
      wallMs += performance.now() - start
      segments++
      reached = segmentHead
      if (reached >= target) return { leader, dbs, wallMs, leaderLayerMs, segments }
      yield* leader.shutdown
      dbs.close()
    }
  })

const replay = Effect.gen(function* () {
  const makeSqliteDb = yield* makeSqlite
  yield* warmup(makeSqliteDb)
  const breakdown = args.breakdown as 'full' | 'noop' | 'nosession' | undefined
  const backend = yield* makeGeneratorBackend({ mix, initialHead: 0, pageSize })
  const timings = makeSqlTimings()

  const rss = trackPeakRss()
  const cpuStart = process.cpuUsage()
  const result = yield* fullReplay({
    makeSqliteDb,
    backend,
    name: args.name ?? `replay-${mix}-${events}`,
    from: Number(args.from ?? 0),
    target: events,
    segment: Number(args.segment ?? 0),
    leaderSchema: breakdown === 'noop' ? noopSchema : schema,
    wrapDbs: (dbs) =>
      breakdown === undefined
        ? dbs
        : {
            dbState: timeDb(dbs.dbState, 'state', timings, { disableSession: breakdown === 'nosession' }),
            dbEventlog: timeDb(dbs.dbEventlog, 'eventlog', timings),
          },
  })
  const cpu = process.cpuUsage(cpuStart)
  // With --expose-gc the reported memory is what the leader still retains after replaying.
  ;(globalThis as { gc?: () => void }).gc?.()
  if (process.env.SNAPSHOT_BENCH_HEAPSNAPSHOT !== undefined) {
    ;(globalThis as { gc?: () => void }).gc?.()
    v8.writeHeapSnapshot(process.env.SNAPSHOT_BENCH_HEAPSNAPSHOT)
  }

  writeResult(args.from === undefined ? 'replay' : 'segments', {
    command: 'replay',
    mix,
    events,
    storage,
    pageSize,
    rep,
    breakdown: breakdown ?? 'none',
    headless: args.headless === 'true',
    from: Number(args.from ?? 0),
    segments: result.segments,
    wallMs: result.wallMs,
    leaderLayerMs: result.leaderLayerMs,
    serverMs: backend.stats.serverMs,
    clientMs: result.wallMs - backend.stats.serverMs,
    eventBytes: backend.stats.bytesServed,
    pages: backend.stats.pages,
    cpuUserMs: cpu.user / 1000,
    cpuSystemMs: cpu.system / 1000,
    peakRssMiB: rss.stop(),
    memory: memorySnapshot(),
    ...result.dbs.sizes(),
    sql: breakdown === undefined ? undefined : timings,
    hardware: hardware(),
  })
  yield* result.leader.shutdown
})

/** Sink that writes the blob and manifest to the snapshot cache directory. */
const makeFileSink = (): Snapshot.SnapshotSink => ({
  publish: ({ manifest, bytes }) =>
    Effect.sync(() => {
      fs.mkdirSync(snapshotDir, { recursive: true })
      const published: Snapshot.SnapshotManifest = { ...manifest, objectKey: `${mix}-${manifest.seqNum}.sqlite` }
      fs.writeFileSync(`${snapshotPath(manifest.seqNum)}.sqlite`, bytes)
      fs.writeFileSync(`${snapshotPath(manifest.seqNum)}.json`, toJson(published))
      return published
    }),
  getManifest: () => Effect.succeedNone,
})

const prepare = Effect.gen(function* () {
  const makeSqliteDb = yield* makeSqlite
  const tails = (args.tails ?? '0,1000,10000,100000')
    .split(',')
    .map(Number)
    .toSorted((a, b) => b - a)
  const positions = tails.map((tail) => events - tail)
  const backend = yield* makeGeneratorBackend({ mix, initialHead: 0, pageSize })
  const rss = trackPeakRss()
  const { leader, dbs, wallMs } = yield* fullReplay({
    makeSqliteDb,
    backend,
    name: args.name ?? `checkpointer-${mix}-${events}`,
    from: Number(args.from ?? 0),
    target: positions[0]!,
    segment: Number(args.segment ?? 0),
  })
  const start = performance.now() - wallMs
  const checkpointer = yield* makeCheckpointer({
    schema,
    storeId,
    sink: makeFileSink(),
    // Only `publishNow` publishes here (the first snapshot always passes the interval gate).
    trigger: { minTailEvents: Number.MAX_SAFE_INTEGER },
  }).pipe(
    Effect.provideService(LeaderThreadCtx, leader.ctx),
    Effect.provideService(StateSqliteDb.StateSqliteDb, dbs.dbState),
  )

  let previous = 0
  /** Wall time spent publishing, excluded from `replayMs` so that it is a pure full-replay time. */
  let publishingMs = 0
  for (const position of positions) {
    yield* backend.advance(position - (yield* SubscriptionRef.get(backend.head)))
    yield* waitForHead(leader, position)
    const reachedMs = performance.now() - start
    const publishStart = performance.now()
    // A failed export (e.g. a state larger than the wasm heap can serialize) is recorded, not fatal: the replay
    // timings to the remaining positions are still valid.
    const published = yield* checkpointer.publishNow.pipe(Effect.result)
    publishingMs += performance.now() - publishStart
    const stats = yield* SubscriptionRef.get(checkpointer.stats)
    const timings = published._tag === 'Success' ? Option.getOrUndefined(stats.lastPublish) : undefined
    writeResult('prepare', {
      command: 'prepare',
      mix,
      events,
      storage,
      seqNum: position,
      reachedMs,
      replayMs: reachedMs - (publishingMs - (performance.now() - publishStart)),
      tailSincePrevious: position - previous,
      byteLength: timings?.byteLength,
      exportMs: timings?.exportMs,
      hashMs: timings?.hashMs,
      uploadMs: timings?.uploadMs,
      error: published._tag === 'Failure' ? String(published.failure) : undefined,
      ...dbs.sizes(),
      memory: memorySnapshot(),
      peakRssMiB: rss.peak(),
      serverMs: backend.stats.serverMs,
      eventBytes: backend.stats.bytesServed,
    })
    previous = position
  }
  yield* leader.shutdown
})

const snapshotBoot = Effect.gen(function* () {
  const makeSqliteDb = yield* makeSqlite
  yield* warmup(makeSqliteDb)
  const tail = Number(args.tail ?? 0)
  const position = events - tail
  const manifest = fromJson(fs.readFileSync(`${snapshotPath(position)}.json`, 'utf8')) as Snapshot.SnapshotManifest
  const bytes = new Uint8Array(fs.readFileSync(`${snapshotPath(position)}.sqlite`))

  // The source serves the cached blob from memory: download time is the copy only, network is modelled separately.
  const store = Snapshot.makeInMemorySnapshotStore({ storeId })
  const { objectKey: _, ...unpublished } = manifest
  yield* store.sink.publish({ manifest: unpublished, bytes })

  const backend = yield* makeGeneratorBackend({ mix, initialHead: events, pageSize })
  const dbs = yield* makeClientDbs({ makeSqliteDb, storage, name: `snapshot-${mix}-${events}-${tail}` })
  const rss = trackPeakRss()
  const cpuStart = process.cpuUsage()
  const start = performance.now()
  const leader = yield* bootBenchLeader({
    makeSqliteDb,
    dbs,
    schema,
    syncOptions: { backend: backend.makeSyncBackend, livePull: true, snapshot: { source: store.makeSource } },
    clientId: 'fresh-client',
    storeId,
  })
  const leaderLayerMs = performance.now() - start
  yield* waitForHead(leader, events)
  const wallMs = performance.now() - start
  const cpu = process.cpuUsage(cpuStart)
  const installed = Option.getOrThrow(leader.ctx.snapshotBoot)
  const installTotalMs =
    installed.timings.manifestMs +
    installed.timings.downloadMs +
    installed.timings.verifyMs +
    installed.timings.installMs

  writeResult('snapshot', {
    command: 'snapshot',
    mix,
    events,
    tail,
    storage,
    pageSize,
    rep,
    wallMs,
    leaderLayerMs,
    serverMs: backend.stats.serverMs,
    clientMs: wallMs - backend.stats.serverMs,
    snapshotBytes: manifest.byteLength,
    eventBytes: backend.stats.bytesServed,
    tailEventsPulled: backend.stats.eventsServed,
    ...installed.timings,
    /** Everything after the install: remaining leader boot plus pulling and applying the tail. */
    afterInstallMs: wallMs - installTotalMs,
    cpuUserMs: cpu.user / 1000,
    cpuSystemMs: cpu.system / 1000,
    peakRssMiB: rss.stop(),
    ...dbs.sizes(),
    hardware: hardware(),
  })
  yield* leader.shutdown
})

const program =
  command === 'replay' ? replay : command === 'prepare' ? prepare : command === 'snapshot' ? snapshotBoot : undefined
if (program === undefined) {
  console.error('usage: node boot.ts <replay|prepare|snapshot> [--mix append|update] [--events N] ...')
  process.exit(1)
}

await Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(PlatformNode.NodeFileSystem.layer)))
process.exit(0)
