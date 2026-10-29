/**
 * Checkpointer under sustained write load: one measurement per process.
 *
 *   node checkpointer.ts --mix append --base 100000 --rate 1000 --seconds 120 [--trigger '{"alpha":0.5}']
 *
 * The checkpointer leader boots from the cached snapshot at `base` (written by `boot.ts prepare`), then the benchmark
 * backend appends `rate` events per second while the default `makeCheckpointer` loop publishes into an in-memory
 * store. Every second it samples the backend head, the checkpointer head, the snapshot lag, memory and CPU; every
 * publication is recorded with its export, hash and upload timings. Results go to `results/checkpointer.jsonl`.
 */
import fs from 'node:fs'
import path from 'node:path'

import { Snapshot, StateSqliteDb } from '@livestore/common'
import { LeaderThreadCtx, makeCheckpointer } from '@livestore/common/leader-thread'
import { Effect, Option, SubscriptionRef } from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import { makeGeneratorBackend } from './generator-backend.ts'
import {
  benchDataDir,
  bootBenchLeader,
  fromJson,
  hardware,
  makeClientDbs,
  makeSqlite,
  memorySnapshot,
  parseArgs,
  waitForHead,
  writeResult,
} from './lib.ts'
import { type MixName, schema } from './workload.ts'

const storeId = 'bench-store'
const args = parseArgs(process.argv.slice(2))
const mix = (args.mix ?? 'append') as MixName
const base = Number(args.base ?? 100_000)
const rate = Number(args.rate ?? 1_000)
const seconds = Number(args.seconds ?? 120)
const trigger =
  args.trigger === undefined ? {} : (fromJson(args.trigger) as Partial<Snapshot.SnapshotTrigger.TriggerParams>)
const label = args.label ?? 'default'
const TICK_MS = 100

const program = Effect.gen(function* () {
  const makeSqliteDb = yield* makeSqlite
  const snapshotFile = path.join(benchDataDir, 'snapshots', `${mix}-${base}`)
  const manifest = fromJson(fs.readFileSync(`${snapshotFile}.json`, 'utf8')) as Snapshot.SnapshotManifest
  const bytes = new Uint8Array(fs.readFileSync(`${snapshotFile}.sqlite`))

  // The boot store seeds the checkpointer; the publish store receives its snapshots (short grace period so replaced
  // blobs do not pile up in this process's memory).
  const bootStore = Snapshot.makeInMemorySnapshotStore({ storeId })
  const { objectKey: _, ...unpublished } = manifest
  yield* bootStore.sink.publish({ manifest: unpublished, bytes })
  const publishStore = Snapshot.makeInMemorySnapshotStore({ storeId, graceMs: 1_000 })
  yield* publishStore.sink.publish({ manifest: unpublished, bytes })

  const backend = yield* makeGeneratorBackend({ mix, initialHead: base })
  const dbs = yield* makeClientDbs({ makeSqliteDb, storage: 'fs', name: `checkpointer-load-${mix}-${base}-${rate}` })
  const leader = yield* bootBenchLeader({
    makeSqliteDb,
    dbs,
    schema,
    syncOptions: { backend: backend.makeSyncBackend, livePull: true, snapshot: { source: bootStore.makeSource } },
    clientId: 'checkpointer',
    storeId,
  })
  yield* waitForHead(leader, base)

  const checkpointer = yield* makeCheckpointer({ schema, storeId, sink: publishStore.sink, trigger }).pipe(
    Effect.provideService(LeaderThreadCtx, leader.ctx),
    Effect.provideService(StateSqliteDb.StateSqliteDb, dbs.dbState),
  )

  const publishes: Array<Record<string, unknown>> = []
  const samples: Array<Record<string, unknown>> = []
  const start = performance.now()
  const cpuStart = process.cpuUsage()
  let lastCpu = cpuStart
  let lastSampleAt = start
  let lastPublishedCount = 0
  let appended = 0

  // Load: `rate` events per second in 100ms ticks, on the wall clock so a slow tick does not lower the rate.
  const deadline = start + seconds * 1000
  while (performance.now() < deadline) {
    const due = Math.floor(((performance.now() - start) / 1000) * rate)
    if (due > appended) {
      yield* backend.advance(due - appended)
      appended = due
    }

    const stats = yield* SubscriptionRef.get(checkpointer.stats)
    if (stats.publishedCount > lastPublishedCount && Option.isSome(stats.lastPublish) === true) {
      lastPublishedCount = stats.publishedCount
      publishes.push({ tSec: (performance.now() - start) / 1000, ...stats.lastPublish.value })
    }

    if (performance.now() - lastSampleAt >= 1000) {
      const cpu = process.cpuUsage(lastCpu)
      const elapsed = performance.now() - lastSampleAt
      lastCpu = process.cpuUsage()
      lastSampleAt = performance.now()
      const leaderHead = (yield* leader.ctx.syncProcessor.syncState.get).upstreamHead.global
      const latestSeqNum = Option.match(stats.latestManifest, { onNone: () => base, onSome: (m) => m.seqNum })
      samples.push({
        tSec: Math.round((performance.now() - start) / 100) / 10,
        backendHead: base + appended,
        leaderHead,
        leaderBehind: base + appended - leaderHead,
        snapshotSeqNum: latestSeqNum,
        snapshotLagEvents: base + appended - latestSeqNum,
        published: stats.publishedCount,
        cpuShare: (cpu.user + cpu.system) / 1000 / elapsed,
        ...memorySnapshot(),
      })
    }
    yield* Effect.sleep(TICK_MS)
  }

  const cpu = process.cpuUsage(cpuStart)
  const wallMs = performance.now() - start
  const final = yield* SubscriptionRef.get(checkpointer.stats)
  writeResult('checkpointer', {
    command: 'checkpointer',
    label,
    mix,
    base,
    rate,
    seconds,
    trigger: { ...Snapshot.SnapshotTrigger.defaultTriggerParams, ...trigger },
    appended,
    finalLeaderBehind: (samples.at(-1)?.leaderBehind as number | undefined) ?? 0,
    published: final.publishedCount,
    failed: final.failedCount,
    cpuShare: (cpu.user + cpu.system) / 1000 / wallMs,
    publishes,
    samples,
    hardware: hardware(),
  })
  yield* leader.shutdown
})

await Effect.runPromise(program.pipe(Effect.scoped, Effect.provide(PlatformNode.NodeFileSystem.layer)))
process.exit(0)
