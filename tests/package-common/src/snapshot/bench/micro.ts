/**
 * Micro-benchmarks for the parts of a replay that the leader-level runs cannot isolate by timing SQLite calls:
 * wire JSON parsing, event arg decoding, materializer statement building, and the snapshot hash.
 *
 *   node micro.ts --mix append --events 50000
 *
 * Each stage runs over the same generated events, 2 warmup passes + 5 measured passes; the median is reported.
 */
import crypto from 'node:crypto'

import { getExecStatementsFromMaterializer, type SqliteDb, Snapshot } from '@livestore/common'
import { LiveStoreEvent } from '@livestore/common/schema'
import { Schema } from '@livestore/utils/effect'

import { hardware, parseArgs, writeResult } from './lib.ts'
import { eventAt, type MixName, schema } from './workload.ts'

const args = parseArgs(process.argv.slice(2))
const mix = (args.mix ?? 'append') as MixName
const events = Number(args.events ?? 50_000)

const generated = Array.from({ length: events }, (_, i) => eventAt(mix, i + 1))
const wire = JSON.stringify(generated)
const clientEvents = generated.map((event) => LiveStoreEvent.Client.fromGlobal(event))
const decoders = new Map(
  [...schema.eventsDefsMap.entries()].map(([name, def]) => [name, Schema.decodeUnknownSync(def.schema)] as const),
)
/** The materializers used here never query state, so the database is never touched. */
const noDb = {} as SqliteDb

const measure = (fn: () => void) => {
  for (let i = 0; i < 2; i++) fn()
  const runs: number[] = []
  for (let i = 0; i < 5; i++) {
    const start = performance.now()
    fn()
    runs.push(performance.now() - start)
  }
  const median = runs.toSorted((a, b) => a - b)[2]!
  return { totalMs: median, perEventUs: (median / events) * 1000 }
}

const results = {
  jsonParse: measure(() => {
    JSON.parse(wire)
  }),
  decodeArgs: measure(() => {
    for (const event of generated) decoders.get(event.name)!(event.args)
  }),
  /** Includes decoding: this is the per-event JS work of materialization before SQLite runs. */
  buildStatements: measure(() => {
    for (const event of clientEvents) {
      getExecStatementsFromMaterializer({
        eventDef: schema.eventsDefsMap.get(event.name)!,
        materializer: schema.state.materializers.get(event.name)!,
        dbState: noDb,
        event: { decoded: undefined, encoded: event },
      })
    }
  }),
}

/** Snapshot hash throughput: the pure-JS hash used by the manifest check vs Node's native implementation. */
const hashBytes = new Uint8Array(64 * 2 ** 20)
crypto.getRandomValues(hashBytes.subarray(0, 65_536))
const hashRuns = (fn: () => void) => {
  fn()
  const start = performance.now()
  fn()
  fn()
  fn()
  return (performance.now() - start) / 3
}
const nobleMs = hashRuns(() => Snapshot.sha256Hex(hashBytes))
const nativeMs = hashRuns(() => crypto.createHash('sha256').update(hashBytes).digest('hex'))

writeResult('micro', {
  command: 'micro',
  mix,
  events,
  wireBytesPerEvent: wire.length / events,
  ...results,
  sha256: {
    bytes: hashBytes.byteLength,
    nobleMBps: hashBytes.byteLength / 2 ** 20 / (nobleMs / 1000),
    nodeCryptoMBps: hashBytes.byteLength / 2 ** 20 / (nativeMs / 1000),
  },
  hardware: hardware(),
})
