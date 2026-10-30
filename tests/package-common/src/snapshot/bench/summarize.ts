/**
 * Turns `results/*.jsonl` into the markdown tables of `contributor-docs/perf/snapshot-boot-benchmarks.md`.
 *
 *   node summarize.ts > /tmp/tables.md
 *
 * Statistics: median and nearest-rank p95 over repetitions (with n ≤ 5, p95 is the maximum). "CPU" is user + system
 * time of the benchmark process, which is less sensitive than wall-clock time to other load on the machine.
 */
import fs from 'node:fs'
import path from 'node:path'

import { fromJson, resultsDir } from './lib.ts'

type Row = Record<string, any>

const read = (file: string): Row[] => {
  const full = path.join(resultsDir, `${file}.jsonl`)
  if (fs.existsSync(full) === false) return []
  return fs
    .readFileSync(full, 'utf8')
    .split('\n')
    .filter((line) => line.trim() !== '')
    .map((line) => fromJson(line) as Row)
}

const median = (values: number[]) => {
  const sorted = values.toSorted((a, b) => a - b)
  const mid = Math.floor(sorted.length / 2)
  return sorted.length % 2 === 1 ? sorted[mid]! : (sorted[mid - 1]! + sorted[mid]!) / 2
}
const p95 = (values: number[]) => values.toSorted((a, b) => a - b)[Math.ceil(0.95 * values.length) - 1]!
const groupBy = <T>(rows: T[], key: (row: T) => string) => {
  const groups = new Map<string, T[]>()
  for (const row of rows) groups.set(key(row), [...(groups.get(key(row)) ?? []), row])
  return groups
}

const s = (ms: number) =>
  ms >= 10_000 ? `${(ms / 1000).toFixed(0)} s` : ms >= 1000 ? `${(ms / 1000).toFixed(1)} s` : `${ms.toFixed(0)} ms`
const mb = (bytes: number) => (bytes >= 1e9 ? `${(bytes / 1e9).toFixed(2)} GB` : `${(bytes / 1e6).toFixed(1)} MB`)
const k = (n: number) => (n >= 1e6 ? `${n / 1e6}M` : n >= 1e3 ? `${n / 1e3}k` : `${n}`)
/** Transfer time of `bytes` at `mbit` Mbit/s. */
const transferMs = (bytes: number, mbit: number) => (bytes * 8) / (mbit * 1000)
const table = (header: string[], rows: string[][]) =>
  [
    `| ${header.join(' | ')} |`,
    `| ${header.map(() => '---').join(' | ')} |`,
    ...rows.map((r) => `| ${r.join(' | ')} |`),
  ].join('\n')

const out: string[] = []

// --- S1/S2/S3: boot time and bytes ---------------------------------------------------------------------------------
const replays = read('replay').filter((r) => r.breakdown === 'none' && r.storage === 'fs' && r.rep > 0)
const prepareReplays = read('prepare-replay')
const snapshots = read('snapshot').filter((r) => r.rep > 0)

interface BootGroup {
  mix: string
  events: number
  mode: string
  n: number
  wall: number[]
  cpu: number[]
  eventBytes: number
  snapshotBytes: number
  tail: number
}
const bootGroups: BootGroup[] = []
for (const [, rows] of groupBy(replays, (r) => `${r.mix}/${r.events}`)) {
  const first = rows[0]!
  bootGroups.push({
    mix: first.mix,
    events: first.events,
    mode: 'full replay',
    n: rows.length,
    wall: rows.map((r) => r.clientMs),
    cpu: rows.map((r) => r.cpuUserMs + r.cpuSystemMs),
    eventBytes: median(rows.map((r) => r.eventBytes)),
    snapshotBytes: 0,
    tail: first.events,
  })
}
for (const [, rows] of groupBy(snapshots, (r) => `${r.mix}/${r.events}/${r.tail}`)) {
  const first = rows[0]!
  bootGroups.push({
    mix: first.mix,
    events: first.events,
    mode: `snapshot + ${k(first.tail)} tail`,
    n: rows.length,
    wall: rows.map((r) => r.clientMs),
    cpu: rows.map((r) => r.cpuUserMs + r.cpuSystemMs),
    eventBytes: median(rows.map((r) => r.eventBytes)),
    snapshotBytes: first.snapshotBytes,
    tail: first.tail,
  })
}
for (const r of prepareReplays) {
  bootGroups.push({
    mix: r.mix,
    events: r.events,
    mode: 'full replay',
    n: 1,
    wall: [r.clientMs],
    cpu: [r.cpuUserMs + r.cpuSystemMs],
    eventBytes: r.eventBytes,
    snapshotBytes: 0,
    tail: r.events,
  })
}
bootGroups.sort((a, b) => a.mix.localeCompare(b.mix) || a.events - b.events || b.tail - a.tail)

/**
 * Full-replay CPU for (mix, events): measured runs when there are any, otherwise the per-event CPU of the closest
 * longer prepare replay scaled to `events` (marked `~`).
 */
const fullReplayCost = (
  mix: string,
  events: number,
  metric: 'cpu' | 'wall',
): { ms: number; approx: boolean } | undefined => {
  const measured = bootGroups.find((g) => g.mode === 'full replay' && g.mix === mix && g.events === events)
  if (measured !== undefined) return { ms: median(metric === 'cpu' ? measured.cpu : measured.wall), approx: false }
  const reference = prepareReplays
    .filter((r) => r.mix === mix)
    .toSorted((a, b) => Math.abs(a.events - events) - Math.abs(b.events - events))[0]
  if (reference === undefined) return undefined
  const ms = metric === 'cpu' ? reference.cpuUserMs + reference.cpuSystemMs : reference.clientMs
  return { ms: (ms / reference.events) * events, approx: true }
}
const speedup = (full: { ms: number; approx: boolean } | undefined, local: number) =>
  full === undefined ? '–' : `${full.approx === true ? '~' : ''}${(full.ms / local).toFixed(0)}×`
/** `wall` on a quiet machine, `cpu` (default) when the machine was loaded and wall time is inflated. */
const localMetric = process.env.SNAPSHOT_BENCH_LOCAL === 'wall' ? 'wall' : 'cpu'

out.push('### Boot time and bytes (S1, S2, S3)\n')
out.push(
  `Network columns: local ${localMetric === 'wall' ? 'wall-clock' : 'CPU'} time plus modelled ` +
    'transfer (replay: `max(local, transfer)` since pages stream; snapshot: `transfer(blob) + max(local, transfer(tail))`).\n',
)
out.push(
  table(
    [
      'mix',
      'events',
      'boot',
      'n',
      'wall median',
      'wall p95',
      'speedup (wall)',
      'CPU median',
      'speedup (CPU)',
      'event bytes',
      'snapshot bytes',
      '@100 Mbit/s',
      '@1 Gbit/s',
    ],
    bootGroups.map((g) => {
      const local = median(localMetric === 'wall' ? g.wall : g.cpu)
      const bytes = g.eventBytes + g.snapshotBytes
      const total = (mbit: number) =>
        g.mode === 'full replay'
          ? Math.max(local, transferMs(bytes, mbit))
          : transferMs(g.snapshotBytes, mbit) + Math.max(local, transferMs(g.eventBytes, mbit))
      return [
        g.mix,
        k(g.events),
        g.mode,
        String(g.n),
        s(median(g.wall)),
        s(p95(g.wall)),
        g.mode === 'full replay' ? '–' : speedup(fullReplayCost(g.mix, g.events, 'wall'), median(g.wall)),
        s(median(g.cpu)),
        g.mode === 'full replay' ? '–' : speedup(fullReplayCost(g.mix, g.events, 'cpu'), median(g.cpu)),
        mb(g.eventBytes),
        g.snapshotBytes > 0 ? mb(g.snapshotBytes) : '–',
        s(total(100)),
        s(total(1000)),
      ]
    }),
  ),
)

if (prepareReplays.length > 0) {
  out.push('\nFull replays measured while preparing snapshots (single run, the first position of `prepare`):\n')
  out.push(
    table(
      ['mix', 'events', 'segments', 'wall', 'CPU', 'CPU ms/event', 'event bytes', 'state bytes', 'eventlog bytes'],
      prepareReplays.map((r) => [
        r.mix,
        k(r.events),
        String(r.segments),
        s(r.clientMs),
        s(r.cpuUserMs + r.cpuSystemMs),
        ((r.cpuUserMs + r.cpuSystemMs) / r.events).toFixed(3),
        mb(r.eventBytes),
        mb(r.stateBytes),
        mb(r.eventlogBytes),
      ]),
    ),
  )
}

out.push('\nPer-event full-replay cost (client wall and CPU per event):\n')
out.push(
  table(
    [
      'mix',
      'events',
      'n',
      'wall ms/event',
      'CPU ms/event',
      'wire bytes/event',
      'state bytes/event',
      'JS heap after replay (GC)',
      'peak RSS',
    ],
    [...groupBy(replays, (r) => `${r.mix}/${r.events}`).values()].map((rows) => [
      rows[0]!.mix,
      k(rows[0]!.events),
      String(rows.length),
      (median(rows.map((r) => r.clientMs)) / rows[0]!.events).toFixed(3),
      (median(rows.map((r) => r.cpuUserMs + r.cpuSystemMs)) / rows[0]!.events).toFixed(3),
      (median(rows.map((r) => r.eventBytes)) / rows[0]!.events).toFixed(0),
      (median(rows.map((r) => r.stateBytes)) / rows[0]!.events).toFixed(0),
      rows[0]!.memory === undefined ? '–' : `${median(rows.map((r) => r.memory.heapUsedMiB as number)).toFixed(0)} MiB`,
      `${median(rows.map((r) => r.peakRssMiB as number)).toFixed(0)} MiB`,
    ]),
  ),
)

// --- S5: snapshot boot breakdown -------------------------------------------------------------------------------------
out.push('\n### Snapshot boot breakdown (S5)\n')
out.push(
  table(
    [
      'mix',
      'events',
      'tail',
      'snapshot',
      'manifest',
      'download',
      'verify (sha256)',
      'install',
      'after install (tail)',
      'verify MB/s',
      'install MB/s',
      'tail ms/event',
    ],
    [...groupBy(snapshots, (r) => `${r.mix}/${r.events}/${r.tail}`).values()]
      .toSorted((a, b) => a[0]!.mix.localeCompare(b[0]!.mix) || a[0]!.events - b[0]!.events || a[0]!.tail - b[0]!.tail)
      .map((rows) => {
        const r0 = rows[0]!
        const m = (key: string) => median(rows.map((r) => r[key] as number))
        return [
          r0.mix,
          k(r0.events),
          k(r0.tail),
          mb(r0.snapshotBytes),
          s(m('manifestMs')),
          s(m('downloadMs')),
          s(m('verifyMs')),
          s(m('installMs')),
          s(m('afterInstallMs')),
          (r0.snapshotBytes / 1e6 / (m('verifyMs') / 1000)).toFixed(0),
          (r0.snapshotBytes / 1e6 / (m('installMs') / 1000)).toFixed(0),
          r0.tail > 0 ? (m('afterInstallMs') / r0.tail).toFixed(3) : '–',
        ]
      }),
  ),
)

// --- Checkpointer exports during prepare -----------------------------------------------------------------------------
const prepares = read('prepare').filter((r) => r.byteLength !== undefined || r.error !== undefined)
out.push('\n### Checkpointer export cost vs state size (S6, from `prepare`)\n')
out.push(
  table(
    ['mix', 'seqNum', 'snapshot', 'export', 'hash', 'upload (in-memory)', 'export MB/s', 'hash MB/s', 'error'],
    prepares.map((r) => [
      r.mix,
      k(r.seqNum),
      r.byteLength === undefined ? '–' : mb(r.byteLength),
      r.exportMs === undefined ? '–' : s(r.exportMs),
      r.hashMs === undefined ? '–' : s(r.hashMs),
      r.uploadMs === undefined ? '–' : s(r.uploadMs),
      r.exportMs === undefined ? '–' : (r.byteLength / 1e6 / (r.exportMs / 1000)).toFixed(0),
      r.hashMs === undefined ? '–' : (r.byteLength / 1e6 / (r.hashMs / 1000)).toFixed(0),
      r.error === undefined ? '' : String(r.error).slice(0, 60),
    ]),
  ),
)

// --- S7: export/import ceiling ---------------------------------------------------------------------------------------
const limits = read('export-limits')
if (limits.length > 0) {
  out.push('\n### Export/import vs database size (S7)\n')
  out.push(
    table(
      [
        'database',
        'export',
        'sha256',
        'import (fs target)',
        'export MB/s',
        'sha256 MB/s',
        'import MB/s',
        'wasm heap after export',
        'result',
      ],
      limits.map((r) => [
        mb(r.bytesOnDisk),
        r.exportMs === undefined ? '–' : s(r.exportMs),
        r.sha256Ms === undefined ? '–' : s(r.sha256Ms),
        r.importMs === undefined ? '–' : s(r.importMs),
        r.exportMs === undefined ? '–' : (r.bytesOnDisk / 1e6 / (r.exportMs / 1000)).toFixed(0),
        r.sha256Ms === undefined ? '–' : (r.bytesOnDisk / 1e6 / (r.sha256Ms / 1000)).toFixed(0),
        r.importMs === undefined ? '–' : (r.bytesOnDisk / 1e6 / (r.importMs / 1000)).toFixed(0),
        `${r.afterExport?.wasmHeapMiB ?? '–'} MiB`,
        r.exportError === undefined ? 'ok' : `export failed: ${r.exportError}`,
      ]),
    ),
  )
}

// --- S4: replay breakdown ---------------------------------------------------------------------------------------------
const breakdowns = read('replay').filter((r) => r.breakdown !== 'none')
const micro = read('micro')
if (breakdowns.length > 0) {
  out.push('\n### Full-replay breakdown (S4)\n')
  for (const [, rows] of groupBy(breakdowns, (r) => `${r.storage}`)) {
    const byVariant = groupBy(rows, (r) => r.breakdown)
    const full = byVariant.get('full') ?? []
    if (full.length === 0) continue
    const events = full[0]!.events
    const us = (ms: number) => ((ms / events) * 1000).toFixed(1)
    const m = (key: string) => median(full.map((r) => r.sql[key] as number))
    const clientMs = median(full.map((r) => r.clientMs))
    const variantMs = (variant: string) => {
      const vs = byVariant.get(variant)
      return vs === undefined ? undefined : median(vs.map((r) => r.clientMs))
    }
    const mic = micro.find((r) => r.mix === full[0]!.mix)
    const parseUs = mic?.jsonParse.perEventUs ?? 0
    const decodeUs = mic?.decodeArgs.perEventUs ?? 0
    const buildUs = (mic?.buildStatements.perEventUs ?? 0) - decodeUs
    const sqlMs = ['stateWrites', 'journalWrites', 'sessionOps', 'stateHead', 'stateTx', 'eventlog'].reduce(
      (t, key) => t + m(key),
      0,
    )
    const jsKnownMs = ((parseUs + decodeUs + buildUs) * events) / 1000
    // The SQLite buckets are wall-clock; on a loaded machine they include time the process waited for a core. Shares
    // of client wall time are robust to that, and scaling a share by the measured CPU per event gives µs of work.
    const cpuPerEventUs = (median(full.map((r) => r.cpuUserMs + r.cpuSystemMs)) / events) * 1000
    const micros = (ms: number) => (ms / events) * 1000
    const sqlShare = (ms: number) => ms / clientMs
    const rowsOut: Array<[string, number, string]> = [
      ['wire JSON parse', micros((parseUs * events) / 1000), 'micro-benchmark (CPU)'],
      ['arg decoding (Schema)', micros((decodeUs * events) / 1000), 'micro-benchmark (CPU)'],
      ['materializer JS: building statements', micros((buildUs * events) / 1000), 'micro-benchmark (CPU)'],
      ['state writes (SQLite, user tables)', sqlShare(m('stateWrites')) * cpuPerEventUs, 'timed SqliteDb wrapper'],
      [
        'journal: session create / changeset / finish',
        sqlShare(m('sessionOps')) * cpuPerEventUs,
        'timed SqliteDb wrapper',
      ],
      ['journal: changeset row writes', sqlShare(m('journalWrites')) * cpuPerEventUs, 'timed SqliteDb wrapper'],
      [
        'state head row + savepoints',
        sqlShare(m('stateHead') + m('stateTx')) * cpuPerEventUs,
        'timed SqliteDb wrapper',
      ],
      [
        'eventlog bookkeeping (inserts, sync status, tx)',
        sqlShare(m('eventlog')) * cpuPerEventUs,
        'timed SqliteDb wrapper',
      ],
    ]
    const attributed = rowsOut.reduce((t, r) => t + r[1], 0)
    rowsOut.push([
      'sync processor, Effect runtime, GC (remainder)',
      cpuPerEventUs - attributed,
      'CPU per event minus the rows above',
    ])
    rowsOut.push(['**total (CPU per event)**', cpuPerEventUs, `n=${full.length}; wall ${us(clientMs)} µs/event`])
    out.push(`\n${full[0]!.storage} storage, ${full[0]!.mix} mix, ${k(events)} events:\n`)
    out.push(
      table(
        ['component', 'µs/event', 'share', 'method'],
        rowsOut.map((r) => [r[0], r[1].toFixed(1), `${((r[1] / cpuPerEventUs) * 100).toFixed(0)}%`, r[2]]),
      ),
    )
    void sqlMs
    void jsKnownMs
    const nosession = variantMs('nosession')
    const noop = variantMs('noop')
    if (nosession !== undefined || noop !== undefined) {
      const cpuOf = (variant: string) => {
        const vs = byVariant.get(variant) ?? []
        return vs.length === 0 ? undefined : (median(vs.map((r) => r.cpuUserMs + r.cpuSystemMs)) / events) * 1000
      }
      out.push('\nVariant runs (whole client, per event):\n')
      out.push(
        table(
          ['variant', 'wall µs/event', 'CPU µs/event', 'CPU delta vs full'],
          (['full', 'nosession', 'noop'] as const)
            .filter((variant) => cpuOf(variant) !== undefined)
            .map((variant) => [
              variant === 'full'
                ? 'full'
                : variant === 'nosession'
                  ? 'no changeset recording (no-op session)'
                  : 'no-op materializers (no state SQL)',
              us(variantMs(variant)!),
              cpuOf(variant)!.toFixed(0),
              variant === 'full' ? '' : (cpuOf(variant)! - cpuOf('full')!).toFixed(0),
            ]),
        ),
      )
    }
  }
}

if (micro.length > 0) {
  out.push('\n### Micro-benchmarks\n')
  out.push(
    table(
      [
        'mix',
        'wire bytes/event',
        'JSON parse µs/event',
        'arg decode µs/event',
        'build statements µs/event',
        'sha256 noble MB/s',
        'sha256 node:crypto MB/s',
      ],
      micro.map((r) => [
        r.mix,
        r.wireBytesPerEvent.toFixed(0),
        r.jsonParse.perEventUs.toFixed(2),
        r.decodeArgs.perEventUs.toFixed(2),
        r.buildStatements.perEventUs.toFixed(1),
        r.sha256.nobleMBps.toFixed(0),
        r.sha256.nodeCryptoMBps.toFixed(0),
      ]),
    ),
  )
}

// --- S6: checkpointer under load -------------------------------------------------------------------------------------
const allCheckpointers = read('checkpointer').filter((r) => r.label !== 'test')
const checkpointers = allCheckpointers.filter((r) => r.clock !== 'virtual' && r.label !== 'sustained')
const sustained = allCheckpointers.filter((r) => r.label === 'sustained')
const virtualCheckpointers = allCheckpointers.filter((r) => r.clock === 'virtual')
if (checkpointers.length > 0) {
  out.push('\n### Checkpointer under sustained load, wall clock (S6)\n')
  out.push(
    table(
      [
        'label',
        'mix',
        'base',
        'rate ev/s',
        'duration',
        'published',
        'median interval',
        'median tail/publish',
        'median snapshot',
        'export+hash median',
        'max snapshot lag',
        'final leader behind',
        'CPU share',
        'heap growth',
      ],
      checkpointers.map((r) => {
        const pubs = r.publishes as Row[]
        const intervals = pubs.slice(1).map((p, i) => p.tSec - pubs[i]!.tSec)
        const samples = r.samples as Row[]
        return [
          r.label,
          r.mix,
          k(r.base),
          String(r.rate),
          `${r.seconds} s`,
          String(r.published),
          intervals.length > 0 ? `${median(intervals).toFixed(1)} s` : '–',
          pubs.length > 0 ? k(median(pubs.map((p) => p.tailEvents))) : '–',
          pubs.length > 0 ? mb(median(pubs.map((p) => p.byteLength))) : '–',
          pubs.length > 0 ? s(median(pubs.map((p) => p.exportMs + p.hashMs))) : '–',
          k(Math.max(...samples.map((x) => x.snapshotLagEvents))),
          k(r.finalLeaderBehind),
          `${(r.cpuShare * 100).toFixed(0)}%`,
          samples.length > 1 ? `${(samples.at(-1)!.heapUsedMiB - samples[0]!.heapUsedMiB).toFixed(0)} MiB` : '–',
        ]
      }),
    ),
  )
}

if (virtualCheckpointers.length > 0) {
  out.push('\n### Checkpointer under sustained load, event time (S6)\n')
  out.push(
    table(
      [
        'trigger',
        'base',
        'rate ev/s',
        'duration',
        'published',
        'median tail/publish',
        'median snapshot',
        'export / hash / upload (median)',
        'max snapshot lag',
        'CPU ms/event',
        'core share at rate',
        'heap growth',
      ],
      virtualCheckpointers.map((r) => {
        const pubs = r.publishes as Row[]
        const samples = r.samples as Row[]
        const med = (key: string) => median(pubs.map((p) => p[key] as number))
        return [
          r.label === 'default' ? 'default' : `${r.label} (replayCostPerEventMs ${r.trigger.replayCostPerEventMs})`,
          k(r.base),
          k(r.rate),
          `${r.seconds} s`,
          String(r.published),
          pubs.length > 0 ? k(med('tailEvents')) : '–',
          pubs.length > 0 ? mb(med('byteLength')) : '–',
          pubs.length > 0 ? `${s(med('exportMs'))} / ${s(med('hashMs'))} / ${s(med('uploadMs'))}` : '–',
          `${k(Math.max(...samples.map((x) => x.snapshotLagEvents)))} (${(Math.max(...samples.map((x) => x.snapshotLagEvents)) / r.rate).toFixed(0)} s)`,
          r.cpuMsPerEvent.toFixed(2),
          `${(r.cpuShare * 100).toFixed(0)}%`,
          samples.length > 1 ? `${(samples.at(-1)!.heapUsedMiB - samples[0]!.heapUsedMiB).toFixed(0)} MiB` : '–',
        ]
      }),
    ),
  )
}

/** Least-squares slope of y over x. */
const slope = (points: ReadonlyArray<readonly [number, number]>) => {
  const n = points.length
  const mx = points.reduce((t, [x]) => t + x, 0) / n
  const my = points.reduce((t, [, y]) => t + y, 0) / n
  const num = points.reduce((t, [x, y]) => t + (x - mx) * (y - my), 0)
  const den = points.reduce((t, [x]) => t + (x - mx) ** 2, 0)
  return den === 0 ? 0 : num / den
}

if (sustained.length > 0) {
  out.push('\n### Sustained checkpointer, wall clock (S6)\n')
  out.push(
    'Heap and wasm growth are least-squares slopes over the samples taken after a forced GC (every 30 s), per applied ' +
      'event. Snapshot lag is the backend head minus the latest published snapshot.\n',
  )
  out.push(
    table(
      [
        'base',
        'rate ev/s',
        'duration',
        'applied',
        'published',
        'median interval',
        'median tail/publish',
        'final snapshot',
        'export / hash / upload (median)',
        'snapshot lag median / max',
        'leader behind max',
        'CPU share',
        'JS heap B/event',
        'wasm heap B/event',
        'RSS start → end',
      ],
      sustained.map((r) => {
        const pubs = r.publishes as Row[]
        const samples = r.samples as Row[]
        const intervals = pubs.slice(1).map((p, i) => p.tSec - pubs[i]!.tSec)
        const gcd = samples.filter((x) => x.afterGc === true)
        const med = (key: string) => median(pubs.map((p) => p[key] as number))
        const lags = samples.map((x) => x.snapshotLagEvents as number)
        return [
          k(r.base),
          k(r.rate),
          `${(r.seconds / 60).toFixed(0)} min`,
          k(Math.round((samples.at(-1)!.leaderHead as number) - r.base)),
          String(r.published),
          intervals.length > 0 ? `${median(intervals).toFixed(1)} s` : '–',
          pubs.length > 0 ? k(Math.round(med('tailEvents'))) : '–',
          pubs.length > 0 ? mb(pubs.at(-1)!.byteLength) : '–',
          pubs.length > 0 ? `${s(med('exportMs'))} / ${s(med('hashMs'))} / ${s(med('uploadMs'))}` : '–',
          `${k(Math.round(median(lags)))} / ${k(Math.max(...lags))}`,
          k(Math.max(...samples.map((x) => x.leaderBehind as number))),
          `${(r.cpuShare * 100).toFixed(0)}%`,
          gcd.length > 2 ? slope(gcd.map((x) => [x.leaderHead, x.heapUsedMiB * 2 ** 20] as const)).toFixed(0) : '–',
          gcd.length > 2 ? slope(gcd.map((x) => [x.leaderHead, x.wasmHeapMiB * 2 ** 20] as const)).toFixed(0) : '–',
          `${samples[0]!.rssMiB} → ${samples.at(-1)!.rssMiB} MiB`,
        ]
      }),
    ),
  )
}

console.log(out.join('\n'))
