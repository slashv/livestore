/**
 * Runs a long full replay as a chain of processes, each replaying `--segment` events on the persisted databases of
 * the previous one (a client restart), and aggregates the result.
 *
 *   node segmented.ts replay  --mix append --events 1000000 --segment 250000
 *   node segmented.ts prepare --mix update --events 10000000 --segment 250000 --tails 0,1000,10000,100000
 *
 * Why processes and not one leader: a leader retains about 6 KB of JS heap per pulled event (V8's 4 GiB default heap
 * fills at ~700k events) and every SQLite `prepare` leaks its SQL text in the wasm heap (about 1.4 KB per event,
 * 2 GiB cap at ~1.4M events). Restarting the process resets both. Each segment process warms up before its timed
 * part; the aggregate is the sum of the segments' boot-to-head times.
 */
import { spawnSync } from 'node:child_process'
import path from 'node:path'

import { parseArgs, writeResult } from './lib.ts'

const command = process.argv[2]
const args = parseArgs(process.argv.slice(3))
const mix = args.mix ?? 'append'
const events = Number(args.events ?? 1_000_000)
const segment = Number(args.segment ?? 250_000)
const rep = args.rep ?? '0'
const tails = (args.tails ?? '0,1000,10000,100000').split(',').map(Number)
const target = command === 'prepare' ? events - Math.max(...tails) : events
const name = command === 'prepare' ? `checkpointer-${mix}-${events}` : `replay-${mix}-${events}`
const bootScript = path.join(import.meta.dirname, 'boot.ts')

const run = (cliArgs: ReadonlyArray<string>) => {
  const child = spawnSync(process.execPath, ['--expose-gc', bootScript, ...cliArgs], {
    encoding: 'utf8',
    maxBuffer: 64 * 2 ** 20,
    stdio: ['ignore', 'pipe', 'inherit'],
  })
  if (child.status !== 0) throw new Error(`${cliArgs.join(' ')} exited with ${child.status}`)
  const lines = child.stdout
    .trim()
    .split('\n')
    .filter((line) => line.startsWith('{'))
  return lines.map((line) => JSON.parse(line) as Record<string, any>)
}

const segments: Array<Record<string, any>> = []
for (let from = 0; from < target; from += segment) {
  const to = Math.min(target, from + segment)
  const [result] = run([
    'replay',
    '--mix',
    mix,
    '--events',
    String(to),
    '--from',
    String(from),
    '--name',
    name,
    '--rep',
    rep,
  ])
  segments.push(result!)
  console.error(
    JSON.stringify({
      segment: `${from}-${to}`,
      wallMs: Math.round(result!.wallMs),
      peakRssMiB: result!.peakRssMiB,
      ...result!.memory,
    }),
  )
}

const sum = (key: string) => segments.reduce((total, s) => total + (s[key] as number), 0)
const last = segments.at(-1)!
const aggregate = {
  command: 'replay',
  mix,
  events: target,
  storage: 'fs',
  pageSize: last.pageSize,
  rep: Number(rep),
  breakdown: 'none',
  segments: segments.length,
  segmentEvents: segment,
  processPerSegment: true,
  wallMs: sum('wallMs'),
  leaderLayerMs: sum('leaderLayerMs'),
  serverMs: sum('serverMs'),
  clientMs: sum('clientMs'),
  eventBytes: sum('eventBytes'),
  pages: sum('pages'),
  cpuUserMs: sum('cpuUserMs'),
  cpuSystemMs: sum('cpuSystemMs'),
  peakRssMiB: Math.max(...segments.map((s) => s.peakRssMiB as number)),
  stateBytes: last.stateBytes,
  eventlogBytes: last.eventlogBytes,
  segmentWallMs: segments.map((s) => Math.round(s.wallMs)),
  hardware: last.hardware,
}
writeResult(command === 'prepare' ? 'prepare-replay' : 'replay', aggregate)

if (command === 'prepare') {
  run([
    'prepare',
    '--mix',
    mix,
    '--events',
    String(events),
    '--tails',
    tails.join(','),
    '--from',
    String(target),
    '--name',
    name,
  ])
}
