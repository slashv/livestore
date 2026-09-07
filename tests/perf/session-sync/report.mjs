import fs from 'node:fs/promises'
import path from 'node:path'

const results = JSON.parse(await fs.readFile(path.join(import.meta.dirname, 'results.json'), 'utf8'))
const groups = new Map()
for (const sample of results.samples) {
  const key = `${sample.scenario} / ${sample.eventCount} events / ${sample.writesPerEvent} writes`
  const group = groups.get(key) ?? { mailbox: [], owner: [] }
  group[sample.implementation].push(sample)
  groups.set(key, group)
}
const median = (samples, field) => {
  const sorted = samples.map((sample) => sample[field]).toSorted((a, b) => a - b)
  return sorted[Math.floor(sorted.length / 2)].toFixed(1)
}
const count = (samples, field, expected = true) => samples.filter((sample) => sample[field] === expected).length
const lines = [
  '# Session sync browser measurements',
  '',
  `Generated from results.json at ${results.generatedAt}. ${results.browser}; ${results.platform}; ${results.cpu}.`,
  '',
  `${results.repetitions} measured runs per variant/workload, after one warmup each. Orders alternate. Values below are medians in milliseconds. The sample is too small for confident tail-latency claims.`,
  '',
  'Input is a synthetic DOM input event scheduled by a timer before batch delivery. Deadline-to-frame includes scheduling delay, a real Store.commit, reactive DOM updates, and the next RAF callback. It is not trusted input, INP, or a display scanout measurement.',
  '',
  '| Workload | Variant | Sync | Input delay | Commit | Deadline → RAF | Max RAF gap | Input during sync | Final head mismatch |',
  '| --- | --- | ---: | ---: | ---: | ---: | ---: | ---: | ---: |',
]
for (const [workload, group] of groups) {
  for (const implementation of ['mailbox', 'owner']) {
    const samples = group[implementation]
    if (samples.length === 0) continue
    lines.push(
      `| ${workload} | ${implementation} | ${median(samples, 'syncMs')} | ${median(samples, 'inputDelayMs')} | ${median(samples, 'commitMs')} | ${median(samples, 'deadlineToFrameMs')} | ${median(samples, 'maxFrameGapMs')} | ${count(samples, 'inputDuringSync')}/${samples.length} | ${count(samples, 'stateHeadCorrect', false)}/${samples.length} |`,
    )
  }
}
lines.push('', '## Correctness and interpretation', '')
for (const implementation of ['mailbox', 'owner']) {
  const samples = results.samples.filter((sample) => sample.implementation === implementation)
  lines.push(
    `- ${implementation}: ${samples.length} samples; row failures ${count(samples, 'rowsCorrect', false)}, pending failures ${count(samples, 'pendingCorrect', false)}, durable-head failures ${count(samples, 'stateHeadCorrect', false)}, immediate-read failures ${count(samples, 'immediateReadCorrect', false)}, propagation failures ${count(samples, 'propagationCorrect', false)}. Input observed partially reconciled rows/state in ${count(samples, 'transientRowsMatchState', false)} samples.`,
  )
}
lines.push(
  '',
  `Runtime/time-out failures: ${results.failedTrials?.length ?? 0}. See the raw artifact for per-trial details.`,
  '',
  'A low input delay does not establish correctness. Samples with a durable-head mismatch must not be treated as semantically equivalent successful runs. The cancellation workload deliberately injects 20ms into an old push finalizer; it is a controlled stress case, not a measured production cancellation cost.',
  '',
  'The in-memory adapter keeps actual browser SQLite, Store materializers, journals, and query subscriptions. Leader transport is controlled. OPFS, worker messaging, remote network latency, and multi-tab behavior are outside this experiment.',
  '',
)
await fs.writeFile(path.join(import.meta.dirname, 'RESULTS.md'), lines.join('\n'))
console.log(lines.join('\n'))
