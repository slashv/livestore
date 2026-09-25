import { execFileSync } from 'node:child_process'
import fs from 'node:fs/promises'
import os from 'node:os'
import path from 'node:path'

import { expect, test } from '@playwright/test'

import type { Sample, Trial } from './app.ts'

test('compare session processors with real Store and browser SQLite', async ({ page, browser }, testInfo) => {
  const pages = { mailbox: page, owner: await page.context().newPage() }
  await pages.mailbox.goto('http://localhost:4178')
  await pages.owner.goto('http://localhost:4179')
  await expect(pages.mailbox.locator('#status')).toHaveText('Ready')
  await expect(pages.owner.locator('#status')).toHaveText('Ready')
  const samples: Sample[] = []
  const failedTrials: { trial: Trial; iteration: number; error: string }[] = []
  const runs = Number(process.env.SESSION_SYNC_RUNS ?? 5)
  const workloads: Omit<Trial, 'implementation'>[] = [
    { scenario: 'idle', eventCount: 0, writesPerEvent: 1 },
    ...[100, 1000].flatMap((eventCount) =>
      [1, 5].flatMap((writesPerEvent) =>
        (['advance', 'rebase', 'cancellation'] as const).map((scenario) => ({ scenario, eventCount, writesPerEvent })),
      ),
    ),
  ]
  const output = path.join(import.meta.dirname, 'results.json')
  const write = () =>
    fs.writeFile(
      output,
      JSON.stringify(
        {
          generatedAt: new Date().toISOString(),
          baselineCommit: execFileSync('git', ['rev-parse', 'HEAD'], {
            cwd: process.env.SESSION_SYNC_BASELINE_WORKTREE,
            encoding: 'utf8',
          }).trim(),
          sourceCommit: execFileSync('git', ['rev-parse', 'HEAD'], {
            cwd: import.meta.dirname,
            encoding: 'utf8',
          }).trim(),
          browser: browser.version(),
          platform: `${os.platform()} ${os.arch()}`,
          cpu: os.cpus()[0]?.model,
          repetitions: runs,
          input:
            'Synthetic input event scheduled by a 5ms timer before pull delivery; not a trusted browser input measurement.',
          samples,
          failedTrials,
        },
        null,
        2,
      ),
    )
  for (const workload of workloads) {
    // Warm both variants once; alternate ordering on every measured pair to reduce order/thermal bias.
    for (let iteration = -1; iteration < runs; iteration++) {
      const order = iteration % 2 === 0 ? (['owner', 'mailbox'] as const) : (['mailbox', 'owner'] as const)
      for (const implementation of order) {
        const trial: Trial = { ...workload, implementation }
        const trialPage = pages[implementation]
        try {
          await trialPage.bringToFront()
          const sample = await trialPage.evaluate((input) => window.sessionSyncBenchmark.run(input), trial)
          if (iteration >= 0) samples.push(sample)
        } catch (error) {
          failedTrials.push({ trial, iteration, error: String(error) })
          await trialPage.reload()
          await expect(trialPage.locator('#status')).toHaveText('Ready')
        } finally {
          await write()
        }
      }
    }
  }
  await testInfo.attach('session-sync-samples', { path: output, contentType: 'application/json' })
  const failures = samples.filter(
    (sample) =>
      sample.inputError !== null ||
      sample.cancellationObservedCorrect === false ||
      !sample.rowsCorrect ||
      !sample.pendingCorrect ||
      !sample.stateHeadCorrect ||
      !sample.transientRowsMatchState ||
      !sample.immediateReadCorrect ||
      !sample.propagationCorrect,
  )
  console.log(JSON.stringify({ sampleCount: samples.length, correctnessFailures: failures.length, output }))
  // Keep the full raw artifact even if a compared implementation violates an invariant.
  expect(failures, 'All final durable and observable state checks').toEqual([])
  expect(failedTrials, 'Trials complete within their bounds').toEqual([])
})
