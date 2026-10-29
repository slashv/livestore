import { describe, expect, it } from 'vitest'

import { Snapshot } from '@livestore/common'
import type { Checkpointer } from '@livestore/common/leader-thread'
import { Vitest } from '@livestore/utils-dev/node-vitest'
import { Effect, Option, SubscriptionRef } from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import { makeVersionedSchemas, waitForUpstreamHead } from './harness.ts'
import { makeRng, makeSnapshotWorld } from './oracle.ts'

/**
 * Verifier 4, checkpointer trigger: the pure cost model (`SnapshotTrigger.decide`) and the checkpointer loop that
 * applies it, driven by a fake clock.
 */

const { decide, defaultTriggerParams } = Snapshot.SnapshotTrigger
const { v1 } = makeVersionedSchemas()

describe('SnapshotTrigger.decide', () => {
  const params = { ...defaultTriggerParams, minIntervalMs: 1_000, minTailEvents: 10 }

  it('waits on a quiet store, whatever else holds', () => {
    for (const tailEvents of [0, -1]) {
      expect(decide({ tailEvents, snapshotBytes: 0, msSinceLastSnapshot: Infinity }, params)).toEqual({
        _tag: 'wait',
        reason: 'quiet',
      })
    }
  })

  it('applies minTailEvents before minIntervalMs and the cost model', () => {
    expect(decide({ tailEvents: 9, snapshotBytes: 0, msSinceLastSnapshot: Infinity }, params)).toEqual({
      _tag: 'wait',
      reason: 'min-tail',
    })
    expect(decide({ tailEvents: 10, snapshotBytes: 0, msSinceLastSnapshot: 999 }, params)).toEqual({
      _tag: 'wait',
      reason: 'min-interval',
    })
    expect(decide({ tailEvents: 10, snapshotBytes: 0, msSinceLastSnapshot: 1_000 })._tag).toBe('wait')
    expect(decide({ tailEvents: 10, snapshotBytes: 0, msSinceLastSnapshot: 1_000 }, params)._tag).toBe('publish')
  })

  it('publishes exactly when tail × replay cost exceeds alpha × download cost', () => {
    // 10 MB at defaults: download 400 ms, alpha 0.5 → threshold 200 ms → 400 events at 0.5 ms/event.
    const snapshotBytes = 10_000_000
    const at = (tailEvents: number) => decide({ tailEvents, snapshotBytes, msSinceLastSnapshot: Infinity }, params)
    expect(at(400)).toEqual({ _tag: 'wait', reason: 'cost' })
    expect(at(401)._tag).toBe('publish')
    expect((at(401) as { downloadCostMs: number }).downloadCostMs).toBeCloseTo(400)
    expect((at(401) as { tailCostMs: number }).tailCostMs).toBeCloseTo(200.5)
  })

  it('agrees with the inequality on random inputs and is monotone in the tail', () => {
    const rng = makeRng(0xc0ffee)
    for (let i = 0; i < 2_000; i++) {
      const p = {
        alpha: rng.next() * 2,
        replayCostPerEventMs: rng.next() * 0.2,
        downloadCostPerByteMs: rng.next() * 0.0001,
        minIntervalMs: rng.int(0, 10_000),
        minTailEvents: rng.int(0, 500),
      }
      const input = {
        tailEvents: rng.int(-5, 5_000),
        snapshotBytes: rng.int(0, 50_000_000),
        msSinceLastSnapshot: rng.chance(0.1) === true ? Infinity : rng.int(0, 20_000),
      }
      const expected =
        input.tailEvents > 0 &&
        input.tailEvents >= p.minTailEvents &&
        input.msSinceLastSnapshot >= p.minIntervalMs &&
        input.tailEvents * p.replayCostPerEventMs > p.alpha * input.snapshotBytes * p.downloadCostPerByteMs
      expect(decide(input, p)._tag, JSON.stringify({ input, p })).toBe(expected === true ? 'publish' : 'wait')
      if (expected === true) {
        expect(decide({ ...input, tailEvents: input.tailEvents + rng.int(1, 1_000) }, p)._tag).toBe('publish')
      }
    }
  })
})

Vitest.describe('checkpointer loop', () => {
  Vitest.live('a quiet store publishes nothing, a moving one publishes, then it is quiet again', (test) =>
    Effect.gen(function* () {
      let now = 0
      const world = yield* makeSnapshotWorld()
      const { checkpointer, leader } = yield* world.startCheckpointer({
        leaderSchema: v1,
        trigger: { minIntervalMs: 0, minTailEvents: 1, alpha: 0 },
        pollInterval: '5 millis',
        now: () => now,
      })
      const stats = () => SubscriptionRef.get(checkpointer.stats)

      // Empty store: many evaluations, no publication.
      yield* Effect.sleep('100 millis')
      expect((yield* stats()).publishedCount).toBe(0)
      expect((yield* stats()).lastDecision).toEqual({ _tag: 'wait', reason: 'quiet' })
      expect(world.store.testing.manifests.size).toBe(0)

      yield* world.append(5)
      yield* waitUntilPublishedAt(checkpointer, 5)

      // Quiet again: time passes, nothing is published, the evaluations report a quiet store.
      now += 1_000_000
      yield* Effect.sleep('100 millis')
      expect((yield* stats()).publishedCount).toBe(1)
      expect((yield* stats()).lastDecision).toEqual({ _tag: 'wait', reason: 'quiet' })
      yield* leader.shutdown

      // A restarted checkpointer on a quiet store stays quiet too (it resumes from the sink).
      const restarted = yield* world.startCheckpointer({
        leaderSchema: v1,
        trigger: { minIntervalMs: 0, minTailEvents: 1, alpha: 0 },
        pollInterval: '5 millis',
        now: () => now,
        clientId: 'restarted',
        waitForHead: 5,
      })
      yield* Effect.sleep('100 millis')
      expect((yield* SubscriptionRef.get(restarted.checkpointer.stats)).publishedCount).toBe(0)
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test, { timeout: 30_000 })),
  )

  Vitest.live('the loop follows minTailEvents, minIntervalMs (fake clock) and the cost model', (test) =>
    Effect.gen(function* () {
      let now = 1_000_000
      const world = yield* makeSnapshotWorld()
      const trigger = {
        minIntervalMs: 10_000,
        minTailEvents: 20,
        alpha: 1,
        replayCostPerEventMs: 1,
        // Cost threshold scales with the state size; pick it so ~50 events are needed for the current state.
        downloadCostPerByteMs: 0,
      }
      const { checkpointer, leader } = yield* world.startCheckpointer({
        leaderSchema: v1,
        trigger,
        pollInterval: '5 millis',
        now: () => now,
      })
      const stats = () => SubscriptionRef.get(checkpointer.stats)
      const settle = (head: number) =>
        Effect.gen(function* () {
          yield* waitForUpstreamHead(leader, head)
          // Let the loop evaluate a few times at this head.
          yield* Effect.sleep('40 millis')
          return yield* stats()
        })

      // Below minTailEvents: no publication even though the interval and cost allow it.
      yield* world.append(19)
      expect((yield* settle(19)).lastDecision).toEqual({ _tag: 'wait', reason: 'min-tail' })
      expect((yield* stats()).publishedCount).toBe(0)

      // Reaching minTailEvents publishes immediately (no previous snapshot, so no interval applies).
      yield* world.append(1)
      yield* waitUntilPublishedAt(checkpointer, 20)

      // Enough tail but inside minIntervalMs: wait; the fake clock moving past it publishes.
      yield* world.append(30)
      expect((yield* settle(50)).lastDecision).toEqual({ _tag: 'wait', reason: 'min-interval' })
      now += 9_999
      expect((yield* settle(50)).lastDecision).toEqual({ _tag: 'wait', reason: 'min-interval' })
      now += 1
      yield* waitUntilPublishedAt(checkpointer, 50)

      // Cost model: with a real download cost the tail must outweigh alpha × bytes × cost. Counter events keep the
      // state size (and so the threshold) constant while the tail grows.
      yield* leader.shutdown
      const costly = yield* world.startCheckpointer({
        leaderSchema: v1,
        dbs: leader.dbs,
        clientId: 'costly',
        trigger: { ...trigger, minIntervalMs: 0, minTailEvents: 1, downloadCostPerByteMs: 0.001 },
        pollInterval: '5 millis',
        now: () => now,
      })
      yield* waitForUpstreamHead(costly.leader, 50)
      const [pageCount] = costly.leader.dbs.dbState.select<{ page_count: number }>('PRAGMA page_count')
      const [pageSize] = costly.leader.dbs.dbState.select<{ page_size: number }>('PRAGMA page_size')
      const threshold = Math.floor(pageCount!.page_count * pageSize!.page_size * 0.001)
      expect(threshold).toBeGreaterThan(1)

      yield* world.append(threshold, () => 'counter')
      yield* waitForUpstreamHead(costly.leader, 50 + threshold)
      yield* Effect.sleep('40 millis')
      const before = yield* SubscriptionRef.get(costly.checkpointer.stats)
      expect(before.lastDecision).toEqual({ _tag: 'wait', reason: 'cost' })
      expect(before.publishedCount).toBe(0)

      yield* world.append(1, () => 'counter')
      yield* waitUntilPublishedAt(costly.checkpointer, 50 + threshold + 1)
      const after = yield* SubscriptionRef.get(costly.checkpointer.stats)
      expect(Option.getOrThrow(after.lastPublish).tailEvents).toBe(threshold + 1)
    }).pipe(Effect.provide(PlatformNode.NodeFileSystem.layer), Vitest.withTestCtx(test, { timeout: 30_000 })),
  )
})

const waitUntilPublishedAt = (checkpointer: Checkpointer, seqNum: number) =>
  Effect.gen(function* () {
    while (
      Option.match((yield* SubscriptionRef.get(checkpointer.stats)).latestManifest, {
        onNone: () => 0,
        onSome: (m) => m.seqNum,
      }) < seqNum
    ) {
      yield* Effect.sleep(5)
    }
  }).pipe(Effect.timeout('10 seconds'), Effect.orDie)
