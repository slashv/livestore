import { expect } from 'vitest'

import { Vitest } from '@livestore/utils-dev/node-vitest'
import { Effect } from '@livestore/utils/effect'
import { PlatformNode } from '@livestore/utils/node'

import {
  type BootedLeader,
  encodeArgs,
  makeVersionedSchemas,
  nextLocalEvent,
  waitForNoPending,
  waitForUpstreamHead,
  withFreshStateDb,
} from './harness.ts'
import { expectEventlogInvariants, makeSnapshotWorld } from './oracle.ts'

/**
 * Minimal reproductions of a leader restart bug the snapshot fuzzer found (seeds 3 and 17 at the time of writing). It
 * was not specific to snapshot boot: the regression tests never install a snapshot, and the same schedule passed when
 * the client booted from a snapshot (because its first own events are then never rebased). The snapshot-booted
 * variants stay as controls.
 *
 * Shared schedule: the backend has 13 events; a client commits 3 events right after boot, before it pulled the
 * backend, so they are rebased (generation 1) and then confirmed at 14..16. It then loses its downstream (offline,
 * or only pulls held back), the backend moves on by 5 events, the client commits 2 more events and is killed.
 * After a restart (optionally with a state schema change, which rebuilds state from the eventlog) it must converge.
 *
 * The confirmed rows keep rebase generation 1 while the events committed after the confirmation carry generation 0, so
 * the boot-time pending set used to include the confirmed event at the backend head (`e16r1`). See
 * `Eventlog.getEventsSince`, which now compares positions without the rebase generation.
 */

const { v1, v2 } = makeVersionedSchemas()

const push = (leader: BootedLeader, id: string) =>
  Effect.gen(function* () {
    const event = yield* nextLocalEvent(leader, encodeArgs('todoCreated', { id, text: id, author: leader.clientId }))
    yield* leader.ctx.syncProcessor.push([event])
  })

const runSchedule = ({ snapshot, upgrade }: { snapshot: boolean; upgrade: boolean }) =>
  Effect.gen(function* () {
    const world = yield* makeSnapshotWorld()
    yield* world.append(13)
    if (snapshot === true) yield* world.publishAtHead(v1)
    const first = yield* world.boot({ clientId: 'c0', leaderSchema: v1, snapshot })
    for (const id of ['a', 'b', 'c']) yield* push(first, id)
    yield* waitForNoPending(first)

    yield* world.network.setOnline('c0', false)
    yield* world.append(5)
    for (const id of ['d', 'e']) yield* push(first, id)
    yield* first.shutdown
    yield* world.network.setOnline('c0', true)

    const dbs = upgrade === true ? yield* withFreshStateDb(world.makeSqliteDb, first.dbs) : first.dbs
    const restarted = yield* world.boot({ clientId: 'c0', dbs, leaderSchema: upgrade === true ? v2 : v1, snapshot })
    yield* waitForNoPending(restarted, '5 seconds')
    const head = yield* world.network.head
    yield* waitForUpstreamHead(restarted, head, '5 seconds')
    expect(restarted.shutdownMessages).toEqual([])
    expectEventlogInvariants({ dbEventlog: dbs.dbEventlog, backendLog: yield* world.network.log, head })
  })

const layer = PlatformNode.NodeFileSystem.layer

Vitest.describe('leader sync findings', () => {
  /**
   * Regression: the restarted leader rolled back the confirmed event at the backend head and stopped with
   * `MaterializationJournalError: Missing materialization journal record for e16r1`, silently under the default
   * `onSyncError: 'ignore'`, and never synced again.
   */
  Vitest.live(
    'restart after rebased-then-confirmed own events and offline commits converges',
    (test) =>
      runSchedule({ snapshot: false, upgrade: false }).pipe(
        Effect.provide(layer),
        Vitest.withTestCtx(test, { timeout: 20_000 }),
      ),
    20_000,
  )

  /**
   * Regression: with a state rebuild on restart the rollback found its journal records, so the leader rebased the
   * confirmed event at 16 (its own `todoCreated c`) together with its pending events: it deleted the row at 16 and
   * pushed `c` to the backend a second time (at 22). The fuzzer hit this for a snapshot-booted client after the backfill
   * path rebuilt its eventlog (missing `25:v1.TodoCompleted:c0`).
   */
  Vitest.live(
    'the same schedule with a schema upgrade keeps every confirmed own event in the eventlog',
    (test) =>
      runSchedule({ snapshot: false, upgrade: true }).pipe(
        Effect.provide(layer),
        Vitest.withTestCtx(test, { timeout: 20_000 }),
      ),
    20_000,
  )

  for (const upgrade of [false, true]) {
    Vitest.live(
      `control: snapshot-booted client with the same schedule converges (upgrade=${upgrade})`,
      (test) =>
        runSchedule({ snapshot: true, upgrade }).pipe(
          Effect.provide(layer),
          Vitest.withTestCtx(test, { timeout: 20_000 }),
        ),
      20_000,
    )
  }
})
