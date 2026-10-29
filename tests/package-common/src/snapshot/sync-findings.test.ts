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
 * Minimal reproductions of leader sync bugs the snapshot fuzzer found (seeds 3 and 17 at the time of writing). They are
 * not specific to snapshot boot: the failing variants never install a snapshot, and the same schedule passes when the
 * client boots from a snapshot (because its first own events are then never rebased). They stay marked as expected
 * failures until the core is fixed; the passing variants are controls.
 *
 * Shared schedule: the backend has 13 events; a client commits 3 events right after boot, before it pulled the
 * backend, so they are rebased (generation 1) and then confirmed at 14..16. It then loses its downstream (offline,
 * or only pulls held back), the backend moves on by 5 events, the client commits 2 more events and is killed.
 * After a restart (optionally with a state schema change, which rebuilds state from the eventlog) it must converge.
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
   * BUG: the restarted leader stops with `MaterializationJournalError: Missing materialization journal record for
   * e16r1` (rollback of an already confirmed event) and never syncs again. With the default `onSyncError: 'ignore'`
   * this is silent. Suspect: the boot-time pending set or the rebase rollback set includes the confirmed head event
   * when the confirmed own events carry a non-zero local rebase generation (see `getInitialSyncState` /
   * `Eventlog.getEventsSince`, which compare positions including `rebaseGeneration`).
   */
  Vitest.live.fails(
    'BUG: restart after rebased-then-confirmed own events and offline commits stops the leader',
    (test) =>
      runSchedule({ snapshot: false, upgrade: false }).pipe(
        Effect.provide(layer),
        Vitest.withTestCtx(test, { timeout: 20_000 }),
      ),
    20_000,
  )

  /**
   * BUG: same schedule with a state rebuild on restart: the leader converges and its state matches a full replay, but
   * the confirmed event at 16 (its own `todoCreated c`) is missing from its eventlog afterwards. The fuzzer hit this for
   * a snapshot-booted client after the backfill path rebuilt its eventlog (missing `25:v1.TodoCompleted:c0`).
   */
  Vitest.live.fails(
    'BUG: the same schedule with a schema upgrade drops a confirmed own event from the eventlog',
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
