import type { LiveStoreEvent } from '@livestore/common/schema'
import { Events, makeSchema, State } from '@livestore/common/schema'
import { Schema } from '@livestore/utils/effect'

/**
 * Agent-style workload for the snapshot boot benchmarks.
 *
 * Every event is a pure function of its sequence number and the mix, so the benchmark backend can serve any position
 * of a 10M-event history without storing it, and every process sees the same log.
 */

const sessions = State.SQLite.table({
  name: 'sessions',
  columns: {
    id: State.SQLite.text({ primaryKey: true }),
    title: State.SQLite.text({ default: '' }),
    agent: State.SQLite.text({ default: '' }),
    status: State.SQLite.text({ default: 'active' }),
    metadata: State.SQLite.json({ schema: Schema.Unknown, default: {} }),
    createdAt: State.SQLite.integer({ default: 0 }),
    updatedAt: State.SQLite.integer({ default: 0 }),
  },
})

const messages = State.SQLite.table({
  name: 'messages',
  columns: {
    id: State.SQLite.text({ primaryKey: true }),
    sessionId: State.SQLite.text({ default: '' }),
    role: State.SQLite.text({ default: 'assistant' }),
    text: State.SQLite.text({ default: '' }),
    toolCall: State.SQLite.json({ schema: Schema.Unknown, nullable: true }),
    createdAt: State.SQLite.integer({ default: 0 }),
  },
  indexes: [{ name: 'idx_messages_session', columns: ['sessionId'] }],
})

const tasks = State.SQLite.table({
  name: 'tasks',
  columns: {
    id: State.SQLite.text({ primaryKey: true }),
    sessionId: State.SQLite.text({ default: '' }),
    title: State.SQLite.text({ default: '' }),
    status: State.SQLite.text({ default: 'pending' }),
    progress: State.SQLite.real({ default: 0 }),
    result: State.SQLite.json({ schema: Schema.Unknown, nullable: true }),
    updatedAt: State.SQLite.integer({ default: 0 }),
  },
  indexes: [{ name: 'idx_tasks_session', columns: ['sessionId'] }],
})

const Json = Schema.Record(Schema.String, Schema.Unknown)

export const events = {
  sessionCreated: Events.synced({
    name: 'v1.SessionCreated',
    schema: Schema.Struct({
      id: Schema.String,
      title: Schema.String,
      agent: Schema.String,
      metadata: Json,
      createdAt: Schema.Int,
    }),
  }),
  sessionUpdated: Events.synced({
    name: 'v1.SessionUpdated',
    schema: Schema.Struct({ id: Schema.String, status: Schema.String, metadata: Json, updatedAt: Schema.Int }),
  }),
  messageAppended: Events.synced({
    name: 'v1.MessageAppended',
    schema: Schema.Struct({
      id: Schema.String,
      sessionId: Schema.String,
      role: Schema.String,
      text: Schema.String,
      toolCall: Schema.NullOr(Json),
      createdAt: Schema.Int,
    }),
  }),
  messageStreamed: Events.synced({
    name: 'v1.MessageStreamed',
    schema: Schema.Struct({ id: Schema.String, text: Schema.String }),
  }),
  taskCreated: Events.synced({
    name: 'v1.TaskCreated',
    schema: Schema.Struct({ id: Schema.String, sessionId: Schema.String, title: Schema.String, createdAt: Schema.Int }),
  }),
  taskUpdated: Events.synced({
    name: 'v1.TaskUpdated',
    schema: Schema.Struct({
      id: Schema.String,
      status: Schema.String,
      // @effect-diagnostics-next-line schemaNumber:off -- kept as recorded: the cached benchmark snapshots are keyed by this schema's hash
      progress: Schema.Number,
      result: Schema.NullOr(Json),
      updatedAt: Schema.Int,
    }),
  }),
}

export const tables = { sessions, messages, tasks }

const materializers = State.SQLite.materializers(events, {
  'v1.SessionCreated': ({ id, title, agent, metadata, createdAt }) =>
    sessions.insert({ id, title, agent, metadata, createdAt, updatedAt: createdAt }),
  'v1.SessionUpdated': ({ id, status, metadata, updatedAt }) =>
    sessions.update({ status, metadata, updatedAt }).where({ id }),
  'v1.MessageAppended': ({ id, sessionId, role, text, toolCall, createdAt }) =>
    messages.insert({ id, sessionId, role, text, toolCall, createdAt }),
  'v1.MessageStreamed': ({ id, text }) => messages.update({ text }).where({ id }),
  'v1.TaskCreated': ({ id, sessionId, title, createdAt }) =>
    tasks.insert({ id, sessionId, title, updatedAt: createdAt }),
  'v1.TaskUpdated': ({ id, status, progress, result, updatedAt }) =>
    tasks.update({ status, progress, result, updatedAt }).where({ id }),
})

export const schema = makeSchema({ state: State.SQLite.makeState({ tables, materializers }), events })

/** Same events with materializers that emit no SQL: isolates decode + sync + eventlog cost from state writes. */
export const noopSchema = makeSchema({
  state: State.SQLite.makeState({
    tables,
    materializers: State.SQLite.materializers(events, {
      'v1.SessionCreated': () => [],
      'v1.SessionUpdated': () => [],
      'v1.MessageAppended': () => [],
      'v1.MessageStreamed': () => [],
      'v1.TaskCreated': () => [],
      'v1.TaskUpdated': () => [],
    }),
  }),
  events,
})

type Kind = 'sessionCreated' | 'sessionUpdated' | 'messageAppended' | 'messageStreamed' | 'taskCreated' | 'taskUpdated'

/**
 * Event mixes, as counts per 100 events.
 *
 * - `append`: an agent transcript store. Messages are mostly appended, tasks and sessions are updated in place.
 *   Roughly half the events add a row, so state grows with history.
 * - `update`: a streaming agent. Most events rewrite a recent message or a task, so state is far smaller than history.
 */
export const mixes = {
  append: {
    sessionCreated: 1,
    sessionUpdated: 9,
    messageAppended: 45,
    messageStreamed: 0,
    taskCreated: 5,
    taskUpdated: 40,
  },
  update: {
    sessionCreated: 1,
    sessionUpdated: 4,
    messageAppended: 5,
    messageStreamed: 50,
    taskCreated: 2,
    taskUpdated: 38,
  },
} as const satisfies Record<string, Record<Kind, number>>

export type MixName = keyof typeof mixes

const CYCLE = 100
const WRITERS = 8

interface CompiledMix {
  readonly slots: ReadonlyArray<Kind>
  /** prefix[kind][slot] = occurrences of `kind` in slots before `slot` */
  readonly prefix: Record<Kind, Int32Array>
  readonly perCycle: Record<Kind, number>
}

const compiled = new Map<MixName, CompiledMix>()

const compileMix = (mixName: MixName): CompiledMix => {
  const cached = compiled.get(mixName)
  if (cached !== undefined) return cached
  const mix = mixes[mixName]
  const slots: Kind[] = []
  for (const [kind, count] of Object.entries(mix) as [Kind, number][]) {
    for (let i = 0; i < count; i++) slots.push(kind)
  }
  if (slots.length !== CYCLE) throw new Error(`Mix ${mixName} has ${slots.length} slots, expected ${CYCLE}`)
  // Deterministic shuffle so kinds interleave, then make sure the first slot creates a session.
  const random = rng(0x5eed)
  for (let i = slots.length - 1; i > 0; i--) {
    const j = Math.floor(random() * (i + 1))
    ;[slots[i], slots[j]] = [slots[j]!, slots[i]!]
  }
  const first = slots.indexOf('sessionCreated')
  ;[slots[0], slots[first]] = [slots[first]!, slots[0]!]

  const prefix = {} as Record<Kind, Int32Array>
  for (const kind of Object.keys(mix) as Kind[]) {
    const arr = new Int32Array(CYCLE)
    let count = 0
    for (let s = 0; s < CYCLE; s++) {
      arr[s] = count
      if (slots[s] === kind) count++
    }
    prefix[kind] = arr
  }
  const result = { slots, prefix, perCycle: mix }
  compiled.set(mixName, result)
  return result
}

const BASE_TIME = 1_760_000_000_000

/** The event at global position `seqNum` (1-based) of the given mix. */
export const eventAt = (mixName: MixName, seqNum: number): LiveStoreEvent.Global.Encoded => {
  const { slots, prefix, perCycle } = compileMix(mixName)
  const i = seqNum - 1
  const cycle = Math.floor(i / CYCLE)
  const slot = i % CYCLE
  const kind = slots[slot]!
  const before = (k: Kind) => cycle * perCycle[k] + prefix[k][slot]!
  const random = rng(seqNum * 2654435761)
  const time = BASE_TIME + seqNum * 250
  const writer = seqNum % WRITERS
  const sessionIndex = Math.max(0, before('sessionCreated') - 1)
  const sessionId = entityId('ses', sessionIndex)

  const make = (name: string, args: Record<string, unknown>): LiveStoreEvent.Global.Encoded => ({
    name,
    args,
    seqNum: seqNum as LiveStoreEvent.Global.Encoded['seqNum'],
    parentSeqNum: (seqNum - 1) as LiveStoreEvent.Global.Encoded['parentSeqNum'],
    clientId: `agent-${writer}`,
    sessionId: `agent-${writer}-session`,
  })

  switch (kind) {
    case 'sessionCreated':
      return make('v1.SessionCreated', {
        id: entityId('ses', before('sessionCreated')),
        title: text(random, 40, 80),
        agent: AGENTS[Math.floor(random() * AGENTS.length)]!,
        metadata: {
          model: 'agent-model-large',
          temperature: 0.2,
          tools: ['search', 'read_file', 'edit_file', 'shell'],
        },
        createdAt: time,
      })
    case 'sessionUpdated':
      return make('v1.SessionUpdated', {
        id: sessionId,
        status: STATUSES[Math.floor(random() * STATUSES.length)]!,
        metadata: {
          tokensIn: Math.floor(random() * 1e6),
          tokensOut: Math.floor(random() * 1e5),
          note: text(random, 60, 160),
        },
        updatedAt: time,
      })
    case 'messageAppended': {
      const withTool = random() < 0.3
      return make('v1.MessageAppended', {
        id: entityId('msg', before('messageAppended')),
        sessionId,
        role: withTool === true ? 'tool' : random() < 0.4 ? 'user' : 'assistant',
        text: text(random, 100, 500),
        toolCall:
          withTool === true
            ? { name: 'read_file', args: { path: `src/${text(random, 8, 24).replaceAll(' ', '/')}.ts`, limit: 200 } }
            : null,
        createdAt: time,
      })
    }
    case 'messageStreamed': {
      // Streaming rewrites one of the last few messages.
      const created = Math.max(1, before('messageAppended'))
      const target = Math.max(0, created - 1 - Math.floor(random() * 4))
      return make('v1.MessageStreamed', { id: entityId('msg', target), text: text(random, 100, 500) })
    }
    case 'taskCreated':
      return make('v1.TaskCreated', {
        id: entityId('tsk', before('taskCreated')),
        sessionId,
        title: text(random, 30, 90),
        createdAt: time,
      })
    case 'taskUpdated': {
      // Updates prefer recent tasks: cubic bias over the last 1000.
      const created = Math.max(1, before('taskCreated'))
      const target = Math.max(0, created - 1 - Math.floor(random() ** 3 * Math.min(created, 1000)))
      const done = random() < 0.2
      return make('v1.TaskUpdated', {
        id: entityId('tsk', target),
        status: done === true ? 'done' : 'running',
        progress: Math.round(random() * 100) / 100,
        result: done === true ? { ok: random() < 0.9, summary: text(random, 60, 200), files: ['a.ts', 'b.ts'] } : null,
        updatedAt: time,
      })
    }
  }
}

const AGENTS = ['planner', 'coder', 'reviewer', 'tester']
const STATUSES = ['active', 'waiting', 'idle', 'active']
const WORDS =
  'the agent reads files then plans a change runs the tests and reports a summary with tool output including paths diffs errors retries tokens context window cache hits latency budget design review notes follow up questions about the schema migration snapshot boot replay cost model'.split(
    ' ',
  )

const entityId = (prefix: string, index: number) => `${prefix}_${index.toString(36).padStart(8, '0')}`

const text = (random: () => number, min: number, max: number) => {
  const length = min + Math.floor(random() * (max - min + 1))
  let out = ''
  while (out.length < length) out += `${WORDS[Math.floor(random() * WORDS.length)]!} `
  return out.slice(0, length)
}

/** mulberry32: small, fast, deterministic. */
const rng = (seed: number) => {
  let a = seed >>> 0
  return () => {
    a = (a + 0x6d2b79f5) >>> 0
    let t = a
    t = Math.imul(t ^ (t >>> 15), t | 1)
    t ^= t + Math.imul(t ^ (t >>> 7), t | 61)
    return ((t ^ (t >>> 14)) >>> 0) / 4294967296
  }
}
