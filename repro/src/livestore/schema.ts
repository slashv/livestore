import { makeSchema, Schema, State } from '@livestore/livestore'

// Mimic LocalFilesStateSchema - a Record (hashmap) structure
const ItemStateSchema = Schema.Struct({
  value: Schema.String,
  status: Schema.Literal('pending', 'done'),
})

const ItemsMapSchema = Schema.Record({
  key: Schema.String,
  value: ItemStateSchema,
})

export const tables = {
  // clientDocument with Record structure - key to reproducing the bug
  itemsState: State.SQLite.clientDocument({
    name: 'itemsState',
    schema: Schema.Struct({
      items: ItemsMapSchema, // HashMap-like structure
    }),
    default: {
      id: 'shared',
      value: { items: {} },
    },
  }),
}

export const events = {
  itemsStateSet: tables.itemsState.set,
}

// No custom materializers needed - clientDocument has implicit materializers
const materializers = State.SQLite.materializers(events, {})

const state = State.SQLite.makeState({ tables, materializers })

export const schema = makeSchema({ events, state })

export const SyncPayload = Schema.Struct({ authToken: Schema.String })
