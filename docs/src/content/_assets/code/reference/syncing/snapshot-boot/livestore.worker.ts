import { makeWorker } from '@livestore/adapter-web/worker'
import { makeHttpSnapshotSource, makeWsSync } from '@livestore/sync-cf/client'

import { schema } from '../cloudflare/schema.ts'

makeWorker({
  schema,
  sync: {
    backend: makeWsSync({ url: 'wss://sync.example.com' }),
    // A client with an empty local eventlog installs the latest snapshot and pulls only the events after it.
    // Without a usable snapshot it falls back to a full replay.
    snapshot: { source: makeHttpSnapshotSource({ url: 'https://sync.example.com' }) },
  },
})
