import { makeWorker } from '@livestore/adapter-web/worker'

import { schema } from './livestore/schema.ts'

// Disable sync for local testing of changeset_apply fix
makeWorker({
  schema,
  // No sync backend - testing changeset operations locally
})
