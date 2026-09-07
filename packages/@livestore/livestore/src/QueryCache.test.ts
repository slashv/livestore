import { expect, it } from 'vitest'

import QueryCache from './QueryCache.ts'

it.each(['begin', 'ROLLBACK TO SAVEPOINT a', 'COMMIT', '  SAVEPOINT a', '\nRELEASE SAVEPOINT a'])(
  'does not introspect tables for transaction control: %s',
  (query) => {
    expect(new QueryCache().ignoreQuery(query)).toBe(true)
  },
)

it.each(['SELECT * FROM todos', 'UPDATE todos SET completed = 1'])(
  'keeps table tracking for data queries: %s',
  (query) => {
    expect(new QueryCache().ignoreQuery(query)).toBe(false)
  },
)
