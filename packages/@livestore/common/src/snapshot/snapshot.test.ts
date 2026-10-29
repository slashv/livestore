import { expect } from 'vitest'

import { Vitest } from '@livestore/utils-dev/node-vitest'
import { Effect } from '@livestore/utils/effect'

import { digestSha256Hex, sha256Hex } from './snapshot.ts'

Vitest.describe('snapshot hashing', () => {
  Vitest.it.effect('the native digest matches the pure-JS digest', () =>
    Effect.gen(function* () {
      for (const size of [0, 1, 63, 64, 65, 1_000_003]) {
        const bytes = new Uint8Array(size)
        for (let i = 0; i < size; i++) bytes[i] = (i * 31 + 7) % 256
        expect(yield* digestSha256Hex(bytes)).toBe(sha256Hex(bytes))
      }
      expect(sha256Hex(new Uint8Array(0))).toBe('e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855')
    }),
  )
})
