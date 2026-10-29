/**
 * Cost model deciding when a checkpointer publishes a new snapshot.
 *
 * A booting client pays `download(snapshot) + replay(tail)`. Publishing once the tail replay would cost more than a
 * fixed share `alpha` of the snapshot download keeps every boot within `(1 + alpha)` of the snapshot download alone:
 *
 * ```
 * tailEvents × replayCostPerEventMs > alpha × snapshotBytes × downloadCostPerByteMs
 * ```
 *
 * The checkpointer's own work per published snapshot is proportional to the snapshot size, and snapshots are spaced by
 * a tail proportional to that size, so its amortized cost per appended event stays roughly constant as state grows.
 *
 * @experimental
 */
export interface TriggerParams {
  /**
   * Share of the snapshot download cost the tail replay may reach before a new snapshot is published.
   * Smaller values mean fresher snapshots and more checkpointer work.
   *
   * @default 0.5
   */
  readonly alpha: number
  /**
   * Client cost of pulling and materializing one tail event, in milliseconds.
   * Calibrated by the snapshot boot benchmarks (`tests/perf-snapshot`).
   *
   * @default 0.05
   */
  readonly replayCostPerEventMs: number
  /**
   * Client cost of downloading and installing one snapshot byte, in milliseconds.
   *
   * @default 0.00004 (about 25 MB/s end to end)
   */
  readonly downloadCostPerByteMs: number
  /**
   * Minimum time between two published snapshots.
   *
   * @default 10_000
   */
  readonly minIntervalMs: number
  /**
   * Never publish for fewer new events than this, regardless of the cost model. A quiet store publishes nothing.
   *
   * @default 100
   */
  readonly minTailEvents: number
}

export const defaultTriggerParams: TriggerParams = {
  alpha: 0.5,
  replayCostPerEventMs: 0.05,
  downloadCostPerByteMs: 0.00004,
  minIntervalMs: 10_000,
  minTailEvents: 100,
}

export interface TriggerInput {
  /** Events between the latest published snapshot (or the log root) and the current head. */
  readonly tailEvents: number
  /** Expected size of the next snapshot, e.g. the current state database size. */
  readonly snapshotBytes: number
  /** Milliseconds since the latest published snapshot, or `Infinity` if there is none. */
  readonly msSinceLastSnapshot: number
}

export type TriggerDecision =
  | { readonly _tag: 'publish'; readonly tailCostMs: number; readonly downloadCostMs: number }
  | { readonly _tag: 'wait'; readonly reason: 'quiet' | 'min-tail' | 'min-interval' | 'cost' }

export const decide = (input: TriggerInput, params: TriggerParams = defaultTriggerParams): TriggerDecision => {
  if (input.tailEvents <= 0) return { _tag: 'wait', reason: 'quiet' }
  if (input.tailEvents < params.minTailEvents) return { _tag: 'wait', reason: 'min-tail' }
  if (input.msSinceLastSnapshot < params.minIntervalMs) return { _tag: 'wait', reason: 'min-interval' }

  const tailCostMs = input.tailEvents * params.replayCostPerEventMs
  const downloadCostMs = input.snapshotBytes * params.downloadCostPerByteMs

  return tailCostMs > params.alpha * downloadCostMs
    ? { _tag: 'publish', tailCostMs, downloadCostMs }
    : { _tag: 'wait', reason: 'cost' }
}
