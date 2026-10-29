/**
 * Experimental snapshot boot (LS.SYS-R06): fresh clients install a recent state snapshot published by a checkpointer
 * and pull only the events after it. See `context/02-system/03-sync/04-snapshots/`.
 *
 * @experimental
 * @module
 */
export * from './snapshot.ts'
export * as SnapshotKey from './SnapshotKey.ts'
export * as SnapshotTrigger from './trigger.ts'
export * from './in-memory-store.ts'
