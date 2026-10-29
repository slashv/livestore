import type * as CfTypes from './cf-types.ts'

export namespace HelperTypes {
  type AnyDON = CfTypes.DurableObjectNamespace<any>

  type DOKeys<T> = {
    [K in keyof T]-?: T[K] extends AnyDON ? K : never
  }[keyof T]

  /**
   * Helper type to extract DurableObject keys from Env to give consumer type safety.
   *
   * @example
   * ```ts
   *  type PlatformEnv = {
   *    DB: D1Database
   *    ADMIN_TOKEN: string
   *    SYNC_BACKEND_DO: DurableObjectNamespace<SyncBackendDO>
   * }
   *  export default makeWorker<PlatformEnv>({
   *    syncBackendBinding: 'SYNC_BACKEND_DO',
   *    // ^ (property) syncBackendBinding: "SYNC_BACKEND_DO"
   *  });
   */
  export type ExtractDurableObjectKeys<TEnv> = DOKeys<TEnv> extends never ? string : DOKeys<TEnv> & string

  type R2BucketKeys<T> = {
    [K in keyof T]-?: T[K] extends CfTypes.R2Bucket ? K : never
  }[keyof T]

  /** Helper type to extract R2 bucket binding keys from Env (falls back to `string` for untyped envs). */
  export type ExtractR2BucketKeys<TEnv> = R2BucketKeys<TEnv> extends never ? string : R2BucketKeys<TEnv> & string
}
