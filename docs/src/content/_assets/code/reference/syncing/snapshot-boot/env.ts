import type { CheckpointerDoRpcInterface } from '@livestore/adapter-cloudflare'
import type { CfTypes, SyncBackendRpcInterface } from '@livestore/sync-cf/cf-worker'

export interface Env {
  SYNC_BACKEND_DO: CfTypes.DurableObjectNamespace<SyncBackendRpcInterface>
  CHECKPOINTER_DO: CfTypes.DurableObjectNamespace<CheckpointerDoRpcInterface>
  SNAPSHOT_BUCKET: CfTypes.R2Bucket
}
