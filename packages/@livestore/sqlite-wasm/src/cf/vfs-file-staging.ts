import type { CfTypes } from '@livestore/common-cf'

import { PAGE_SIZE } from './CloudflareDurableObjectVFS.ts'

/**
 * A consistent copy of a {@link CloudflareDurableObjectVFS} database file, staged inside the Durable Object's SQL
 * storage. Lets a caller stream a large database out (e.g. to object storage) without holding it in memory and without
 * blocking writers while it streams.
 */
export interface StagedVfsFile {
  /** Size of the staged file, `pageCount * pageSize`. */
  readonly byteLength: number
  /**
   * Reads `length` bytes at `offset` (both page aligned, except a shorter final read) from the staged copy into a new
   * buffer. Missing pages read as zeros, like the VFS reads them.
   */
  readonly read: (offset: number, length: number) => Uint8Array<ArrayBuffer>
  /** Deletes the staged copy. Idempotent. */
  readonly release: () => void
}

/**
 * Copies the first `pageCount` pages of a VFS file into a staging table, entirely inside DO storage (no JS buffers).
 *
 * Must run while no transaction of the database is open: the VFS writes every committed page synchronously and keeps
 * uncommitted pages in SQLite's page cache (`cache_spill=OFF`), so between transactions `vfs_pages` holds exactly the
 * committed database. Pass the pager's `PRAGMA page_count` as `pageCount`; together with the fixed page size this makes
 * the staged bytes identical to what `sqlite3_serialize` returns for the same moment.
 *
 * Any copy left over from an interrupted earlier run is dropped first, so at most one staged copy exists.
 */
export const stageVfsFile = ({
  sql,
  fileName,
  pageCount,
}: {
  sql: CfTypes.SqlStorage
  /** File name the database was opened with (the VFS keys pages by its URL path). */
  fileName: string
  pageCount: number
}): StagedVfsFile => {
  const filePath = vfsFilePath(fileName)
  const stagingId = crypto.randomUUID()

  sql.exec(
    `CREATE TABLE IF NOT EXISTS ${STAGING_TABLE} (
      staging_id TEXT NOT NULL,
      page_no INTEGER NOT NULL,
      page_data BLOB NOT NULL,
      PRIMARY KEY (staging_id, page_no)
    )`,
  )
  sql.exec(`DELETE FROM ${STAGING_TABLE}`)
  sql.exec(
    `INSERT INTO ${STAGING_TABLE} (staging_id, page_no, page_data)
      SELECT ?, page_no, page_data FROM vfs_pages WHERE file_path = ? AND page_no < ?`,
    stagingId,
    filePath,
    pageCount,
  )

  let released = false

  return {
    byteLength: pageCount * PAGE_SIZE,
    read: (offset, length) => {
      if (released === true) throw new Error('Staged VFS file was already released')
      if (offset % PAGE_SIZE !== 0) throw new Error(`Staged reads must be page aligned, got offset ${offset}`)

      const firstPage = offset / PAGE_SIZE
      const endPage = Math.ceil((offset + length) / PAGE_SIZE)
      const out = new Uint8Array(length)
      const cursor = sql.exec<{ page_no: number; page_data: ArrayBuffer }>(
        `SELECT page_no, page_data FROM ${STAGING_TABLE} WHERE staging_id = ? AND page_no >= ? AND page_no < ? ORDER BY page_no`,
        stagingId,
        firstPage,
        endPage,
      )
      for (const row of cursor) {
        const target = (row.page_no - firstPage) * PAGE_SIZE
        const page = new Uint8Array(row.page_data)
        out.set(page.subarray(0, Math.min(page.byteLength, length - target)), target)
      }
      return out
    },
    release: () => {
      if (released === true) return
      released = true
      sql.exec(`DELETE FROM ${STAGING_TABLE} WHERE staging_id = ?`, stagingId)
    },
  }
}

const STAGING_TABLE = 'vfs_snapshot_staging'

/** Same mapping as the VFS uses from an opened file name to its `vfs_pages.file_path` key. */
const vfsFilePath = (fileName: string) => new URL(fileName, 'file://localhost/').pathname
