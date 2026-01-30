import { test, expect, type Page } from '@playwright/test'

/**
 * Reproduction test for LiveStore WASM SQLite bug:
 * "function signature mismatch during changeset_apply with concurrent multi-client sync"
 *
 * KEY INSIGHT: The bug is specific to clientDocument tables with Schema.Record (hashmap)
 * structure when multiple tabs in the same browser context try to update the same
 * clientDocument concurrently.
 *
 * Error: LiveStore.SqliteError: { "query": undefined, "code": -1,
 *   "cause": RuntimeError: function signature mismatch,
 *   "note": "Failed calling makeChangeset.apply" }
 */

const waitForLiveStore = async (page: Page) => {
  await expect(page.locator('[data-testid="livestore-ready"]')).toBeVisible({ timeout: 15000 })
}

test.describe('clientDocument Changeset Bug Reproduction', () => {
  /**
   * Core reproduction scenario:
   * - Single browser context with multiple tabs (sharing SharedWorker)
   * - All tabs update the same clientDocument concurrently on initial load
   * - Uses Schema.Record (hashmap) structure like LocalFilesStateSchema
   */
  test('multiple tabs in same context updating clientDocument concurrently on load', async ({ browser }) => {
    const storeId = `repro-same-ctx-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    const url = `/?storeId=${storeId}`

    const errors: string[] = []
    const consoleMessages: string[] = []

    const trackErrors = (page: Page, label: string) => {
      page.on('pageerror', (error) => {
        errors.push(`[${label}] PAGE_ERROR: ${error.message}`)
        console.log(`[${label}] PAGE_ERROR: ${error.message}`)
      })
      page.on('console', (msg) => {
        const text = msg.text()
        // Log all console errors for debugging
        if (msg.type() === 'error') {
          console.log(`[${label}] CONSOLE_ERROR: ${text}`)
        }
        if (
          text.includes('SqliteError') ||
          text.includes('function signature mismatch') ||
          text.includes('makeChangeset') ||
          text.includes('changeset_apply') ||
          text.includes('RuntimeError') ||
          text.includes('rollback') ||
          text.includes('ERROR')
        ) {
          consoleMessages.push(`[${label}] CONSOLE: ${text}`)
        }
      })
    }

    // Single browser context - tabs share SharedWorker
    const context = await browser.newContext()

    // Create multiple tabs
    const tabA = await context.newPage()
    const tabB = await context.newPage()

    trackErrors(tabA, 'TabA')
    trackErrors(tabB, 'TabB')

    // Navigate both tabs simultaneously
    await Promise.all([tabA.goto(url), tabB.goto(url)])

    // Wait for both to initialize
    await Promise.all([waitForLiveStore(tabA), waitForLiveStore(tabB)])

    // Both tabs update clientDocument concurrently - this is the trigger
    // Rapid concurrent updates to the same clientDocument from different tabs
    await Promise.all([
      (async () => {
        for (let i = 0; i < 10; i++) {
          await tabA.locator('[data-testid="add-item"]').click()
        }
      })(),
      (async () => {
        for (let i = 0; i < 10; i++) {
          await tabB.locator('[data-testid="add-item"]').click()
        }
      })(),
    ])

    // Wait for updates to settle
    await tabA.waitForTimeout(2000)

    // Both tabs should eventually have the same count (merged updates)
    const countA = await tabA.locator('[data-testid="item-count"]').textContent()
    const countB = await tabB.locator('[data-testid="item-count"]').textContent()

    console.log(`TabA count: ${countA}, TabB count: ${countB}`)

    // Log captured messages
    if (consoleMessages.length > 0) {
      console.log('Console messages:')
      consoleMessages.forEach((msg) => console.log('  ', msg))
    }
    if (errors.length > 0) {
      console.log('Page errors:')
      errors.forEach((err) => console.log('  ', err))
    }

    const changesetErrors = [...errors, ...consoleMessages].filter(
      (e) =>
        e.includes('function signature mismatch') ||
        e.includes('makeChangeset.apply') ||
        e.includes('changeset_apply'),
    )

    expect(changesetErrors, 'Expected no changeset errors but found some').toHaveLength(0)

    await context.close()
  })

  /**
   * More aggressive: 2 browser contexts x 2 tabs each
   * - Each context has its own SharedWorker
   * - Tabs within same context share clientDocument
   * - Cross-context sync via WebSocket
   */
  test('2 contexts x 2 tabs each, all updating clientDocument', async ({ browser }) => {
    const storeId = `repro-multi-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    const url = `/?storeId=${storeId}`

    const errors: string[] = []
    const consoleMessages: string[] = []

    const trackErrors = (page: Page, label: string) => {
      page.on('pageerror', (error) => {
        errors.push(`[${label}] PAGE_ERROR: ${error.message}`)
      })
      page.on('console', (msg) => {
        const text = msg.text()
        if (
          text.includes('SqliteError') ||
          text.includes('function signature mismatch') ||
          text.includes('makeChangeset') ||
          text.includes('changeset_apply') ||
          text.includes('RuntimeError') ||
          text.includes('rollback') ||
          text.includes('ERROR')
        ) {
          consoleMessages.push(`[${label}] CONSOLE: ${text}`)
        }
      })
    }

    // Two browser contexts
    const context1 = await browser.newContext()
    const context2 = await browser.newContext()

    // 2 tabs per context
    const c1TabA = await context1.newPage()
    const c1TabB = await context1.newPage()
    const c2TabC = await context2.newPage()
    const c2TabD = await context2.newPage()

    trackErrors(c1TabA, 'C1-TabA')
    trackErrors(c1TabB, 'C1-TabB')
    trackErrors(c2TabC, 'C2-TabC')
    trackErrors(c2TabD, 'C2-TabD')

    // Navigate all tabs simultaneously
    await Promise.all([c1TabA.goto(url), c1TabB.goto(url), c2TabC.goto(url), c2TabD.goto(url)])

    // Wait for all to initialize
    await Promise.all([
      waitForLiveStore(c1TabA),
      waitForLiveStore(c1TabB),
      waitForLiveStore(c2TabC),
      waitForLiveStore(c2TabD),
    ])

    // All tabs update clientDocument concurrently
    await Promise.all([
      (async () => {
        for (let i = 0; i < 5; i++) {
          await c1TabA.locator('[data-testid="add-item"]').click()
        }
      })(),
      (async () => {
        for (let i = 0; i < 5; i++) {
          await c1TabB.locator('[data-testid="add-item"]').click()
        }
      })(),
      (async () => {
        for (let i = 0; i < 5; i++) {
          await c2TabC.locator('[data-testid="add-item"]').click()
        }
      })(),
      (async () => {
        for (let i = 0; i < 5; i++) {
          await c2TabD.locator('[data-testid="add-item"]').click()
        }
      })(),
    ])

    // Wait for sync
    await c1TabA.waitForTimeout(3000)

    // Log counts
    const counts = await Promise.all([
      c1TabA.locator('[data-testid="item-count"]').textContent(),
      c1TabB.locator('[data-testid="item-count"]').textContent(),
      c2TabC.locator('[data-testid="item-count"]').textContent(),
      c2TabD.locator('[data-testid="item-count"]').textContent(),
    ])
    console.log(`Counts: C1-TabA=${counts[0]}, C1-TabB=${counts[1]}, C2-TabC=${counts[2]}, C2-TabD=${counts[3]}`)

    if (consoleMessages.length > 0) {
      console.log('Console messages:')
      consoleMessages.forEach((msg) => console.log('  ', msg))
    }
    if (errors.length > 0) {
      console.log('Page errors:')
      errors.forEach((err) => console.log('  ', err))
    }

    const changesetErrors = [...errors, ...consoleMessages].filter(
      (e) =>
        e.includes('function signature mismatch') ||
        e.includes('makeChangeset.apply') ||
        e.includes('changeset_apply'),
    )

    expect(changesetErrors, 'Expected no changeset errors but found some').toHaveLength(0)

    await context1.close()
    await context2.close()
  })

  /**
   * Start committing immediately after navigation (before full init)
   * This more closely mimics the original bug scenario
   */
  test('immediate commits during initialization', async ({ browser }) => {
    const storeId = `repro-immediate-${Date.now()}-${Math.random().toString(36).slice(2, 8)}`
    const url = `/?storeId=${storeId}`

    const errors: string[] = []
    const consoleMessages: string[] = []

    const trackErrors = (page: Page, label: string) => {
      page.on('pageerror', (error) => {
        const msg = `[${label}] PAGE_ERROR: ${error.message}`
        errors.push(msg)
        console.log(msg)
      })
      page.on('console', (msg) => {
        const text = msg.text()
        // Log all errors
        if (msg.type() === 'error') {
          console.log(`[${label}] CONSOLE_ERROR: ${text}`)
        }
        if (
          text.includes('SqliteError') ||
          text.includes('function signature mismatch') ||
          text.includes('makeChangeset') ||
          text.includes('changeset_apply') ||
          text.includes('RuntimeError') ||
          text.includes('rollback') ||
          text.includes('ERROR')
        ) {
          consoleMessages.push(`[${label}] CONSOLE: ${text}`)
        }
      })
    }

    const context1 = await browser.newContext()
    const context2 = await browser.newContext()

    const c1TabA = await context1.newPage()
    const c1TabB = await context1.newPage()

    trackErrors(c1TabA, 'C1-TabA')
    trackErrors(c1TabB, 'C1-TabB')

    // Start first context
    await Promise.all([c1TabA.goto(url), c1TabB.goto(url)])
    await Promise.all([waitForLiveStore(c1TabA), waitForLiveStore(c1TabB)])

    // Start committing from C1 tabs
    const commitPromise = Promise.all([
      (async () => {
        for (let i = 0; i < 10; i++) {
          await c1TabA.locator('[data-testid="add-item"]').click()
          await c1TabA.waitForTimeout(30)
        }
      })(),
      (async () => {
        for (let i = 0; i < 10; i++) {
          await c1TabB.locator('[data-testid="add-item"]').click()
          await c1TabB.waitForTimeout(30)
        }
      })(),
    ])

    // While C1 is committing, start C2 tabs
    const c2TabC = await context2.newPage()
    const c2TabD = await context2.newPage()

    trackErrors(c2TabC, 'C2-TabC')
    trackErrors(c2TabD, 'C2-TabD')

    await Promise.all([c2TabC.goto(url), c2TabD.goto(url)])

    // Wait for C1 commits to finish
    await commitPromise

    // Wait for C2 to initialize (this pulls changesets during/after C1 commits)
    await Promise.all([waitForLiveStore(c2TabC), waitForLiveStore(c2TabD)])

    // Now C2 tabs also commit
    await Promise.all([
      (async () => {
        for (let i = 0; i < 10; i++) {
          await c2TabC.locator('[data-testid="add-item"]').click()
        }
      })(),
      (async () => {
        for (let i = 0; i < 10; i++) {
          await c2TabD.locator('[data-testid="add-item"]').click()
        }
      })(),
    ])

    await c1TabA.waitForTimeout(3000)

    if (consoleMessages.length > 0) {
      console.log('Console messages:')
      consoleMessages.forEach((msg) => console.log('  ', msg))
    }
    if (errors.length > 0) {
      console.log('Page errors:')
      errors.forEach((err) => console.log('  ', err))
    }

    const changesetErrors = [...errors, ...consoleMessages].filter(
      (e) =>
        e.includes('function signature mismatch') ||
        e.includes('makeChangeset.apply') ||
        e.includes('changeset_apply'),
    )

    expect(changesetErrors, 'Expected no changeset errors but found some').toHaveLength(0)

    await context1.close()
    await context2.close()
  })
})
