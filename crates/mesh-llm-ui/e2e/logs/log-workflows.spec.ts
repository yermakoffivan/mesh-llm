import AxeBuilder from '@axe-core/playwright'
import { expect, test, type Page } from '../fixtures/base'

const REQUEST_ID = '00000000-0000-4000-8000-000000000001'
const EVENT_ID = '00000000-0000-4000-8000-000000000002'
const ARTIFACT_ID = '00000000-0000-4000-8000-000000000003'
const OPERATION_ID = '00000000-0000-4000-8000-000000000004'
const AUDIT_ID = '00000000-0000-4000-8000-000000000005'
const OCCURRED_AT = '2026-08-04T12:00:00Z'
const TERMINAL_AT = '2026-08-04T12:00:01Z'
const FILTER_FROM = '2026-08-04T00:00:00Z'
const FILTER_TO = '2026-08-04T13:00:00Z'

type Lifecycle = 'active' | 'completed' | 'failed' | 'rejected' | 'cancelled' | 'dropped'
type StreamMode = 'event' | 'gap' | 'unavailable'
type MaintenanceResult = 'completed' | 'partial' | 'failure'

type LogsBackendOptions = {
  lifecycle?: Lifecycle
  streamMode?: StreamMode
  cleanupRunResults?: readonly MaintenanceResult[]
  deleteResults?: readonly MaintenanceResult[]
}

type LogsBackend = {
  cleanupRunBodies: string[]
  cleanupRunResults: MaintenanceResult[]
  deleteBodies: string[]
  deleteResults: MaintenanceResult[]
  lifecycle: Lifecycle
  listCalls: number
  operationBodies: string[]
  streamUrls: string[]
  releaseStream: (() => void) | undefined
  streamMode: StreamMode
}

function request(outcome: Lifecycle, source: 'active' | 'durable' = outcome === 'active' ? 'active' : 'durable') {
  return {
    requestId: REQUEST_ID,
    outcome,
    createdAt: OCCURRED_AT,
    terminalAt: outcome === 'active' ? null : TERMINAL_AT,
    route: 'reserve',
    model: 'Qwen3',
    provider: 'reserve-a',
    engine: 'skippy',
    statusCode: outcome === 'completed' ? 200 : outcome === 'active' ? null : 502,
    source
  }
}

function logsPage(items: readonly object[]) {
  return { items, nextCursor: null }
}

function artifact(contentState: 'available' | 'missing') {
  return {
    artifactId: ARTIFACT_ID,
    requestId: REQUEST_ID,
    occurredAt: TERMINAL_AT,
    kind: contentState === 'available' ? 'request' : 'response',
    mediaKind: 'application/json',
    checksum: null,
    bytes: 0,
    version: 1,
    redacted: contentState === 'available',
    truncated: false,
    contentState,
    contentBase64: null
  }
}

function artifactDeletion(state: 'previewed' | 'completed' | 'partial') {
  const failed = state === 'partial' ? 1 : 0
  return { removed: failed, failed, ...(failed > 0 ? { failureClass: 'unsafe_path' } : {}) }
}

function cleanupReceipt(state: 'previewed' | 'completed' | 'partial') {
  return {
    operationId: OPERATION_ID,
    auditId: AUDIT_ID,
    cutoffBefore: TERMINAL_AT,
    requestLimit: 1,
    scope: {
      source: 'durable',
      cutoffBefore: TERMINAL_AT,
      requestLimit: 1,
      route: 'reserve',
      model: 'Qwen3',
      provider: 'reserve-a',
      engine: 'skippy',
      outcome: 'completed'
    },
    state,
    hasMore: state === 'partial',
    selectionFingerprint: 'bounded-selection',
    planned: { requests: 1, events: 1, artifacts: 2, proxyRecords: 0, databaseRows: 4 },
    executed: {
      requests: state === 'previewed' ? 0 : 1,
      events: 0,
      artifacts: 0,
      proxyRecords: 0,
      databaseRows: state === 'previewed' ? 0 : 1
    },
    artifactDeletion: artifactDeletion(state)
  }
}

function deleteReceipt(state: 'completed' | 'partial') {
  return {
    operationId: OPERATION_ID,
    auditId: AUDIT_ID,
    requestId: REQUEST_ID,
    state,
    selectionFingerprint: 'bounded-selection',
    planned: { requests: 1, events: 1, artifacts: 2, proxyRecords: 0, databaseRows: 4 },
    executed: { requests: 1, events: 0, artifacts: 0, proxyRecords: 0, databaseRows: 1 },
    artifactDeletion: artifactDeletion(state)
  }
}

async function tabTo(page: Page, locator: ReturnType<Page['getByLabel']>, maxTabs = 32) {
  for (let attempt = 0; attempt < maxTabs; attempt += 1) {
    if (await locator.evaluate((element) => element === document.activeElement)) return
    await page.keyboard.press('Tab')
  }
  await expect(locator).toBeFocused()
}

async function previewScopedCleanup(page: Page, reason = 'retention review') {
  await page.getByRole('button', { name: 'Scoped cleanup' }).click()
  const cleanupDialog = page.getByRole('dialog', { name: 'Preview scoped cleanup' })
  await cleanupDialog.getByLabel('Delete terminal logs before').fill(TERMINAL_AT)
  await cleanupDialog.getByLabel('Request scope').fill('1')
  await cleanupDialog.getByLabel('Required audit reason').fill(reason)
  await cleanupDialog.getByRole('button', { name: 'Preview cleanup' }).click()
  return page.getByRole('dialog', { name: 'Confirm scoped cleanup' })
}

async function installLogsBackend(page: Page, options: LogsBackendOptions = {}) {
  const state: LogsBackend = {
    cleanupRunBodies: [],
    cleanupRunResults: [...(options.cleanupRunResults ?? ['completed'])],
    deleteBodies: [],
    deleteResults: [...(options.deleteResults ?? ['completed'])],
    lifecycle: options.lifecycle ?? 'active',
    listCalls: 0,
    operationBodies: [],
    streamUrls: [],
    releaseStream: undefined,
    streamMode: options.streamMode ?? 'event'
  }

  await page.context().route('**/api/logs/**', async (route) => {
    const url = new URL(route.request().url())
    const method = route.request().method()

    if (url.pathname === '/api/logs/requests' && method === 'GET') {
      state.listCalls += 1
      await route.fulfill({ json: logsPage([request(state.lifecycle)]) })
      return
    }
    if (url.pathname === '/api/logs/events') {
      state.streamUrls.push(url.search)
      if (state.streamMode === 'unavailable') {
        await route.abort('failed')
        return
      }
      await new Promise<void>((resolve) => {
        state.releaseStream = resolve
      })
      if (state.streamMode === 'gap') {
        await route.fulfill({
          contentType: 'text/event-stream',
          body:
            'id: v1:2.0.0\n' +
            'event: replay_gap\n' +
            'data: {"channel":"requests","fromSequence":1,"toSequence":2,"recovery":{"endpoint":"/api/logs/requests","cursor":null}}\n\n'
        })
        return
      }
      state.lifecycle = 'completed'
      await route.fulfill({
        contentType: 'text/event-stream',
        body:
          'id: v1:1.0.0\n' +
          'event: log_event\n' +
          `data: {"eventId":"${EVENT_ID}","requestId":"${REQUEST_ID}","occurredAt":"${TERMINAL_AT}","channel":"requests","sequence":1,"kind":"completed"}\n\n`
      })
      return
    }
    if (url.pathname === `/api/logs/requests/${REQUEST_ID}` && method === 'GET') {
      await route.fulfill({ json: request(state.lifecycle) })
      return
    }
    if (url.pathname === `/api/logs/requests/${REQUEST_ID}/delete` && method === 'POST') {
      const body = route.request().postData() ?? ''
      state.operationBodies.push(body)
      state.deleteBodies.push(body)
      const result = state.deleteResults.shift() ?? 'completed'
      if (result === 'failure') {
        await route.fulfill({ status: 500, json: { error: { code: 'internal' } } })
        return
      }
      await route.fulfill({ json: deleteReceipt(result) })
      return
    }
    if (url.pathname === `/api/logs/requests/${REQUEST_ID}/events`) {
      await route.fulfill({
        json: logsPage([
          {
            eventId: EVENT_ID,
            requestId: REQUEST_ID,
            occurredAt: TERMINAL_AT,
            kind: state.lifecycle === 'failed' ? 'failed' : 'completed',
            model: 'Qwen3',
            provider: 'reserve-a',
            engine: 'skippy',
            attemptId: null,
            statusCode: state.lifecycle === 'completed' ? 200 : 502,
            durationMs: 1,
            tokens: 0
          }
        ])
      })
      return
    }
    if (url.pathname === `/api/logs/requests/${REQUEST_ID}/artifacts`) {
      await route.fulfill({ json: logsPage([artifact('available'), artifact('missing')]) })
      return
    }
    if (url.pathname === `/api/logs/artifacts/${ARTIFACT_ID}` && method === 'GET') {
      await route.fulfill({ json: { ...artifact('available'), contentBase64: 'eA==' } })
      return
    }
    if (url.pathname === '/api/logs/proxy') {
      await route.fulfill({ json: logsPage([]) })
      return
    }
    if (url.pathname === '/api/logs/requests/export' && method === 'POST') {
      state.operationBodies.push(route.request().postData() ?? '')
      await route.fulfill({
        json: { items: [], nextCursor: null, truncated: false, retryRequired: false, artifactContentIncluded: false }
      })
      return
    }
    if (url.pathname === '/api/logs/cleanup/preview' && method === 'POST') {
      state.operationBodies.push(route.request().postData() ?? '')
      await route.fulfill({ json: cleanupReceipt('previewed') })
      return
    }
    if (url.pathname === '/api/logs/cleanup/run' && method === 'POST') {
      const body = route.request().postData() ?? ''
      state.operationBodies.push(body)
      state.cleanupRunBodies.push(body)
      const result = state.cleanupRunResults.shift() ?? 'completed'
      if (result === 'failure') {
        await route.fulfill({ status: 500, json: { error: { code: 'internal' } } })
        return
      }
      await route.fulfill({ json: cleanupReceipt(result) })
      return
    }
    if (url.pathname.startsWith('/api/logs/webhooks/') && url.pathname.endsWith('/retry') && method === 'POST') {
      state.operationBodies.push(route.request().postData() ?? '')
      await route.fulfill({ json: { outcome: 'scheduled' } })
      return
    }
    await route.fulfill({ status: 404, json: { error: { code: 'unsupported' } } })
  })

  return state
}

test('logs ledger follows a lifecycle event into immediate details and safe artifact states', async ({
  page: browserPage
}) => {
  const backend = await installLogsBackend(browserPage)

  await browserPage.goto(
    `/logs?from=${encodeURIComponent(FILTER_FROM)}&to=${encodeURIComponent(FILTER_TO)}&model=Qwen3&provider=reserve-a&engine=skippy&route=reserve&source=durable&outcome=completed`
  )
  await expect(browserPage.getByRole('heading', { name: 'Request logs' })).toBeVisible()
  await expect(browserPage.getByText('active', { exact: true }).first()).toBeVisible()
  await expect.poll(() => backend.releaseStream).toBeDefined()
  expect(backend.streamUrls[0]).toBe(
    '?channel=requests&channel=operations&filter=from%3A2026-08-04T00%3A00%3A00Z&filter=to%3A2026-08-04T13%3A00%3A00Z&filter=model%3AQwen3&filter=provider%3Areserve-a&filter=engine%3Askippy&filter=route%3Areserve&filter=outcome%3Acompleted'
  )
  expect(backend.streamUrls[0]).toContain('filter=route%3Areserve')
  expect(backend.streamUrls[0]).not.toContain('filter=source%3A')
  backend.releaseStream?.()
  await expect(browserPage.getByText('completed', { exact: true })).toBeVisible()
  expect(backend.listCalls).toBeGreaterThanOrEqual(2)

  await browserPage.getByRole('button', { name: `Open request ${REQUEST_ID}` }).click()
  await expect(browserPage.getByRole('heading', { name: REQUEST_ID })).toBeVisible()
  await expect(browserPage.getByText('Request summary', { exact: true })).toBeVisible()

  await browserPage.getByRole('tab', { name: 'Request' }).click()
  await expect(browserPage.getByText('Redacted before retention')).toBeVisible()
  await expect(browserPage.getByText('retained; not loaded')).toBeVisible()
  await browserPage.getByRole('button', { name: 'Download redacted artifact' }).click()
  await expect(browserPage.getByText('Artifact download started.')).toBeVisible()

  await browserPage.getByRole('tab', { name: 'Response' }).click()
  await expect(browserPage.getByText('missing', { exact: true })).toBeVisible()
})

test('logs recovery uses the dedicated stream gap and bounded polling fallback', async ({ page: browserPage }) => {
  const backend = await installLogsBackend(browserPage, { lifecycle: 'failed', streamMode: 'gap' })

  await browserPage.goto('/logs')
  await expect(browserPage.getByText('failed', { exact: true })).toBeVisible()
  await expect.poll(() => backend.releaseStream).toBeDefined()
  backend.releaseStream?.()
  await expect.poll(() => backend.listCalls).toBeGreaterThanOrEqual(2)

  backend.streamMode = 'unavailable'
  await browserPage.reload()
  await expect(browserPage.getByText('Reconnecting', { exact: true })).toBeVisible()
  await expect(browserPage.getByText('Polling', { exact: true })).toBeVisible({ timeout: 4_000 })
})

test('metadata-only export and previewed cleanup keep an audited operator flow', async ({ page: browserPage }) => {
  const backend = await installLogsBackend(browserPage, { lifecycle: 'completed', streamMode: 'unavailable' })

  await browserPage.goto('/logs')
  await browserPage.getByRole('button', { name: 'Export view' }).click()
  const exportDialog = browserPage.getByRole('dialog', { name: 'Export current log view' })
  await exportDialog.getByLabel('Required audit reason').fill('retention review')
  await exportDialog.getByRole('button', { name: 'Download export' }).click()
  await expect(browserPage.getByText('Bounded log export downloaded.')).toBeVisible()
  expect(backend.operationBodies[0]).toContain('"includeArtifacts":false')
  await exportDialog.getByRole('button', { name: 'Cancel' }).click()

  await browserPage.getByRole('button', { name: 'Scoped cleanup' }).click()
  const cleanupDialog = browserPage.getByRole('dialog', { name: 'Preview scoped cleanup' })
  await cleanupDialog.getByLabel('Delete terminal logs before').fill(TERMINAL_AT)
  await cleanupDialog.getByLabel('Request scope').fill('1')
  await cleanupDialog.getByLabel('Required audit reason').fill('retention review')
  await cleanupDialog.getByRole('button', { name: 'Preview cleanup' }).click()
  const confirmDialog = browserPage.getByRole('dialog', { name: 'Confirm scoped cleanup' })
  await expect(confirmDialog.getByRole('heading', { name: 'Confirm scoped cleanup' })).toBeVisible()
  await confirmDialog.getByRole('button', { name: 'Confirm cleanup' }).click()
  await expect(browserPage.getByText('Cleanup completed.')).toBeVisible()
  await confirmDialog.getByRole('button', { name: 'Cancel' }).click()

  const retryRegion = browserPage.getByRole('region', { name: 'Log operations' })
  await retryRegion.getByLabel('Webhook delivery ID').fill(`webhook:${REQUEST_ID}`)
  await retryRegion.getByLabel('Required audit reason').fill('retry dead-letter delivery')
  await retryRegion.getByRole('button', { name: 'Retry dead-letter delivery' }).click()
  await expect(browserPage.getByText('Dead-letter retry scheduled.')).toBeVisible()
  expect(backend.operationBodies).toHaveLength(4)
  expect(backend.operationBodies[3]).toContain('"reason":"retry dead-letter delivery"')
})

test('partial cleanup retries retained artifact work and refetches the active ledger after successful receipts', async ({
  page: browserPage
}) => {
  const backend = await installLogsBackend(browserPage, {
    lifecycle: 'completed',
    cleanupRunResults: ['partial', 'completed']
  })

  await browserPage.goto('/logs')
  await expect(browserPage.getByRole('heading', { name: 'Request logs' })).toBeVisible()
  await expect.poll(() => backend.releaseStream).toBeDefined()
  await expect.poll(() => backend.listCalls).toBeGreaterThanOrEqual(2)
  const listCallsBeforeCleanup = backend.listCalls

  const confirmDialog = await previewScopedCleanup(browserPage)
  await expect(confirmDialog.getByRole('heading', { name: 'Confirm scoped cleanup' })).toBeVisible()
  expect(backend.listCalls).toBe(listCallsBeforeCleanup)

  await confirmDialog.getByRole('button', { name: 'Confirm cleanup' }).click()
  await expect(confirmDialog.getByText('Cleanup completed with diagnostics.')).toBeVisible()
  await expect(confirmDialog.getByRole('button', { name: 'Retry cleanup' })).toBeVisible()
  await expect.poll(() => backend.listCalls).toBeGreaterThan(listCallsBeforeCleanup)
  const listCallsAfterPartialCleanup = backend.listCalls

  await confirmDialog.getByRole('button', { name: 'Retry cleanup' }).click()
  await expect(confirmDialog.getByText('Cleanup completed.')).toBeVisible()
  await expect(confirmDialog.getByRole('button', { name: 'Retry cleanup' })).toHaveCount(0)
  await expect.poll(() => backend.listCalls).toBeGreaterThan(listCallsAfterPartialCleanup)

  expect(backend.cleanupRunBodies).toHaveLength(2)
  expect(JSON.parse(backend.cleanupRunBodies[1] ?? '')).toEqual(JSON.parse(backend.cleanupRunBodies[0] ?? ''))
})

test('failed cleanup mutation does not refetch the active ledger', async ({ page: browserPage }) => {
  const backend = await installLogsBackend(browserPage, {
    lifecycle: 'completed',
    cleanupRunResults: ['failure']
  })

  await browserPage.goto('/logs')
  await expect(browserPage.getByRole('heading', { name: 'Request logs' })).toBeVisible()
  await expect.poll(() => backend.releaseStream).toBeDefined()
  await expect.poll(() => backend.listCalls).toBeGreaterThanOrEqual(2)
  const listCallsBeforeFailure = backend.listCalls

  const confirmDialog = await previewScopedCleanup(browserPage, 'failed cleanup should not refresh')
  await confirmDialog.getByRole('button', { name: 'Confirm cleanup' }).click()
  await expect(confirmDialog.getByText('Logs API request failed with HTTP 500')).toBeVisible()
  expect(backend.cleanupRunBodies).toHaveLength(1)
  expect(backend.listCalls).toBe(listCallsBeforeFailure)
})

test('terminal request deletion uses the details control and sends its audited operation', async ({
  page: browserPage
}) => {
  const backend = await installLogsBackend(browserPage, { lifecycle: 'completed' })

  await browserPage.goto('/logs')
  await expect(browserPage.getByRole('button', { name: `Open request ${REQUEST_ID}` })).toBeVisible()
  await browserPage.getByRole('button', { name: `Open request ${REQUEST_ID}` }).click()
  await expect(browserPage.getByRole('heading', { name: REQUEST_ID })).toBeVisible()

  await browserPage.getByRole('button', { name: 'Delete terminal request' }).click()
  const deleteDialog = browserPage.getByRole('dialog', { name: 'Delete terminal request?' })
  await deleteDialog.getByLabel('Required audit reason').fill('remove invalid request')
  await deleteDialog.getByRole('button', { name: 'Confirm deletion' }).click()
  await expect(deleteDialog.getByText('Request removed.')).toBeVisible()
  expect(backend.deleteBodies).toHaveLength(1)
  expect(JSON.parse(backend.deleteBodies[0] ?? '')).toMatchObject({ reason: 'remove invalid request' })
})

test('older hosts present an accessible unsupported state', async ({ page: browserPage }) => {
  let streamAttempts = 0
  await browserPage.context().route('**/api/logs/events', async (route) => {
    streamAttempts += 1
    await route.abort('failed')
  })
  await browserPage
    .context()
    .route('**/api/logs/requests', (route) => route.fulfill({ status: 404, json: { error: { code: 'unsupported' } } }))

  await browserPage.goto('/logs')
  await expect(browserPage.getByText('Log history is unavailable on this host')).toBeVisible()
  await expect(browserPage.getByText(/Upgrade the host to inspect request history here/)).toBeVisible()
  expect(streamAttempts).toBe(0)
})

test('logs pages stay accessible and unclipped across supported visual modes', async ({ page: browserPage }) => {
  await installLogsBackend(browserPage, { lifecycle: 'completed', streamMode: 'unavailable' })

  for (const colorScheme of ['light', 'dark'] as const) {
    await browserPage.emulateMedia({ colorScheme, reducedMotion: 'reduce' })
    for (const width of [375, 768, 1280]) {
      await browserPage.setViewportSize({ width, height: 900 })
      await browserPage.goto('/logs')
      await expect(browserPage.getByRole('heading', { name: 'Request logs' })).toBeVisible()
      await expect
        .poll(() => browserPage.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth))
        .toBe(true)
      const results = await new AxeBuilder({ page: browserPage })
        .withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa'])
        .analyze()
      expect(
        results.violations.filter((violation) => ['serious', 'critical'].includes(violation.impact ?? ''))
      ).toEqual([])

      await tabTo(browserPage, browserPage.getByLabel('Filter logs from time'))
    }
  }
})
