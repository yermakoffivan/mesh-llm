import AxeBuilder from '@axe-core/playwright'
import { expect, test } from '@playwright/test'

const enabled = process.env.MESH_LOGS_E2E === '1'
const openAiUrl = process.env.MESH_LOGS_E2E_OPENAI_URL
const persistedRequestId = process.env.MESH_LOGS_E2E_PERSISTED_REQUEST_ID

type LogPage = {
  readonly items: Array<{ readonly requestId: string; readonly outcome: string; readonly source: string }>
}

function requireHarnessValue(value: string | undefined, name: string) {
  if (!value) throw new Error(`real console harness did not provide ${name}`)
  return value
}

test.describe('real embedded logging console', () => {
  test.skip(!enabled, 'runs only under scripts/qa-logging-console-e2e.sh')
  test.describe.configure({ mode: 'serial' })

  test('renders actual logging DTOs, durable lifecycle records, and real operator receipts', async ({
    page
  }, testInfo) => {
    test.setTimeout(60_000)
    const openAiEndpoint = requireHarnessValue(openAiUrl, 'MESH_LOGS_E2E_OPENAI_URL')
    const persistedId = requireHarnessValue(persistedRequestId, 'MESH_LOGS_E2E_PERSISTED_REQUEST_ID')
    const observedRequests: string[] = []
    let ledgerGets = 0
    let streamReconnectUrl = ''

    page.on('request', (request) => {
      const url = new URL(request.url())
      if (url.pathname === '/api/logs/requests' && request.method() === 'GET') ledgerGets += 1
      if (url.pathname === '/api/logs/events' && url.searchParams.has('cursor')) streamReconnectUrl = url.toString()
    })

    await page.goto(`/logs?replayCursor=${encodeURIComponent('v1:0.0.0')}`)
    await expect(page.getByRole('heading', { name: 'Request logs' })).toBeVisible()
    await expect(page.getByRole('button', { name: `Open request ${persistedId}` })).toBeVisible()
    await expect.poll(() => ledgerGets).toBeGreaterThanOrEqual(2)
    await expect.poll(() => streamReconnectUrl).not.toBe('')

    await page.getByRole('button', { name: `Open request ${persistedId}` }).click()
    await expect(page.getByRole('heading', { name: persistedId })).toBeVisible()
    await expect(page.getByText('Request summary', { exact: true })).toBeVisible()

    await page.getByRole('button', { name: 'Back to logs' }).click()
    const firstResponse = await page.request.post(openAiEndpoint, {
      data: { model: 'qa-no-model', messages: [{ role: 'user', content: 'real logging console lifecycle' }] }
    })
    expect(firstResponse.status()).toBeGreaterThanOrEqual(400)

    await expect
      .poll(async () => {
        const response = await page.request.get('/api/logs/requests?limit=10')
        expect(response.ok()).toBeTruthy()
        const body = (await response.json()) as LogPage
        const match = body.items.find((item) => item.requestId !== persistedId && item.source === 'durable')
        if (match) observedRequests.push(match.requestId)
        return match?.requestId
      })
      .toBeTruthy()
    const lifecycleRequestId = observedRequests.at(-1)
    expect(lifecycleRequestId).toBeTruthy()
    await expect(page.getByRole('button', { name: `Open request ${lifecycleRequestId}` })).toBeVisible()
    await page.getByRole('button', { name: `Open request ${lifecycleRequestId}` }).click()
    await expect(page.getByRole('heading', { name: lifecycleRequestId })).toBeVisible()
    await page.getByRole('tab', { name: 'Errors' }).click()
    await expect(page.getByText('rejected', { exact: true }).first()).toBeVisible()

    const artifactsResponse = await page.request.get(`/api/logs/requests/${lifecycleRequestId}/artifacts`)
    expect(artifactsResponse.ok()).toBeTruthy()
    const artifacts = await artifactsResponse.json()
    const artifactStates = Array.isArray(artifacts.items)
      ? artifacts.items.map((artifact: { contentState?: unknown; redacted?: unknown }) => ({
          contentState: artifact.contentState,
          redacted: artifact.redacted
        }))
      : []
    await testInfo.attach('real-log-artifact-states.json', {
      body: JSON.stringify({ artifactStates }, null, 2),
      contentType: 'application/json'
    })

    await page.getByRole('button', { name: 'Back to logs' }).click()
    await page.getByRole('button', { name: 'Export view' }).click()
    const exportDialog = page.getByRole('dialog', { name: 'Export current log view' })
    await exportDialog.getByLabel('Required audit reason').fill('real embedded console certification')
    const download = page.waitForEvent('download')
    await exportDialog.getByRole('button', { name: 'Download export' }).click()
    await (await download).saveAs(testInfo.outputPath('mesh-llm-log-export.json'))
    await expect(exportDialog.getByRole('status')).toContainText(
      /(Bounded log export downloaded|bounded partial export was downloaded)/i
    )
    await exportDialog.getByRole('button', { name: 'Cancel' }).click()

    await page.getByRole('button', { name: 'Scoped cleanup' }).click()
    const cleanupDialog = page.getByRole('dialog', { name: 'Preview scoped cleanup' })
    await cleanupDialog.getByLabel('Delete terminal logs before').fill(new Date(Date.now() + 60_000).toISOString())
    await cleanupDialog.getByLabel('Request scope').fill('1')
    await cleanupDialog.getByLabel('Required audit reason').fill('real scoped cleanup certification')
    await cleanupDialog.getByRole('button', { name: 'Preview cleanup' }).click()
    const confirmDialog = page.getByRole('dialog', { name: 'Confirm scoped cleanup' })
    await expect(confirmDialog.getByText('Operation ID', { exact: true })).toBeVisible()
    await expect(confirmDialog.getByText('Audit ID', { exact: true })).toBeVisible()
    await confirmDialog.getByRole('button', { name: 'Confirm cleanup' }).click()
    await expect(confirmDialog.getByRole('status').last()).toContainText(/Cleanup completed(\.| with diagnostics\.)/)
    await confirmDialog.getByRole('button', { name: 'Cancel' }).click()

    const secondResponse = await page.request.post(openAiEndpoint, {
      data: { model: 'qa-no-model', messages: [{ role: 'user', content: 'real delete receipt lifecycle' }] }
    })
    expect(secondResponse.status()).toBeGreaterThanOrEqual(400)
    await expect
      .poll(async () => {
        const response = await page.request.get('/api/logs/requests?limit=10')
        const body = (await response.json()) as LogPage
        return body.items.find((item) => item.requestId !== persistedId && item.requestId !== lifecycleRequestId)
          ?.requestId
      })
      .toBeTruthy()

    const current = await page.request.get('/api/logs/requests?limit=10')
    const currentPage = (await current.json()) as LogPage
    const deleteId = currentPage.items.find(
      (item) => item.requestId !== persistedId && item.requestId !== lifecycleRequestId
    )?.requestId
    expect(deleteId).toBeTruthy()
    await page.reload()
    await page.getByRole('button', { name: `Open request ${deleteId}` }).click()
    await page.getByRole('button', { name: 'Delete terminal request' }).click()
    const deleteDialog = page.getByRole('dialog', { name: 'Delete terminal request?' })
    await deleteDialog.getByLabel('Required audit reason').fill('real delete receipt certification')
    await deleteDialog.getByRole('button', { name: 'Confirm deletion' }).click()
    await expect(deleteDialog.getByText('Request removed.')).toBeVisible()
    await expect(deleteDialog.getByText('Audit ID', { exact: true })).toBeVisible()
    await deleteDialog.getByRole('button', { name: 'Cancel' }).click()

    await page.getByRole('button', { name: 'Back to logs' }).click()
    for (const colorScheme of ['light', 'dark'] as const) {
      await page.emulateMedia({ colorScheme, reducedMotion: 'reduce' })
      for (const width of [375, 768, 1280]) {
        await page.setViewportSize({ width, height: 900 })
        await page.goto('/logs')
        await expect(page.getByRole('heading', { name: 'Request logs' })).toBeVisible()
        await expect
          .poll(() => page.evaluate(() => document.documentElement.scrollWidth <= window.innerWidth))
          .toBe(true)
      }
    }
    const axe = await new AxeBuilder({ page }).withTags(['wcag2a', 'wcag2aa', 'wcag21a', 'wcag21aa']).analyze()
    await testInfo.attach('real-console-axe.json', {
      body: JSON.stringify(axe, null, 2),
      contentType: 'application/json'
    })
    expect(axe.violations.filter((violation) => ['serious', 'critical'].includes(violation.impact ?? ''))).toEqual([])
    await page.screenshot({ path: testInfo.outputPath('real-console-logs.png'), fullPage: true })
    await testInfo.attach('real-console-summary.json', {
      body: JSON.stringify({ persistedId, lifecycleRequestId, deleteId, ledgerGets, streamReconnectUrl }, null, 2),
      contentType: 'application/json'
    })
  })
})
