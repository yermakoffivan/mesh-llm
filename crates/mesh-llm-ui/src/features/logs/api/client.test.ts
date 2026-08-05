import { describe, expect, it, vi } from 'vitest'
import { LogsApiClient } from './client'
import {
  LogArtifactId,
  LogOperationId,
  LogPageCursor,
  LogReplayCursor,
  LogRequestId,
  LogWebhookDeliveryId
} from './ids'

const REQUEST_ID = '00000000-0000-4000-8000-000000000001'
const ARTIFACT_ID = '00000000-0000-4000-8000-000000000003'
const AUDIT_ID = '00000000-0000-4000-8000-000000000004'
const TIMESTAMP = '2026-08-04T12:00:00Z'

function jsonResponse(body: unknown, status = 200) {
  return new Response(JSON.stringify(body), { status, headers: { 'Content-Type': 'application/json' } })
}

function artifactDto(contentState: string, contentBase64: string | null) {
  return {
    artifactId: ARTIFACT_ID,
    requestId: REQUEST_ID,
    occurredAt: TIMESTAMP,
    kind: 'request',
    mediaKind: 'text/plain',
    checksum: 'sha256:abc',
    bytes: 5,
    version: 1,
    redacted: contentState === 'available',
    truncated: false,
    contentState,
    contentBase64
  }
}

function cleanupReceiptDto(
  operationId: LogOperationId,
  state: 'completed' | 'partial',
  failedArtifacts: number,
  hasMore: boolean
) {
  return {
    operationId: operationId.toString(),
    auditId: AUDIT_ID,
    cutoffBefore: TIMESTAMP,
    requestLimit: 1,
    scope: {
      source: 'durable',
      cutoffBefore: TIMESTAMP,
      requestLimit: 1
    },
    state,
    hasMore,
    selectionFingerprint: 'safe',
    planned: { requests: 1, events: 0, artifacts: 1, proxyRecords: 0, databaseRows: 2 },
    executed: { requests: 1, events: 0, artifacts: 1, proxyRecords: 0, databaseRows: 2 },
    artifactDeletion: {
      removed: 1,
      failed: failedArtifacts,
      failureClass: failedArtifacts > 0 ? 'unsafe_path' : undefined
    }
  }
}

function deleteReceiptDto(operationId: LogOperationId, state: 'completed' | 'partial', failedArtifacts: number) {
  return {
    operationId: operationId.toString(),
    auditId: AUDIT_ID,
    requestId: REQUEST_ID,
    state,
    selectionFingerprint: 'safe',
    planned: { requests: 1, events: 0, artifacts: 1, proxyRecords: 0, databaseRows: 2 },
    executed: { requests: 1, events: 0, artifacts: 1, proxyRecords: 0, databaseRows: 2 },
    artifactDeletion: {
      removed: 1,
      failed: failedArtifacts,
      failureClass: failedArtifacts > 0 ? 'unsafe_path' : undefined
    }
  }
}

describe('LogsApiClient', () => {
  it('binds the default browser fetch before issuing a request', async () => {
    const browserFetch = vi.fn(function (this: typeof globalThis, input: RequestInfo | URL) {
      expect(this).toBe(globalThis)
      expect(input).toBe('/api/logs/requests')
      return Promise.resolve(jsonResponse({ items: [], nextCursor: null }))
    })
    vi.stubGlobal('fetch', browserFetch)

    try {
      const result = await new LogsApiClient().listRequests()

      expect(result).toMatchObject({ state: 'supported', value: { items: [] } })
      expect(browserFetch).toHaveBeenCalledTimes(1)
    } finally {
      vi.unstubAllGlobals()
    }
  })

  it('uses an injected fetch without rebinding it', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ items: [], nextCursor: null }))

    await new LogsApiClient(fetchMock).listRequests()

    expect(fetchMock).toHaveBeenCalledWith('/api/logs/requests')
  })

  it('serializes repeated SSE channels and filters with an explicit replay cursor', () => {
    const client = new LogsApiClient(vi.fn())
    const url = client.logsEventSourceUrl({
      channels: ['requests', 'operations'],
      filters: [
        { key: 'from', value: '2026-08-03T00:00:00Z' },
        { key: 'route', value: 'chat' },
        { key: 'model', value: 'Qwen/Qwen3' },
        { key: 'model', value: 'Qwen/Qwen2.5' },
        { key: 'provider', value: 'reserve-a' },
        { key: 'engine', value: 'skippy' },
        { key: 'outcome', value: 'completed' }
      ],
      requestIds: [LogRequestId.parse(REQUEST_ID), LogRequestId.parse('00000000-0000-4000-8000-000000000002')],
      cursor: LogReplayCursor.parse('v1:2.3.4')
    })

    expect(url).toBe(
      '/api/logs/events?channel=requests&channel=operations&filter=from%3A2026-08-03T00%3A00%3A00Z&filter=route%3Achat&filter=model%3AQwen%2FQwen3&filter=model%3AQwen%2FQwen2.5&filter=provider%3Areserve-a&filter=engine%3Askippy&filter=outcome%3Acompleted&filter=request_id%3A00000000-0000-4000-8000-000000000001&filter=request_id%3A00000000-0000-4000-8000-000000000002&cursor=v1%3A2.3.4'
    )
  })

  it('returns a typed download only for available redacted artifact content', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(artifactDto('available', 'SGVsbG8=')))
    const client = new LogsApiClient(fetchMock)
    const result = await client.downloadArtifact(LogArtifactId.parse(ARTIFACT_ID))

    expect(result.state).toBe('download')
    if (result.state === 'download') {
      expect(new TextDecoder().decode(result.download.bytes)).toBe('Hello')
      expect(result.download.mediaType).toBe('text/plain')
      expect(result.download.fileName).toBe(`mesh-llm-log-${ARTIFACT_ID}.bin`)
    }
  })

  it('keeps missing and corrupt artifacts out of the download path', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse(artifactDto('missing', null)))
    const client = new LogsApiClient(fetchMock)
    const result = await client.downloadArtifact(LogArtifactId.parse(ARTIFACT_ID))

    expect(result).toMatchObject({ state: 'unavailable', artifact: { contentState: 'missing' } })
  })

  it('maps an older host 404 to unsupported after exactly one request', async () => {
    const fetchMock = vi.fn().mockResolvedValue(jsonResponse({ error: { code: 'not_found' } }, 404))
    const client = new LogsApiClient(fetchMock)
    const result = await client.listRequests({
      cursor: undefined,
      model: 'model-a',
      source: 'durable'
    })

    expect(result).toEqual({ state: 'unsupported' })
    expect(fetchMock).toHaveBeenCalledTimes(1)
    expect(fetchMock).toHaveBeenCalledWith('/api/logs/requests?model=model-a&source=durable')
  })

  it('serializes an opaque REST cursor without imposing backend limits', async () => {
    const fetchMock = vi.fn().mockResolvedValue(
      jsonResponse({
        items: [],
        nextCursor: null
      })
    )
    const client = new LogsApiClient(fetchMock)
    await client.listRequests({ cursor: LogPageCursor.parse('opaque cursor+/=') })

    expect(fetchMock).toHaveBeenCalledWith('/api/logs/requests?cursor=opaque+cursor%2B%2F%3D')
  })

  it('uses strict POST bodies for bounded export, cleanup, delete, and a supplied webhook delivery ID', async () => {
    const operationId = LogOperationId.parse('00000000-0000-4000-8000-000000000002')
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(
        jsonResponse({
          items: [],
          nextCursor: null,
          truncated: false,
          retryRequired: false,
          artifactContentIncluded: false
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          operationId: operationId.toString(),
          auditId: AUDIT_ID,
          cutoffBefore: TIMESTAMP,
          requestLimit: 1,
          scope: {
            source: 'durable',
            cutoffBefore: TIMESTAMP,
            requestLimit: 1,
            from: '2026-08-01T00:00:00Z',
            to: TIMESTAMP,
            route: 'reserve',
            model: 'Qwen/Qwen3',
            provider: 'reserve-a',
            engine: 'skippy',
            outcome: 'completed'
          },
          state: 'previewed',
          hasMore: false,
          selectionFingerprint: 'safe',
          planned: { requests: 1, events: 0, artifacts: 0, proxyRecords: 0, databaseRows: 1 },
          executed: { requests: 0, events: 0, artifacts: 0, proxyRecords: 0, databaseRows: 0 },
          artifactDeletion: { removed: 0, failed: 0 }
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          operationId: operationId.toString(),
          auditId: AUDIT_ID,
          cutoffBefore: TIMESTAMP,
          requestLimit: 1,
          scope: {
            source: 'durable',
            cutoffBefore: TIMESTAMP,
            requestLimit: 1,
            from: '2026-08-01T00:00:00Z',
            to: TIMESTAMP,
            route: 'reserve',
            model: 'Qwen/Qwen3',
            provider: 'reserve-a',
            engine: 'skippy',
            outcome: 'completed'
          },
          state: 'completed',
          hasMore: false,
          selectionFingerprint: 'safe',
          planned: { requests: 1, events: 0, artifacts: 0, proxyRecords: 0, databaseRows: 1 },
          executed: { requests: 1, events: 0, artifacts: 0, proxyRecords: 0, databaseRows: 1 },
          artifactDeletion: { removed: 0, failed: 0 }
        })
      )
      .mockResolvedValueOnce(
        jsonResponse({
          operationId: operationId.toString(),
          auditId: AUDIT_ID,
          requestId: REQUEST_ID,
          state: 'completed',
          selectionFingerprint: 'safe',
          planned: { requests: 1, events: 0, artifacts: 0, proxyRecords: 0, databaseRows: 1 },
          executed: { requests: 1, events: 0, artifacts: 0, proxyRecords: 0, databaseRows: 1 },
          artifactDeletion: { removed: 0, failed: 0 }
        })
      )
      .mockResolvedValueOnce(jsonResponse({ outcome: 'scheduled' }))
    const client = new LogsApiClient(fetchMock)

    await client.exportRequests(
      { cursor: LogPageCursor.parse('page-2'), model: 'Qwen3' },
      { reason: 'audit copy', includeArtifacts: false }
    )
    const preview = await client.previewCleanup({
      operationId,
      cutoffBefore: TIMESTAMP,
      requestLimit: 1,
      source: 'durable',
      from: '2026-08-01T00:00:00Z',
      to: TIMESTAMP,
      route: 'reserve',
      model: 'Qwen/Qwen3',
      provider: 'reserve-a',
      engine: 'skippy',
      outcome: 'completed',
      reason: 'retention'
    })
    const completed = await client.runCleanup({ operationId, reason: 'retention' })
    const deleted = await client.deleteRequest(LogRequestId.parse(REQUEST_ID), {
      operationId,
      reason: 'incident cleanup'
    })
    await client.retryWebhookDelivery(LogWebhookDeliveryId.parse('webhook:delivery'), 'operator retry')

    expect(preview.auditId.toString()).toBe(AUDIT_ID)
    expect(preview.scope).toMatchObject({ source: 'durable', model: 'Qwen/Qwen3', outcome: 'completed' })
    expect(completed.auditId.toString()).toBe(AUDIT_ID)
    expect(completed.scope).toMatchObject({ source: 'durable', model: 'Qwen/Qwen3', outcome: 'completed' })
    expect(deleted.auditId.toString()).toBe(AUDIT_ID)

    expect(fetchMock).toHaveBeenNthCalledWith(1, '/api/logs/requests/export?cursor=page-2&model=Qwen3', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ reason: 'audit copy', includeArtifacts: false })
    })
    expect(fetchMock).toHaveBeenNthCalledWith(2, '/api/logs/cleanup/preview', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        operationId: operationId.toString(),
        cutoffBefore: TIMESTAMP,
        requestLimit: 1,
        source: 'durable',
        from: '2026-08-01T00:00:00Z',
        to: TIMESTAMP,
        route: 'reserve',
        model: 'Qwen/Qwen3',
        provider: 'reserve-a',
        engine: 'skippy',
        outcome: 'completed',
        reason: 'retention'
      })
    })
    expect(fetchMock).toHaveBeenNthCalledWith(3, '/api/logs/cleanup/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ operationId: operationId.toString(), reason: 'retention' })
    })
    expect(fetchMock).toHaveBeenNthCalledWith(
      4,
      `/api/logs/requests/${REQUEST_ID}/delete`,
      expect.objectContaining({ method: 'POST' })
    )
    expect(fetchMock).toHaveBeenNthCalledWith(
      5,
      '/api/logs/webhooks/webhook%3Adelivery/retry',
      expect.objectContaining({ method: 'POST' })
    )
  })

  it('reuses a partial receipt operation and audit reason for cleanup and deletion retries', async () => {
    const operationId = LogOperationId.parse('00000000-0000-4000-8000-000000000002')
    const cleanupReason = 'retention cleanup'
    const deletionReason = 'incident cleanup'
    const fetchMock = vi
      .fn()
      .mockResolvedValueOnce(jsonResponse(cleanupReceiptDto(operationId, 'partial', 1, true)))
      .mockResolvedValueOnce(jsonResponse(cleanupReceiptDto(operationId, 'completed', 0, true)))
      .mockResolvedValueOnce(jsonResponse(deleteReceiptDto(operationId, 'partial', 1)))
      .mockResolvedValueOnce(jsonResponse(deleteReceiptDto(operationId, 'completed', 0)))
    const client = new LogsApiClient(fetchMock)

    const cleanupPartial = await client.runCleanup({ operationId, reason: cleanupReason })
    const cleanupCompleted = await client.runCleanup({ operationId: cleanupPartial.operationId, reason: cleanupReason })
    const deletionPartial = await client.deleteRequest(LogRequestId.parse(REQUEST_ID), {
      operationId,
      reason: deletionReason
    })
    const deletionCompleted = await client.deleteRequest(LogRequestId.parse(REQUEST_ID), {
      operationId: deletionPartial.operationId,
      reason: deletionReason
    })

    expect(cleanupPartial).toMatchObject({ state: 'partial', hasMore: true, artifactDeletion: { failed: 1 } })
    expect(cleanupCompleted).toMatchObject({ state: 'completed', hasMore: true, artifactDeletion: { failed: 0 } })
    expect(deletionPartial).toMatchObject({ state: 'partial', artifactDeletion: { failed: 1 } })
    expect(deletionCompleted).toMatchObject({ state: 'completed', artifactDeletion: { failed: 0 } })
    expect(fetchMock).toHaveBeenNthCalledWith(1, '/api/logs/cleanup/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ operationId: operationId.toString(), reason: cleanupReason })
    })
    expect(fetchMock).toHaveBeenNthCalledWith(2, '/api/logs/cleanup/run', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ operationId: operationId.toString(), reason: cleanupReason })
    })
    expect(fetchMock).toHaveBeenNthCalledWith(3, `/api/logs/requests/${REQUEST_ID}/delete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ operationId: operationId.toString(), reason: deletionReason })
    })
    expect(fetchMock).toHaveBeenNthCalledWith(4, `/api/logs/requests/${REQUEST_ID}/delete`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ operationId: operationId.toString(), reason: deletionReason })
    })
  })
})
