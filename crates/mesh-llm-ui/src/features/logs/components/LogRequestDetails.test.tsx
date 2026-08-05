import '@testing-library/jest-dom/vitest'

import { render, screen, waitFor } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { LogArtifactId, LogEventId, LogRequestId } from '@/features/logs/api/ids'
import type { LogArtifact, LogLifecycleEvent, LogProxyAttempt, LogRequest } from '@/features/logs/api/schemas'
import type { LogRequestDetailTab } from '@/features/logs/lib/log-request-details'

const hooks = vi.hoisted(() => ({
  summary: vi.fn(),
  events: vi.fn(),
  artifacts: vi.fn(),
  attempts: vi.fn()
}))

const api = vi.hoisted(() => ({ downloadArtifact: vi.fn() }))

vi.mock('@/features/logs/api/use-log-request-details-query', () => ({
  useLogRequestSummaryQuery: (...args: unknown[]) => hooks.summary(...args),
  useLogRequestEventsQuery: (...args: unknown[]) => hooks.events(...args),
  useLogRequestArtifactsQuery: (...args: unknown[]) => hooks.artifacts(...args),
  useLogRequestAttemptsQuery: (...args: unknown[]) => hooks.attempts(...args)
}))

vi.mock('@/features/logs/api/client', () => ({
  LogsApiClient: class {
    downloadArtifact = api.downloadArtifact
  }
}))

import { LogRequestDetails } from '@/features/logs/components/LogRequestDetails'

const REQUEST_ID = LogRequestId.parse('00000000-0000-4000-8000-000000000001')

function request(): LogRequest {
  return {
    requestId: REQUEST_ID,
    outcome: 'failed',
    createdAt: '2026-08-04T12:00:00Z',
    terminalAt: '2026-08-04T12:00:03Z',
    route: 'reserve',
    model: 'Qwen3',
    provider: 'reserve-a',
    engine: 'skippy',
    statusCode: 502,
    source: 'durable'
  }
}

function event(eventId: string, kind: LogLifecycleEvent['kind'], occurredAt: string): LogLifecycleEvent {
  return {
    eventId: LogEventId.parse(eventId),
    requestId: REQUEST_ID,
    occurredAt,
    kind,
    model: undefined,
    provider: undefined,
    engine: undefined,
    attemptId: kind === 'attempt_failed' ? 'attempt-two' : undefined,
    statusCode: 502,
    durationMs: 12,
    tokens: 3
  }
}

function artifact(kind: string, contentState: LogArtifact['contentState'], redacted = true): LogArtifact {
  const base = {
    artifactId: LogArtifactId.parse('00000000-0000-4000-8000-000000000011'),
    requestId: REQUEST_ID,
    occurredAt: '2026-08-04T12:00:02Z',
    kind,
    mediaKind: 'application/json',
    checksum: 'sha256:0123456789abcdef',
    bytes: 384,
    version: 2,
    redacted,
    truncated: true
  }
  if (contentState === 'available') return { ...base, contentState, contentBase64: 'ZXhhbXBsZQ==' }
  return { ...base, contentState, contentBase64: undefined }
}

function attempt(attemptId: string, occurredAt: string): LogProxyAttempt {
  return {
    attemptId,
    requestId: REQUEST_ID,
    occurredAt,
    target: 'opaque',
    provider: 'reserve-a',
    engine: 'skippy',
    startedAt: occurredAt,
    completedAt: occurredAt,
    statusCode: 502
  }
}

function ready<T>(data: T) {
  return { data, isLoading: false, isError: false }
}

function renderDetails(tab: 'summary' | 'request' | 'response' | 'routing' | 'stream' | 'errors' = 'summary') {
  return render(<LogRequestDetails onBack={vi.fn()} onTabChange={vi.fn()} requestId={REQUEST_ID} tab={tab} />)
}

describe('LogRequestDetails', () => {
  beforeEach(() => {
    hooks.summary.mockReset()
    hooks.events.mockReset()
    hooks.artifacts.mockReset()
    hooks.attempts.mockReset()
    api.downloadArtifact.mockReset()
    hooks.summary.mockReturnValue(ready(request()))
    hooks.events.mockReturnValue(
      ready({
        items: [
          event('00000000-0000-4000-8000-000000000003', 'stream_chunk', '2026-08-04T12:00:03Z'),
          event('00000000-0000-4000-8000-000000000002', 'attempt_failed', '2026-08-04T12:00:02Z')
        ]
      })
    )
    hooks.artifacts.mockReturnValue(
      ready({
        items: [
          artifact('request', 'available'),
          artifact('response', 'missing'),
          artifact('<img src=x onerror=error>', 'corrupt')
        ]
      })
    )
    hooks.attempts.mockReturnValue(
      ready({ items: [attempt('second', '2026-08-04T12:00:04Z'), attempt('first', '2026-08-04T12:00:01Z')] })
    )
  })

  it('loads only the summary initially and focuses the request heading', () => {
    renderDetails()

    expect(screen.getByRole('heading', { name: REQUEST_ID.toString() })).toHaveFocus()
    expect(hooks.events).toHaveBeenCalledWith(REQUEST_ID, false)
    expect(hooks.artifacts).toHaveBeenCalledWith(REQUEST_ID, false)
    expect(hooks.attempts).toHaveBeenCalledWith(REQUEST_ID, false)
    expect(screen.getByRole('button', { name: 'Copy Request ID' })).toBeInTheDocument()
  })

  const tabCases: Array<[LogRequestDetailTab, string]> = [
    ['summary', 'Request summary'],
    ['request', 'Request artifact'],
    ['response', 'Response artifact'],
    ['routing', 'Routing attempts'],
    ['stream', 'Stream timeline'],
    ['errors', 'Errors']
  ]

  it.each(tabCases)('renders the %s tab', (tab, title) => {
    renderDetails(tab)
    expect(screen.getByRole('tabpanel')).toHaveTextContent(title)
  })

  it('orders proxy and stream records and exposes keyboard-accessible detail tabs', async () => {
    const user = userEvent.setup()
    renderDetails('routing')

    expect(screen.getByRole('list', { name: 'Ordered routing attempts' }).textContent).toMatch(/first[\s\S]*second/)
    await user.click(screen.getByRole('tab', { name: 'Stream timeline' }))
    expect(screen.getByRole('tab', { name: 'Stream timeline' })).toHaveFocus()
  })

  it('renders missing and corrupt artifacts as metadata without exposing their payloads', () => {
    hooks.artifacts.mockReturnValue(
      ready({ items: [artifact('response-missing', 'missing'), artifact('response-corrupt', 'corrupt')] })
    )
    renderDetails('response')

    expect(screen.getByText('missing')).toBeInTheDocument()
    expect(screen.getByText('corrupt')).toBeInTheDocument()
    expect(screen.getAllByText('Redacted before retention')).toHaveLength(2)
    expect(screen.queryByText('ZXhhbXBsZQ==')).not.toBeInTheDocument()
    expect(screen.queryByRole('button', { name: 'Download redacted artifact' })).not.toBeInTheDocument()
  })

  it('downloads only available redacted artifacts after an explicit action', async () => {
    const user = userEvent.setup()
    const createObjectUrl = vi.fn(() => 'blob:artifact')
    const revokeObjectUrl = vi.fn()
    const anchorClick = vi.spyOn(HTMLAnchorElement.prototype, 'click').mockImplementation(() => undefined)
    Object.assign(URL, { createObjectURL: createObjectUrl, revokeObjectURL: revokeObjectUrl })
    const available = artifact('request-available', 'available')
    hooks.artifacts.mockReturnValue(
      ready({
        items: [available, artifact('request-unredacted', 'available', false), artifact('request-missing', 'missing')]
      })
    )
    api.downloadArtifact.mockResolvedValue({
      state: 'download',
      download: {
        artifact: available,
        bytes: new Uint8Array([1, 2, 3]),
        fileName: 'mesh-llm-redacted-artifact.bin',
        mediaType: 'application/octet-stream'
      }
    })

    try {
      renderDetails('request')
      const download = screen.getByRole('button', { name: 'Download redacted artifact' })
      expect(api.downloadArtifact).not.toHaveBeenCalled()

      await user.click(download)

      await waitFor(() => expect(api.downloadArtifact).toHaveBeenCalledWith(available.artifactId))
      expect(createObjectUrl).toHaveBeenCalledTimes(1)
      await waitFor(() => expect(revokeObjectUrl).toHaveBeenCalledWith('blob:artifact'))
      expect(screen.getByRole('status')).toHaveTextContent('Artifact download started.')
    } finally {
      anchorClick.mockRestore()
    }
  })

  it('does not start a browser download when an artifact becomes unavailable', async () => {
    const user = userEvent.setup()
    const createObjectUrl = vi.fn(() => 'blob:artifact')
    const revokeObjectUrl = vi.fn()
    Object.assign(URL, { createObjectURL: createObjectUrl, revokeObjectURL: revokeObjectUrl })
    const available = artifact('request-available', 'available')
    hooks.artifacts.mockReturnValue(ready({ items: [available] }))
    api.downloadArtifact.mockResolvedValue({ state: 'unavailable', artifact: artifact('request-missing', 'missing') })

    renderDetails('request')
    await user.click(screen.getByRole('button', { name: 'Download redacted artifact' }))

    await waitFor(() => expect(api.downloadArtifact).toHaveBeenCalledWith(available.artifactId))
    expect(createObjectUrl).not.toHaveBeenCalled()
    expect(revokeObjectUrl).not.toHaveBeenCalled()
    expect(screen.getByRole('status')).toHaveTextContent('This artifact is no longer available for download.')
  })

  it('renders hostile error artifact text as text rather than markup', () => {
    hooks.artifacts.mockReturnValue(ready({ items: [artifact('error-<img src=x onerror=error>', 'corrupt')] }))
    renderDetails('errors')

    expect(screen.getByText('error-<img src=x onerror=error>')).toBeInTheDocument()
    expect(screen.queryByRole('img')).not.toBeInTheDocument()
  })

  it.each([
    ['loading', { data: undefined, isLoading: true, isError: false }, /Loading request summary/],
    ['error', { data: undefined, isLoading: false, isError: true }, /request summary could not be loaded/]
  ])('renders the request summary %s state', (_state, state, label) => {
    hooks.summary.mockReturnValue(state)
    renderDetails()

    expect(screen.getByText(label)).toBeInTheDocument()
  })

  it('announces when response payload capture is disabled instead of loading content', () => {
    hooks.artifacts.mockReturnValue(ready({ items: [] }))
    renderDetails('response')

    expect(screen.getByRole('status')).toHaveTextContent(
      'Response payload capture is disabled or no matching artifact was retained.'
    )
    expect(screen.queryByRole('button', { name: 'Download redacted artifact' })).not.toBeInTheDocument()
  })

  it('returns to the ledger context with a labeled back action', async () => {
    const user = userEvent.setup()
    const onBack = vi.fn()
    render(<LogRequestDetails onBack={onBack} onTabChange={vi.fn()} requestId={REQUEST_ID} tab="summary" />)

    await user.click(screen.getByRole('button', { name: 'Back to logs' }))
    expect(onBack).toHaveBeenCalledOnce()
  })
})
