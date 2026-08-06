import '@testing-library/jest-dom/vitest'

import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest'
import { LogRequestId } from '@/features/logs/api/ids'
import type { LogRequest } from '@/features/logs/api/schemas'
import { RequestsOverTimeChart } from '@/features/logs/components/RequestsOverTimeChart'

const NOW = Date.UTC(2026, 7, 4, 12, 0, 0)

function requestAt(createdAt: string): LogRequest {
  return {
    requestId: LogRequestId.parse('00000000-0000-4000-8000-000000000001'),
    outcome: 'completed',
    createdAt,
    terminalAt: undefined,
    route: 'reserve',
    model: 'Qwen3',
    provider: 'reserve-a',
    engine: 'skippy',
    statusCode: 200,
    source: 'durable'
  }
}

function iso(ms: number): string {
  return new Date(ms).toISOString()
}

const EMPTY_MESSAGE = 'No requests during the selected time range.'

describe('RequestsOverTimeChart', () => {
  beforeEach(() => {
    class ResizeObserverStub {
      observe() {}
      unobserve() {}
      disconnect() {}
    }
    vi.stubGlobal('ResizeObserver', ResizeObserverStub)
  })

  afterEach(() => {
    vi.unstubAllGlobals()
  })

  it('renders the card header with bucket and time range selectors', () => {
    render(<RequestsOverTimeChart rows={[]} now={NOW} />)

    expect(screen.getByText('Requests Over Time')).toBeInTheDocument()
    expect(screen.getByText('Request volume by time bucket')).toBeInTheDocument()

    const bucketSelect = screen.getByLabelText('Bucket interval') as HTMLSelectElement
    const rangeSelect = screen.getByLabelText('Chart time range') as HTMLSelectElement
    expect(bucketSelect.value).toBe('5m')
    expect(rangeSelect.value).toBe('12h')
  })

  it('shows the empty state when there are no rows', () => {
    render(<RequestsOverTimeChart rows={[]} now={NOW} />)

    expect(screen.getByText(EMPTY_MESSAGE)).toBeInTheDocument()
    expect(screen.queryByLabelText('Requests over time bar chart')).not.toBeInTheDocument()
  })

  it('shows the empty state when every request falls outside the window', () => {
    const rows = [requestAt(iso(NOW - 13 * 3_600_000))]
    render(<RequestsOverTimeChart rows={rows} now={NOW} />)

    expect(screen.getByText(EMPTY_MESSAGE)).toBeInTheDocument()
  })

  it('renders the chart frame when requests fall inside the window', () => {
    const rows = [
      requestAt(iso(NOW - 10 * 60_000)),
      requestAt(iso(NOW - 5 * 60_000)),
      requestAt(iso(NOW))
    ]
    render(<RequestsOverTimeChart rows={rows} now={NOW} />)

    expect(screen.queryByText(EMPTY_MESSAGE)).not.toBeInTheDocument()
    expect(screen.getByLabelText('Requests over time bar chart')).toBeInTheDocument()
  })

  it('switches the bucket interval and time range via the selectors', async () => {
    const user = userEvent.setup()
    render(<RequestsOverTimeChart rows={[]} now={NOW} />)

    const bucketSelect = screen.getByLabelText('Bucket interval') as HTMLSelectElement
    const rangeSelect = screen.getByLabelText('Chart time range') as HTMLSelectElement

    await user.selectOptions(bucketSelect, '1h')
    expect(bucketSelect.value).toBe('1h')

    await user.selectOptions(rangeSelect, '24h')
    expect(rangeSelect.value).toBe('24h')
  })
})
