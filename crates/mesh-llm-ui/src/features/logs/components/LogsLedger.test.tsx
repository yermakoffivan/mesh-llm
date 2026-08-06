import '@testing-library/jest-dom/vitest'

import { render, screen } from '@testing-library/react'
import userEvent from '@testing-library/user-event'
import { beforeEach, describe, expect, it, vi } from 'vitest'
import { LogRequestId } from '@/features/logs/api/ids'
import type { LogRequest } from '@/features/logs/api/schemas'
import { parseLogsLedgerSearch } from '@/features/logs/lib/log-search'

const queryState = vi.hoisted(() => ({ current: {} }))
const liveState = vi.hoisted(() => ({ current: { state: 'connected', liveRequestIds: [] } }))
const useLogsLiveRecoveryMock = vi.hoisted(() => vi.fn())

vi.mock('@/features/logs/api/use-logs-ledger-query', () => ({
  useLogsLedgerQuery: () => queryState.current
}))

vi.mock('@/features/logs/api/use-logs-live-recovery', () => ({
  useLogsLiveRecovery: useLogsLiveRecoveryMock
}))

import { LogsLedger } from '@/features/logs/components/LogsLedger'

const REQUEST_A = '00000000-0000-4000-8000-000000000001'

function request(id: string, outcome: LogRequest['outcome'], source: LogRequest['source']): LogRequest {
  return {
    requestId: LogRequestId.parse(id),
    outcome,
    createdAt: '2026-08-04T12:00:00Z',
    terminalAt: outcome === 'active' ? undefined : '2026-08-04T12:00:01Z',
    route: 'reserve',
    model: 'Qwen3',
    provider: 'reserve-a',
    engine: 'skippy',
    statusCode: outcome === 'completed' ? 200 : undefined,
    source
  }
}

function supported(rows: readonly LogRequest[], nextCursor?: string) {
  return {
    isLoading: false,
    isError: false,
    isFetching: false,
    refetch: vi.fn(),
    data: {
      state: 'supported',
      value: {
        items: rows,
        nextCursor: nextCursor ? { toString: () => nextCursor } : undefined
      }
    }
  }
}

describe('LogsLedger', () => {
  beforeEach(() => {
    useLogsLiveRecoveryMock.mockReset()
    useLogsLiveRecoveryMock.mockImplementation(() => liveState.current)
    liveState.current = { state: 'connected', liveRequestIds: [] }
    queryState.current = supported(
      [request(REQUEST_A, 'failed', 'durable'), request(REQUEST_A, 'active', 'active')],
      'next-page'
    )
  })

  it('keeps an active row in place when it supersedes durable history and renders stable table pagination', async () => {
    const user = userEvent.setup()
    const onSearchChange = vi.fn()
    const onRequestOpen = vi.fn()
    render(
      <LogsLedger onRequestOpen={onRequestOpen} search={parseLogsLedgerSearch({})} onSearchChange={onSearchChange} />
    )

    expect(screen.getAllByText(REQUEST_A)).toHaveLength(1)
    expect(screen.getAllByText('active')).toHaveLength(2)
    expect(screen.getByRole('combobox', { name: 'Rows per page' })).toBeInTheDocument()
    expect(screen.getByRole('button', { name: 'Go to previous page' })).toBeDisabled()
    expect(screen.getByRole('button', { name: 'Go to next page' })).toBeDisabled()

    await user.click(screen.getByRole('button', { name: `Open request ${REQUEST_A}` }))
    expect(onRequestOpen).toHaveBeenCalledWith(REQUEST_A, expect.objectContaining({ focusRequestId: REQUEST_A }))
  })

  it('clears time and category filters with an accessible reset action', async () => {
    const user = userEvent.setup()
    const onSearchChange = vi.fn()
    render(
      <LogsLedger
        search={parseLogsLedgerSearch({ from: '2026-08-01T00:00:00Z', model: 'Qwen3', provider: 'reserve-a' })}
        onRequestOpen={vi.fn()}
        onSearchChange={onSearchChange}
      />
    )

    await user.click(screen.getByRole('button', { name: 'Reset view' }))
    expect(onSearchChange).toHaveBeenCalledWith({})
  })

  it('keeps filter controls in the keyboard order and labels the time range control', async () => {
    const user = userEvent.setup()
    render(<LogsLedger onRequestOpen={vi.fn()} search={parseLogsLedgerSearch({})} onSearchChange={vi.fn()} />)

    await user.tab()
    expect(screen.getByLabelText('Filter logs by time range')).toHaveFocus()
  })

  it('filters the loaded page by request ID', async () => {
    const user = userEvent.setup()
    const REQUEST_B = '00000000-0000-4000-8000-000000000002'
    queryState.current = supported([
      request(REQUEST_A, 'completed', 'durable'),
      request(REQUEST_B, 'failed', 'durable')
    ])
    render(<LogsLedger onRequestOpen={vi.fn()} search={parseLogsLedgerSearch({})} onSearchChange={vi.fn()} />)

    expect(screen.getAllByRole('button', { name: /Open request/ })).toHaveLength(2)

    await user.type(screen.getByLabelText('Filter by request ID'), REQUEST_A)

    expect(screen.getByRole('button', { name: `Open request ${REQUEST_A}` })).toBeInTheDocument()
    expect(screen.queryByRole('button', { name: `Open request ${REQUEST_B}` })).not.toBeInTheDocument()
  })

  it('restores focus to the opened request after returning from the inspector', () => {
    render(
      <LogsLedger
        onRequestOpen={vi.fn()}
        onSearchChange={vi.fn()}
        search={parseLogsLedgerSearch({ focusRequestId: REQUEST_A })}
      />
    )

    expect(screen.getByRole('button', { name: `Open request ${REQUEST_A}` })).toHaveFocus()
  })

  it('shows the dedicated logs connection state without changing the authoritative ledger query', () => {
    liveState.current = { state: 'polling', liveRequestIds: [] }
    render(<LogsLedger onRequestOpen={vi.fn()} onSearchChange={vi.fn()} search={parseLogsLedgerSearch({})} />)

    expect(screen.getByText('Polling')).toBeInTheDocument()
    expect(useLogsLiveRecoveryMock).toHaveBeenCalledWith(expect.objectContaining({ enabled: true }))
  })

  it('keeps live recovery disabled when the ledger API is unsupported', () => {
    queryState.current = {
      isLoading: false,
      isError: false,
      isFetching: false,
      data: { state: 'unsupported' },
      refetch: vi.fn()
    }

    render(<LogsLedger onRequestOpen={vi.fn()} onSearchChange={vi.fn()} search={parseLogsLedgerSearch({})} />)

    expect(screen.getByRole('status')).toHaveTextContent('Log history is unavailable on this host')
    expect(screen.queryByRole('table', { name: 'Request logs' })).not.toBeInTheDocument()
    expect(useLogsLiveRecoveryMock).toHaveBeenCalledWith(expect.objectContaining({ enabled: false }))
  })

  it('renders an empty ledger as an announced state without an empty request table', () => {
    queryState.current = supported([], undefined)
    render(<LogsLedger onRequestOpen={vi.fn()} search={parseLogsLedgerSearch({})} onSearchChange={vi.fn()} />)

    expect(screen.getByRole('status')).toHaveTextContent('No log requests match this view')
    expect(screen.queryByRole('table', { name: 'Request logs' })).not.toBeInTheDocument()
  })

  it('offers a stable, labeled retry action when the logs API fails', async () => {
    const user = userEvent.setup()
    const refetch = vi.fn()
    queryState.current = { isLoading: false, isError: true, isFetching: false, data: undefined, refetch }
    render(<LogsLedger onRequestOpen={vi.fn()} search={parseLogsLedgerSearch({})} onSearchChange={vi.fn()} />)

    expect(screen.getByRole('alert')).toHaveTextContent('Log history could not be loaded')
    const retry = screen.getByRole('button', { name: 'Retry' })
    expect(retry).toBeEnabled()

    await user.click(retry)

    expect(refetch).toHaveBeenCalledOnce()
  })
})
