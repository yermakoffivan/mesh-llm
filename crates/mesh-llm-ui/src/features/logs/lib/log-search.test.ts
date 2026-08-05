import { describe, expect, it } from 'vitest'
import { advanceLogsPage, parseLogsLedgerSearch, resetLogsSearch, toLogsRequestQuery } from './log-search'

describe('logs ledger URL search', () => {
  it('restores supported filters and an opaque cursor from the route search', () => {
    const search = parseLogsLedgerSearch({
      from: '2026-08-01T00:00:00Z',
      model: 'Qwen3',
      provider: 'reserve-a',
      engine: 'skippy',
      route: 'reserve',
      source: 'durable',
      outcome: 'failed',
      cursor: 'next-page',
      trail: ['previous-page']
    })

    expect(toLogsRequestQuery(search)).toMatchObject({
      from: '2026-08-01T00:00:00Z',
      model: 'Qwen3',
      provider: 'reserve-a',
      engine: 'skippy',
      route: 'reserve',
      source: 'durable',
      outcome: 'failed'
    })
    expect(toLogsRequestQuery(search).cursor?.toString()).toBe('next-page')
    expect(search.trail).toEqual(['previous-page'])
  })

  it('keeps opaque cursor history for next and previous pages without inventing a server limit', () => {
    const first = parseLogsLedgerSearch({ model: 'Qwen3' })
    const second = advanceLogsPage(first, 'cursor-2')
    const third = advanceLogsPage(second, 'cursor-3')

    expect(second).toMatchObject({ cursor: 'cursor-2', trail: [] })
    expect(third).toMatchObject({ cursor: 'cursor-3', trail: ['cursor-2'] })
    expect(advanceLogsPage(third, undefined)).toMatchObject({ cursor: 'cursor-2', trail: [] })
  })

  it('clears filters and pagination together', () => {
    const reset = resetLogsSearch(
      parseLogsLedgerSearch({ model: 'Qwen3', source: 'active', cursor: 'next-page', trail: ['previous-page'] })
    )

    expect(reset).toEqual({})
  })
})
