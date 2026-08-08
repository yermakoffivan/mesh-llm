import { describe, expect, it } from 'vitest'
import {
  advanceLogsPage,
  formatRelativeTime,
  parseLogsLedgerSearch,
  resetLogsSearch,
  resolveRelativeTime,
  toLogsRequestQuery,
  updateLogsFilter,
  updateLogsTimeRange
} from './log-search'

describe('logs ledger URL search', () => {
  it('restores supported filters and an opaque cursor from the route search (no time bounds without preset)', () => {
    const search = parseLogsLedgerSearch({
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
      from: undefined,
      to: undefined,
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

  it('resolves timeRange preset to from/to bounds at query time', () => {
    const search = parseLogsLedgerSearch({
      model: 'Qwen3',
      timeRange: '24h'
    })

    expect(search.timeRange).toBe('24h')
    const query = toLogsRequestQuery(search)
    expect(query.from).toBeDefined()
    expect(query.to).toBeDefined()

    if (query.from && query.to) {
      const diffHours = (new Date(query.to).getTime() - new Date(query.from).getTime()) / 3_600_000
      expect(diffHours).toBeCloseTo(24, 1)
    }

    const bounds7d = resolveRelativeTime('7d')
    if (bounds7d?.from && bounds7d?.to) {
      const diffDays = (new Date(bounds7d.to).getTime() - new Date(bounds7d.from).getTime()) / 86_400_000
      expect(diffDays).toBeCloseTo(7, 1)
    }

    expect(resolveRelativeTime('')).toBeUndefined()
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

  it('update helpers clear pagination on filter change and preserve other filters', () => {
    const base = parseLogsLedgerSearch({ model: 'Qwen3', source: 'active', cursor: 'next' })
    const updatedFilter = updateLogsFilter(base, 'engine', 'skippy')
    expect(updatedFilter.engine).toBe('skippy')
    expect(updatedFilter.cursor).toBeUndefined()

    const updatedTimeRange = updateLogsTimeRange({ ...base }, '6h')
    expect(updatedTimeRange.timeRange).toBe('6h')
    expect(updatedTimeRange.cursor).toBeUndefined()
  })

  it('formatRelativeTime produces readable labels for various age ranges', () => {
    const now = new Date().toISOString()
    expect(formatRelativeTime(now)).toContain('just now')

    const thirtyMinAgo = new Date(Date.now() - 30 * 60_000).toISOString()
    expect(formatRelativeTime(thirtyMinAgo)).toMatch(/\d+m ago/)

    const twoHoursAgo = new Date(Date.now() - 2 * 3_600_000).toISOString()
    expect(formatRelativeTime(twoHoursAgo)).toContain('2h')

    const threeDaysAgo = new Date(Date.now() - 3 * 86_400_000).toISOString()
    expect(formatRelativeTime(threeDaysAgo)).toContain('3d')

    const thirtyDaysAgo = new Date(Date.now() - 30 * 86_400_000).toISOString()
    const oldLabel = formatRelativeTime(thirtyDaysAgo)
    expect(oldLabel.length).toBeGreaterThan(5)
  })
})
