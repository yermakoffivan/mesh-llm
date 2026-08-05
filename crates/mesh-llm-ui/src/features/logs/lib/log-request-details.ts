import { parseLogsLedgerSearch, type LogsLedgerSearch } from '@/features/logs/lib/log-search'
import type { LogLifecycleEvent, LogProxyAttempt } from '@/features/logs/api/schemas'

export type LogRequestDetailTab = 'summary' | 'request' | 'response' | 'routing' | 'stream' | 'errors'

export type LogRequestDetailsSearch = LogsLedgerSearch & {
  readonly tab?: LogRequestDetailTab
}

const detailTabs: readonly LogRequestDetailTab[] = ['summary', 'request', 'response', 'routing', 'stream', 'errors']

export function isLogRequestDetailTab(value: string): value is LogRequestDetailTab {
  return detailTabs.some((tab) => tab === value)
}

export function parseLogRequestDetailsSearch(search: Record<string, unknown>): LogRequestDetailsSearch {
  const ledgerSearch = parseLogsLedgerSearch(search)
  const candidate = typeof search['tab'] === 'string' ? search['tab'] : undefined
  const tab = detailTabs.find((value) => value === candidate)
  return { ...ledgerSearch, ...(tab ? { tab } : {}) }
}

export function ledgerSearchFromDetails(search: LogRequestDetailsSearch): LogsLedgerSearch {
  const { tab: _tab, ...ledgerSearch } = search
  return ledgerSearch
}

export function sortLifecycleEvents(events: readonly LogLifecycleEvent[]): LogLifecycleEvent[] {
  return [...events].sort((left, right) => left.occurredAt.localeCompare(right.occurredAt))
}

export function sortProxyAttempts(attempts: readonly LogProxyAttempt[]): LogProxyAttempt[] {
  return [...attempts].sort((left, right) => left.occurredAt.localeCompare(right.occurredAt))
}

export function isStreamEvent(event: LogLifecycleEvent): boolean {
  return (
    event.kind === 'stream_started' ||
    event.kind === 'stream_chunk' ||
    event.kind === 'stream_completed' ||
    event.kind === 'stream_error'
  )
}

export function isErrorEvent(event: LogLifecycleEvent): boolean {
  return (
    event.kind === 'attempt_failed' ||
    event.kind === 'stream_error' ||
    event.kind === 'audit_error' ||
    event.kind === 'failed'
  )
}

export function artifactMatchesTab(kind: string, tab: 'request' | 'response' | 'errors'): boolean {
  const normalized = kind.toLowerCase()
  return normalized.includes(tab === 'errors' ? 'error' : tab)
}
