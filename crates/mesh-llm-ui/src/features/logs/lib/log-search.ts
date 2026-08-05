import { LogPageCursor, LogReplayCursor } from '@/features/logs/api/ids'
import type { LogsRequestQuery } from '@/features/logs/api/client'

export type LogsFilterKey = 'from' | 'to' | 'model' | 'provider' | 'engine' | 'route' | 'source' | 'outcome'

const FILTER_KEYS: readonly LogsFilterKey[] = [
  'from',
  'to',
  'model',
  'provider',
  'engine',
  'route',
  'source',
  'outcome'
]

export type LogsLedgerSearch = {
  readonly focusRequestId?: string
  readonly replayCursor?: string
  readonly cursor?: string
  readonly trail?: readonly string[]
  readonly from?: string
  readonly to?: string
  readonly model?: string
  readonly provider?: string
  readonly engine?: string
  readonly route?: string
  readonly source?: string
  readonly outcome?: string
}

function optionalString(value: unknown) {
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

function cursor(value: unknown) {
  const candidate = optionalString(value)
  if (!candidate) return undefined
  try {
    return LogPageCursor.parse(candidate).toString()
  } catch {
    return undefined
  }
}

function cursorTrail(value: unknown) {
  const entries = Array.isArray(value) ? value : [value]
  return entries.flatMap((entry) => {
    const parsed = cursor(entry)
    return parsed ? [parsed] : []
  })
}

export function parseLogsLedgerSearch(search: Record<string, unknown>): LogsLedgerSearch {
  const parsed: Partial<Record<LogsFilterKey, string>> = {}
  for (const key of FILTER_KEYS) parsed[key] = optionalString(search[key])
  const pageCursor = cursor(search['cursor'])
  const focusRequestId = optionalString(search['focusRequestId'])
  const replayCursor = optionalString(search['replayCursor'])
  const trail = cursorTrail(search['trail'])
  return {
    ...parsed,
    ...(focusRequestId ? { focusRequestId } : {}),
    ...(replayCursor && isReplayCursor(replayCursor) ? { replayCursor } : {}),
    ...(pageCursor ? { cursor: pageCursor } : {}),
    ...(trail.length > 0 ? { trail } : {})
  }
}

function isReplayCursor(value: string) {
  try {
    LogReplayCursor.parse(value)
    return true
  } catch {
    return false
  }
}

export function toLogsRequestQuery(search: LogsLedgerSearch): LogsRequestQuery {
  const parsedCursor = search.cursor ? LogPageCursor.parse(search.cursor) : undefined
  return {
    cursor: parsedCursor,
    from: search.from,
    to: search.to,
    model: search.model,
    provider: search.provider,
    engine: search.engine,
    route: search.route,
    source: search.source,
    outcome: search.outcome
  }
}

export function advanceLogsPage(search: LogsLedgerSearch, nextCursor: string | undefined): LogsLedgerSearch {
  if (!nextCursor) {
    const trail = search.trail ?? []
    const previous = trail.at(-1)
    return {
      ...search,
      ...(previous ? { cursor: previous } : {}),
      ...(previous ? { trail: trail.slice(0, -1) } : { cursor: undefined, trail: undefined })
    }
  }
  return {
    ...search,
    cursor: nextCursor,
    trail: search.cursor ? [...(search.trail ?? []), search.cursor] : []
  }
}

export function resetLogsSearch(_search: LogsLedgerSearch): LogsLedgerSearch {
  return {}
}

export function updateLogsFilter(
  search: LogsLedgerSearch,
  key: LogsFilterKey,
  value: string | undefined
): LogsLedgerSearch {
  return {
    ...search,
    [key]: value,
    cursor: undefined,
    trail: undefined
  }
}
