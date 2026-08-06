import { LogPageCursor, LogReplayCursor } from '@/features/logs/api/ids'
import type { LogsRequestQuery } from '@/features/logs/api/client'
// Time presets replace raw RFC 3339 inputs per shape plan.
export type RelativeTimePreset = '1h' | '6h' | '24h' | '7d' | ''

export const RELATIVE_TIME_PRESETS: readonly { value: RelativeTimePreset; label: string }[] = [


  { value: '', label: 'All time' },
  { value: '1h', label: 'Last hour' },
  { value: '6h', label: 'Last 6 hours' },
  { value: '24h', label: 'Last 24 hours' },
  { value: '7d', label: 'Last week' }
]

export function resolveRelativeTime(preset: RelativeTimePreset): { from?: string; to?: string } | undefined {
  if (!preset) return undefined
  const now = new Date()
  const from = new Date(now.getTime())

  switch (preset) {
    case '1h':
      from.setHours(from.getHours() - 1)
      break
    case '6h':
      from.setHours(from.getHours() - 6)
      break
    case '24h':
      from.setDate(from.getDate() - 1)
      break
    case '7d':
      from.setDate(from.getDate() - 7)
      break
  }

  return { from: from.toISOString(), to: now.toISOString() }
}

function hoursAgo(isoString: string): number | undefined {
  const date = new Date(isoString)
  if (Number.isNaN(date.getTime())) return undefined
  const diffMs = Date.now() - date.getTime()
  return Math.round(diffMs / 60_000) // minutes for sub-hour, hours otherwise
}

export function formatRelativeTime(isoString: string): string {
  const minsAgo = hoursAgo(isoString) ?? Infinity
  if (minsAgo < 2) return 'just now'
  if (minsAgo < 60) return `${minsAgo}m ago`

  const hours = Math.floor(minsAgo / 60)
  if (hours < 24) {
    const remainMins = minsAgo % 60
    if (remainMins === 0) return `${hours}h ago`
    return `${hours}h ${remainMins}m ago`
  }

  const days = Math.floor(hours / 24)
  if (days < 7) {
    const remainHours = hours % 24
    if (remainHours === 0) return `${days}d ago`
    return `${days}d ${remainHours}h ago`
  }

  // Fallback to date for older entries — still more readable than raw ISO.
  const date = new Date(isoString)
  if (Number.isNaN(date.getTime())) return isoString
  return date.toLocaleDateString() + ' ' + date.toLocaleTimeString([], { hour: '2-digit', minute: '2-digit' })
}

export type LogsFilterKey = 'model' | 'provider' | 'engine' | 'route' | 'source' | 'outcome'

const FILTER_KEYS: readonly LogsFilterKey[] = [
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
  readonly model?: string
  readonly provider?: string
  readonly engine?: string
  readonly route?: string
  readonly source?: string
  readonly outcome?: string
  readonly timeRange?: RelativeTimePreset | ''
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

  // Support timeRange preset; fall back to legacy 'from'/'to' in URL.
  let timeRange = '' as RelativeTimePreset | ''
  const rawTimeRange = optionalString(search['timeRange'])
  if (rawTimeRange && RELATIVE_TIME_PRESETS.some((p) => p.value === rawTimeRange)) {
    timeRange = rawTimeRange as RelativeTimePreset
  }

  const pageCursor = cursor(search['cursor'])


  const focusRequestId = optionalString(search['focusRequestId'])
  const replayCursor = optionalString(search['replayCursor'])
  const trail = cursorTrail(search['trail'])
  return {
    ...parsed,
    ...(timeRange ? { timeRange } : {}),
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
  const timeBounds = resolveRelativeTime(search.timeRange ?? '')
  return {
    cursor: parsedCursor,
    from: timeBounds?.from,
    to: timeBounds?.to,
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


export function updateLogsFilter(search: LogsLedgerSearch, key: LogsFilterKey, value: string | undefined): LogsLedgerSearch {
  return { ...search, [key]: value, cursor: undefined, trail: undefined }
}

export function updateLogsTimeRange(search: LogsLedgerSearch, timeRange: RelativeTimePreset | ''): LogsLedgerSearch {
  return { ...search, timeRange, cursor: undefined, trail: undefined }
}
