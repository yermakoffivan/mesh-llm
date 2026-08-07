import { useEffect, useMemo, useRef, useState } from 'react'
import { LogsApiClient } from '@/features/logs/api/client'
import { LogAuditCursor, LogReplayCursor, type LogReplayChannel } from '@/features/logs/api/ids'
import { parseLogsSseFrame, type LogsSseFilter } from '@/features/logs/api/sse'
import { resolveRelativeTime, type RelativeTimePreset } from '@/features/logs/lib/log-search'
import type { LogsLedgerSearch } from '@/features/logs/lib/log-search'

const POLL_INTERVAL_MS = 5_000
const FALLBACK_DELAY_MS = 1_000
const DEFAULT_CHANNELS: readonly LogReplayChannel[] = ['requests', 'operations']

export type LogsLiveConnectionState = 'connected' | 'reconnecting' | 'polling' | 'gap' | 'stale'

type LogsEventSource = {
  close: () => void
  onopen: ((event: Event) => void) | null
  onerror: ((event: Event) => void) | null
  addEventListener: (type: string, listener: (event: MessageEvent<string>) => void) => void
}

export type LogsEventSourceFactory = (url: string) => LogsEventSource

export type LogsLiveRecoveryOptions = {
  readonly enabled: boolean
  readonly search: LogsLedgerSearch
  readonly hydrate: () => Promise<unknown>
  readonly channels?: readonly LogReplayChannel[]
  readonly eventSourceFactory?: LogsEventSourceFactory
}

export type LogsLiveRecovery = {
  readonly state: LogsLiveConnectionState
  readonly liveRequestIds: readonly string[]
}

type LiveRequest = {
  readonly requestId: string
  readonly occurredAt: string
}

type LiveRequestState = {
  readonly subscriptionKey: string
  readonly entries: readonly LiveRequest[]
}

function eventSourceFactory(url: string): LogsEventSource {
  return new EventSource(url)
}

function parseReplayCursor(value: string | undefined) {
  if (!value) return undefined
  try {
    return LogReplayCursor.parse(value)
  } catch {
    return undefined
  }
}

/** Resolve timeRange to raw from/to bounds for SSE filter scope. */
function resolvedTimeBounds(timeRange: RelativeTimePreset | '' | undefined): { from?: string; to?: string } {
  if (!timeRange) return {}
  const result = resolveRelativeTime(timeRange as RelativeTimePreset)
  return result ?? {}
}

function activeFilterScope(
  timeBounds: { from?: string; to?: string },
  model: string | undefined,
  provider: string | undefined,
  engine: string | undefined,
  route: string | undefined,
  source: string | undefined,
  outcome: string | undefined
): string[] {
  const entries: Array<[string, string | undefined]> = [
    ['from', timeBounds.from],
    ['to', timeBounds.to],
    ['model', model],
    ['provider', provider],
    ['engine', engine],
    ['route', route],
    ['source', source],
    ['outcome', outcome]
  ]
  return entries.flatMap(([key, value]) => (value ? [`${key}:${value}`] : []))
}

function streamFilters(
  timeBounds: { from?: string; to?: string },
  model: string | undefined,
  provider: string | undefined,
  engine: string | undefined,
  route: string | undefined,
  outcome: string | undefined
): LogsSseFilter[] {
  const entries: Array<[LogsSseFilter['key'], string | undefined]> = [
    ['from', timeBounds.from],
    ['to', timeBounds.to],
    ['model', model],
    ['provider', provider],
    ['engine', engine],
    ['route', route],
    ['outcome', outcome]
  ]
  return entries.flatMap(([key, value]) => (value ? [{ key, value }] : []))
}

function subscriptionKey(
  channels: readonly LogReplayChannel[],
  filterScope: readonly string[],
  replayCursor: string | undefined
) {
  return `${channels.join(',')}|${filterScope.join('|')}|${replayCursor ?? ''}`
}

function mergeLiveRequests(current: readonly LiveRequest[], next: LiveRequest): LiveRequest[] {
  if (current.some((entry) => entry.requestId === next.requestId)) return [...current]
  return [...current, next].sort((left, right) => left.occurredAt.localeCompare(right.occurredAt)).slice(-32)
}

export function useLogsLiveRecovery({
  enabled,
  search,
  hydrate,
  channels = DEFAULT_CHANNELS,
  eventSourceFactory: createEventSource = eventSourceFactory
}: LogsLiveRecoveryOptions): LogsLiveRecovery {
  const [state, setState] = useState<LogsLiveConnectionState>('reconnecting')
  const [liveRequests, setLiveRequests] = useState<LiveRequestState>({ subscriptionKey: '', entries: [] })
  const sequenceByChannelRef = useRef(new Map<LogReplayChannel, number>())
  const eventIdsRef = useRef(new Set<string>())
  const requestIdsRef = useRef(new Set<string>())
  const hydrateInFlightRef = useRef(false)
  const hydratePendingRef = useRef(false)
  const latestCursorRef = useRef<LogReplayCursor | LogAuditCursor | undefined>(undefined)
  const restoredCursorValueRef = useRef<string | undefined>(undefined)

  /* Resolve timeRange → from/to bounds once; used for both filter scope and SSE subscription. */
  const timeBounds = useMemo(() => resolvedTimeBounds(search.timeRange), [search.timeRange])

  const filterScope = useMemo(
    () => activeFilterScope(timeBounds, search.model, search.provider, search.engine, search.route, search.source, search.outcome),
    [timeBounds.from ?? '', timeBounds.to ?? '', search.model ?? '', search.provider ?? '', search.engine ?? '', search.route ?? '', search.source ?? '']
  )

  const key = subscriptionKey(channels, filterScope, search.replayCursor)
  const subscriptionFilters = useMemo(
    () => streamFilters(timeBounds, search.model, search.provider, search.engine, search.route, search.outcome),
    [timeBounds.from ?? '', timeBounds.to ?? '', search.model ?? '', search.provider ?? '', search.engine ?? '', search.route ?? '']
  )

  useEffect(() => {
    if (!enabled) return

    let disposed = false
    let source: LogsEventSource | undefined
    let pollingTimer: number | undefined
    let fallbackTimer: number | undefined

    sequenceByChannelRef.current = new Map()
    eventIdsRef.current = new Set()
    requestIdsRef.current = new Set()
    if (restoredCursorValueRef.current !== search.replayCursor) {
      restoredCursorValueRef.current = search.replayCursor
      latestCursorRef.current = parseReplayCursor(search.replayCursor)
    }

    const clearPolling = () => {
      if (pollingTimer === undefined) return
      window.clearInterval(pollingTimer)
      pollingTimer = undefined
    }

    const clearFallback = () => {
      if (fallbackTimer === undefined) return
      window.clearTimeout(fallbackTimer)
      fallbackTimer = undefined
    }

    const closeSource = () => {
      if (!source) return
      source.onopen = null
      source.onerror = null
      source.close()
      source = undefined
    }

    const hydrateAuthoritatively = (clearGap: boolean) => {
      if (disposed) return
      if (hydrateInFlightRef.current) {
        hydratePendingRef.current = true
        return
      }
      hydrateInFlightRef.current = true
      void Promise.resolve(hydrate())
        .then(() => {
          if (!disposed && clearGap) setState(source ? 'connected' : 'polling')
        })
        .catch(() => {
          if (!disposed) setState('stale')
        })
        .finally(() => {
          hydrateInFlightRef.current = false
          if (!disposed && hydratePendingRef.current) {
            hydratePendingRef.current = false
            hydrateAuthoritatively(false)
          }
        })
    }

    const startPolling = () => {
      if (pollingTimer !== undefined) return
      setState('polling')
      pollingTimer = window.setInterval(() => hydrateAuthoritatively(false), POLL_INTERVAL_MS)
    }

    const queuePollingFallback = () => {
      if (fallbackTimer !== undefined) return
      setState('reconnecting')
      fallbackTimer = window.setTimeout(() => {
        fallbackTimer = undefined
        startPolling()
      }, FALLBACK_DELAY_MS)
    }

    const acceptEvent = (event: MessageEvent<string>) => {
      if (disposed) return
      try {
        const frame = parseLogsSseFrame({ event: event.type, lastEventId: event.lastEventId, data: event.data })
        latestCursorRef.current = frame.cursor
        if (frame.type === 'replay_gap') {
          setState('gap')
          hydrateAuthoritatively(true)
          return
        }
        if (frame.type !== 'log_event') {
          queuePollingFallback()
          return
        }

        const channelSequence = sequenceByChannelRef.current.get(frame.event.channel)
        if (channelSequence !== undefined && frame.event.sequence <= channelSequence) return
        sequenceByChannelRef.current.set(frame.event.channel, frame.event.sequence)

        const eventId = frame.event.eventId.toString()
        if (eventIdsRef.current.has(eventId)) return
        eventIdsRef.current.add(eventId)

        const requestId = frame.event.requestId.toString()
        if (!requestIdsRef.current.has(requestId)) {
          requestIdsRef.current.add(requestId)
          setLiveRequests((current) => ({
            subscriptionKey: key,
            entries: mergeLiveRequests(current.subscriptionKey === key ? current.entries : [], {
              requestId,
              occurredAt: frame.event.occurredAt
            })
          }))
        }
        hydrateAuthoritatively(false)
      } catch {
        queuePollingFallback()
      }
    }

    const url = new LogsApiClient().logsEventSourceUrl({
      channels,
      filters: subscriptionFilters,
      cursor: latestCursorRef.current instanceof LogReplayCursor ? latestCursorRef.current : undefined
    })
    hydrateAuthoritatively(false)

    try {
      const connectedSource = createEventSource(url)
      source = connectedSource
      connectedSource.onopen = () => {
        if (disposed) return
        clearFallback()
        clearPolling()
        setState('connected')
      }
      connectedSource.onerror = () => {
        if (!disposed) queuePollingFallback()
      }
      connectedSource.addEventListener('log_event', acceptEvent)
      connectedSource.addEventListener('replay_gap', acceptEvent)
      connectedSource.addEventListener('stream_error', acceptEvent)
    } catch {
      startPolling()
    }

    return () => {
      disposed = true
      clearFallback()
      clearPolling()
      closeSource()
    }
  }, [channels, createEventSource, enabled, filterScope, hydrate, key, search.replayCursor, subscriptionFilters])

  return {
    state,
    liveRequestIds: (enabled && liveRequests.subscriptionKey === key ? liveRequests.entries : []).map(
      (entry) => entry.requestId
    )
  }
}
