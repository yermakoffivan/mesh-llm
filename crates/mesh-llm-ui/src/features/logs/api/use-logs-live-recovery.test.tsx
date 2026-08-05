import { act, renderHook } from '@testing-library/react'
import { afterEach, describe, expect, it, vi } from 'vitest'
import { useLogsLiveRecovery, type LogsEventSourceFactory } from '@/features/logs/api/use-logs-live-recovery'
import type { LogsLedgerSearch } from '@/features/logs/lib/log-search'

const REQUEST_A = '00000000-0000-4000-8000-000000000001'
const REQUEST_B = '00000000-0000-4000-8000-000000000002'
const unsupportedEventSourceFactory: LogsEventSourceFactory = () => {
  throw new Error('unsupported')
}

type Listener = (event: MessageEvent<string>) => void

class FakeEventSource {
  readonly listeners = new Map<string, Listener>()
  readonly url: string
  closed = false
  onopen: ((event: Event) => void) | null = null
  onerror: ((event: Event) => void) | null = null

  constructor(url: string) {
    this.url = url
  }

  addEventListener(type: string, listener: Listener) {
    this.listeners.set(type, listener)
  }

  close() {
    this.closed = true
  }

  open() {
    this.onopen?.(new Event('open'))
  }

  error() {
    this.onerror?.(new Event('error'))
  }

  emit(type: string, data: string, lastEventId: string) {
    const event = new MessageEvent<string>(type, { data })
    Object.defineProperty(event, 'lastEventId', { value: lastEventId })
    this.listeners.get(type)?.(event)
  }
}

function eventData(requestId: string, eventId: string, sequence: number, occurredAt = '2026-08-04T12:00:00Z') {
  return JSON.stringify({
    eventId,
    requestId,
    occurredAt,
    channel: 'requests',
    sequence,
    kind: 'completed',
    model: null,
    provider: null,
    engine: null,
    attemptId: null,
    statusCode: null,
    durationMs: null,
    tokens: null
  })
}

function renderLive(options: Partial<Parameters<typeof useLogsLiveRecovery>[0]> = {}) {
  const sources: FakeEventSource[] = []
  const factory: LogsEventSourceFactory = (url) => {
    const source = new FakeEventSource(url)
    sources.push(source)
    return source
  }
  const hydrate = vi.fn(async () => undefined)
  const search: LogsLedgerSearch = options.search ?? { model: 'Qwen3', provider: 'reserve-a' }
  const result = renderHook(
    (input: { search: LogsLedgerSearch }) =>
      useLogsLiveRecovery({
        enabled: options.enabled ?? true,
        search: input.search,
        hydrate,
        eventSourceFactory: options.eventSourceFactory ?? factory,
        channels: options.channels
      }),
    { initialProps: { search } }
  )
  return { ...result, hydrate, sources }
}

async function flush() {
  await act(async () => {
    await Promise.resolve()
  })
}

afterEach(() => {
  vi.useRealTimers()
})

describe('useLogsLiveRecovery', () => {
  it('does not hydrate, open a stream, or schedule timers while logs are unsupported', async () => {
    vi.useFakeTimers()
    const { hydrate, sources } = renderLive({ enabled: false })

    await flush()
    expect(hydrate).not.toHaveBeenCalled()
    expect(sources).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)

    act(() => vi.advanceTimersByTime(60_000))
    await flush()
    expect(hydrate).not.toHaveBeenCalled()
    expect(sources).toHaveLength(0)
    expect(vi.getTimerCount()).toBe(0)
  })

  it('serializes active stream-supported filters into the dedicated logs stream', async () => {
    const search: LogsLedgerSearch = {
      from: '2026-08-01T00:00:00Z',
      to: '2026-08-04T00:00:00Z',
      model: 'Qwen3',
      provider: 'reserve-a',
      engine: 'skippy',
      route: 'chat',
      outcome: 'completed'
    }
    const { hydrate, rerender, sources, result, unmount } = renderLive({
      channels: ['requests', 'operations'],
      search
    })

    await flush()
    expect(hydrate).toHaveBeenCalledTimes(1)
    expect(sources[0]?.url).toBe(
      '/api/logs/events?channel=requests&channel=operations&filter=from%3A2026-08-01T00%3A00%3A00Z&filter=to%3A2026-08-04T00%3A00%3A00Z&filter=model%3AQwen3&filter=provider%3Areserve-a&filter=engine%3Askippy&filter=route%3Achat&filter=outcome%3Acompleted'
    )
    act(() => sources[0]?.open())
    expect(result.current.state).toBe('connected')
    rerender({ search })
    expect(sources).toHaveLength(1)
    expect(sources[0]?.closed).toBe(false)
    unmount()
    expect(sources[0]?.closed).toBe(true)
  })

  it('serializes route and reconnects while source remains unsupported', async () => {
    const { rerender, sources } = renderLive({ search: { route: 'reserve', source: 'active' } })
    await flush()

    expect(sources[0]?.url).toBe('/api/logs/events?channel=requests&channel=operations&filter=route%3Areserve')
    rerender({ search: { route: 'mesh', source: 'durable' } })

    expect(sources[0]?.closed).toBe(true)
    expect(sources[1]?.url).toBe('/api/logs/events?channel=requests&channel=operations&filter=route%3Amesh')
  })

  it('merges new request IDs in order while suppressing repeated sequence and event frames', async () => {
    const { hydrate, sources, result } = renderLive()
    await flush()
    const source = sources[0]
    act(() => source?.open())
    act(() =>
      source?.emit(
        'log_event',
        eventData(REQUEST_A, '00000000-0000-4000-8000-000000000003', 1, '2026-08-04T12:00:01Z'),
        'v1:1.0.0'
      )
    )
    await flush()
    act(() =>
      source?.emit(
        'log_event',
        eventData(REQUEST_B, '00000000-0000-4000-8000-000000000004', 2, '2026-08-04T12:00:02Z'),
        'v1:2.0.0'
      )
    )
    act(() =>
      source?.emit(
        'log_event',
        eventData(REQUEST_B, '00000000-0000-4000-8000-000000000004', 2, '2026-08-04T12:00:02Z'),
        'v1:2.0.0'
      )
    )

    expect(result.current.liveRequestIds).toEqual([REQUEST_A, REQUEST_B])
    expect(hydrate).toHaveBeenCalledTimes(3)
  })

  it('hydrates later lifecycle events for the same request while keeping the live request projection deduped', async () => {
    const { hydrate, sources, result } = renderLive()
    await flush()
    const source = sources[0]
    act(() => source?.emit('log_event', eventData(REQUEST_A, '00000000-0000-4000-8000-000000000003', 1), 'v1:1.0.0'))
    await flush()
    act(() => source?.emit('log_event', eventData(REQUEST_A, '00000000-0000-4000-8000-000000000004', 2), 'v1:2.0.0'))
    await flush()

    expect(result.current.liveRequestIds).toEqual([REQUEST_A])
    expect(hydrate).toHaveBeenCalledTimes(3)
  })

  it('keeps the native EventSource instance for reconnect and falls back to bounded polling only while disconnected', async () => {
    vi.useFakeTimers()
    const { hydrate, sources, result } = renderLive()
    await flush()
    const source = sources[0]
    act(() => source?.error())
    expect(result.current.state).toBe('reconnecting')
    act(() => vi.advanceTimersByTime(1_000))
    expect(result.current.state).toBe('polling')
    act(() => vi.advanceTimersByTime(15_000))
    expect(sources).toHaveLength(1)
    act(() => source?.open())
    expect(result.current.state).toBe('connected')
    expect(hydrate.mock.calls.length).toBeLessThanOrEqual(2)
  })

  it('uses polling when the dedicated stream cannot be constructed and never overlaps hydration', async () => {
    vi.useFakeTimers()
    let resolveHydration: (() => void) | undefined
    const hydrate = vi.fn(
      () =>
        new Promise<void>((resolve) => {
          resolveHydration = resolve
        })
    )
    const result = renderHook(() =>
      useLogsLiveRecovery({
        enabled: true,
        search: {},
        hydrate,
        eventSourceFactory: unsupportedEventSourceFactory
      })
    )

    expect(result.result.current.state).toBe('polling')
    act(() => vi.advanceTimersByTime(15_000))
    expect(hydrate).toHaveBeenCalledTimes(1)
    act(() => resolveHydration?.())
    await flush()
    expect(hydrate).toHaveBeenCalledTimes(2)
  })

  it('refetches authoritatively after a replay gap', async () => {
    const { hydrate, sources, result } = renderLive()
    await flush()
    act(() => sources[0]?.open())
    act(() =>
      sources[0]?.emit(
        'replay_gap',
        JSON.stringify({
          channel: 'requests',
          fromSequence: 3,
          toSequence: 4,
          recovery: { endpoint: '/api/logs/requests', cursor: null }
        }),
        'v1:4.0.0'
      )
    )
    expect(result.current.state).toBe('gap')
    await flush()
    expect(hydrate).toHaveBeenCalledTimes(2)
    expect(result.current.state).toBe('connected')
  })

  it('closes and reopens on filter change with the last received cursor and fresh dedupe state', async () => {
    const { hydrate, rerender, result, sources } = renderLive({ search: { provider: 'reserve-a' } })
    await flush()
    act(() =>
      sources[0]?.emit('log_event', eventData(REQUEST_A, '00000000-0000-4000-8000-000000000003', 1), 'v1:1.0.0')
    )
    await flush()
    rerender({ search: { provider: 'reserve-b' } })

    expect(sources[0]?.closed).toBe(true)
    expect(sources[1]?.url).toBe(
      '/api/logs/events?channel=requests&channel=operations&filter=provider%3Areserve-b&cursor=v1%3A1.0.0'
    )
    act(() =>
      sources[1]?.emit('log_event', eventData(REQUEST_A, '00000000-0000-4000-8000-000000000003', 1), 'v1:1.0.0')
    )
    await flush()
    expect(result.current.liveRequestIds).toEqual([REQUEST_A])
    expect(hydrate).toHaveBeenCalledTimes(4)
  })
})
