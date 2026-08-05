import { LogReplayCursor, LogRequestId, type LogReplayChannel } from './ids'
import { LogsDtoError, parseReplayEvent, parseReplayGap, type ParsedReplayEvent, type ParsedReplayGap } from './schemas'

export type LogsSseFilterKey = 'from' | 'to' | 'route' | 'model' | 'provider' | 'engine' | 'outcome'

export type LogsSseFilter = {
  readonly key: LogsSseFilterKey
  readonly value: string
}

export type LogsSseSubscription = {
  readonly channels: readonly LogReplayChannel[]
  readonly filters?: readonly LogsSseFilter[]
  readonly requestIds?: readonly LogRequestId[]
  readonly cursor?: LogReplayCursor
}

export type LogsSseFrame =
  | { readonly type: 'log_event'; readonly cursor: LogReplayCursor; readonly event: ParsedReplayEvent }
  | { readonly type: 'replay_gap'; readonly cursor: LogReplayCursor; readonly gap: ParsedReplayGap }
  | { readonly type: 'stream_error'; readonly cursor: LogReplayCursor; readonly code: 'invalid_event' }

export type LogsSseFrameInput = {
  readonly event: string
  readonly lastEventId: string
  readonly data: string
}

function parseJson(data: string): unknown {
  try {
    return JSON.parse(data)
  } catch {
    throw new LogsDtoError()
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return value !== null && typeof value === 'object'
}

function parseStreamError(input: unknown): 'invalid_event' {
  if (isRecord(input) && input['code'] === 'invalid_event') {
    return 'invalid_event'
  }
  throw new LogsDtoError()
}

export function parseLogsSseFrame(input: LogsSseFrameInput): LogsSseFrame {
  const cursor = LogReplayCursor.parse(input.lastEventId)
  const data = parseJson(input.data)
  switch (input.event) {
    case 'log_event': {
      const event = parseReplayEvent(data)
      if (cursor.sequence(event.channel) !== BigInt(event.sequence)) throw new LogsDtoError()
      return { type: 'log_event', cursor, event }
    }
    case 'replay_gap':
      return { type: 'replay_gap', cursor, gap: parseReplayGap(data) }
    case 'stream_error':
      return { type: 'stream_error', cursor, code: parseStreamError(data) }
    default:
      throw new LogsDtoError()
  }
}

export function serializeLogsSseSubscription(subscription: LogsSseSubscription) {
  const query = new URLSearchParams()
  for (const channel of subscription.channels) query.append('channel', channel)
  for (const filter of subscription.filters ?? []) query.append('filter', `${filter.key}:${filter.value}`)
  for (const requestId of subscription.requestIds ?? []) query.append('filter', `request_id:${requestId.toString()}`)
  if (subscription.cursor) query.set('cursor', subscription.cursor.toString())
  return query.toString()
}
