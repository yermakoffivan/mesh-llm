/** Realistic harness-mode fixture data for the Logs feature. */
import type { LogLifecycleEvent, LogArtifact, LogProxyAttempt, LogRequest } from '@/features/logs/api/schemas'
import { LogRequestId, LogEventId, LogArtifactId } from '../api/ids'

const now = Date.now()

function ts(minutesAgo: number) {
  return new Date(now - minutesAgo * 60_000).toISOString()
}

export const HARNESS_LOG_FIXTURES: readonly LogRequest[] = [
/* SUCCESS — recent, mesh routed */{ requestId:LogRequestId.parse('a3f2b7c1-4d8e-4a9f-b5c6-e5d0f1a2b3c4'), outcome:'completed', createdAt:ts(2), terminalAt:ts(1) , route:'chat.completions', model:'qwen2.5-coder-7b-instruct.Q4_K_M.gguf', provider:'mesh-routed', engine:'skippy', statusCode:200, source:'durable' },
{ requestId:LogRequestId.parse('f8e9a1b2-3c4d-5e6f-a7b8-c9d0e1f2a3b4'), outcome:'completed', createdAt:ts(5), terminalAt:ts(4) , route:'chat.completions', model:'llama3.1-8b-instruct.Q4_0.gguf', provider:'mesh-routed'  , engine:'native', statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('7c6f5e4d-9a8b-7c6d-a5f4-a3b2c1d0e9f8'), outcome:'completed', createdAt:ts(12), terminalAt:ts(11) , route:'chat.completions'  , model:'qwen2.5-coder-7b-instruct.Q4_K_M.gguf', provider:'mesh-routed', engine:'skippy', statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('9e8f7a6b-1c2d-3e4f-a5b6-c7d8e9f0a1b2'), outcome:'completed'  , createdAt:ts(15), terminalAt:ts(14) , route:'completions', model:'phi3-mini.Q4_K_M.gguf', provider:'local-native', engine:'native', statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('b2c3d4e5-f6a7-48c9-b0e1-f2a3b4c5d6e7'), outcome:'completed', createdAt:ts(22), terminalAt:ts(21) , route:'chat.completions', model:'llama3.1-8b-instruct.Q4_0.gguf'  , provider:'mesh-routed', engine:'native'  , statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('c5d6e7f8-a9b0-41d2-a3f4-a5b6c7d8e9f0'), outcome:'completed', createdAt:ts(35), terminalAt:ts(34) , route:'chat.completions', model:'qwen2.5-14b-instruct.Q5_K_M.gguf', provider:'mesh-routed'  , engine:'skippy', statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('e8f9a0b1-c2d3-44f5-a6b7-c8d9e0f1a2b3'), outcome:'completed', createdAt:ts(45), terminalAt:ts(44) , route:'chat.completions'  , model:'qwen2.5-coder-7b-instruct.Q4_K_M.gguf', provider:'mesh-routed', engine:'skippy', statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('1a2b3c4d-e5f6-47b8-a9d0-e1f2a3b4c5d6'), outcome:'completed', createdAt:ts(62), terminalAt:ts(61) , route:'completions'  , model:'phi3-mini.Q4_K_M.gguf', provider:'local-native', engine:'native', statusCode:200, source:'active'},
{ requestId:LogRequestId.parse('d9e8f7a6-b5c4-43e2-b1a0-b9c8d7e6f5a4'), outcome:'completed', createdAt:ts(75), terminalAt:ts(74) , route:'chat.completions', model:'llama3.1-8b-instruct.Q4_0.gguf', provider:'mesh-routed', engine:'native', statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('a5f6e7d8-c9b0-41c2-b3e4-f5a6b7c8d9e0'), outcome:'completed', createdAt:ts(90), terminalAt:ts(89) , route:'chat.completions', model:'qwen2.5-14b-instruct.Q5_K_M.gguf', provider:'mesh-routed'  , engine:'skippy', statusCode:200, source:'durable'},
/* FAILURE — various error scenarios */{ requestId:LogRequestId.parse('f3e4d5c6-b7a8-49e0-b1c2-b3a4b5c6d7e8'), outcome:'failed', createdAt:ts(3), terminalAt:ts(2) , route:'chat.completions', model:'qwen2.5-coder-7b-instruct.Q4_K_M.gguf', provider:'mesh-routed', engine:'skippy', statusCode:502, source:'durable'},
{ requestId:LogRequestId.parse('e1f2a3b4-d5c6-47f8-a9b0-c1d2e3f4a5b6'), outcome:'failed'  , createdAt:ts(18), terminalAt:ts(17) , route:'chat.completions', model:'llama3.1-8b-instruct.Q4_0.gguf', provider:'mesh-routed', engine:'native', statusCode:504, source:'durable'},
{ requestId:LogRequestId.parse('c9d0e1f2-a3b4-45d6-a7f8-a9b0c1d2e3f4'), outcome:'failed', createdAt:ts(28), terminalAt:ts(27) , route:'chat.completions', model:'qwen2.5-14b-instruct.Q5_K_M.gguf', provider:'mesh-routed', engine:'skippy', statusCode:503, source:'durable'},
/* TIMEOUT */{ requestId:LogRequestId.parse('a9b8c7d6-e5f4-43b2-a1d0-e9f8e7d6c5b4'), outcome:'failed', createdAt:ts(40), terminalAt:ts(38) , route:'chat.completions'  , model:'qwen2.5-14b-instruct.Q5_K_M.gguf', provider:'mesh-routed', engine:'skippy', statusCode:408, source:'durable'},
{ requestId:LogRequestId.parse('f7e6d5c4-b3a2-41e0-b9c8-e7f6a5b4c3d2'), outcome:'failed', createdAt:ts(55), terminalAt:ts(54) , route:'completions', model:'phi3-mini.Q4_K_M.gguf', provider:'local-native'  , engine:'native'  , statusCode:408, source:'durable'},
/* REJECTED */{ requestId:LogRequestId.parse('b1c2d3e4-f5a6-47c8-b9e0-a1f2b3c4d5e6'), outcome:'rejected', createdAt:ts(65), terminalAt:ts(64) , route:'chat.completions'  , model:undefined, provider:undefined, engine:undefined, statusCode:400, source:'active'},
/* CANCELLED */{ requestId:LogRequestId.parse('e3f4a5b6-c7d8-49f0-a1b2-d3c4e5f6a7b8'), outcome:'cancelled', createdAt:ts(80), terminalAt:ts(79) , route:'chat.completions', model:'llama3.1-8b-instruct.Q4_0.gguf', provider:undefined, engine:undefined, statusCode:200, source:'active'},
/* ACTIVE (in-flight) */{ requestId:LogRequestId.parse('d5e6f7a8-b9c0-41e2-b3a4-a5b6c7d8e9f0'), outcome:'active', createdAt:ts(1), terminalAt:undefined , route:'chat.completions', model:'qwen2.5-coder-7b-instruct.Q4_K_M.gguf', provider:undefined, engine:'skippy', statusCode:undefined, source:'active'},
{ requestId:LogRequestId.parse('a8f9e0d1-b2c3-44b5-b6e7-f8a9b0c1d2e3'), outcome:'completed', createdAt:ts(10), terminalAt:ts(9) , route:'chat.completions', model:'qwen2.5-coder-7b-instruct.Q4_K_M.gguf', provider:'mesh-routed'  , engine:'skippy', statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('f1e2d3c4-b5a6-47e8-b9c0-a1b2c3d4e5f6'), outcome:'completed', createdAt:ts(50), terminalAt:ts(49) , route:'chat.completions', model:'llama3.1-8b-instruct.Q4_0.gguf', provider:'mesh-routed'  , engine:'native'  , statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('c7f8e9a0-b1c2-43e4-b5a6-a7b8c9d0e1f2'), outcome:'failed', createdAt:ts(110), terminalAt:ts(109) , route:'chat.completions', model:undefined, provider:undefined, engine:undefined, statusCode:429, source:'active'},
{ requestId:LogRequestId.parse('e5d6c7f8-a9b0-41e2-b3a4-a5b6c7d8e9f0'), outcome:'completed', createdAt:ts(120), terminalAt:ts(119) , route:'chat.completions'  , model:'phi3-mini.Q4_K_M.gguf', provider:'local-native', engine:'native', statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('b8c7f6e5-d4a3-42c1-b0e9-a8b7c6d5e4f3'), outcome:'completed', createdAt:ts(135), terminalAt:ts(134) , route:'completions', model:'qwen2.5-14b-instruct.Q5_K_M.gguf', provider:undefined, engine:undefined, statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('a6f7e8c9-d0b1-42e3-b4a5-b6c7d8e9f0ab'), outcome:'completed', createdAt:ts(150), terminalAt:ts(149) , route:'chat.completions', model:'llama3.1-8b-instruct.Q4_0.gguf', provider:undefined, engine:undefined, statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('f9e8c7a6-b5d4-43f2-a1b0-c9d8e7f6a5b4'), outcome:'completed', createdAt:ts(165), terminalAt:ts(164) , route:'chat.completions', model:'qwen2.5-coder-7b-instruct.Q4_K_M.gguf', provider:undefined, engine:undefined, statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('c3f8e9a0-b1d2-43f4-a5b6-c7d8e9f0a1b2'), outcome:'completed', createdAt:ts(180), terminalAt:ts(179) , route:'chat.completions', model:undefined, provider:undefined, engine:undefined, statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('a5b6c7f8-e9d0-41e2-b3a4-b5c6d7e8f9ab'), outcome:'completed', createdAt:ts(200), terminalAt:ts(199) , route:'chat.completions', model:undefined, provider:undefined, engine:undefined, statusCode:200, source:'durable'},
{ requestId:LogRequestId.parse('f3e4c5a6-b7d8-49f0-a1b2-c3d4e5f6a7b8'), outcome:'completed', createdAt:ts(220), terminalAt:ts(219) , route:'completions', model:undefined, provider:undefined, engine:undefined, statusCode:200, source:'durable'}
]

/* ─── Detail fixtures (events / artifacts / proxy attempts per request) ─── */

function lookupFixture(idStr: string) {
  return HARNESS_LOG_FIXTURES.find(f => f.requestId.toString() === idStr)
}

export function generateLifecycleEvents(requestIdStr: string): readonly LogLifecycleEvent[] {
  const fixture = lookupFixture(requestIdStr)
  if (!fixture || !fixture.terminalAt && fixture.outcome !== 'active') return []

  const events: LogLifecycleEvent[] = [
    { eventId:eventUuid(requestIdStr, 1), requestId:fixture.requestId, occurredAt:ts(minutesSince(fixture.createdAt) - 0), kind:'admitted', model:undefined, provider:undefined, engine:undefined, attemptId:undefined, statusCode:undefined, durationMs:undefined },
    { eventId:eventUuid(requestIdStr, 2), requestId:fixture.requestId, occurredAt:ts(minutesSince(fixture.createdAt) - 0), kind:'route_selected', model:fixture.model, provider: fixture.provider ?? 'local-native', engine: undefined, attemptId:undefined, statusCode:undefined, durationMs:undefined },
    { eventId:eventUuid(requestIdStr, 3), requestId:fixture.requestId, occurredAt:ts(minutesSince(fixture.createdAt) - 0), kind:'attempt_started', model: fixture.model, provider: undefined, engine: fixture.engine ?? 'native', attemptId:'att-1', statusCode:undefined, durationMs:undefined },
    { eventId:eventUuid(requestIdStr, 4), requestId:fixture.requestId, occurredAt:ts(minutesSince(fixture.terminalAt)), kind:'stream_started', model: fixture.model, provider: undefined, engine: undefined, attemptId:'att-1', statusCode:undefined, durationMs:undefined },
    { eventId:eventUuid(requestIdStr, 5), requestId:fixture.requestId, occurredAt:ts(minutesSince(fixture.terminalAt) - 0), kind:'stream_chunk', model: undefined, provider: undefined, engine: undefined, attemptId:'att-1', statusCode:undefined, durationMs:undefined },
    { eventId:eventUuid(requestIdStr, 6), requestId:fixture.requestId, occurredAt:ts(minutesSince(fixture.terminalAt) - 0), kind:'stream_completed', model: undefined, provider: undefined, engine: undefined, attemptId:'att-1', statusCode:undefined, durationMs:250 },
    { eventId:eventUuid(requestIdStr, 7), requestId:fixture.requestId, occurredAt:ts(minutesSince(fixture.terminalAt) - 0), kind:'attempt_completed', model: undefined, provider: undefined, engine: fixture.engine ?? 'native', attemptId:'att-1', statusCode: fixture.statusCode, durationMs:250 },
    { eventId:eventUuid(requestIdStr, 8), requestId:fixture.requestId, occurredAt:ts(minutesSince(fixture.terminalAt) - 0), kind:(fixture.outcome as LogLifecycleEvent['kind']), model: undefined, provider: undefined, engine:undefined, attemptId:'att-1', statusCode: fixture.statusCode ?? 200, durationMs:300 },
  ]

  if (fixture.outcome === 'failed') {
    events[5] = { eventId:eventUuid(requestIdStr, 6), requestId:fixture.requestId, occurredAt:ts(minutesSince(fixture.terminalAt) - 0), kind:'stream_error', model:undefined, provider:undefined, engine:undefined, attemptId:'att-1', statusCode: fixture.statusCode ?? 502, durationMs: undefined }
    events[6] = { eventId:eventUuid(requestIdStr, 7), requestId:fixture.requestId, occurredAt:ts(minutesSince(fixture.terminalAt) - 0), kind:'attempt_failed', model:undefined, provider:undefined, engine:(fixture.engine ?? 'native'), attemptId:'att-1', statusCode: fixture.statusCode ?? 502, durationMs: undefined }
    events[7] = { eventId:eventUuid(requestIdStr, 8), requestId:fixture.requestId, occurredAt:ts(minutesSince(fixture.terminalAt) - 0), kind:'audit_error', model:undefined, provider:undefined, engine:undefined, attemptId:'att-1', statusCode:(fixture.statusCode ?? 502), durationMs: undefined }
  }

  // For active requests (no terminal event yet) — strip the final completed/failed/rejected/cancelled events
  if (fixture.outcome === 'active') {
    return events.slice(0, 4) as readonly LogLifecycleEvent[]
  }

  return events as readonly LogLifecycleEvent[]
}

export function generateArtifacts(requestIdStr: string): readonly LogArtifact[] {
  const fixture = lookupFixture(requestIdStr)
  if (!fixture || !fixture.terminalAt && fixture.outcome !== 'active') return []

  const baseTime = minutesSince(fixture.createdAt) - 0 // createdAt offset in minAgo terms (≈ same as creation time)
  
  return [
    { artifactId:artifactUuid(requestIdStr, 1), requestId:fixture.requestId, occurredAt:ts(baseTime), kind:'request_body', mediaKind:'application/json', checksum:b64Len(256), bytes:256, version:0, redacted:true, truncated:false, contentState:'available' as const, contentBase64:dUMMY_JSON_BODY },
    { artifactId:artifactUuid(requestIdStr, 2), requestId:fixture.requestId, occurredAt:ts(minutesSince(fixture.terminalAt ?? fixture.createdAt) - 0), kind:'response_body', mediaKind:'application/json', checksum:b64Len(512), bytes:512, version:0, redacted:false, truncated:true, contentState:(fixture.outcome === 'completed' ? ('available' as const) : ('unavailable' as const)), contentBase64:(fixture.outcome === 'completed') ? dUMMY_JSON_BODY : undefined },
  ] as readonly LogArtifact[]
}

export function generateProxyAttempts(requestIdStr: string): readonly LogProxyAttempt[] {
  const fixture = lookupFixture(requestIdStr)
  if (!fixture || !fixture.terminalAt && fixture.outcome !== 'active') return []

  const host1 = (fixture.engine ?? 'native').replace('skippy', 'localhost') + ':9337'
  const target1 = `${(fixture.provider ?? 'local-native')}://${host1}`
  const attempts: LogProxyAttempt[] = [
    { attemptId:'att-1', requestId:fixture.requestId, occurredAt:ts(minutesSince(fixture.createdAt)), target:target1, provider:(fixture.provider ?? undefined), engine:(fixture.engine ?? undefined), startedAt: ts(minutesSince(fixture.createdAt) - 0), completedAt: fixture.terminalAt, statusCode: fixture.statusCode }
  ]

  // Failed requests sometimes have a retry attempt before giving up
  if (fixture.outcome === 'failed' && fixture.provider !== undefined) {
    const host2 = (fixture.engine ?? 'native').replace('skippy', 'localhost') + ':9447'
    attempts.push({
      attemptId:'att-2', requestId:fixture.requestId, occurredAt:(fixture.terminalAt ?? ts(0)), target:host2, provider:(fixture.provider), engine:(fixture.engine ?? undefined), startedAt:(ts(minutesSince(fixture.createdAt) + 1)), completedAt: fixture.terminalAt, statusCode: (fixture.statusCode === 502 ? 503 : 499)
    })
  }

  return attempts as readonly LogProxyAttempt[]
}

/* ─── Helpers ─── */

function minutesSince(isoDate: string | undefined): number {
  if (!isoDate) return 0
  const diffMs = now - new Date(isoDate).getTime()
  return Math.round(diffMs / 60_000)
}

const b64Len = (len: number): string => `sha256:${'a'.repeat(len.toString(16).length)}${Math.random().toString(36).slice(2,8)}`

// Dummy base-64 JSON body for harness artifacts  
const dUMMY_JSON_BODY = btoa(JSON.stringify({ role:'assistant', content:'This is sample output from the model.', finish_reason:'stop' }))

/** Build a valid UUID v1-like ID derived from requestId + index.
 *  Pattern: [0-9a-f]{8}-[0-9a-f]{4}-1[index]-[8/9/a/b][hex3]-[hex12] */
function deriveId(requestIdStr: string, index: number): string {
  // Strip dashes from requestId to get raw hex digits as a source of entropy.
  const hex = requestIdStr.replace(/-/g, '')
  return `${hex.slice(0,8)}-${hex.slice(8,12)}-1${String(index).padStart(3, 'a').slice(-3)}-9${hex.slice(16,19)}-${hex.slice(20)}`.toLowerCase()
}

function eventUuid(requestIdStr: string, index: number): LogEventId {
  return LogEventId.parse(deriveId(requestIdStr, 50 + index)) // offset to avoid collision with other ID ranges
}

function artifactUuid(requestIdStr: string, kindIndex: number): LogArtifactId {
  return LogArtifactId.parse(deriveId(requestIdStr, 100 + kindIndex))
}