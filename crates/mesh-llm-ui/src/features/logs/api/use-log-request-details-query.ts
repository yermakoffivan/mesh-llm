import { useQuery } from '@tanstack/react-query'
import { LogsApiClient } from '@/features/logs/api/client'
import type { LogRequestId } from '@/features/logs/api/ids'

export const logRequestDetailsKeys = {
  all: ['logs', 'request-details'],
  summary: (requestId: LogRequestId) => [...logRequestDetailsKeys.all, 'summary', requestId.toString()],
  events: (requestId: LogRequestId) => [...logRequestDetailsKeys.all, 'events', requestId.toString()],
  artifacts: (requestId: LogRequestId) => [...logRequestDetailsKeys.all, 'artifacts', requestId.toString()],
  attempts: (requestId: LogRequestId) => [...logRequestDetailsKeys.all, 'attempts', requestId.toString()]
}

export function useLogRequestSummaryQuery(requestId: LogRequestId) {
  return useQuery({
    queryKey: logRequestDetailsKeys.summary(requestId),
    queryFn: () => new LogsApiClient().getRequest(requestId),
    staleTime: 10_000
  })
}

export function useLogRequestEventsQuery(requestId: LogRequestId, enabled: boolean) {
  return useQuery({
    queryKey: logRequestDetailsKeys.events(requestId),
    queryFn: () => new LogsApiClient().listRequestEvents(requestId),
    enabled,
    staleTime: 10_000
  })
}

export function useLogRequestArtifactsQuery(requestId: LogRequestId, enabled: boolean) {
  return useQuery({
    queryKey: logRequestDetailsKeys.artifacts(requestId),
    queryFn: () => new LogsApiClient().listRequestArtifacts(requestId),
    enabled,
    staleTime: 10_000
  })
}

export function useLogRequestAttemptsQuery(requestId: LogRequestId, enabled: boolean) {
  return useQuery({
    queryKey: logRequestDetailsKeys.attempts(requestId),
    queryFn: () => new LogsApiClient().listProxy({ requestId }),
    enabled,
    staleTime: 10_000
  })
}
