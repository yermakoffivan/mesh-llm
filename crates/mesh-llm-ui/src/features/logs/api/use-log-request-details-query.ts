import { useQuery } from '@tanstack/react-query'
import { useDataMode, type DataMode } from '@/lib/data-mode'
import { LogsApiClient } from '@/features/logs/api/client'
import type { LogRequestId } from '@/features/logs/api/ids'

export const logRequestDetailsKeys = {
  all: ['logs', 'request-details'],
  summary: (requestId: LogRequestId, mode: DataMode) => [...logRequestDetailsKeys.all, 'summary', requestId.toString(), mode],
  events: (requestId: LogRequestId, mode: DataMode) => [...logRequestDetailsKeys.all, 'events', requestId.toString(), mode],
  artifacts: (requestId: LogRequestId, mode: DataMode) => [...logRequestDetailsKeys.all, 'artifacts', requestId.toString(), mode],
  attempts: (requestId: LogRequestId, mode: DataMode) => [...logRequestDetailsKeys.all, 'attempts', requestId.toString(), mode]
}

export function useLogRequestSummaryQuery(requestId: LogRequestId) {
  const dataMode = useDataMode()
  return useQuery({
    queryKey: logRequestDetailsKeys.summary(requestId, dataMode.mode),
    queryFn: () => new LogsApiClient().getRequest(requestId, dataMode.mode as DataMode),
    staleTime: 10_000
  })
}

export function useLogRequestEventsQuery(requestId: LogRequestId, enabled: boolean) {
  const dataMode = useDataMode()
  return useQuery({
    queryKey: logRequestDetailsKeys.events(requestId, dataMode.mode),
    queryFn: () => new LogsApiClient().listRequestEvents(requestId, {}, dataMode.mode as DataMode),
    enabled,
    staleTime: 10_000
  })
}

export function useLogRequestArtifactsQuery(requestId: LogRequestId, enabled: boolean) {
  const dataMode = useDataMode()
  return useQuery({
    queryKey: logRequestDetailsKeys.artifacts(requestId, dataMode.mode),
    queryFn: () => new LogsApiClient().listRequestArtifacts(requestId, {}, dataMode.mode as DataMode),
    enabled,
    staleTime: 10_000
  })
}

export function useLogRequestAttemptsQuery(requestId: LogRequestId, enabled: boolean) {
  const dataMode = useDataMode()
  return useQuery({
    queryKey: logRequestDetailsKeys.attempts(requestId, dataMode.mode),
    queryFn: () => new LogsApiClient().listProxy({ requestId }, dataMode.mode as DataMode),
    enabled,
    staleTime: 10_000
  })
}
