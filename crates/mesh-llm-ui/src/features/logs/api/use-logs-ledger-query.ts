import { useQuery } from '@tanstack/react-query'
import { LogsApiClient } from '@/features/logs/api/client'
import type { LogsLedgerSearch } from '@/features/logs/lib/log-search'
import { toLogsRequestQuery } from '@/features/logs/lib/log-search'

export const logsKeys = {
  all: ['logs'],
  ledger: (search: LogsLedgerSearch) => [...logsKeys.all, 'ledger', search]
}

export function useLogsLedgerQuery(search: LogsLedgerSearch) {
  return useQuery({
    queryKey: logsKeys.ledger(search),
    queryFn: () => new LogsApiClient().listRequests(toLogsRequestQuery(search)),
    staleTime: 10_000
  })
}
