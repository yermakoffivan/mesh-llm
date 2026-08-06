import { useQuery } from '@tanstack/react-query'
import { useDataMode, type DataMode } from '@/lib/data-mode'
import { LogsApiClient } from '@/features/logs/api/client'
import type { LogsLedgerSearch } from '@/features/logs/lib/log-search'
import { toLogsRequestQuery } from '@/features/logs/lib/log-search'

export const logsKeys = {
  all: ['logs'],
  ledger: (search: LogsLedgerSearch, mode: DataMode) => [...logsKeys.all, 'ledger', search, mode]
}

export function useLogsLedgerQuery(search: LogsLedgerSearch) {
  const dataMode = useDataMode()
  return useQuery({
    queryKey: logsKeys.ledger(search, dataMode.mode),
    queryFn: () => new LogsApiClient().listRequests(toLogsRequestQuery(search), dataMode.mode as DataMode),
    staleTime: 10_000,
  })
}
