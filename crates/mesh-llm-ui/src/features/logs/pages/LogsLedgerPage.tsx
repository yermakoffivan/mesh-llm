import { useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useNavigate, useSearch } from '@tanstack/react-router'
import { logsKeys } from '@/features/logs/api/use-logs-ledger-query'
import { LogsLedger } from '@/features/logs/components/LogsLedger'
import type { LogsLedgerSearch } from '@/features/logs/lib/log-search'

export function LogsLedgerPage() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const search = useSearch({ from: '/logs' })
  const invalidateLedger = useCallback(() => {
    void queryClient.invalidateQueries({ queryKey: logsKeys.all, refetchType: 'active' })
  }, [queryClient])
  const updateSearch = useCallback(
    (nextSearch: LogsLedgerSearch) => {
      void navigate({ to: '/logs', search: nextSearch })
    },
    [navigate]
  )
  const openRequest = useCallback(
    (requestId: string, nextSearch: LogsLedgerSearch) => {
      void navigate({ to: '/logs/$requestId', params: { requestId }, search: { ...nextSearch, tab: 'summary' } })
    },
    [navigate]
  )

  return (
    <LogsLedger
      onMaintenanceMutationSucceeded={invalidateLedger}
      onRequestOpen={openRequest}
      onSearchChange={updateSearch}
      search={search}
    />
  )
}
