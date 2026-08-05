import { useCallback } from 'react'
import { useQueryClient } from '@tanstack/react-query'
import { useNavigate, useParams, useSearch } from '@tanstack/react-router'
import { LogRequestId } from '@/features/logs/api/ids'
import { logsKeys } from '@/features/logs/api/use-logs-ledger-query'
import { LogRequestDetails } from '@/features/logs/components/LogRequestDetails'
import { ledgerSearchFromDetails, type LogRequestDetailTab } from '@/features/logs/lib/log-request-details'

export function LogRequestDetailsPage() {
  const navigate = useNavigate()
  const queryClient = useQueryClient()
  const { requestId: requestIdParam } = useParams({ from: '/logs/$requestId' })
  const search = useSearch({ from: '/logs/$requestId' })
  const requestId = LogRequestId.parse(requestIdParam)
  const tab: LogRequestDetailTab = search.tab ?? 'summary'
  const invalidateLedger = useCallback(() => {
    void queryClient.invalidateQueries({ queryKey: logsKeys.all, refetchType: 'active' })
  }, [queryClient])
  const back = useCallback(() => {
    void navigate({ to: '/logs', search: ledgerSearchFromDetails(search) })
  }, [navigate, search])
  const changeTab = useCallback(
    (nextTab: LogRequestDetailTab) => {
      void navigate({
        to: '/logs/$requestId',
        params: { requestId: requestId.toString() },
        search: { ...search, tab: nextTab }
      })
    },
    [navigate, requestId, search]
  )

  return (
    <LogRequestDetails
      onBack={back}
      onMaintenanceMutationSucceeded={invalidateLedger}
      onTabChange={changeTab}
      requestId={requestId}
      tab={tab}
    />
  )
}
