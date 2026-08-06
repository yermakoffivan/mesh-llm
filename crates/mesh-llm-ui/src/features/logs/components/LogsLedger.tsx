import { useCallback, useEffect, useMemo } from 'react'
import { RefreshCw } from 'lucide-react'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { FilterPopover, type FilterCategory, type FilterValueOption } from '@/components/ui/FilterPopover'
import { NativeSelect } from '@/components/ui/NativeSelect'
import { ScrollArea } from '@/components/ui/scroll-area'
import { StatusBadge, type StatusBadgeTone } from '@/components/ui/StatusBadge'
import { DataTable } from '@/components/ui/data-table'
import { DataTableViewOptions } from '@/components/ui/data-table-view-options'
import { useLogsLedgerQuery } from '@/features/logs/api/use-logs-ledger-query'
import { useLogsLiveRecovery, type LogsLiveConnectionState } from '@/features/logs/api/use-logs-live-recovery'
import { LogOperations } from '@/features/logs/components/LogOperations'
import { buildLogsLedgerColumns } from '@/features/logs/components/LogsLedgerColumns'
import type { LogRequest } from '@/features/logs/api/schemas'
import {
  RELATIVE_TIME_PRESETS,
  resetLogsSearch,
  toLogsRequestQuery,
  updateLogsFilter,
  updateLogsTimeRange,
  type LogsLedgerSearch
} from '@/features/logs/lib/log-search'

type LedgerFilterKey = 'model' | 'provider' | 'engine' | 'route' | 'source' | 'outcome'

type LogsLedgerProps = {
  readonly search: LogsLedgerSearch
  readonly onSearchChange: (search: LogsLedgerSearch) => void
  readonly onRequestOpen: (requestId: string, search: LogsLedgerSearch) => void
  readonly onMaintenanceMutationSucceeded?: () => void
}

const ledgerFilterCategories: Array<FilterCategory<LedgerFilterKey>> = [
  { key: 'model', label: 'Model' },
  { key: 'provider', label: 'Provider' },
  { key: 'engine', label: 'Engine' },
  { key: 'route', label: 'Route' },
  { key: 'source', label: 'Source' },
  { key: 'outcome', label: 'Outcome' }
]

function mergeLedgerRows(rows: readonly LogRequest[]) {
  const locations = new Map<string, number>()
  const merged: LogRequest[] = []
  for (const row of rows) {
    const key = row.requestId.toString()
    const priorLocation = locations.get(key)
    if (priorLocation === undefined) {
      locations.set(key, merged.length)
      merged.push(row)
      continue
    }
    const prior = merged[priorLocation]
    if (prior?.source !== 'active' && row.source === 'active') merged[priorLocation] = row
  }
  return merged
}

function filterOptions(rows: readonly LogRequest[], key: LedgerFilterKey): FilterValueOption[] {
  const counts = new Map<string, number>()
  for (const row of rows) {
    const value = row[key]
    if (!value) continue
    counts.set(value, (counts.get(value) ?? 0) + 1)
  }
  return [...counts.entries()]
    .sort(([left], [right]) => left.localeCompare(right))
    .map(([value, count]) => ({ value, count }))
}

function filterSelections(search: LogsLedgerSearch): Record<LedgerFilterKey, Set<string>> {
  return {
    model: new Set(search.model ? [search.model] : []),
    provider: new Set(search.provider ? [search.provider] : []),
    engine: new Set(search.engine ? [search.engine] : []),
    route: new Set(search.route ? [search.route] : []),
    source: new Set(search.source ? [search.source] : []),
    outcome: new Set(search.outcome ? [search.outcome] : [])
  }
}

function activeFilterGroupCount(search: LogsLedgerSearch) {
  return [
    search.timeRange,
    search.model,
    search.provider,
    search.engine,
    search.route,
    search.source,
    search.outcome
  ].filter(Boolean).length
}

function liveStateLabel(state: LogsLiveConnectionState) {
  switch (state) {
    case 'connected':
      return 'Live'
    case 'reconnecting':
      return 'Reconnecting'
    case 'polling':
      return 'Polling'
    case 'gap':
      return 'Recovering gap'
    case 'stale':
      return 'Live data stale'
  }
}

function liveStateTone(state: LogsLiveConnectionState): StatusBadgeTone {
  switch (state) {
    case 'connected':
      return 'good'
    case 'reconnecting':
    case 'polling':
    case 'gap':
      return 'accent'
    case 'stale':
      return 'warn'
  }
}

function getLogRequestRowId(row: LogRequest) {
  return row.requestId.toString()
}

export function LogsLedger({ search, onSearchChange, onRequestOpen, onMaintenanceMutationSucceeded }: LogsLedgerProps) {
  const query = useLogsLedgerQuery(search)
  const { refetch } = query
  const result = query.data
  const hydrate = useCallback(async () => refetch(), [refetch])
  const live = useLogsLiveRecovery({ enabled: result?.state === 'supported', search, hydrate })
  const rows = useMemo(() => (result?.state === 'supported' ? mergeLedgerRows(result.value.items) : []), [result])
  const columns = useMemo(() => buildLogsLedgerColumns({ onRequestOpen, search }), [onRequestOpen, search])
  const optionsByCategory = useMemo<Record<LedgerFilterKey, FilterValueOption[]>>(
    () => ({
      model: filterOptions(rows, 'model'),
      provider: filterOptions(rows, 'provider'),
      engine: filterOptions(rows, 'engine'),
      route: filterOptions(rows, 'route'),
      source: filterOptions(rows, 'source'),
      outcome: filterOptions(rows, 'outcome')
    }),
    [rows]
  )
  const selectedValuesByCategory = useMemo(() => filterSelections(search), [search])
  const activeFilterGroups = activeFilterGroupCount(search)

  useEffect(() => {
    if (!search.focusRequestId) return
    document.getElementById(`log-request-${search.focusRequestId}`)?.focus()
  }, [rows, search.focusRequestId])

  return (
    <section
      className="mx-auto flex w-full max-w-[1440px] flex-col gap-[var(--shell-normal)]"
      aria-labelledby="logs-ledger-title"
    >
      <header className="flex flex-wrap items-end justify-between gap-3 border-b border-border-soft pb-[var(--panel-y)]">
        <div>
          <div className="type-label text-fg-faint">Operations ledger</div>
          <h1 className="type-display mt-1 text-foreground" id="logs-ledger-title">
            Request logs
          </h1>
          <p className="type-body mt-1 max-w-[72ch] text-fg-dim">
            Local request history, including active work and durable outcomes. This ledger is an operational index;
            payload details remain separate.
          </p>
        </div>
        <div className="flex items-center gap-2" aria-live="polite">
          {result?.state === 'supported' ? (
            <StatusBadge dot size="caption" tone={query.isFetching ? 'accent' : liveStateTone(live.state)}>
              {query.isFetching ? 'Updating' : liveStateLabel(live.state)}
            </StatusBadge>
          ) : null}
          {result?.state === 'supported' ? (
            <StatusBadge tone="muted" size="caption">
              Local only
            </StatusBadge>
          ) : null}
        </div>
      </header>

      <div className="panel-shell flex flex-wrap items-end gap-2 rounded-[var(--radius)] border border-border bg-panel px-[var(--panel-x)] py-[var(--panel-y)]">
        <div className="grid min-w-[13rem] flex-1 gap-1 text-[length:var(--density-type-caption)] text-fg-dim">
          <span className="type-label text-fg-faint">Time range</span>
          <NativeSelect
            ariaLabel="Filter logs by time range"
            className="min-w-[13rem] flex-1"
            name="logs-time-range"
            onValueChange={(value) => {
              const preset = RELATIVE_TIME_PRESETS.find((option) => option.value === value)
              if (preset) onSearchChange(updateLogsTimeRange(search, preset.value))
            }}
            options={RELATIVE_TIME_PRESETS}
            value={search.timeRange ?? ''}
          />
        </div>
        <FilterPopover
          activeFilterGroups={activeFilterGroups}
          categories={ledgerFilterCategories}
          contentLabel="Request log filters"
          formatOptionLabel={(value) => value}
          id="logs-ledger-filters"
          itemLabel="requests"
          onClear={() => onSearchChange(resetLogsSearch(search))}
          onSelectAll={(key) => onSearchChange(updateLogsFilter(search, key, undefined))}
          onSelectNone={(key) => onSearchChange(updateLogsFilter(search, key, undefined))}
          onValueChange={(key, value, checked) =>
            onSearchChange(updateLogsFilter(search, key, checked ? value : undefined))
          }
          optionsByCategory={optionsByCategory}
          selectedValuesByCategory={selectedValuesByCategory}
          title="Request filters"
          totalCount={rows.length}
          triggerLabel="Filter request logs"
          visibleCount={rows.length}
        />
        <Button
          className="ui-control h-8 rounded-[var(--radius)] px-2.5 text-[length:var(--density-type-caption)]"
          disabled={activeFilterGroups === 0 && !search.cursor}
          onClick={() => onSearchChange(resetLogsSearch(search))}
          size="sm"
          variant="outline"
        >
          Reset view
        </Button>
        <LogOperations
          onMaintenanceMutationSucceeded={onMaintenanceMutationSucceeded}
          query={toLogsRequestQuery(search)}
        />
      </div>

      {query.isLoading ? (
        <div
          className="panel-shell min-h-[14rem] rounded-[var(--radius)] border border-border bg-panel p-[var(--panel-x)]"
          aria-label="Loading logs"
        >
          <div className="type-label text-fg-faint">Loading request ledger</div>
          <p className="type-body mt-2 text-fg-dim">Retrieving the local request index.</p>
        </div>
      ) : null}

      {query.isError ? (
        <Alert
          className="panel-shell rounded-[var(--radius)] border border-[color:color-mix(in_oklab,var(--color-bad)_35%,var(--color-border))] bg-panel p-[var(--panel-x)]"
          variant="destructive"
        >
          <div className="flex flex-wrap items-center justify-between gap-3">
            <div>
              <AlertTitle className="type-panel-title text-foreground">Log history could not be loaded</AlertTitle>
              <AlertDescription className="type-caption mt-1 text-fg-dim">
                The local logging service did not return a usable response.
              </AlertDescription>
            </div>
            <Button className="ui-control gap-1.5" onClick={() => void query.refetch()} size="sm" variant="outline">
              <RefreshCw className="size-3.5" aria-hidden="true" />
              Retry
            </Button>
          </div>
        </Alert>
      ) : null}

      {result?.state === 'unsupported' ? (
        <div
          className="panel-shell rounded-[var(--radius)] border border-border bg-panel p-[var(--panel-x)]"
          role="status"
        >
          <div className="type-panel-title text-foreground">Log history is unavailable on this host</div>
          <p className="type-body mt-1 max-w-[68ch] text-fg-dim">
            This MeshLLM host does not expose the local logs API. Upgrade the host to inspect request history here.
          </p>
        </div>
      ) : null}

      {result?.state === 'supported' && rows.length === 0 ? (
        <div
          className="panel-shell rounded-[var(--radius)] border border-border bg-panel p-[var(--panel-x)]"
          role="status"
        >
          <div className="type-panel-title text-foreground">No log requests match this view</div>
          <p className="type-body mt-1 text-fg-dim">Clear filters or wait for a local request to be recorded.</p>
        </div>
      ) : null}

      {result?.state === 'supported' && rows.length > 0 ? (
        <div className="panel-shell overflow-hidden rounded-[var(--radius)] border border-border bg-panel">
          <ScrollArea className="max-h-[71rem]">
            <DataTable
              ariaLabel="Request logs"
              columns={columns}
              data={rows}
              defaultPageSize={20}
              emptyMessage="No log requests match this view."
              enablePagination
              filterColumnId="requestId"
              filterPlaceholder="Filter by request ID"
              getRowId={getLogRequestRowId}
              tableClassName="min-w-[780px] text-[length:var(--density-type-caption)]"
            >
              {(table) => (
                <div className="flex items-center justify-end px-[var(--panel-x)] py-2">
                  <DataTableViewOptions table={table} />
                </div>
              )}
            </DataTable>
          </ScrollArea>
        </div>
      ) : null}
    </section>
  )
}
