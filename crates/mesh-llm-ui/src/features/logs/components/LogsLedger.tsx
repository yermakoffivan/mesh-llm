import { useCallback, useEffect, useMemo, useState } from 'react'
import { Activity, Calendar, CircleCheckBig, CircleX, Database, RotateCcw, Search as SearchIcon, X } from 'lucide-react'
import type { LucideIcon } from 'lucide-react'
import { Alert, AlertDescription, AlertTitle } from '@/components/ui/alert'
import { Button } from '@/components/ui/button'
import { Card } from '@/components/ui/card'
import { FilterPopover, type FilterCategory, type FilterValueOption } from '@/components/ui/FilterPopover'
import { Input } from '@/components/ui/input'
import { NativeSelect } from '@/components/ui/NativeSelect'
import { ScrollArea } from '@/components/ui/scroll-area'
import { Sparkline } from '@/components/ui/Sparkline'
import { StatusBadge, type StatusBadgeTone } from '@/components/ui/StatusBadge'
import { DataTable, TanStackTable as DataTableTanStackTableType } from '@/components/ui/data-table'
import { DataTablePagination } from '@/components/ui/data-table-pagination'
import { DataTableViewOptions } from '@/components/ui/data-table-view-options'
import { useLogsLedgerQuery } from '@/features/logs/api/use-logs-ledger-query'
import { useLogsLiveRecovery, type LogsLiveConnectionState } from '@/features/logs/api/use-logs-live-recovery'
import { LogOperations } from '@/features/logs/components/LogOperations'
import { buildLogsLedgerColumns } from '@/features/logs/components/LogsLedgerColumns'
import { RequestsOverTimeChart } from '@/features/logs/components/RequestsOverTimeChart'
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

/* ------------------------------------------------------------------ */
/* KPI helpers & components                                             */
/* ------------------------------------------------------------------ */

function isFailedOutcome(outcome?: string): boolean {
  return outcome === 'failed' || outcome === 'rejected' || outcome === 'dropped'
}

const KPI_BUCKET_COUNT = 12

function kpiBucketCounts(rows: readonly LogRequest[]) {
  const bucketMs = 60 * 60 * 1000 // 1 hour
  const buckets = new Array<number>(KPI_BUCKET_COUNT).fill(0)
  if (rows.length === 0) {
    return {
      values: buckets,
      total: 0,
      completedCount: 0,
      failedCount: 0,
      activeCount: 0,
      completedShare: '—',
      failedShare: '—',
      activeShare: '—'
    }
  }

  let minTs = Number.MAX_SAFE_INTEGER
  for (const row of rows) {
    const ts = Date.parse(row.createdAt ?? '')
    if (!Number.isNaN(ts)) minTs = Math.min(minTs, ts)
  }
  if (minTs === Number.MAX_SAFE_INTEGER) {
    return {
      values: buckets,
      total: rows.length,
      completedCount: rows.filter((r) => r.outcome === 'completed').length,
      failedCount: rows.filter((r) => isFailedOutcome(r.outcome)).length,
      activeCount: rows.filter((r) => r.outcome === 'active').length,
      completedShare: '—',
      failedShare: '—',
      activeShare: '—'
    }
  }

  const startOfRange = new Date(Math.floor(minTs / bucketMs) * bucketMs).getTime()
  for (const row of rows) {
    const ts = Date.parse(row.createdAt ?? '')
    if (Number.isNaN(ts)) continue
    const idx = Math.round((startOfRange - ts) / bucketMs) + KPI_BUCKET_COUNT - 1
    if (idx >= 0 && idx < buckets.length) buckets[idx]++
  }

  const total = rows.length
  const completedCount = rows.filter((r) => r.outcome === 'completed').length
  const failedCount = rows.filter((r) => isFailedOutcome(r.outcome)).length
  const activeCount = rows.filter((r) => r.outcome === 'active').length

  return {
    values: buckets,
    total,
    completedCount,
    failedCount,
    activeCount,
    completedShare: total > 0 ? `${((completedCount / total) * 100).toFixed(1)}%` : '—',
    failedShare: total > 0 ? `${((failedCount / total) * 100).toFixed(1)}%` : '—',
    activeShare: total > 0 ? `${((activeCount / total) * 100).toFixed(1)}%` : '—'
  }
}

type KpiTileProps = {
  readonly Icon: LucideIcon
  readonly label: string
  readonly valueText: string
  readonly valueColor: string
  readonly secondaryLabel?: string
  readonly sparklineValues: number[]
  readonly sparklineColor: string
  readonly sparklineLabel: string
}

function KpiTile({
  Icon,
  label,
  valueText,
  valueColor,
  secondaryLabel,
  sparklineValues,
  sparklineColor,
  sparklineLabel
}: KpiTileProps) {
  return (
    <div className="panel-shell min-w-0 rounded-[var(--radius-lg)] border border-border bg-panel px-[var(--panel-x)] py-[var(--panel-y)]">
      <div className="flex items-center gap-1.5">
        <Icon className="size-3.5 shrink-0" style={{ color: valueColor }} aria-hidden="true" />
        <span className="type-label truncate text-fg-faint">{label}</span>
      </div>
      <div
        className="mt-[var(--panel-y,12px)] font-mono text-[length:var(--density-type-headline)] font-semibold leading-none tracking-tight"
        style={{ color: valueColor }}
      >
        {valueText}
      </div>
      <Sparkline
        className="mt-[calc(var(--panel-y,12px)*0.667)] h-5 w-full max-w-full"
        values={sparklineValues}
        color={sparklineColor}
        width={200}
        height={20}
        preserveAspectRatio="none"
        strokeWidth={1.5}
        ariaLabel={sparklineLabel}
      />
      {secondaryLabel ? (
        <div className="mt-1 type-caption text-fg-dim">{secondaryLabel}</div>
      ) : null}
    </div>
  )
}

function KpiStrip({ rows }: { readonly rows: readonly LogRequest[] }) {
  const counts = kpiBucketCounts(rows)
  const tiles: KpiTileProps[] = [
    {
      Icon: Database,
      label: 'Total requests',
      valueText: String(rows.length),
      valueColor: 'var(--color-foreground)',
      secondaryLabel: 'Last 12 hours',
      sparklineValues: counts.values,
      sparklineColor: 'var(--color-foreground)',
      sparklineLabel: 'Total requests trend'
    },
    {
      Icon: CircleCheckBig,
      label: 'Completed',
      valueText: String(counts.completedCount),
      valueColor: 'var(--color-good)',
      secondaryLabel: counts.completedShare,
      sparklineValues: counts.values,
      sparklineColor: 'var(--color-good)',
      sparklineLabel: 'Completed requests trend'
    },
    {
      Icon: CircleX,
      label: 'Failed',
      valueText: String(counts.failedCount),
      valueColor: 'var(--color-bad)',
      secondaryLabel: counts.failedShare,
      sparklineValues: counts.values,
      sparklineColor: 'var(--color-bad)',
      sparklineLabel: 'Failed requests trend'
    },
    {
      Icon: Activity,
      label: 'Active',
      valueText: String(counts.activeCount),
      valueColor: 'var(--color-accent)',
      secondaryLabel: counts.activeShare,
      sparklineValues: counts.values,
      sparklineColor: 'var(--color-accent)',
      sparklineLabel: 'Active requests trend'
    }
  ]
  return (
    <section
      className="grid grid-cols-1 gap-[calc(var(--shell-normal)*1.25)] sm:grid-cols-2 xl:grid-cols-4"
      aria-label="Request summary"
    >
      {tiles.map((tile) => (
        <KpiTile key={tile.label} {...tile} />
      ))}
    </section>
  )
}

/* ------------------------------------------------------------------ */
/* Table capture helper                                                */
/* ------------------------------------------------------------------ */

type TableCaptureProps = {
  readonly table: DataTableTanStackTableType<LogRequest>
  readonly onCapture: (table: DataTableTanStackTableType<LogRequest> | null) => void
}

function TableCapture({ table, onCapture }: TableCaptureProps) {
  useEffect(() => {
    onCapture(table)
    return () => onCapture(null)
  }, [table, onCapture])
  return null
}

/* ------------------------------------------------------------------ */
/* LogsLedger                                                          */
/* ------------------------------------------------------------------ */

export function LogsLedger({
  search,
  onSearchChange,
  onRequestOpen,
  onMaintenanceMutationSucceeded
}: LogsLedgerProps) {
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

  const [table, setTable] = useState<DataTableTanStackTableType<LogRequest> | null>(null)
  const [requestQuery, setRequestQuery] = useState('')
  const trimmedQuery = useMemo(() => requestQuery.trim().toLowerCase(), [requestQuery])

  const handleRequestSearchChange = useCallback((event: React.ChangeEvent<HTMLInputElement>) => {
    setRequestQuery(event.target.value)
  }, [])

  const clearRequestSearch = useCallback(() => {
    setRequestQuery('')
  }, [])

  const handleSetTable = useCallback((next: DataTableTanStackTableType<LogRequest> | null) => {
    setTable(next)
  }, [])

  const visibleRows = useMemo(() => {
    if (!trimmedQuery) return rows
    return rows.filter((row) => row.requestId.toString().toLowerCase().includes(trimmedQuery))
  }, [rows, trimmedQuery])

  useEffect(() => {
    if (!search.focusRequestId) return
    document.getElementById(`log-request-${search.focusRequestId}`)?.focus()
    // eslint-disable-next-line react-hooks/exhaustive-deps -- rows dep ensures the target row is mounted before focusing
  }, [rows, search.focusRequestId])

  return (
    <section
      className="mx-auto flex w-full max-w-[1440px] flex-col gap-[calc(var(--shell-normal)*2)]"
      aria-labelledby="logs-ledger-title"
    >
      <header className="border-b border-border-soft pb-[var(--panel-y)]">
        <div className="flex min-h-[58px] flex-wrap items-center justify-between gap-x-4 gap-y-2 py-0" aria-live="polite">
          <h1 className="type-headline text-foreground" id="logs-ledger-title">Request logs</h1>
          <div className="flex flex-wrap items-center gap-2">
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
            {result?.state === 'supported' ? (
              <LogOperations
                onMaintenanceMutationSucceeded={onMaintenanceMutationSucceeded}
                query={toLogsRequestQuery(search)}
              />
            ) : null}
          </div>
        </div>
      </header>

      {result?.state === 'supported' ? <RequestsOverTimeChart rows={rows} /> : null}

      {result?.state === 'supported' ? <KpiStrip rows={rows} /> : null}

      {query.isLoading ? (
        <div
          role="status"
          className="panel-shell min-h-[14rem] rounded-[var(--radius)] border border-border bg-panel p-[var(--panel-x)]"
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

      {result?.state === 'supported' ? (
        <Card className="overflow-hidden rounded-[var(--radius-lg)] border-border bg-panel shadow-none">
          <header className="flex flex-wrap items-center justify-between gap-3 border-b border-border-soft px-4 py-3">
            <p className="type-caption min-w-0 truncate font-mono text-fg-faint">
              <span className="text-fg-dim">
                {visibleRows.length === rows.length
                  ? visibleRows.length
                  : `${visibleRows.length} of ${rows.length}`}
              </span>{' '}
              requests
            </p>
            <div className="flex min-w-0 flex-wrap items-center gap-2">
              {/* 1. Search by request ID */}
              <div className="relative min-w-0 flex-1 sm:flex-none sm:w-64">
                <SearchIcon
                  className="pointer-events-none absolute left-3 top-1/2 size-4 -translate-y-1/2 text-fg-faint"
                  aria-hidden="true"
                />
                <Input
                  aria-label="Filter by request ID"
                  className="ui-control h-8 w-full rounded-[var(--radius)] border-border-soft pl-9 pr-9 text-[length:var(--density-type-caption)]"
                  value={requestQuery}
                  onChange={handleRequestSearchChange}
                  placeholder="Search request ID, model, provider..."
                />
                {requestQuery ? (
                  <Button
                    aria-label="Clear request filter"
                    onClick={clearRequestSearch}
                    size="icon"
                    variant="ghost"
                    className="ui-control-ghost absolute right-1.5 top-1/2 h-7 w-7 -translate-y-1/2 rounded-[var(--radius-sm)] text-fg-faint hover:text-foreground"
                  >
                    <X className="size-3.5" aria-hidden="true" />
                  </Button>
                ) : null}
              </div>
              {/* 2. Time range */}
              <div className="flex items-center gap-1.5 text-[length:var(--density-type-caption)] text-fg-dim">
                <Calendar className="size-3.5 text-fg-faint" aria-hidden="true" />
                <NativeSelect
                  ariaLabel="Filter logs by time range"
                  className="w-[11.5rem] min-w-0 pl-7"
                  name="logs-time-range"
                  onValueChange={(value) => {
                    const preset = RELATIVE_TIME_PRESETS.find((option) => option.value === value)
                    if (preset) onSearchChange(updateLogsTimeRange(search, preset.value))
                  }}
                  options={RELATIVE_TIME_PRESETS}
                  value={search.timeRange ?? ''}
                />
              </div>
              {/* 3. Reset view */}
              <Button
                className="ui-control h-8 gap-1.5 rounded-[var(--radius)] px-2.5 text-[length:var(--density-type-caption)]"
                disabled={activeFilterGroups === 0 && !search.cursor && !trimmedQuery}
                onClick={() => {
                  onSearchChange(resetLogsSearch(search))
                  setRequestQuery('')
                }}
                size="sm"
                variant="outline"
              >
                <RotateCcw className="size-3.5" aria-hidden="true" />
                Reset view
              </Button>
              {/* 4. Filter popover */}
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
              {/* 5. Columns / view options (only once the table instance is captured) */}
              {table ? <DataTableViewOptions table={table} /> : null}
            </div>
          </header>

          {rows.length === 0 ? (
            <div className="flex flex-col items-start gap-1 px-[var(--panel-x)] py-8" role="status">
              <div className="type-panel-title text-foreground">
                {activeFilterGroups > 0 || trimmedQuery ? 'No log requests match this view' : 'No log requests yet'}
              </div>
              <p className="type-body text-fg-dim">
                {activeFilterGroups > 0 || trimmedQuery
                  ? 'Clear the filters or search to broaden this view.'
                  : 'Requests appear here as they are recorded.'}
              </p>
            </div>
          ) : (
            <ScrollArea className="max-h-[71rem]">
              <DataTable
                ariaLabel="Request logs"
                columns={columns}
                data={visibleRows}
                defaultPageSize={20}
                emptyMessage="No log requests match this view."
                enablePagination
                footerClassName=""
                getRowId={getLogRequestRowId}
                tableClassName="min-w-[780px] text-[length:var(--density-type-caption-lg)] [&_td]:px-4 [&_td]:py-3 [&_th]:px-4 [&_thead]:bg-transparent [&_tr:hover_td:last-child_button]:opacity-100"
              >
                {(tableInstance) => <TableCapture onCapture={handleSetTable} table={tableInstance} />}
              </DataTable>
            </ScrollArea>
          )}

          {table ? (
            <div className="border-t border-border-soft">
              <DataTablePagination table={table} />
            </div>
          ) : null}
        </Card>
      ) : null}
    </section>
  )
}
