import type { ColumnDef } from '@tanstack/react-table'
import { StatusBadge, type StatusBadgeTone } from '@/components/ui/StatusBadge'
import { DataTableColumnHeader } from '@/components/ui/data-table-column-header'
import type { LogRequest } from '@/features/logs/api/schemas'
import type { LogsLedgerSearch } from '@/features/logs/lib/log-search'

function requestTone(outcome: LogRequest['outcome']): StatusBadgeTone {
  switch (outcome) {
    case 'active':
      return 'accent'
    case 'completed':
      return 'good'
    case 'failed':
    case 'rejected':
    case 'dropped':
      return 'bad'
    case 'cancelled':
      return 'warn'
  }
}

function formatTimestamp(value: string) {
  const timestamp = new Date(value)
  if (Number.isNaN(timestamp.getTime())) return value
  return timestamp.toLocaleString()
}

function machineValue(value: string | undefined) {
  return value ?? '—'
}

export function buildLogsLedgerColumns({
  onRequestOpen,
  search
}: {
  readonly onRequestOpen: (requestId: string, search: LogsLedgerSearch) => void
  readonly search: LogsLedgerSearch
}): ColumnDef<LogRequest>[] {
  return [
    {
      accessorKey: 'createdAt',
      header: ({ column }) => <DataTableColumnHeader column={column} title="Occurred" />,
      cell: ({ row }) => (
        <span className="font-mono tabular-nums text-fg-dim">{formatTimestamp(row.original.createdAt)}</span>
      )
    },
    {
      accessorKey: 'requestId',
      header: ({ column }) => <DataTableColumnHeader column={column} title="Request" />,
      filterFn: (row, _columnId, filterValue) => {
        const requestId = row.original.requestId.toString()
        const query = String(filterValue).toLowerCase()
        return requestId.toLowerCase().includes(query)
      },
      cell: ({ row }) => {
        const requestId = row.original.requestId.toString()
        return (
          <button
            aria-label={`Open request ${requestId}`}
            className="break-all -mx-1 rounded-[var(--radius-sm)] px-1 text-accent focus-visible:bg-panel-strong focus-visible:outline-none"
            id={`log-request-${requestId}`}
            onClick={() => onRequestOpen(requestId, { ...search, focusRequestId: requestId })}
            type="button"
          >
            {requestId}
          </button>
        )
      }
    },
    {
      accessorKey: 'model',
      header: ({ column }) => <DataTableColumnHeader column={column} title="Model / route" />,
      cell: ({ row }) => (
        <div>
          <div className="font-mono text-foreground">{machineValue(row.original.model)}</div>
          <div className="mt-0.5 font-mono text-fg-faint">{machineValue(row.original.route)}</div>
        </div>
      )
    },
    {
      accessorKey: 'provider',
      header: ({ column }) => <DataTableColumnHeader column={column} title="Provider / engine" />,
      cell: ({ row }) => (
        <div>
          <div className="font-mono text-fg-dim">{machineValue(row.original.provider)}</div>
          <div className="mt-0.5 font-mono text-fg-faint">{machineValue(row.original.engine)}</div>
        </div>
      )
    },
    {
      accessorKey: 'outcome',
      header: ({ column }) => <DataTableColumnHeader column={column} title="Outcome" />,
      cell: ({ row }) => (
        <div className="flex items-center gap-2">
          <StatusBadge dot size="caption" tone={requestTone(row.original.outcome)}>
            {row.original.outcome}
          </StatusBadge>
          <span className="font-mono text-fg-faint">{row.original.source}</span>
        </div>
      )
    }
  ]
}
