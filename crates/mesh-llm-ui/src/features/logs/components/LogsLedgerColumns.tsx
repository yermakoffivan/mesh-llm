import { useCallback, useEffect, useRef, useState } from 'react'
import type { ColumnDef } from '@tanstack/react-table'
import {
  Check,
  CircleCheckBig,
  CircleSlash,
  CircleX,
  Copy,
  Cpu,
  ExternalLink,
  LoaderCircle,
  MoreHorizontal,
  Network,
  RadioTower,
  ShieldAlert,
  ShieldCheck,
  type LucideIcon
} from 'lucide-react'
import { Button } from '@/components/ui/button'
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

function outcomeIcon(outcome: LogRequest['outcome']): LucideIcon {
  switch (outcome) {
    case 'completed':
      return CircleCheckBig
    case 'failed':
    case 'rejected':
    case 'dropped':
      return CircleX
    case 'cancelled':
      return CircleSlash
    case 'active':
      return LoaderCircle
  }
}

function durabilityIcon(row: LogRequest): LucideIcon {
  if (row.source === 'active') return RadioTower
  return isFailedOutcome(row.outcome) ? ShieldAlert : ShieldCheck
}

function isFailedOutcome(outcome: LogRequest['outcome']) {
  return outcome === 'failed' || outcome === 'rejected' || outcome === 'dropped'
}

function formatTimestamp(value: string) {
  const timestamp = new Date(value)
  if (Number.isNaN(timestamp.getTime())) return value
  return timestamp.toLocaleString()
}

function machineValue(value: string | undefined) {
  return value ?? '—'
}

function CopyRequestIdButton({ requestId }: { readonly requestId: string }) {
  const [copied, setCopied] = useState(false)
  const resetTimer = useRef<number | undefined>(undefined)

  useEffect(() => () => window.clearTimeout(resetTimer.current), [])

  const handleCopy = useCallback(() => {
    if (!navigator.clipboard) return
    void navigator.clipboard.writeText(requestId).then(
      () => {
        setCopied(true)
        resetTimer.current = window.setTimeout(() => setCopied(false), 1500)
      },
      () => setCopied(false)
    )
  }, [requestId])

  return (
    <Button
      aria-label={`Copy request ID ${requestId}`}
      className="ui-control-ghost h-6 w-6 shrink-0 rounded-[var(--radius-sm)] text-fg-faint hover:text-foreground"
      onClick={handleCopy}
      size="icon"
      title="Copy request ID"
      type="button"
      variant="ghost"
    >
      {copied ? <Check className="size-3" aria-hidden="true" /> : <Copy className="size-3" aria-hidden="true" />}
    </Button>
  )
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
          <div className="group/req flex min-w-0 items-center gap-1.5">
            <button
              aria-label={`Open request ${requestId}`}
              className="break-all rounded-[var(--radius-sm)] px-1 text-accent underline-offset-2 transition-[filter] hover:underline focus-visible:bg-panel-strong focus-visible:outline-none group-hover/req:brightness-125 group-focus-within/req:brightness-125"
              id={`log-request-${requestId}`}
              onClick={() => onRequestOpen(requestId, { ...search, focusRequestId: requestId })}
              type="button"
            >
              {requestId}
            </button>
            <ExternalLink
              aria-hidden="true"
              className="size-3 shrink-0 text-fg-faint opacity-0 transition-opacity group-hover/req:opacity-100 group-focus-within/req:opacity-100"
            />
            <div className="opacity-0 transition-opacity group-hover/req:opacity-100 group-focus-within/req:opacity-100">
              <CopyRequestIdButton requestId={requestId} />
            </div>
          </div>
        )
      }
    },
    {
      accessorKey: 'model',
      header: ({ column }) => <DataTableColumnHeader column={column} title="Model / route" />,
      cell: ({ row }) => (
        <div>
          <div className="font-mono font-medium tracking-tight text-foreground">{machineValue(row.original.model)}</div>
          <div className="mt-1 font-mono text-fg-faint">{machineValue(row.original.route)}</div>
        </div>
      )
    },
    {
      accessorKey: 'provider',
      header: ({ column }) => <DataTableColumnHeader column={column} title="Provider / engine" />,
      cell: ({ row }) => (
        <div className="flex flex-col items-start gap-1">
          <span className="flex items-center gap-1.5 font-mono text-fg-dim" title={machineValue(row.original.provider)}>
            <Network className="size-3 shrink-0 text-fg-faint" aria-hidden="true" />
            mesh-routed
          </span>
          <span className="inline-flex w-fit items-center gap-1 rounded-[var(--radius-sm)] border border-border-soft bg-panel-strong px-1.5 py-px font-mono text-[10px] text-fg-faint">
            <Cpu className="size-2.5 shrink-0" aria-hidden="true" />
            {machineValue(row.original.engine)}
          </span>
        </div>
      )
    },
    {
      accessorKey: 'outcome',
      header: ({ column }) => <DataTableColumnHeader column={column} title="Outcome" />,
      cell: ({ row }) => {
        const outcome = row.original.outcome
        const Icon = outcomeIcon(outcome)
        return (
          <StatusBadge size="caption" tone={requestTone(outcome)}>
            <Icon className="size-3" aria-hidden="true" />
            {outcome}
          </StatusBadge>
        )
      }
    },
    {
      accessorKey: 'source',
      header: ({ column }) => <DataTableColumnHeader column={column} title="Durability" />,
      cell: ({ row }) => {
        const Icon = durabilityIcon(row.original)
        return (
          <div className="flex items-center gap-1.5 font-mono text-fg-dim">
            <Icon className="size-3.5 shrink-0 text-fg-faint" aria-hidden="true" />
            <span>{row.original.source}</span>
          </div>
        )
      }
    },
    {
      id: 'actions',
      header: () => <span className="sr-only">Row actions</span>,
      enableHiding: false,
      enableSorting: false,
      cell: ({ row }) => {
        const requestId = row.original.requestId.toString()
        return (
          <Button
            aria-label={`Request actions for ${requestId}`}
            className="ui-control-ghost h-7 w-7 rounded-[var(--radius-sm)] text-fg-faint opacity-0 transition-opacity duration-150 hover:opacity-100 hover:text-foreground focus-visible:opacity-100"
            onClick={() => onRequestOpen(requestId, { ...search, focusRequestId: requestId })}
            size="icon"
            title="Open request details"
            type="button"
            variant="ghost"
          >
            <MoreHorizontal className="size-3.5" aria-hidden="true" />
          </Button>
        )
      }
    }
  ]
}
