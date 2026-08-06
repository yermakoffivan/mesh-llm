import { useEffect, useMemo, useRef, useState } from 'react'
import { ArrowLeft, ChevronDown, DatabaseZap, Download, Route, ShieldAlert, Workflow } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { Collapsible, CollapsibleContent, CollapsibleTrigger } from '@/components/ui/collapsible'
import { CopyInstructionRow } from '@/components/ui/CopyInstructionRow'
import { Separator } from '@/components/ui/separator'
import { StatusBadge, type StatusBadgeTone } from '@/components/ui/StatusBadge'
import { Tabs, TabsContent, TabsList, TabsTrigger } from '@/components/ui/tabs'
import { LogRequestDeleteControl } from '@/features/logs/components/LogOperations'
import { LogsApiClient } from '@/features/logs/api/client'
import {
  useLogRequestArtifactsQuery,
  useLogRequestAttemptsQuery,
  useLogRequestEventsQuery,
  useLogRequestSummaryQuery
} from '@/features/logs/api/use-log-request-details-query'
import type { LogRequestId } from '@/features/logs/api/ids'
import type { LogArtifact, LogLifecycleEvent, LogRequest } from '@/features/logs/api/schemas'
import {
  artifactMatchesTab,
  isErrorEvent,
  isLogRequestDetailTab,
  isStreamEvent,
  sortLifecycleEvents,
  sortProxyAttempts,
  type LogRequestDetailTab
} from '@/features/logs/lib/log-request-details'

type LogRequestDetailsProps = {
  readonly requestId: LogRequestId
  readonly tab: LogRequestDetailTab
  readonly onBack: () => void
  readonly onTabChange: (tab: LogRequestDetailTab) => void
  readonly onMaintenanceMutationSucceeded?: () => void
}

const tabs: Array<{ readonly id: LogRequestDetailTab; readonly label: string }> = [
  { id: 'summary', label: 'Summary' },
  { id: 'request', label: 'Request' },
  { id: 'response', label: 'Response' },
  { id: 'routing', label: 'Routing attempts' },
  { id: 'stream', label: 'Stream timeline' },
  { id: 'errors', label: 'Errors' }
]

function formatTimestamp(value: string | undefined): string {
  if (!value) return '—'
  const timestamp = new Date(value)
  return Number.isNaN(timestamp.getTime()) ? value : timestamp.toLocaleString()
}

function machineValue(value: string | number | undefined): string {
  return value === undefined ? '—' : String(value)
}

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

function artifactTone(artifact: LogArtifact): StatusBadgeTone {
  switch (artifact.contentState) {
    case 'available':
      return 'good'
    case 'unavailable':
      return 'warn'
    case 'missing':
    case 'corrupt':
      return 'bad'
  }
}

function eventTone(event: LogLifecycleEvent): StatusBadgeTone {
  return isErrorEvent(event) ? 'bad' : event.kind === 'stream_chunk' ? 'accent' : 'muted'
}

function QueryState({
  label,
  error,
  loading
}: {
  readonly label: string
  readonly error: boolean
  readonly loading: boolean
}) {
  if (loading) {
    return <p className="type-body text-fg-dim">Loading {label}.</p>
  }
  if (error) {
    return (
      <p className="type-body text-fg-dim" role="alert">
        {label} could not be loaded. The local log service did not return a usable response.
      </p>
    )
  }
  return null
}

function MetadataGrid({ request }: { readonly request: LogRequest }) {
  const values = [
    ['Created', formatTimestamp(request.createdAt)],
    ['Terminal', formatTimestamp(request.terminalAt)],
    ['Model', machineValue(request.model)],
    ['Route', machineValue(request.route)],
    ['Provider', machineValue(request.provider)],
    ['Engine', machineValue(request.engine)],
    ['HTTP status', machineValue(request.statusCode)],
    ['Record source', request.source]
  ]

  return (
    <dl className="grid gap-x-[var(--shell-normal)] gap-y-3 sm:grid-cols-2 xl:grid-cols-4">
      {values.map(([label, value]) => (
        <div className="min-w-0" key={label}>
          <dt className="type-label text-fg-faint">{label}</dt>
          <dd className="mt-1 break-words font-mono text-[length:var(--density-type-caption-lg)] text-foreground">
            {value}
          </dd>
        </div>
      ))}
    </dl>
  )
}

function saveArtifactDownload(bytes: Uint8Array, fileName: string, mediaType: string) {
  if (
    typeof document === 'undefined' ||
    typeof URL === 'undefined' ||
    typeof URL.createObjectURL !== 'function' ||
    typeof URL.revokeObjectURL !== 'function'
  ) {
    throw new Error('This browser cannot save the retained artifact.')
  }

  const copy = new Uint8Array(bytes.byteLength)
  copy.set(bytes)
  const url = URL.createObjectURL(new Blob([copy.buffer], { type: mediaType }))
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = fileName
  anchor.rel = 'noopener'
  anchor.hidden = true
  document.body.append(anchor)
  anchor.click()
  anchor.remove()
  window.setTimeout(() => URL.revokeObjectURL(url), 0)
}

function ArtifactDownloadControl({
  artifact
}: {
  readonly artifact: Extract<LogArtifact, { contentState: 'available' }>
}) {
  const [action, setAction] = useState<{ readonly message: string; readonly tone: 'error' | 'success' }>()
  const [pending, setPending] = useState(false)

  async function downloadArtifact() {
    setPending(true)
    setAction(undefined)
    try {
      const result = await new LogsApiClient().downloadArtifact(artifact.artifactId)
      if (result.state === 'unavailable') {
        setAction({ tone: 'error', message: 'This artifact is no longer available for download.' })
        return
      }
      saveArtifactDownload(result.download.bytes, result.download.fileName, result.download.mediaType)
      setAction({ tone: 'success', message: 'Artifact download started.' })
    } catch {
      setAction({ tone: 'error', message: 'The retained artifact could not be downloaded.' })
    } finally {
      setPending(false)
    }
  }

  return (
    <div className="mt-3 flex flex-wrap items-center gap-2">
      <Button
        className="ui-control"
        disabled={pending}
        onClick={() => void downloadArtifact()}
        size="sm"
        type="button"
        variant="outline"
      >
        <Download className="size-3.5" aria-hidden="true" />
        {pending ? 'Preparing download…' : 'Download redacted artifact'}
      </Button>
      {action ? (
        <p className={`type-caption ${action.tone === 'error' ? 'text-bad' : 'text-good'}`} role="status">
          {action.message}
        </p>
      ) : null}
    </div>
  )
}

function ArtifactMetadata({ artifact }: { readonly artifact: LogArtifact }) {
  const payloadState = artifact.contentState === 'available' ? 'retained; not loaded' : artifact.contentState
  return (
    <article className="border-b border-border-soft py-3 last:border-b-0">
      <Collapsible defaultOpen>
        <CollapsibleTrigger className="group flex w-full flex-wrap items-center justify-between gap-2 text-left focus-visible:outline-none focus-visible:ring-2 focus-visible:ring-ring">
          <div className="min-w-0 break-words font-mono text-[length:var(--density-type-caption-lg)] text-foreground">
            {artifact.kind}
          </div>
          <div className="flex items-center gap-2">
            <StatusBadge size="caption" tone={artifactTone(artifact)}>
              {payloadState}
            </StatusBadge>
            <span className="type-caption text-fg-faint" aria-hidden="true">
              <ChevronDown className="size-3.5 transition-transform group-data-[state=open]:rotate-180" />
            </span>
          </div>
        </CollapsibleTrigger>
        <CollapsibleContent>
          <dl className="mt-3 grid gap-x-[var(--shell-normal)] gap-y-2 sm:grid-cols-2 lg:grid-cols-4">
            <div>
              <dt className="type-label text-fg-faint">Captured</dt>
              <dd className="mt-1 font-mono text-[length:var(--density-type-caption)] text-fg-dim">
                {formatTimestamp(artifact.occurredAt)}
              </dd>
            </div>
            <div>
              <dt className="type-label text-fg-faint">Bytes / version</dt>
              <dd className="mt-1 font-mono text-[length:var(--density-type-caption)] text-fg-dim">
                {artifact.bytes} B / v{artifact.version}
              </dd>
            </div>
            <div>
              <dt className="type-label text-fg-faint">Redaction</dt>
              <dd className="mt-1 text-[length:var(--density-type-caption)] text-fg-dim">
                {artifact.redacted ? 'Redacted before retention' : 'Not retained as payload'}
              </dd>
            </div>
            <div>
              <dt className="type-label text-fg-faint">Truncation</dt>
              <dd className="mt-1 text-[length:var(--density-type-caption)] text-fg-dim">
                {artifact.truncated ? 'Truncated' : 'Complete metadata'}
              </dd>
            </div>
          </dl>
          <div className="mt-3 min-w-0">
            <Separator />
            <div className="mt-2">
              <div className="type-label text-fg-faint">Checksum</div>
              <div className="mt-1 break-all font-mono text-[length:var(--density-type-caption)] text-fg-dim">
                {artifact.checksum ?? 'Not recorded'}
              </div>
            </div>
          </div>
          {artifact.contentState === 'available' && artifact.redacted ? (
            <ArtifactDownloadControl artifact={artifact} />
          ) : null}
        </CollapsibleContent>
      </Collapsible>
    </article>
  )
}

function ArtifactPanel({
  artifacts,
  kind,
  loading,
  error
}: {
  readonly artifacts: readonly LogArtifact[] | undefined
  readonly kind: 'request' | 'response' | 'errors'
  readonly loading: boolean
  readonly error: boolean
}) {
  const matchingArtifacts = useMemo(
    () => (artifacts ?? []).filter((artifact) => artifactMatchesTab(artifact.kind, kind)),
    [artifacts, kind]
  )
  const label = kind === 'errors' ? 'error artifact metadata' : `${kind} artifact metadata`
  if (loading || error) return <QueryState error={error} label={label} loading={loading} />
  if (matchingArtifacts.length === 0) {
    return (
      <div className="type-body text-fg-dim" role="status">
        {kind === 'errors'
          ? 'No retained error artifacts. Error payload capture may be disabled.'
          : `${kind[0].toUpperCase()}${kind.slice(1)} payload capture is disabled or no matching artifact was retained.`}
      </div>
    )
  }
  return (
    <div>
      {matchingArtifacts.map((artifact) => (
        <ArtifactMetadata artifact={artifact} key={artifact.artifactId.toString()} />
      ))}
    </div>
  )
}

function EventTimeline({
  events,
  emptyLabel
}: {
  readonly events: readonly LogLifecycleEvent[]
  readonly emptyLabel: string
}) {
  if (events.length === 0) return <p className="type-body text-fg-dim">{emptyLabel}</p>
  return (
    <ol aria-label="Ordered request lifecycle" className="divide-y divide-border-soft">
      {events.map((event) => (
        <li className="flex flex-wrap items-start justify-between gap-3 py-3" key={event.eventId.toString()}>
          <div className="min-w-0">
            <div className="flex flex-wrap items-center gap-2">
              <StatusBadge size="caption" tone={eventTone(event)}>
                {event.kind}
              </StatusBadge>
              <span className="break-words font-mono text-[length:var(--density-type-caption)] text-fg-dim">
                {event.attemptId ?? 'request lifecycle'}
              </span>
            </div>
            <p className="mt-1 font-mono text-[length:var(--density-type-caption)] text-fg-faint">
              status {machineValue(event.statusCode)} · {machineValue(event.durationMs)} ms ·{' '}
              {machineValue(event.tokens)} tokens
            </p>
          </div>
          <time
            className="shrink-0 font-mono text-[length:var(--density-type-caption)] text-fg-faint"
            dateTime={event.occurredAt}
          >
            {formatTimestamp(event.occurredAt)}
          </time>
        </li>
      ))}
    </ol>
  )
}

function RoutingAttempts({ attempts }: { readonly attempts: ReturnType<typeof sortProxyAttempts> }) {
  if (attempts.length === 0)
    return <p className="type-body text-fg-dim">No proxy attempts were retained for this request.</p>
  return (
    <ol aria-label="Ordered routing attempts" className="divide-y divide-border-soft">
      {attempts.map((attempt) => (
        <li className="flex flex-wrap items-start justify-between gap-3 py-3" key={attempt.attemptId}>
          <div className="min-w-0">
            <div className="break-words font-mono text-[length:var(--density-type-caption-lg)] text-foreground">
              {attempt.target}
            </div>
            <div className="mt-1 font-mono text-[length:var(--density-type-caption)] text-fg-faint">
              {attempt.attemptId} · {machineValue(attempt.provider)} / {machineValue(attempt.engine)} · HTTP{' '}
              {machineValue(attempt.statusCode)}
            </div>
          </div>
          <time
            className="shrink-0 font-mono text-[length:var(--density-type-caption)] text-fg-faint"
            dateTime={attempt.occurredAt}
          >
            {formatTimestamp(attempt.occurredAt)}
          </time>
        </li>
      ))}
    </ol>
  )
}

export function LogRequestDetails({
  requestId,
  tab,
  onBack,
  onTabChange,
  onMaintenanceMutationSucceeded
}: LogRequestDetailsProps) {
  const headingRef = useRef<HTMLHeadingElement>(null)
  const summaryQuery = useLogRequestSummaryQuery(requestId)
  const eventsQuery = useLogRequestEventsQuery(requestId, tab === 'stream' || tab === 'errors')
  const artifactsQuery = useLogRequestArtifactsQuery(
    requestId,
    tab === 'request' || tab === 'response' || tab === 'errors'
  )
  const attemptsQuery = useLogRequestAttemptsQuery(requestId, tab === 'routing')
  const events = useMemo(() => sortLifecycleEvents(eventsQuery.data?.items ?? []), [eventsQuery.data])
  const attempts = useMemo(() => sortProxyAttempts(attemptsQuery.data?.items ?? []), [attemptsQuery.data])

  useEffect(() => {
    headingRef.current?.focus()
  }, [requestId])

  return (
    <section
      className="mx-auto flex w-full max-w-[1440px] flex-col gap-[var(--shell-normal)]"
      aria-labelledby="log-request-details-title"
    >
      <header className="border-b border-border-soft pb-[var(--panel-y)]">
        <Button
          className="ui-control h-8 gap-1.5 px-2.5 text-[length:var(--density-type-caption)]"
          onClick={onBack}
          size="sm"
          variant="outline"
        >
          <ArrowLeft className="size-3.5" aria-hidden="true" />
          Back to logs
        </Button>
        <div className="mt-4 flex flex-wrap items-start justify-between gap-3">
          <div className="min-w-0">
            <div className="type-label text-fg-faint">Request inspector</div>
            <h1
              className="type-display mt-1 break-all text-foreground outline-none"
              id="log-request-details-title"
              ref={headingRef}
              tabIndex={-1}
            >
              {requestId.toString()}
            </h1>
          </div>
          {summaryQuery.data ? (
            <StatusBadge dot tone={requestTone(summaryQuery.data.outcome)}>
              {summaryQuery.data.outcome}
            </StatusBadge>
          ) : null}
        </div>
      </header>

      {summaryQuery.isLoading ? <QueryState error={false} label="request summary" loading /> : null}
      {summaryQuery.isError ? <QueryState error label="request summary" loading={false} /> : null}

      {summaryQuery.data ? (
        <Tabs
          onValueChange={(value) => {
            if (isLogRequestDetailTab(value)) onTabChange(value)
          }}
          value={tab}
        >
          <TabsList
            aria-label="Request detail tabs"
            className="h-auto max-w-full flex-wrap justify-start gap-1 rounded-[var(--radius)] bg-panel-strong p-1"
          >
            {tabs.map((item) => (
              <TabsTrigger className="text-[length:var(--density-type-caption)]" key={item.id} value={item.id}>
                {item.label}
              </TabsTrigger>
            ))}
          </TabsList>

          <TabsContent
            className="panel-shell rounded-[var(--radius)] border border-border bg-panel p-[var(--panel-x)]"
            value="summary"
          >
            <div className="flex flex-wrap items-start justify-between gap-3">
              <div>
                <div className="type-panel-title text-foreground">Request summary</div>
                <p className="type-caption mt-1 text-fg-dim">
                  Canonical request record, separate from outbound proxy attempts.
                </p>
              </div>
              <StatusBadge dot size="caption" tone={requestTone(summaryQuery.data.outcome)}>
                {summaryQuery.data.outcome}
              </StatusBadge>
            </div>
            <div className="mt-4">
              <CopyInstructionRow label="Request ID" value={summaryQuery.data.requestId.toString()} />
            </div>
            <div className="mt-4">
              <MetadataGrid request={summaryQuery.data} />
            </div>
            {summaryQuery.data.source === 'durable' && summaryQuery.data.outcome !== 'active' ? (
              <LogRequestDeleteControl
                onMaintenanceMutationSucceeded={onMaintenanceMutationSucceeded}
                requestId={summaryQuery.data.requestId}
              />
            ) : null}
          </TabsContent>

          <TabsContent
            className="panel-shell rounded-[var(--radius)] border border-border bg-panel p-[var(--panel-x)]"
            value="request"
          >
            <div className="type-panel-title text-foreground">Request artifact</div>
            <p className="type-caption mt-1 text-fg-dim">
              Metadata only. Retained payload content is not opened automatically; available redacted content requires
              an explicit download.
            </p>
            <div className="mt-3">
              <ArtifactPanel
                artifacts={artifactsQuery.data?.items}
                error={artifactsQuery.isError}
                kind="request"
                loading={artifactsQuery.isLoading}
              />
            </div>
          </TabsContent>

          <TabsContent
            className="panel-shell rounded-[var(--radius)] border border-border bg-panel p-[var(--panel-x)]"
            value="response"
          >
            <div className="type-panel-title text-foreground">Response artifact</div>
            <p className="type-caption mt-1 text-fg-dim">
              Metadata only. Retained payload content is not opened automatically; available redacted content requires
              an explicit download.
            </p>
            <div className="mt-3">
              <ArtifactPanel
                artifacts={artifactsQuery.data?.items}
                error={artifactsQuery.isError}
                kind="response"
                loading={artifactsQuery.isLoading}
              />
            </div>
          </TabsContent>

          <TabsContent
            className="panel-shell rounded-[var(--radius)] border border-border bg-panel p-[var(--panel-x)]"
            value="routing"
          >
            <div className="flex items-center gap-2">
              <Route className="size-4 text-accent" aria-hidden="true" />
              <div className="type-panel-title text-foreground">Routing attempts</div>
            </div>
            <p className="type-caption mt-1 text-fg-dim">
              Outbound proxy history, kept distinct from the request summary.
            </p>
            <div className="mt-3">
              <QueryState error={attemptsQuery.isError} label="routing attempts" loading={attemptsQuery.isLoading} />
              {!attemptsQuery.isLoading && !attemptsQuery.isError ? <RoutingAttempts attempts={attempts} /> : null}
            </div>
          </TabsContent>

          <TabsContent
            className="panel-shell rounded-[var(--radius)] border border-border bg-panel p-[var(--panel-x)]"
            value="stream"
          >
            <div className="flex items-center gap-2">
              <Workflow className="size-4 text-accent" aria-hidden="true" />
              <div className="type-panel-title text-foreground">Stream timeline</div>
            </div>
            <p className="type-caption mt-1 text-fg-dim">Persisted lifecycle markers, ordered by occurrence time.</p>
            <div className="mt-3">
              <QueryState error={eventsQuery.isError} label="stream timeline" loading={eventsQuery.isLoading} />
              {!eventsQuery.isLoading && !eventsQuery.isError ? (
                <EventTimeline
                  emptyLabel="No stream lifecycle markers were retained."
                  events={events.filter(isStreamEvent)}
                />
              ) : null}
            </div>
          </TabsContent>

          <TabsContent
            className="panel-shell rounded-[var(--radius)] border border-border bg-panel p-[var(--panel-x)]"
            value="errors"
          >
            <div className="flex items-center gap-2">
              <ShieldAlert className="size-4 text-bad" aria-hidden="true" />
              <div className="type-panel-title text-foreground">Errors</div>
            </div>
            <p className="type-caption mt-1 text-fg-dim">
              Diagnostic labels are rendered as text only; no payload or stack markup is interpreted.
            </p>
            <div className="mt-3">
              <QueryState error={eventsQuery.isError} label="error timeline" loading={eventsQuery.isLoading} />
              {!eventsQuery.isLoading && !eventsQuery.isError ? (
                <EventTimeline
                  emptyLabel="No failure lifecycle markers were retained."
                  events={events.filter(isErrorEvent)}
                />
              ) : null}
            </div>
            <div className="mt-4">
              <Separator />
              <div className="pt-3">
                <DatabaseZap className="mr-2 inline size-4 text-fg-faint" aria-hidden="true" />
                <span className="type-label text-fg-faint">Error artifacts</span>
                <div className="mt-2">
                  <ArtifactPanel
                    artifacts={artifactsQuery.data?.items}
                    error={artifactsQuery.isError}
                    kind="errors"
                    loading={artifactsQuery.isLoading}
                  />
                </div>
              </div>
            </div>
          </TabsContent>
        </Tabs>
      ) : null}
    </section>
  )
}
