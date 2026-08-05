import * as DialogPrimitive from '@radix-ui/react-dialog'
import { useRef, useState, type RefObject } from 'react'
import { Download, RotateCcw, ShieldAlert, Trash2 } from 'lucide-react'
import {
  SharedModal,
  SharedModalActionStrip,
  SharedModalBody,
  SharedModalContent,
  SharedModalDescription,
  SharedModalHeader,
  SharedModalTitle
} from '@/components/ui/SharedModal'
import { Button } from '@/components/ui/button'
import { Input } from '@/components/ui/input'
import { LogsApiClient, type LogCleanupPreviewRequest, type LogsRequestQuery } from '@/features/logs/api/client'
import { LogOperationId, LogWebhookDeliveryId, type LogRequestId } from '@/features/logs/api/ids'
import type { LogCleanupOutcome, LogCleanupReceipt, LogDeleteReceipt, LogExport } from '@/features/logs/api/schemas'

type ActionState = { readonly message: string; readonly tone: 'success' | 'error' } | undefined

type LogOperationsProps = {
  readonly query: LogsRequestQuery
  readonly onMaintenanceMutationSucceeded?: () => void
}

type LogRequestDeleteControlProps = {
  readonly requestId: LogRequestId
  readonly onMaintenanceMutationSucceeded?: () => void
}

type LogWebhookRetryProps = {
  readonly deliveryId?: LogWebhookDeliveryId
}

type FrozenLogOperation = {
  readonly operationId: LogOperationId
  readonly reason: string
}

function newOperationId() {
  return LogOperationId.create()
}

function isReasonValid(reason: string) {
  return reason.trim().length > 0
}

function actionError(error: unknown) {
  return error instanceof Error ? error.message : 'The local log service did not complete the operation.'
}

function isCleanupOutcome(value: string | undefined): value is LogCleanupOutcome {
  return value !== undefined && ['completed', 'failed', 'rejected', 'cancelled', 'dropped'].includes(value)
}

function supportsCleanup(query: LogsRequestQuery) {
  return (
    (query.source === undefined || query.source === 'durable') &&
    (query.outcome === undefined || isCleanupOutcome(query.outcome))
  )
}

function hasRetryableArtifactWork(receipt: LogCleanupReceipt | LogDeleteReceipt) {
  return receipt.state === 'partial' && receipt.artifactDeletion.failed > 0
}

function cleanupScopeFromQuery(
  query: LogsRequestQuery
): Pick<LogCleanupPreviewRequest, 'source' | 'from' | 'to' | 'route' | 'model' | 'provider' | 'engine' | 'outcome'> {
  return {
    source: query.source === 'durable' ? 'durable' : undefined,
    from: query.from,
    to: query.to,
    route: query.route,
    model: query.model,
    provider: query.provider,
    engine: query.engine,
    outcome: isCleanupOutcome(query.outcome) ? query.outcome : undefined
  }
}

function downloadExport(exportResult: LogExport) {
  const blob = new Blob([JSON.stringify(exportResult, null, 2)], { type: 'application/json' })
  const url = URL.createObjectURL(blob)
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = 'mesh-llm-log-export.json'
  anchor.click()
  URL.revokeObjectURL(url)
}

function ReceiptDiagnostics({ receipt }: { readonly receipt: LogCleanupReceipt | LogDeleteReceipt }) {
  const partial = receipt.state === 'partial' || receipt.artifactDeletion.failed > 0
  return (
    <div className="mt-3 space-y-2 rounded-[var(--radius)] border border-border-soft bg-panel-strong/60 p-3">
      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="type-label text-fg-faint">Operation ID</span>
        <code className="break-all font-mono text-[length:var(--density-type-caption)] text-foreground">
          {receipt.operationId.toString()}
        </code>
      </div>
      <div className="flex flex-wrap items-center justify-between gap-2">
        <span className="type-label text-fg-faint">Audit ID</span>
        <code className="break-all font-mono text-[length:var(--density-type-caption)] text-foreground">
          {receipt.auditId.toString()}
        </code>
      </div>
      <p className="type-caption text-fg-dim">
        Planned {receipt.planned.requests} request(s), {receipt.planned.events} event(s), and{' '}
        {receipt.planned.artifacts} artifact record(s). Executed {receipt.executed.databaseRows} database row change(s).
      </p>
      {partial ? (
        <p className="type-caption text-warn" role="status">
          Partial cascade: {receipt.artifactDeletion.removed} artifact file(s) removed and{' '}
          {receipt.artifactDeletion.failed} could not be removed
          {receipt.artifactDeletion.failureClass ? ` (${receipt.artifactDeletion.failureClass})` : ''}.
        </p>
      ) : null}
    </div>
  )
}

function ExportDialog({
  open,
  onOpenChange,
  query,
  returnFocusRef
}: {
  readonly open: boolean
  readonly onOpenChange: (open: boolean) => void
  readonly query: LogsRequestQuery
  readonly returnFocusRef: RefObject<HTMLButtonElement | null>
}) {
  const [reason, setReason] = useState('')
  const [action, setAction] = useState<ActionState>()
  const [pending, setPending] = useState(false)

  async function exportLogs() {
    if (!isReasonValid(reason)) return
    setPending(true)
    setAction(undefined)
    try {
      // The UI deliberately exports metadata only. Artifact body inclusion is
      // server-capture gated and must never be inferred from client state.
      const exportResult = await new LogsApiClient().exportRequests(query, {
        reason: reason.trim(),
        includeArtifacts: false
      })
      downloadExport(exportResult)
      setAction({
        tone: 'success',
        message: exportResult.truncated
          ? 'A bounded partial export was downloaded. Narrow the retained filter context before retrying.'
          : 'Bounded log export downloaded.'
      })
    } catch (error) {
      setAction({ tone: 'error', message: actionError(error) })
    } finally {
      setPending(false)
    }
  }

  return (
    <SharedModal open={open} onOpenChange={onOpenChange}>
      <SharedModalContent
        onCloseAutoFocus={(event) => {
          if (!returnFocusRef.current) return
          event.preventDefault()
          returnFocusRef.current.focus()
        }}
      >
        <SharedModalHeader>
          <SharedModalTitle>Export current log view</SharedModalTitle>
          <SharedModalDescription>
            The server applies its bounded export limit to the current durable filters and cursor. Artifact bodies stay
            excluded.
          </SharedModalDescription>
        </SharedModalHeader>
        <SharedModalBody>
          <label
            className="grid gap-1.5 text-[length:var(--density-type-caption)] text-fg-dim"
            htmlFor="log-export-reason"
          >
            <span className="type-label text-fg-faint">Required audit reason</span>
            <Input
              id="log-export-reason"
              aria-describedby="log-export-metadata-note"
              className="border-border bg-panel-strong"
              onChange={(event) => setReason(event.currentTarget.value)}
              placeholder="Why is this export needed?"
              value={reason}
            />
          </label>
          <p className="mt-2 type-caption text-fg-dim" id="log-export-metadata-note">
            Metadata-only export. Retained artifact payloads are never loaded or included by this control.
          </p>
          {action ? (
            <p className={`mt-3 type-caption ${action.tone === 'error' ? 'text-bad' : 'text-good'}`} role="status">
              {action.message}
            </p>
          ) : null}
        </SharedModalBody>
        <SharedModalActionStrip>
          <DialogPrimitive.Close asChild>
            <Button className="ui-control" size="sm" type="button" variant="outline">
              Cancel
            </Button>
          </DialogPrimitive.Close>
          <Button
            className="ui-control-primary"
            disabled={!isReasonValid(reason) || pending}
            onClick={() => void exportLogs()}
            size="sm"
            type="button"
          >
            {pending ? 'Exporting…' : 'Download export'}
          </Button>
        </SharedModalActionStrip>
      </SharedModalContent>
    </SharedModal>
  )
}

function CleanupDialog({
  open,
  onOpenChange,
  onMaintenanceMutationSucceeded,
  query,
  returnFocusRef
}: {
  readonly open: boolean
  readonly onOpenChange: (open: boolean) => void
  readonly onMaintenanceMutationSucceeded?: () => void
  readonly query: LogsRequestQuery
  readonly returnFocusRef: RefObject<HTMLButtonElement | null>
}) {
  const [cutoffBefore, setCutoffBefore] = useState('')
  const [requestLimit, setRequestLimit] = useState('')
  const [reason, setReason] = useState('')
  const [preview, setPreview] = useState<LogCleanupReceipt>()
  const [operation, setOperation] = useState<FrozenLogOperation>()
  const [action, setAction] = useState<ActionState>()
  const [pending, setPending] = useState(false)
  const parsedLimit = Number(requestLimit)
  const validScope =
    supportsCleanup(query) &&
    !Number.isNaN(Date.parse(cutoffBefore)) &&
    Number.isSafeInteger(parsedLimit) &&
    parsedLimit > 0

  function handleOpenChange(nextOpen: boolean) {
    if (!nextOpen) {
      // A cancelled preview is never carried into a later dialog session.
      // Reopening must obtain a new server-side selection before it can run.
      setPreview(undefined)
      setOperation(undefined)
      setAction(undefined)
    }
    onOpenChange(nextOpen)
  }

  async function previewCleanup() {
    if (!validScope || !isReasonValid(reason)) return
    setPending(true)
    setAction(undefined)
    try {
      const nextOperation = { operationId: newOperationId(), reason: reason.trim() }
      const receipt = await new LogsApiClient().previewCleanup({
        operationId: nextOperation.operationId,
        cutoffBefore,
        requestLimit: parsedLimit,
        ...cleanupScopeFromQuery(query),
        reason: nextOperation.reason
      })
      setPreview(receipt)
      setOperation({ operationId: receipt.operationId, reason: nextOperation.reason })
    } catch (error) {
      setAction({ tone: 'error', message: actionError(error) })
    } finally {
      setPending(false)
    }
  }

  async function runCleanup() {
    if (!preview || !operation) return
    setPending(true)
    setAction(undefined)
    try {
      const receipt = await new LogsApiClient().runCleanup(operation)
      setPreview(receipt)
      setAction({
        tone: 'success',
        message: receipt.state === 'partial' ? 'Cleanup completed with diagnostics.' : 'Cleanup completed.'
      })
      onMaintenanceMutationSucceeded?.()
    } catch (error) {
      setAction({ tone: 'error', message: actionError(error) })
    } finally {
      setPending(false)
    }
  }

  return (
    <SharedModal open={open} onOpenChange={handleOpenChange}>
      <SharedModalContent
        onCloseAutoFocus={(event) => {
          if (!returnFocusRef.current) return
          event.preventDefault()
          returnFocusRef.current.focus()
        }}
      >
        <SharedModalHeader>
          <SharedModalTitle>{preview ? 'Confirm scoped cleanup' : 'Preview scoped cleanup'}</SharedModalTitle>
          <SharedModalDescription>
            {preview
              ? 'Review the recorded selection before the server executes this same audited operation.'
              : 'Cleanup applies only to terminal records before the supplied cutoff, within the server-validated request scope.'}
          </SharedModalDescription>
        </SharedModalHeader>
        <SharedModalBody className="space-y-3">
          {!preview ? (
            <>
              <label
                className="grid gap-1.5 text-[length:var(--density-type-caption)] text-fg-dim"
                htmlFor="log-cleanup-cutoff"
              >
                <span className="type-label text-fg-faint">Delete terminal logs before</span>
                <Input
                  id="log-cleanup-cutoff"
                  className="border-border bg-panel-strong font-mono"
                  onChange={(event) => setCutoffBefore(event.currentTarget.value)}
                  placeholder="2026-08-01T00:00:00Z"
                  value={cutoffBefore}
                />
              </label>
              <label
                className="grid gap-1.5 text-[length:var(--density-type-caption)] text-fg-dim"
                htmlFor="log-cleanup-limit"
              >
                <span className="type-label text-fg-faint">Request scope</span>
                <Input
                  id="log-cleanup-limit"
                  inputMode="numeric"
                  min="1"
                  onChange={(event) => setRequestLimit(event.currentTarget.value)}
                  placeholder="Number of matching requests"
                  type="number"
                  value={requestLimit}
                />
              </label>
              <label
                className="grid gap-1.5 text-[length:var(--density-type-caption)] text-fg-dim"
                htmlFor="log-cleanup-reason"
              >
                <span className="type-label text-fg-faint">Required audit reason</span>
                <Input
                  id="log-cleanup-reason"
                  onChange={(event) => setReason(event.currentTarget.value)}
                  placeholder="Why is this scoped cleanup needed?"
                  value={reason}
                />
              </label>
            </>
          ) : (
            <>
              <p className="type-caption text-fg-dim">
                Server-recorded durable scope: cutoff{' '}
                <code className="font-mono text-foreground">{preview.scope.cutoffBefore}</code> · up to{' '}
                {preview.scope.requestLimit} request(s) ·{' '}
                {preview.hasMore ? 'more matching records remain' : 'no additional matching records'}.
              </p>
              <p className="type-caption text-fg-dim">
                Filters:{' '}
                {[
                  preview.scope.from && `from ${preview.scope.from}`,
                  preview.scope.to && `to ${preview.scope.to}`,
                  preview.scope.route && `route ${preview.scope.route}`,
                  preview.scope.model && `model ${preview.scope.model}`,
                  preview.scope.provider && `provider ${preview.scope.provider}`,
                  preview.scope.engine && `engine ${preview.scope.engine}`,
                  preview.scope.outcome && `outcome ${preview.scope.outcome}`
                ]
                  .filter(Boolean)
                  .join(' · ') || 'none'}
              </p>
              <ReceiptDiagnostics receipt={preview} />
            </>
          )}
          {action ? (
            <p className={`type-caption ${action.tone === 'error' ? 'text-bad' : 'text-good'}`} role="status">
              {action.message}
            </p>
          ) : null}
        </SharedModalBody>
        <SharedModalActionStrip>
          <DialogPrimitive.Close asChild>
            <Button className="ui-control" size="sm" type="button" variant="outline">
              Cancel
            </Button>
          </DialogPrimitive.Close>
          {preview?.state === 'previewed' ? (
            <Button
              className="ui-control-destructive"
              disabled={!operation || pending}
              onClick={() => void runCleanup()}
              size="sm"
              type="button"
              variant="outline"
            >
              {pending ? 'Cleaning…' : 'Confirm cleanup'}
            </Button>
          ) : preview && hasRetryableArtifactWork(preview) ? (
            <Button
              className="ui-control-destructive"
              disabled={!operation || pending}
              onClick={() => void runCleanup()}
              size="sm"
              type="button"
              variant="outline"
            >
              {pending ? 'Retrying…' : 'Retry cleanup'}
            </Button>
          ) : !preview ? (
            <Button
              className="ui-control-primary"
              disabled={!validScope || !isReasonValid(reason) || pending}
              onClick={() => void previewCleanup()}
              size="sm"
              type="button"
            >
              {pending ? 'Previewing…' : 'Preview cleanup'}
            </Button>
          ) : null}
        </SharedModalActionStrip>
      </SharedModalContent>
    </SharedModal>
  )
}

export function LogWebhookDeadLetterRetry({ deliveryId }: LogWebhookRetryProps) {
  const contextualDeliveryId = deliveryId?.toString()
  const [enteredDeliveryId, setEnteredDeliveryId] = useState('')
  const deliveryIdValue = contextualDeliveryId ?? enteredDeliveryId
  const [reason, setReason] = useState('')
  const [action, setAction] = useState<ActionState>()
  const [pending, setPending] = useState(false)

  async function retry() {
    if (!isReasonValid(reason)) return
    let typedDeliveryId: LogWebhookDeliveryId
    try {
      typedDeliveryId = LogWebhookDeliveryId.parse(deliveryIdValue.trim())
    } catch {
      setAction({ tone: 'error', message: 'Enter a valid webhook delivery ID before scheduling a retry.' })
      return
    }
    setPending(true)
    setAction(undefined)
    try {
      const receipt = await new LogsApiClient().retryWebhookDelivery(typedDeliveryId, reason.trim())
      setAction({
        tone: 'success',
        message: receipt.outcome === 'scheduled' ? 'Dead-letter retry scheduled.' : 'Retry was already scheduled.'
      })
    } catch (error) {
      setAction({ tone: 'error', message: actionError(error) })
    } finally {
      setPending(false)
    }
  }

  return (
    <div className="space-y-2">
      <label
        className="grid gap-1.5 text-[length:var(--density-type-caption)] text-fg-dim"
        htmlFor="webhook-retry-delivery-id"
      >
        <span className="type-label text-fg-faint">Webhook delivery ID</span>
        <Input
          disabled={contextualDeliveryId !== undefined}
          id="webhook-retry-delivery-id"
          onChange={(event) => setEnteredDeliveryId(event.currentTarget.value)}
          placeholder="Delivery ID from the dead-letter record"
          value={deliveryIdValue}
        />
      </label>
      <label
        className="grid gap-1.5 text-[length:var(--density-type-caption)] text-fg-dim"
        htmlFor="webhook-retry-reason"
      >
        <span className="type-label text-fg-faint">Required audit reason</span>
        <Input
          id="webhook-retry-reason"
          onChange={(event) => setReason(event.currentTarget.value)}
          placeholder="Why retry this dead-letter delivery?"
          value={reason}
        />
      </label>
      <Button
        className="ui-control"
        disabled={deliveryIdValue.trim().length === 0 || !isReasonValid(reason) || pending}
        onClick={() => void retry()}
        size="sm"
        type="button"
        variant="outline"
      >
        <RotateCcw className="size-3.5" aria-hidden="true" />
        {pending ? 'Scheduling…' : 'Retry dead-letter delivery'}
      </Button>
      {action ? (
        <p className={`type-caption ${action.tone === 'error' ? 'text-bad' : 'text-good'}`} role="status">
          {action.message}
        </p>
      ) : null}
    </div>
  )
}

export function LogRequestDeleteControl({ requestId, onMaintenanceMutationSucceeded }: LogRequestDeleteControlProps) {
  const [open, setOpen] = useState(false)
  const [reason, setReason] = useState('')
  const [receipt, setReceipt] = useState<LogDeleteReceipt>()
  const [operation, setOperation] = useState<FrozenLogOperation>()
  const [action, setAction] = useState<ActionState>()
  const [pending, setPending] = useState(false)
  const triggerRef = useRef<HTMLButtonElement | null>(null)

  async function submitDeletion(nextOperation: FrozenLogOperation) {
    setPending(true)
    setAction(undefined)
    try {
      const nextReceipt = await new LogsApiClient().deleteRequest(requestId, nextOperation)
      setReceipt(nextReceipt)
      setOperation(
        (currentOperation) => currentOperation ?? { operationId: nextReceipt.operationId, reason: nextOperation.reason }
      )
      setAction({
        tone: 'success',
        message:
          nextReceipt.state === 'partial' ? 'Request removed with partial cascade diagnostics.' : 'Request removed.'
      })
      onMaintenanceMutationSucceeded?.()
    } catch (error) {
      setAction({ tone: 'error', message: actionError(error) })
    } finally {
      setPending(false)
    }
  }

  async function deleteRequest() {
    if (!isReasonValid(reason)) return
    await submitDeletion({ operationId: newOperationId(), reason: reason.trim() })
  }

  async function retryDeletion() {
    if (!receipt || !operation || !hasRetryableArtifactWork(receipt)) return
    await submitDeletion(operation)
  }

  return (
    <div className="mt-5 border-t border-border-soft pt-3">
      <Button
        ref={triggerRef}
        className="ui-control-destructive"
        onClick={() => setOpen(true)}
        size="sm"
        type="button"
        variant="outline"
      >
        <Trash2 className="size-3.5" aria-hidden="true" />
        Delete terminal request
      </Button>
      <SharedModal open={open} onOpenChange={setOpen}>
        <SharedModalContent
          onCloseAutoFocus={(event) => {
            if (!triggerRef.current) return
            event.preventDefault()
            triggerRef.current.focus()
          }}
        >
          <SharedModalHeader>
            <SharedModalTitle>Delete terminal request?</SharedModalTitle>
            <SharedModalDescription>
              This removes the selected durable request and its retained child records. Review and confirm with an audit
              reason.
            </SharedModalDescription>
          </SharedModalHeader>
          <SharedModalBody className="space-y-3">
            <code className="block break-all font-mono text-[length:var(--density-type-caption)] text-fg-dim">
              {requestId.toString()}
            </code>
            {!receipt ? (
              <label
                className="grid gap-1.5 text-[length:var(--density-type-caption)] text-fg-dim"
                htmlFor="log-delete-reason"
              >
                <span className="type-label text-fg-faint">Required audit reason</span>
                <Input
                  id="log-delete-reason"
                  onChange={(event) => setReason(event.currentTarget.value)}
                  placeholder="Why remove this request?"
                  value={reason}
                />
              </label>
            ) : null}
            {receipt ? <ReceiptDiagnostics receipt={receipt} /> : null}
            {action ? (
              <p className={`type-caption ${action.tone === 'error' ? 'text-bad' : 'text-good'}`} role="status">
                {action.message}
              </p>
            ) : null}
          </SharedModalBody>
          <SharedModalActionStrip>
            <DialogPrimitive.Close asChild>
              <Button className="ui-control" size="sm" type="button" variant="outline">
                Cancel
              </Button>
            </DialogPrimitive.Close>
            {!receipt ? (
              <Button
                className="ui-control-destructive"
                disabled={!isReasonValid(reason) || pending}
                onClick={() => void deleteRequest()}
                size="sm"
                type="button"
                variant="outline"
              >
                {pending ? 'Deleting…' : 'Confirm deletion'}
              </Button>
            ) : hasRetryableArtifactWork(receipt) ? (
              <Button
                className="ui-control-destructive"
                disabled={!operation || pending}
                onClick={() => void retryDeletion()}
                size="sm"
                type="button"
                variant="outline"
              >
                {pending ? 'Retrying…' : 'Retry deletion'}
              </Button>
            ) : null}
          </SharedModalActionStrip>
        </SharedModalContent>
      </SharedModal>
    </div>
  )
}

export function LogOperations({ query, onMaintenanceMutationSucceeded }: LogOperationsProps) {
  const [exportOpen, setExportOpen] = useState(false)
  const [cleanupOpen, setCleanupOpen] = useState(false)
  const exportButtonRef = useRef<HTMLButtonElement | null>(null)
  const cleanupButtonRef = useRef<HTMLButtonElement | null>(null)

  return (
    <section className="flex flex-wrap items-center gap-2" aria-label="Log operations">
      <Button
        ref={exportButtonRef}
        className="ui-control h-8 gap-1.5 rounded-[var(--radius)] px-2.5 text-[length:var(--density-type-caption)]"
        disabled={query.source !== undefined}
        onClick={() => setExportOpen(true)}
        size="sm"
        type="button"
        variant="outline"
      >
        <Download className="size-3.5" aria-hidden="true" />
        Export view
      </Button>
      {query.source !== undefined ? (
        <span className="type-caption text-fg-dim">Clear source selection to export durable records.</span>
      ) : null}
      <Button
        ref={cleanupButtonRef}
        className="ui-control-destructive h-8 gap-1.5 rounded-[var(--radius)] px-2.5 text-[length:var(--density-type-caption)]"
        disabled={!supportsCleanup(query)}
        onClick={() => setCleanupOpen(true)}
        size="sm"
        type="button"
        variant="outline"
      >
        <ShieldAlert className="size-3.5" aria-hidden="true" />
        Scoped cleanup
      </Button>
      {!supportsCleanup(query) ? (
        <span className="type-caption text-fg-dim">
          Clear active source or outcome selection before cleaning durable records.
        </span>
      ) : null}
      <div className="basis-full">
        <LogWebhookDeadLetterRetry />
      </div>
      <ExportDialog open={exportOpen} onOpenChange={setExportOpen} query={query} returnFocusRef={exportButtonRef} />
      <CleanupDialog
        open={cleanupOpen}
        onMaintenanceMutationSucceeded={onMaintenanceMutationSucceeded}
        onOpenChange={setCleanupOpen}
        query={query}
        returnFocusRef={cleanupButtonRef}
      />
    </section>
  )
}
