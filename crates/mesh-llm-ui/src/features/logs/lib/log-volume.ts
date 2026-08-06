import type { LogRequest } from '@/features/logs/api/schemas'

export type BucketIntervalKey = '1m' | '5m' | '15m' | '30m' | '1h'
export type VolumeTimeRangeKey = '1h' | '6h' | '12h' | '24h' | 'all'

export const BUCKET_INTERVALS: readonly { value: BucketIntervalKey; label: string; ms: number }[] = [
  { value: '1m', label: '1m', ms: 60_000 },
  { value: '5m', label: '5m', ms: 300_000 },
  { value: '15m', label: '15m', ms: 900_000 },
  { value: '30m', label: '30m', ms: 1_800_000 },
  { value: '1h', label: '1h', ms: 3_600_000 }
]

export const VOLUME_TIME_RANGES: readonly { value: VolumeTimeRangeKey; label: string; ms: number }[] = [
  { value: '1h', label: 'Last hour', ms: 3_600_000 },
  { value: '6h', label: 'Last 6 hours', ms: 21_600_000 },
  { value: '12h', label: 'Last 12 hours', ms: 43_200_000 },
  { value: '24h', label: 'Last 24 hours', ms: 86_400_000 },
  { value: 'all', label: 'All time', ms: Number.POSITIVE_INFINITY }
]

export type RequestVolumeBucket = {
  readonly bucketStart: number
  readonly bucketEnd: number
  readonly label: string
  readonly total: number
}

/**
 * Bucket request-log entries by `createdAt` into fixed-width time buckets.
 *
 * `now` is injected so callers can render deterministically in tests. Finite
 * `rangeMs` windows are anchored to `[now - rangeMs, now]`; an infinite range
 * spans the earliest to the latest request. Buckets are emitted contiguously
 * (including zero-count buckets) so the bar chart renders a true timeline.
 */
export function buildRequestVolumeBuckets(
  rows: readonly LogRequest[],
  options: { readonly intervalMs: number; readonly rangeMs: number; readonly now: number }
): RequestVolumeBucket[] {
  const { intervalMs, rangeMs, now } = options
  if (intervalMs <= 0) return []

  const timestamps: number[] = []
  for (const row of rows) {
    const parsed = Date.parse(row.createdAt)
    if (!Number.isNaN(parsed)) timestamps.push(parsed)
  }
  if (timestamps.length === 0) return []

  let startBoundary: number
  let endBoundary: number
  if (Number.isFinite(rangeMs)) {
    startBoundary = now - rangeMs
    endBoundary = now
  } else {
    startBoundary = timestamps[0]
    endBoundary = timestamps[0]
    for (const timestamp of timestamps) {
      if (timestamp < startBoundary) startBoundary = timestamp
      if (timestamp > endBoundary) endBoundary = timestamp
    }
  }

  const firstIndex = Math.floor(startBoundary / intervalMs)
  const lastIndex = Math.floor(endBoundary / intervalMs)
  const bucketCount = lastIndex - firstIndex + 1
  const totals = new Array<number>(bucketCount).fill(0)

  for (const timestamp of timestamps) {
    if (timestamp < startBoundary || timestamp > endBoundary) continue
    const index = Math.floor(timestamp / intervalMs) - firstIndex
    if (index >= 0 && index < bucketCount) totals[index] += 1
  }

  const buckets: RequestVolumeBucket[] = []
  for (let index = 0; index < bucketCount; index += 1) {
    const bucketStart = (firstIndex + index) * intervalMs
    buckets.push({
      bucketStart,
      bucketEnd: bucketStart + intervalMs,
      label: formatBucketTick(bucketStart, intervalMs),
      total: totals[index]
    })
  }
  return buckets
}

export function formatClock(ms: number): string {
  const date = new Date(ms)
  const rawHours = date.getHours()
  const minutes = date.getMinutes()
  const period = rawHours >= 12 ? 'PM' : 'AM'
  const hours = rawHours % 12 === 0 ? 12 : rawHours % 12
  return `${hours}:${String(minutes).padStart(2, '0')} ${period}`
}

function formatHour(ms: number): string {
  const date = new Date(ms)
  const rawHours = date.getHours()
  const period = rawHours >= 12 ? 'PM' : 'AM'
  const hours = rawHours % 12 === 0 ? 12 : rawHours % 12
  return `${hours} ${period}`
}

export function formatBucketTick(bucketStart: number, intervalMs: number): string {
  return intervalMs >= 3_600_000 ? formatHour(bucketStart) : formatClock(bucketStart)
}

export function formatBucketRange(bucketStart: number, bucketEnd: number): string {
  return `${formatClock(bucketStart)}\u2013${formatClock(bucketEnd)}`
}
