import { useCallback, useMemo, useState } from 'react'
import { Bar, BarChart, CartesianGrid, Cell, XAxis, YAxis } from 'recharts'
import { Card } from '@/components/ui/card'
import { ChartContainer, ChartTooltip, ChartTooltipContent, type ChartConfig } from '@/components/ui/chart'
import { NativeSelect } from '@/components/ui/NativeSelect'
import type { LogRequest } from '@/features/logs/api/schemas'
import {
  BUCKET_INTERVALS,
  VOLUME_TIME_RANGES,
  buildRequestVolumeBuckets,
  formatBucketRange,
  type BucketIntervalKey,
  type VolumeTimeRangeKey
} from '@/features/logs/lib/log-volume'

type RequestsOverTimeChartProps = {
  readonly rows: readonly LogRequest[]
  /** Test seam: overrides the wall clock used to anchor the time window. */
  readonly now?: number
}

const chartConfig = {
  total: { label: 'Requests', color: 'var(--color-accent)' }
} satisfies ChartConfig

export function RequestsOverTimeChart({ rows, now }: RequestsOverTimeChartProps) {
  const [intervalKey, setIntervalKey] = useState<BucketIntervalKey>('5m')
  const [rangeKey, setRangeKey] = useState<VolumeTimeRangeKey>('12h')
  const [initialNow] = useState(() => Date.now())

  const intervalMs = BUCKET_INTERVALS.find((option) => option.value === intervalKey)?.ms ?? 300_000
  const rangeMs = VOLUME_TIME_RANGES.find((option) => option.value === rangeKey)?.ms ?? 43_200_000
  const current = now ?? initialNow

  const data = useMemo(
    () => buildRequestVolumeBuckets(rows, { intervalMs, rangeMs, now: current }),
    [rows, intervalMs, rangeMs, current]
  )
  const totalRequests = useMemo(() => data.reduce((sum, bucket) => sum + bucket.total, 0), [data])

  const [activeIndex, setActiveIndex] = useState<number | undefined>(undefined)
  const handleBarMouseEnter = useCallback((_entry: unknown, index: number) => setActiveIndex(index), [])
  const handleBarMouseLeave = useCallback(() => setActiveIndex(undefined), [])

  return (
    <Card className="w-full rounded-[var(--radius)] border-border-soft bg-panel px-[var(--panel-x)] py-[var(--panel-y)] shadow-none">
      <div className="flex flex-wrap items-start justify-between gap-x-6 gap-y-3">
        <div className="min-w-0">
          <h2 className="type-panel-title text-foreground">Requests Over Time</h2>
          <p className="type-caption mt-1 text-fg-dim">Request volume by time bucket</p>
        </div>
        <div className="flex flex-wrap items-center gap-2">
          <NativeSelect
            ariaLabel="Bucket interval"
            className="w-[6.5rem] min-w-0"
            name="volume-bucket-interval"
            onValueChange={(value) => setIntervalKey(value as BucketIntervalKey)}
            options={BUCKET_INTERVALS.map(({ value, label }) => ({ value, label }))}
            value={intervalKey}
          />
          <NativeSelect
            ariaLabel="Chart time range"
            className="w-[10rem] min-w-0"
            name="volume-time-range"
            onValueChange={(value) => setRangeKey(value as VolumeTimeRangeKey)}
            options={VOLUME_TIME_RANGES.map(({ value, label }) => ({ value, label }))}
            value={rangeKey}
          />
        </div>
      </div>

      {totalRequests === 0 ? (
        <div className="flex h-[170px] items-center justify-center">
          <p className="type-caption text-fg-dim">No requests during the selected time range.</p>
        </div>
      ) : (
        <div className="mt-4 h-[170px] w-full">
          <ChartContainer config={chartConfig} className="h-full w-full" aria-label="Requests over time bar chart">
            <BarChart data={data} margin={{ top: 8, right: 4, left: 0, bottom: 0 }} barCategoryGap={1.5}>
              <CartesianGrid vertical={false} stroke="var(--color-border-soft)" />
              <XAxis
                axisLine={false}
                dataKey="label"
                minTickGap={48}
                tick={{ fill: 'var(--color-fg-faint)', fontSize: 11 }}
                tickLine={false}
                tickMargin={8}
              />
              <YAxis
                allowDecimals={false}
                axisLine={false}
                tick={{ fill: 'var(--color-fg-faint)', fontSize: 11 }}
                tickLine={false}
                tickFormatter={(value: number) => (value >= 1000 ? `${Math.round(value / 1000)}k` : String(value))}
                width={36}
              />
              <ChartTooltip
                content={
                  <ChartTooltipContent
                    formatter={(value) => `${String(value)} requests`}
                    hideName
                    labelFormatter={(_label, payload) => {
                      const first = payload[0]?.payload
                      return formatBucketRange(Number(first?.bucketStart), Number(first?.bucketEnd))
                    }}
                    labelKey="label"
                  />
                }
                cursor={{ fill: 'var(--color-fg-faint)', fillOpacity: 0.08 }}
              />
              <Bar
                dataKey="total"
                fill="var(--color-total)"
                isAnimationActive={false}
                maxBarSize={8}
                onMouseEnter={handleBarMouseEnter}
                onMouseLeave={handleBarMouseLeave}
                radius={[2, 2, 0, 0]}
              >
                {data.map((bucket, index) => (
                  <Cell
                    key={bucket.bucketStart}
                    fillOpacity={activeIndex === undefined || activeIndex === index ? 1 : 0.35}
                  />
                ))}
              </Bar>
            </BarChart>
          </ChartContainer>
        </div>
      )}
    </Card>
  )
}
