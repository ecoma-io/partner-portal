<script setup lang="ts">
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { useDashboardStore, type RequestItem, type TimeseriesPoint } from '@/stores/dashboard'

const store = useDashboardStore()
const {
  me,
  summary,
  timeseries,
  requests,
  loading,
  error,
  range,
  model,
  status,
  pageSize,
  dataChangeVersion,
  hasMore,
  hasPrevious,
  currentPage,
  models,
  isManager,
  selectedConsumers,
  latestRequest,
} = storeToRefs(store)

const rangeOptions = [
  { value: 'today', label: 'Today' },
  { value: '24h', label: 'Last 24h' },
  { value: '7d', label: 'Last 7 days' },
  { value: '14d', label: 'Last 14 days' },
  { value: '30d', label: 'Last 30 days' },
]

const pageSizeOptions = [10, 20, 50, 100, 200]

const isMobile = ref(isNarrow())
const alertsEnabled = computed(() => store.automaticUpdatesEnabled)

interface ChangeAlert {
  id: number
}

const changeAlerts = ref<ChangeAlert[]>([])
const alertTimers = new Map<number, ReturnType<typeof setTimeout>>()
let nextAlertId = 0

/**
 * The newest request seen, shaped for the toast. Read live rather than snapped
 * at announce time: an SSE event bumps the version *before* the refetch lands,
 * so a snapshot would show the previous request. Rendering against this makes
 * the toast catch up to the newest request as soon as the fetch completes.
 */
const liveChange = computed(() => {
  const request = latestRequest.value
  if (request === null) return null
  return {
    model: request.model,
    httpStatus: request.http_status,
    duration: formatDuration(request.duration_ms),
  }
})

function isNarrow() {
  return typeof window !== 'undefined' && window.innerWidth < 700
}

function dismissAlert(id: number) {
  const timer = alertTimers.get(id)
  if (timer) clearTimeout(timer)
  alertTimers.delete(id)
  changeAlerts.value = changeAlerts.value.filter((alert) => alert.id !== id)
}

function scheduleAlertDismiss(id: number) {
  const existing = alertTimers.get(id)
  if (existing) clearTimeout(existing)
  alertTimers.set(id, setTimeout(() => dismissAlert(id), 5_000))
}

/**
 * Every data change is its own toast, and a new arrival starts the oldest one
 * fading immediately — the stack holds at most two, so a burst reads as a
 * stream of fresh toasts instead of one throttled toast stretching over it.
 */
function announceDataChange() {
  if (!alertsEnabled.value) return

  const alert = { id: ++nextAlertId }
  if (changeAlerts.value.length === 2) dismissAlert(changeAlerts.value[0].id)
  changeAlerts.value.push(alert)
  scheduleAlertDismiss(alert.id)
}

function toggleAlerts() {
  const next = !alertsEnabled.value
  void store.setAutomaticUpdates(next)
  if (!next) {
    for (const alert of changeAlerts.value) dismissAlert(alert.id)
  }
}

onMounted(async () => {
  window.addEventListener('resize', onResize)
  // The SVG is drawn in real pixel units so labels and strokes never scale
  // with the container; the observer is what keeps the geometry honest.
  if (chartEl.value && typeof ResizeObserver !== 'undefined') {
    chartObserver = new ResizeObserver((entries) => {
      const width = entries[0]?.contentRect.width
      if (width && width > 0) chartWidth.value = Math.round(width)
    })
    chartObserver.observe(chartEl.value)
  }
  // Only fetch when the store is already authenticated. This view also mounts on
  // the way *into* the dashboard, before `signIn` has written the key to
  // localStorage — refreshing unconditionally there fires every dashboard
  // query with no Authorization header, and the server answers each one 401.
  // Signing in fetches for itself; a reload lands here with `authenticated`
  // already true, because `App.vue` validated the stored key before rendering.
  if (store.authenticated) {
    await store.refresh()
    store.connect()
  }
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', onResize)
  chartObserver?.disconnect()
  chartObserver = null
  for (const timer of alertTimers.values()) clearTimeout(timer)
  alertTimers.clear()
  store.disconnect()
})

function onResize() {
  isMobile.value = isNarrow()
}

function reloadForRangeOrModel() {
  // The bucket sequence is rebuilt, so a crosshair parked on the old index
  // would point at a different interval than the one it inspected.
  activeIndex.value = null
  store.resetAndFetchRequests()
  void Promise.all([store.fetchSummary(), store.fetchTimeseries(), store.fetchModels()])
}

watch([range, model], reloadForRangeOrModel)
watch(dataChangeVersion, (version) => {
  if (version > 0) announceDataChange()
})

function formatNumber(n: number | null | undefined): string {
  if (n === null || n === undefined) return '—'
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toLocaleString()
}

/**
 * The exact value, grouped with Vietnamese thousands separators (periods).
 * The summary cards are a billing surface: a count that is real must never be
 * hidden behind a K/M abbreviation, so they render this directly. Only the
 * chart axis still abbreviates for layout — the chart tooltip uses this form.
 */
const fullNumber = new Intl.NumberFormat('vi-VN', { maximumFractionDigits: 0 })
function formatFullNumber(n: number | null | undefined): string {
  if (n === null || n === undefined) return '—'
  return fullNumber.format(n)
}

/**
 * Milliseconds are whole numbers — an average is rounded, never shown with
 * decimals — and seconds carry exactly two. Billing reads these figures, so a
 * value like 1234.5ms renders as "1.23s", not "1234.5ms".
 */
function formatDuration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return '—'
  if (ms < 1000) return `${Math.round(ms)}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

function formatPercent(rate: number | null | undefined): string {
  if (rate === null || rate === undefined) return '—'
  return `${(rate * 100).toFixed(1)}%`
}

function formatDate(iso: string): string {
  return new Date(iso).toLocaleString()
}

function cachedRate(): string {
  if (!summary.value || summary.value.total_input_tokens === 0) return '—'
  return formatPercent(summary.value.total_cached_tokens / summary.value.total_input_tokens)
}

function endpointLabel(endpoint: string): string {
  // The three proxied paths, rendered short. `models` is unmetered and never
  // becomes a ledger row, so it realistically never appears here — but if it
  // does, say so rather than mislabeling it as completions.
  if (endpoint === 'responses') return 'responses'
  if (endpoint === 'chat_completions') return 'completions'
  return endpoint
}

function endpointDescription(request: RequestItem): string {
  return `${request.streaming ? 'Streaming' : 'Non-streaming'} ${endpointLabel(request.endpoint)}`
}

function statusClass(httpStatus: number | null): string {
  if (httpStatus === null) return 'unknown'
  if (httpStatus >= 200 && httpStatus < 300) return 'success'
  if (httpStatus >= 300 && httpStatus < 400) return 'redirect'
  if (httpStatus >= 400 && httpStatus < 500) return 'client-error'
  if (httpStatus >= 500 && httpStatus < 600) return 'server-error'
  return 'unknown'
}

function hasRequestDetail(request: RequestItem): boolean {
  return request.http_status === null || request.http_status < 200 || request.http_status >= 300 || request.request_status !== 'completed'
}

function updateStatus(event: Event) {
  store.setStatus((event.target as HTMLSelectElement).value)
}

function updatePageSize(event: Event) {
  store.setPageSize(Number((event.target as HTMLSelectElement).value))
}

/**
 * Toggle one consumer on the manager's multi-select. Any change re-scopes every
 * query — the signature includes the selection, so the requests table goes back
 * to page one and the summary/chart refetch for the narrower view.
 */
function toggleConsumer(consumer: string) {
  const next = selectedConsumers.value.includes(consumer)
    ? selectedConsumers.value.filter((entry) => entry !== consumer)
    : [...selectedConsumers.value, consumer]
  selectedConsumers.value = next
  store.resetAndFetchRequests()
  void Promise.all([store.fetchSummary(), store.fetchTimeseries(), store.fetchModels()])
}

/**
 * The "All" chip: clear the narrowing entirely. An empty selection is how the
 * store says "every consumer" — the manager's default view — so the guard only
 * skips the refetch when nothing would change.
 */
function selectAllConsumers() {
  if (selectedConsumers.value.length === 0) return
  selectedConsumers.value = []
  store.resetAndFetchRequests()
  void Promise.all([store.fetchSummary(), store.fetchTimeseries(), store.fetchModels()])
}

interface ChartBucket {
  key: string
  label: string
  fullLabel: string
  requests: number
  success_count: number
  failure_count: number
  input_tokens: number
  output_tokens: number
  cached_tokens: number
  /** Rollup inputs, not means: an interval's mean is recomputed after hours merge. */
  total_duration_ms: number
  total_ttft_ms: number
  ttft_count: number
}

function localDayKey(date: Date): string {
  return `${date.getFullYear()}-${date.getMonth()}-${date.getDate()}`
}

function localHourKey(date: Date): string {
  return `${localDayKey(date)}-${date.getHours()}`
}

function hourLabel(date: Date): string {
  return new Intl.DateTimeFormat(undefined, { hour: 'numeric' }).format(date)
}

function dayLabel(date: Date): string {
  return new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric' }).format(date)
}

function fullHourLabel(date: Date): string {
  return new Intl.DateTimeFormat(undefined, {
    month: 'short',
    day: 'numeric',
    hour: 'numeric',
    minute: '2-digit',
  }).format(date)
}

function fullDayLabel(date: Date): string {
  return new Intl.DateTimeFormat(undefined, { month: 'short', day: 'numeric', year: 'numeric' }).format(date)
}

function aggregateTimeseries(points: TimeseriesPoint[], selectedRange: string): ChartBucket[] {
  const isHourly = selectedRange === 'today' || selectedRange === '24h'
  const count = isHourly ? 24 : Number.parseInt(selectedRange, 10)
  const now = new Date()
  now.setMinutes(0, 0, 0)
  const first = new Date(now)

  if (selectedRange === 'today') {
    first.setHours(0, 0, 0, 0)
  } else if (isHourly) {
    first.setHours(first.getHours() - (count - 1))
  } else {
    first.setHours(0, 0, 0, 0)
    first.setDate(first.getDate() - (count - 1))
  }

  const buckets: ChartBucket[] = Array.from({ length: count }, (_, index) => {
    const date = new Date(first)
    if (isHourly) date.setHours(date.getHours() + index)
    else date.setDate(date.getDate() + index)
    return {
      key: isHourly ? localHourKey(date) : localDayKey(date),
      label: isHourly ? hourLabel(date) : dayLabel(date),
      fullLabel: isHourly ? fullHourLabel(date) : fullDayLabel(date),
      requests: 0,
      success_count: 0,
      failure_count: 0,
      input_tokens: 0,
      output_tokens: 0,
      cached_tokens: 0,
      total_duration_ms: 0,
      total_ttft_ms: 0,
      ttft_count: 0,
    }
  })

  const byKey = new Map(buckets.map((bucket) => [bucket.key, bucket]))
  for (const point of points) {
    // The backend emits `hour` as a UTC hour string like "2026-09-24T11". Parsing
    // it with `new Date` would read it in local time (or reject it outright), so
    // pull the wall-clock components out and interpret them as UTC, then let the
    // local bucket keys place each point in the right local-time column.
    const timestamp = utcHourToDate(point.hour)
    if (Number.isNaN(timestamp.getTime())) continue
    const bucket = byKey.get(isHourly ? localHourKey(timestamp) : localDayKey(timestamp))
    if (!bucket) continue
    bucket.requests += point.requests
    bucket.success_count += point.success_count
    bucket.failure_count += point.failure_count
    bucket.input_tokens += point.input_tokens
    bucket.output_tokens += point.output_tokens
    bucket.cached_tokens += point.cached_tokens
    // Latency inputs accumulate as sums so a daily bucket can re-weight the
    // hourly means it merges (dividing merged means would misweight by volume).
    bucket.total_duration_ms += point.total_duration_ms
    bucket.total_ttft_ms += point.total_ttft_ms
    bucket.ttft_count += point.ttft_count
  }
  return buckets
}

/**
 * Parse a UTC hour string ("2026-09-24T11") as a UTC instant. The string has no
 * timezone suffix, so `new Date(str)` would interpret it in the local timezone
 * (or reject it, when the local hour notation is invalid). Dates are what the
 * bucket keys below want, so return one.
 */
function utcHourToDate(hour: string): Date {
  const match = /^(\d{4})-(\d{2})-(\d{2})[T ](\d{2})$/.exec(hour.trim())
  if (match === null) return new Date(NaN)
  const [, year, month, day, hours] = match
  return new Date(Date.UTC(Number(year), Number(month) - 1, Number(day), Number(hours)))
}

const chartBuckets = computed(() => aggregateTimeseries(timeseries.value, range.value))

// --- Selectable line chart --------------------------------------------------
//
// One chart, three metric selections, drawn as SVG paths in real pixel
// coordinates (the container is measured, so text and strokes never scale).
// A single y-scale per selection — never two axes. Series colors are the
// validated fixed-order categorical set against the #1a1a1a card surface;
// status meaning is carried by labels, not by hue.

const BLUE = '#3987e5'
const ORANGE = '#d95926'
const GREEN = '#199e70'

type Metric = 'requests' | 'tokens' | 'latency'

interface SeriesDef {
  key: string
  label: string
  color: string
  /** The interval's value, or null when the interval has no reportable value (a gap, never a fabricated 0). */
  value: (b: ChartBucket) => number | null
}

const SERIES_BY_METRIC: Record<Metric, SeriesDef[]> = {
  requests: [
    { key: 'total', label: 'Total', color: BLUE, value: b => b.requests },
    { key: 'success', label: 'Successful', color: ORANGE, value: b => b.success_count },
    { key: 'failure', label: 'Failed', color: GREEN, value: b => b.failure_count },
  ],
  tokens: [
    { key: 'input', label: 'Input', color: BLUE, value: b => b.input_tokens },
    { key: 'cached', label: 'Cached', color: ORANGE, value: b => b.cached_tokens },
    { key: 'output', label: 'Output', color: GREEN, value: b => b.output_tokens },
  ],
  latency: [
    // Weighted means re-derived from the accumulated sums; nulls where nothing
    // reported — an hour with no stream reports no TTFT, and a bucket with no
    // requests reports no latency. Unavailable is not zero.
    { key: 'ttft', label: 'TTFT', color: BLUE, value: b => b.ttft_count > 0 ? b.total_ttft_ms / b.ttft_count : null },
    { key: 'latency', label: 'Latency', color: ORANGE, value: b => b.requests > 0 ? b.total_duration_ms / b.requests : null },
  ],
}

const metricOptions: { value: Metric; label: string }[] = [
  { value: 'requests', label: 'Requests' },
  { value: 'tokens', label: 'Tokens' },
  { value: 'latency', label: 'Latency' },
]

const metric = ref<Metric>('requests')
const metricTitle = computed(() => metricOptions.find(o => o.value === metric.value)?.label ?? 'Requests')

const PAD = { top: 14, right: 84, bottom: 28, left: 52 }
const MIN_CHART_H = 260
const DESKTOP_CHART_H = 340

const chartEl = ref<HTMLElement | null>(null)
const chartWidth = ref(900)
/** Measured container width; ResizeObserver keeps the SVG in 1:1 pixel units. */
let chartObserver: ResizeObserver | null = null

const chartHeightPx = computed(() => (isMobile.value ? MIN_CHART_H : DESKTOP_CHART_H))
const plotW = computed(() => Math.max(40, chartWidth.value - PAD.left - PAD.right))
const plotH = computed(() => Math.max(40, chartHeightPx.value - PAD.top - PAD.bottom))

function niceStep(raw: number): number {
  if (!(raw > 0)) return 1
  const exp = Math.floor(Math.log10(raw))
  const base = 10 ** exp
  const frac = raw / base
  const nice = frac <= 1 ? 1 : frac <= 2 ? 2 : frac <= 5 ? 5 : 10
  const step = nice * base
  // Counts are whole things; fractional y ticks read as measurement noise.
  return metric.value === 'latency' ? step : Math.max(1, step)
}

/** Top of the y-scale: four equal, rounded steps above the largest value. */
const yMax = computed(() => {
  let max = 0
  const series = SERIES_BY_METRIC[metric.value]
  for (const bucket of chartBuckets.value) {
    for (const s of series) {
      const v = s.value(bucket)
      if (v !== null && v > max) max = v
    }
  }
  return niceStep(max / 4) * 4
})

/**
 * A y-axis carries one unit. `formatDuration` switches from ms to s at 1000, so
 * left alone it labels one axis `0ms … 2.00s` — two units on one scale, which
 * reads as two axes. The axis unit is chosen from its top value and every tick
 * is rendered in it.
 */
function formatDurationIn(value: number, top: number): string {
  const useSeconds = top >= 1000
  if (useSeconds) {
    const s = value / 1000
    return `${s.toFixed(Number.isInteger(s) ? 0 : 2)}s`
  }
  return `${Math.round(value)}ms`
}

const yTicks = computed(() => {
  const top = yMax.value
  return [0, 1, 2, 3, 4].map((i) => {
    const v = (top / 4) * i
    return {
      v,
      y: yFor(v),
      label: metric.value === 'latency' ? formatDurationIn(v, top) : formatNumber(v),
    }
  })
})

function xFor(index: number, n: number): number {
  if (n <= 1) return PAD.left + plotW.value / 2
  return PAD.left + (index * plotW.value) / (n - 1)
}

function yFor(value: number): number {
  const max = yMax.value || 1
  const clamped = Math.min(Math.max(value, 0), max)
  return PAD.top + plotH.value * (1 - clamped / max)
}

interface RenderedSeries {
  key: string
  label: string
  color: string
  /** SVG path data; gaps (null values) break the path instead of plotting 0. */
  d: string
  endpoint: { x: number; y: number } | null
}

const renderedSeries = computed<RenderedSeries[]>(() => {
  const buckets = chartBuckets.value
  const n = buckets.length
  return SERIES_BY_METRIC[metric.value].map((s) => {
    let d = ''
    let pen = false
    let endpoint: { x: number; y: number } | null = null
    buckets.forEach((bucket, i) => {
      const v = s.value(bucket)
      if (v === null) {
        pen = false
        return
      }
      const x = xFor(i, n)
      const y = yFor(v)
      d += `${pen ? 'L' : 'M'} ${x.toFixed(1)} ${y.toFixed(1)} `
      pen = true
      endpoint = { x, y } // walking forward leaves the latest valid point here
    })
    return { key: s.key, label: s.label, color: s.color, d: d.trim(), endpoint }
  })
})

/** Endpoint labels, de-overlapped vertically so two close series stay readable. */
const directLabels = computed(() => {
  const items = renderedSeries.value
    .filter((s): s is RenderedSeries & { endpoint: { x: number; y: number } } => s.endpoint !== null)
    .map(s => ({ x: s.endpoint.x, y: s.endpoint.y, label: s.label }))
    .sort((a, b) => a.y - b.y)
  const minGap = 15
  for (let i = 1; i < items.length; i++) {
    if (items[i].y - items[i - 1].y < minGap) items[i].y = items[i - 1].y + minGap
  }
  const minY = PAD.top + 6
  const maxY = PAD.top + plotH.value
  for (const item of items) item.y = Math.min(Math.max(item.y, minY), maxY)
  return items
})

const xLabels = computed(() => {
  const buckets = chartBuckets.value
  const n = buckets.length
  if (n === 0) return []
  if (n === 1) return [{ text: buckets[0].label, x: xFor(0, 1), anchor: 'middle' as const }]
  const mid = Math.floor((n - 1) / 2)
  return [
    { text: buckets[0].label, x: xFor(0, n), anchor: 'start' as const },
    { text: buckets[mid].label, x: xFor(mid, n), anchor: 'middle' as const },
    { text: buckets[n - 1].label, x: xFor(n - 1, n), anchor: 'end' as const },
  ].filter((label, i, all) => i === 0 || label.x - all[i - 1].x > 48)
})

/** True when no interval carries any reportable value for the selected metric. */
const chartEmpty = computed(() => {
  const buckets = chartBuckets.value
  if (buckets.length === 0) return true
  return SERIES_BY_METRIC[metric.value].every(s => buckets.every(b => s.value(b) === null))
})

const chartDescription = computed(() => {
  const names = SERIES_BY_METRIC[metric.value].map(s => s.label).join(', ')
  const rangeLabel = rangeOptions.find(o => o.value === range.value)?.label ?? range.value
  return `Line chart of ${names} over ${rangeLabel}. Use the left and right arrow keys to inspect each interval; the same values are in the table that follows.`
})

// --- Crosshair + tooltip ----------------------------------------------------

const activeIndex = ref<number | null>(null)
/** Pointer-driven inspection must not fire the screen reader's live region. */
const inputMode = ref<'pointer' | 'keyboard'>('pointer')

function setActive(index: number | null, mode: 'pointer' | 'keyboard') {
  inputMode.value = mode
  activeIndex.value = index
}

function hitRect(index: number) {
  const n = chartBuckets.value.length
  if (n <= 1) return { x: PAD.left, w: plotW.value }
  const step = plotW.value / (n - 1)
  const center = xFor(index, n)
  const x0 = Math.max(PAD.left, center - step / 2)
  const x1 = Math.min(PAD.left + plotW.value, center + step / 2)
  return { x: x0, w: Math.max(1, x1 - x0) }
}

interface TooltipModel {
  bucket: ChartBucket
  rows: { label: string; color: string; text: string }[]
  left: string
  flip: boolean
}

function formatMetricValue(v: number | null): string {
  if (v === null) return '—'
  return metric.value === 'latency' ? formatDuration(Math.round(v)) : formatFullNumber(v)
}

const tooltip = computed<TooltipModel | null>(() => {
  const index = activeIndex.value
  if (index === null) return null
  const bucket = chartBuckets.value[index]
  if (!bucket) return null
  const rows = SERIES_BY_METRIC[metric.value].map(s => ({
    label: s.label,
    color: s.color,
    text: formatMetricValue(s.value(bucket)),
  }))
  const xPct = (xFor(index, chartBuckets.value.length) / chartWidth.value) * 100
  return { bucket, rows, left: `${xPct}%`, flip: xPct > 65 }
})

/** Screen-reader mirror of the tooltip, written only during keyboard inspection. */
const keyboardAnnouncement = computed(() => {
  if (inputMode.value !== 'keyboard') return ''
  const model = tooltip.value
  if (!model) return ''
  return `${model.bucket.fullLabel}. ${model.rows.map(r => `${r.label}: ${r.text}`).join('. ')}`
})

function onChartKeydown(event: KeyboardEvent) {
  const n = chartBuckets.value.length
  if (n === 0) return
  const current = activeIndex.value
  let next: number | null = current
  if (event.key === 'ArrowRight') next = current === null ? 0 : Math.min(n - 1, current + 1)
  else if (event.key === 'ArrowLeft') next = current === null ? n - 1 : Math.max(0, current - 1)
  else if (event.key === 'Home') next = 0
  else if (event.key === 'End') next = n - 1
  else if (event.key === 'Escape') next = null
  else return
  event.preventDefault()
  setActive(next, 'keyboard')
}

function onChartFocus() {
  if (activeIndex.value === null && chartBuckets.value.length > 0) {
    setActive(chartBuckets.value.length - 1, 'keyboard')
  }
}

function onChartBlur() {
  setActive(null, 'keyboard')
}

function onChartLeave() {
  setActive(null, 'pointer')
}

/** Marks drawn on each series at the crosshair; series with a gap here are skipped. */
const activePoints = computed(() => {
  const index = activeIndex.value
  if (index === null) return []
  const bucket = chartBuckets.value[index]
  if (!bucket) return []
  const n = chartBuckets.value.length
  return SERIES_BY_METRIC[metric.value]
    .map(s => ({ key: s.key, color: s.color, v: s.value(bucket) }))
    .filter((p): p is { key: string; color: string; v: number } => p.v !== null)
    .map(p => ({ key: p.key, color: p.color, x: xFor(index, n), y: yFor(p.v) }))
})
</script>

<template>
  <div class="dashboard">
    <header class="header">
      <div class="header-left">
        <h1>Partner Portal</h1>
      </div>
      <div class="header-controls">
        <button
          class="icon-button live-toggle"
          type="button"
          :aria-pressed="alertsEnabled"
          :aria-label="alertsEnabled ? 'Pause automatic updates and data-change alerts' : 'Resume automatic updates and data-change alerts'"
          :title="alertsEnabled ? 'Pause automatic updates and data-change alerts' : 'Resume automatic updates and data-change alerts'"
          @click="toggleAlerts"
        >
          <svg v-if="alertsEnabled" class="icon" viewBox="0 0 16 16" aria-hidden="true">
            <path d="M4 3h2.4v10H4zM9.6 3H12v10H9.6z" fill="currentColor"></path>
          </svg>
          <svg v-else class="icon" viewBox="0 0 16 16" aria-hidden="true">
            <path d="M4.5 3l8 5-8 5z" fill="currentColor"></path>
          </svg>
        </button>
        <button
          class="icon-button signout"
          type="button"
          aria-label="Sign out — forget the stored key and return to the login screen"
          title="Sign out — forget the stored key and return to the login screen"
          @click="store.signOut"
        >
          <svg class="icon" viewBox="0 0 16 16" aria-hidden="true">
            <path d="M2 2h6v1.5H3.5v9H8V14H2z" fill="currentColor"></path>
            <path d="M10.5 3.5l4 4.5-4 4.5-.9-.8L12.7 8.5H6v-1h6.7l-3.1-3.2z" fill="currentColor"></path>
          </svg>
        </button>
      </div>
    </header>

    <!-- Consumer identity + data filters, below the header. -->
    <div class="filter-row">
      <div v-if="isManager" class="filter-side consumer-filter">
        <span class="filter-label" aria-hidden="true">Consumers</span>
        <div class="consumer-toggles">
          <button
            type="button"
            class="consumer-chip"
            :aria-pressed="selectedConsumers.length === 0"
            :title="selectedConsumers.length === 0 ? 'Showing every consumer' : 'Show every consumer'"
            @click="selectAllConsumers"
          >
            All
          </button>
          <button
            v-for="consumer in me?.consumers ?? []"
            :key="consumer"
            type="button"
            class="consumer-chip"
            :aria-pressed="selectedConsumers.includes(consumer)"
            :title="selectedConsumers.includes(consumer) ? 'Showing only this consumer' : 'Include this consumer'"
            @click="toggleConsumer(consumer)"
          >
            {{ consumer }}
          </button>
        </div>
        <!-- The selector is fed from the ledger: nothing recorded yet means
             nothing to offer but All (docs/adr/0013). -->
        <p v-if="!me?.consumers?.length" class="filter-muted">No usage recorded yet</p>
      </div>
      <div v-else-if="me" class="filter-side whoami" :title="`key: ${me.key_name}`">
        <span class="filter-label">Key</span>
        <span class="whoami-name">{{ me.key_name }}</span>
        <span class="whoami-consumer">{{ me.consumer_id }}</span>
      </div>
      <div class="filter-side">
        <div class="header-filters">
          <select v-model="range" aria-label="Time range">
            <option v-for="opt in rangeOptions" :key="opt.value" :value="opt.value">{{ opt.label }}</option>
          </select>
          <select v-model="model" aria-label="Model">
            <option value="all">All models</option>
            <option v-for="item in models" :key="item" :value="item">{{ item }}</option>
          </select>
        </div>
      </div>
    </div>

    <div class="alert-stack" aria-live="polite" aria-relevant="additions text">
      <TransitionGroup name="data-alert">
        <div v-for="alert in changeAlerts" :key="alert.id" class="data-alert" role="status">
          <span v-if="liveChange">
            <strong class="alert-model">{{ liveChange.model }}</strong>
            <span class="alert-sep">·</span>
            <span :class="['alert-status', statusClass(liveChange.httpStatus)]">{{ liveChange.httpStatus ?? '-' }}</span>
            <span class="alert-sep">·</span>
            <span class="alert-duration">{{ liveChange.duration }}</span>
          </span>
          <span v-else>Dashboard data updated</span>
          <button type="button" class="alert-dismiss" :aria-label="'Dismiss update alert'" @click="dismissAlert(alert.id)">×</button>
        </div>
      </TransitionGroup>
    </div>

    <div v-if="error" class="error-banner" role="alert">{{ error }}</div>

    <section class="summary" aria-label="Usage summary">
      <article class="card summary-card">
        <div class="summary-label">Requests</div>
        <!-- One number only: the successful total, with the parentheses naming
             it. Failures stay visible in the chart's Failed series and the
             table's status column; usage that the provider never reported is
             still flagged per request in the table. -->
        <div class="summary-value">{{ formatFullNumber(summary?.success_count) }}</div>
        <div class="summary-sub">(total successful requests)</div>
      </article>
      <article class="card summary-card">
        <div class="summary-label">Input tokens</div>
        <div class="summary-value">{{ formatFullNumber(summary?.total_input_tokens) }}</div>
        <div class="summary-metrics">
          <span><strong class="accent-text">{{ formatFullNumber(summary?.total_cached_tokens) }}</strong> cached</span>
          <span><strong>{{ cachedRate() }}</strong> cached rate</span>
        </div>
      </article>
      <article class="card summary-card">
        <div class="summary-label">Output tokens</div>
        <div class="summary-value">{{ formatFullNumber(summary?.total_output_tokens) }}</div>
        <div class="summary-metrics">
          <span><strong>{{ formatDuration(summary?.avg_ttft_ms) }}</strong> avg TTFT</span>
          <span><strong>{{ formatDuration(summary?.avg_latency_ms) }}</strong> avg latency</span>
        </div>
      </article>
    </section>

    <section class="card timeseries-section" :class="{ 'is-refreshing': loading }">
      <div class="section-heading">
        <div>
          <h2>{{ metricTitle }} over time</h2>
          <p>{{ rangeOptions.find((option) => option.value === range)?.label }} · {{ chartBuckets.length }} intervals</p>
        </div>
        <div class="metric-selector" role="group" aria-label="Chart metric">
          <button
            v-for="option in metricOptions"
            :key="option.value"
            type="button"
            :aria-pressed="metric === option.value"
            :title="`Show ${option.label.toLowerCase()} over time`"
            @click="metric = option.value"
          >
            {{ option.label }}
          </button>
        </div>
      </div>

      <!-- Legend: identity is never color-alone, so every series is named here. -->
      <ul class="chart-legend">
        <li v-for="series in renderedSeries" :key="series.key">
          <span class="legend-swatch" :style="{ '--swatch': series.color }" aria-hidden="true"></span>
          {{ series.label }}
        </li>
      </ul>

      <div ref="chartEl" class="timeseries-chart">
        <p v-if="chartEmpty" class="chart-empty">No {{ metricTitle.toLowerCase() }} in the selected time range</p>
        <svg
          v-else
          class="chart-svg"
          :viewBox="`0 0 ${chartWidth} ${chartHeightPx}`"
          :width="chartWidth"
          :height="chartHeightPx"
          role="img"
          :aria-label="chartDescription"
          @keydown="onChartKeydown"
          @focus="onChartFocus"
          @blur="onChartBlur"
          @pointerleave="onChartLeave"
        >
          <!-- Recessive grid and y-axis: the data is the loudest thing in the plot. -->
          <g aria-hidden="true">
            <line
              v-for="tick in yTicks"
              :key="tick.v"
              class="grid-line"
              :x1="PAD.left"
              :x2="PAD.left + plotW"
              :y1="tick.y"
              :y2="tick.y"
            />
            <text v-for="tick in yTicks" :key="`y-${tick.v}`" class="axis-label" :x="PAD.left - 8" :y="tick.y + 4" text-anchor="end">
              {{ tick.label }}
            </text>
            <text v-for="label in xLabels" :key="label.text" class="axis-label" :x="label.x" :y="PAD.top + plotH + 20" :text-anchor="label.anchor">
              {{ label.text }}
            </text>
          </g>

          <path
            v-for="series in renderedSeries"
            :key="series.key"
            class="series-line"
            :d="series.d"
            :stroke="series.color"
          />

          <!-- Direct labels at the latest valid point, so the right edge is self-describing. -->
          <text
            v-for="label in directLabels"
            :key="`end-${label.label}`"
            class="endpoint-label"
            :x="label.x + 8"
            :y="label.y + 4"
          >
            {{ label.label }}
          </text>

          <!-- Crosshair and per-series marks for the inspected interval. -->
          <g v-if="activeIndex !== null" aria-hidden="true">
            <line
              class="crosshair"
              :x1="xFor(activeIndex, chartBuckets.length)"
              :x2="xFor(activeIndex, chartBuckets.length)"
              :y1="PAD.top"
              :y2="PAD.top + plotH"
            />
            <circle
              v-for="point in activePoints"
              :key="point.key"
              class="crosshair-point"
              :cx="point.x"
              :cy="point.y"
              r="4.5"
              :fill="point.color"
            />
          </g>

          <!-- Hit targets are wider than the marks, per interaction guidance. -->
          <g role="presentation">
            <rect
              v-for="(bucket, index) in chartBuckets"
              :key="`hit-${bucket.key}`"
              class="chart-hit"
              :x="hitRect(index).x"
              :y="PAD.top"
              :width="hitRect(index).w"
              :height="plotH"
              @pointerenter="setActive(index, 'pointer')"
            />
          </g>
        </svg>

        <div
          v-if="tooltip"
          class="chart-tooltip"
          :class="{ flip: tooltip.flip }"
          :style="{ left: tooltip.left }"
          role="presentation"
        >
          <strong>{{ tooltip.bucket.fullLabel }}</strong>
          <span v-for="row in tooltip.rows" :key="row.label" class="tooltip-row">
            <span class="legend-swatch" :style="{ '--swatch': row.color }" aria-hidden="true"></span>
            {{ row.label }}
            <b>{{ row.text }}</b>
          </span>
        </div>

        <!-- Keyboard inspection speaks the same values the pointer tooltip shows. -->
        <p class="sr-only" aria-live="polite">{{ keyboardAnnouncement }}</p>
      </div>

      <table class="sr-only">
        <caption>{{ metricTitle }} per interval</caption>
        <thead>
          <tr>
            <th scope="col">Interval</th>
            <th v-for="series in renderedSeries" :key="series.key" scope="col">{{ series.label }}</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="bucket in chartBuckets" :key="`${bucket.key}-table`">
            <td>{{ bucket.fullLabel }}</td>
            <td v-for="series in renderedSeries" :key="`${bucket.key}-${series.key}`">
              {{ formatMetricValue(SERIES_BY_METRIC[metric].find(s => s.key === series.key)?.value(bucket) ?? null) }}
            </td>
          </tr>
        </tbody>
      </table>
    </section>

    <section class="card requests-section">
      <div class="requests-heading">
        <div>
          <h2>Recent requests</h2>
          <p>Page {{ currentPage }} · {{ pageSize }} per page</p>
        </div>
        <div class="request-filters">
          <label>Status <select :value="status" aria-label="Request status" @change="updateStatus"><option value="all">All</option><option value="completed">Completed</option><option value="failed">Failed</option><option value="interrupted">Interrupted</option><option value="in_flight">In flight</option></select></label>
          <label>Rows <select :value="pageSize" aria-label="Page size" @change="updatePageSize"><option v-for="size in pageSizeOptions" :key="size" :value="size">{{ size }}</option></select></label>
        </div>
      </div>
      <div v-if="requests.length === 0 && !loading" class="empty-state">No requests in the selected time range</div>
      <table v-else-if="!isMobile" class="requests-table">
        <thead><tr><th>Time</th><th v-if="isManager">Consumer</th><th>Model</th><th>Endpoint</th><th>Status</th><th>Tokens</th><th>Duration</th></tr></thead>
        <tbody>
          <tr v-for="request in requests" :key="request.request_id">
            <td class="time">{{ formatDate(request.created_at) }}</td>
            <td v-if="isManager" class="consumer">{{ request.consumer_id }}</td>
            <td class="model">{{ request.model }}</td>
            <td><span class="endpoint-badge" :class="{ streaming: request.streaming }" tabindex="0" :data-tooltip="endpointDescription(request)">{{ endpointLabel(request.endpoint) }}</span></td>
            <td class="status-cell">
              <span v-if="hasRequestDetail(request)" class="status-badge-anchor">
                <span
                  :class="['http-status', statusClass(request.http_status)]"
                  tabindex="0"
                  :aria-label="`${request.http_status ?? 'no'} HTTP status; more detail on hover or focus`"
                >{{ request.http_status ?? '-' }}</span>
                <span class="request-popover" role="tooltip">
                  <strong v-if="request.error_message" class="popover-label">Error detail: {{ request.error_message }}</strong>
                  <strong v-if="request.error_body" class="popover-label">Error body</strong>
                  <pre v-if="request.error_body">{{ request.error_body }}</pre>
                  <span v-if="!request.error_message && !request.error_body" class="popover-label">No error detail was recorded.</span>
                </span>
              </span>
              <span
                v-else
                :class="['http-status', statusClass(request.http_status)]"
                :aria-label="`${request.http_status ?? 'no'} HTTP status`"
              >{{ request.http_status ?? '-' }}</span>
            </td>
            <td class="tokens"><span v-if="request.input_tokens !== null || request.output_tokens !== null">{{ formatNumber(request.input_tokens) }} / {{ formatNumber(request.output_tokens) }}<span v-if="request.cached_tokens !== null && request.cached_tokens > 0" class="cached"> (+{{ formatNumber(request.cached_tokens) }} cached)</span></span><span v-else class="muted">—</span><span v-if="request.usage_status === 'unavailable'" class="usage-flag" title="The provider reported no usage for this request; its tokens are unknown, not zero">usage unavailable</span><span v-else-if="request.usage_status === 'partial'" class="usage-flag" title="The provider reported only part of this request's usage; the missing part is unknown, not zero">usage partial</span></td>
            <td class="duration">{{ formatDuration(request.duration_ms) }}</td>
          </tr>
        </tbody>
      </table>
      <ul v-else class="requests-cards" aria-label="Recent requests">
        <li v-for="request in requests" :key="request.request_id" class="request-card">
          <div v-if="isManager" class="request-card-row"><span class="request-card-label">Consumer</span><span class="request-card-value consumer">{{ request.consumer_id }}</span></div>
          <div class="request-card-row"><span class="request-card-label">Time</span><span class="request-card-value time">{{ formatDate(request.created_at) }}</span></div>
          <div class="request-card-row"><span class="request-card-label">Model</span><span class="request-card-value model">{{ request.model }}</span></div>
          <div class="request-card-row"><span class="request-card-label">Endpoint</span><span class="request-card-value"><span class="endpoint-badge" :class="{ streaming: request.streaming }" tabindex="0" :data-tooltip="endpointDescription(request)">{{ endpointLabel(request.endpoint) }}</span></span></div>
          <div class="request-card-row"><span class="request-card-label">Status</span><span class="request-card-value status-cell"><span v-if="hasRequestDetail(request)" class="status-badge-anchor"><span :class="['http-status', statusClass(request.http_status)]" tabindex="0" :aria-label="`${request.http_status ?? 'no'} HTTP status; more detail on hover or focus`">{{ request.http_status ?? '-' }}</span><span class="request-popover" role="tooltip"><strong v-if="request.error_message" class="popover-label">Error detail: {{ request.error_message }}</strong><strong v-if="request.error_body" class="popover-label">Error body</strong><pre v-if="request.error_body">{{ request.error_body }}</pre><span v-if="!request.error_message && !request.error_body" class="popover-label">No error detail was recorded.</span></span></span><span v-else :class="['http-status', statusClass(request.http_status)]" :aria-label="`${request.http_status ?? 'no'} HTTP status`">{{ request.http_status ?? '-' }}</span></span></div>
          <div class="request-card-row"><span class="request-card-label">Tokens</span><span class="request-card-value tokens"><span v-if="request.input_tokens !== null || request.output_tokens !== null">{{ formatNumber(request.input_tokens) }} / {{ formatNumber(request.output_tokens) }}<span v-if="request.cached_tokens !== null && request.cached_tokens > 0" class="cached"> (+{{ formatNumber(request.cached_tokens) }} cached)</span></span><span v-else class="muted">—</span><span v-if="request.usage_status === 'unavailable'" class="usage-flag" title="The provider reported no usage for this request; its tokens are unknown, not zero">usage unavailable</span><span v-else-if="request.usage_status === 'partial'" class="usage-flag" title="The provider reported only part of this request's usage; the missing part is unknown, not zero">usage partial</span></span></div>
          <div class="request-card-row"><span class="request-card-label">Duration</span><span class="request-card-value duration">{{ formatDuration(request.duration_ms) }}</span></div>
        </li>
      </ul>
      <nav class="pagination" aria-label="Request pages">
        <button type="button" :disabled="loading || !hasPrevious" @click="store.firstPage">First</button>
        <button type="button" :disabled="loading || !hasPrevious" @click="store.previousPage">Previous</button>
        <span aria-current="page">Page {{ currentPage }}</span>
        <button type="button" :disabled="loading || !hasMore" @click="store.nextPage">Next</button>
      </nav>
    </section>
  </div>
</template>

<style scoped>
.dashboard { max-width: 1760px; margin: 0 auto; padding: 2rem; }
.header, .header-left, .header-controls, .header-filters, .summary-metrics, .section-heading, .requests-heading, .request-filters, .pagination, .status-cell { display: flex; align-items: center; }
.header { justify-content: space-between; gap: 1rem; flex-wrap: wrap; margin-bottom: 2rem; }
.header-left, .header-controls, .header-filters { gap: .75rem; flex-wrap: wrap; }
.header h1 { font-size: 1.5rem; font-weight: 650; }
.whoami { color: var(--muted); font-size: .8125rem; white-space: nowrap; }
/* Icon-only controls: 36px keeps them above the 24px minimum touch target. */
.icon-button { display: inline-flex; align-items: center; justify-content: center; width: 2.25rem; height: 2.25rem; padding: 0; background: transparent; border: 1px solid var(--border); border-radius: .375rem; color: var(--muted); }
.icon-button .icon { width: 1rem; height: 1rem; }
.icon-button:hover { color: var(--fg); border-color: var(--fg); background: rgba(255,255,255,.04); }
.live-toggle[aria-pressed='true'] { color: var(--accent); border-color: rgba(59,130,246,.6); }
.icon-button:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
/* Toasts sit in the bottom-right corner, small: at most two compact rows. */
.alert-stack { position: fixed; z-index: 10; right: .75rem; bottom: .75rem; display: grid; gap: .375rem; max-width: min(20rem, calc(100vw - 1.5rem)); }
.data-alert { display: flex; align-items: center; justify-content: space-between; gap: .625rem; padding: .3rem .55rem; color: var(--fg); background: #172554; border: 1px solid rgba(96,165,250,.65); border-radius: .375rem; box-shadow: 0 8px 20px rgba(0,0,0,.35); font-size: .75rem; line-height: 1.3; }.alert-model { font-weight: 650; }.alert-sep { margin: 0 .3rem; color: var(--muted); }.alert-status.success { color: var(--success); }.alert-status.redirect { color: #7dd3fc; }.alert-status.client-error, .alert-status.unknown { color: var(--warning); }.alert-status.server-error { color: var(--error); }
.alert-dismiss { display: inline-flex; align-items: center; justify-content: center; padding: 0; min-width: 1.125rem; height: 1.125rem; border-radius: .25rem; background: transparent; color: var(--muted); font-size: .9rem; line-height: 1; }
.alert-dismiss:hover { background: rgba(255,255,255,.12); color: var(--fg); }
.data-alert-enter-active, .data-alert-leave-active { transition: opacity .18s ease, transform .18s ease; }
.data-alert-enter-from, .data-alert-leave-to { opacity: 0; transform: translateY(.375rem); }
.data-alert-move { transition: transform .18s ease; }
.error-banner { background: rgba(239,68,68,.1); border: 1px solid var(--error); color: #fca5a5; padding: .75rem 1rem; border-radius: .375rem; margin-bottom: 1.5rem; }
.summary { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 1rem; margin-bottom: 1.5rem; }
.summary-card { min-width: 0; }
.summary-label { color: var(--muted); font-size: .75rem; font-weight: 600; letter-spacing: .05em; text-transform: uppercase; }
/* Full dotted figures are the point (billing); `overflow-wrap` only breaks a
   value that would otherwise overflow a narrow card. */
.summary-value { margin: .45rem 0 .7rem; font-size: clamp(1.8rem, 3vw, 2.75rem); font-weight: 650; line-height: 1; font-variant-numeric: tabular-nums; overflow-wrap: anywhere; }
.summary-metrics { align-items: baseline; gap: .75rem; flex-wrap: wrap; color: var(--muted); font-size: .8125rem; }
.summary-metrics strong { color: var(--fg); font-weight: 650; }
.success-text { color: var(--success) !important; }.failure-text { color: var(--error) !important; }.accent-text { color: var(--accent) !important; }
.summary-sub { margin-top: .35rem; color: var(--muted); font-size: .8125rem; }.summary-sub.unavailable { color: var(--warning); }
.timeseries-section, .requests-section { margin-bottom: 1.5rem; }.timeseries-section.is-refreshing { opacity: .75; }
.section-heading, .requests-heading { justify-content: space-between; gap: 1rem; flex-wrap: wrap; margin-bottom: 1.25rem; }
.section-heading h2, .requests-heading h2 { font-size: 1rem; font-weight: 650; }.section-heading p, .requests-heading p { color: var(--muted); font-size: .8125rem; margin-top: .2rem; }
.metric-selector { display: inline-flex; padding: .2rem; gap: .2rem; background: var(--bg); border: 1px solid var(--border); border-radius: .375rem; }
.metric-selector button { padding: .35rem .75rem; background: transparent; border: 1px solid transparent; border-radius: .25rem; color: var(--muted); font-size: .8125rem; }
.metric-selector button:hover { color: var(--fg); }
.metric-selector button[aria-pressed='true'] { color: var(--fg); background: rgba(255,255,255,.08); border-color: var(--border); font-weight: 600; }
.metric-selector button:focus-visible { outline: 2px solid var(--accent); outline-offset: 1px; }
.chart-legend { display: flex; gap: 1.1rem; flex-wrap: wrap; margin: 0 0 .35rem; padding: 0; list-style: none; color: var(--muted); font-size: .8125rem; }
.chart-legend li { display: flex; align-items: center; gap: .4rem; }
.legend-swatch { display: inline-block; width: .625rem; height: .625rem; border-radius: .125rem; background: var(--swatch); }
.timeseries-chart { position: relative; min-height: 13.5rem; }
.chart-empty { display: flex; align-items: center; justify-content: center; height: 15rem; margin: 0; color: var(--muted); }
.chart-svg { display: block; width: 100%; height: auto; overflow: visible; touch-action: pan-y; }
.chart-svg:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.grid-line { stroke: var(--border); stroke-width: 1; }
.axis-label { fill: var(--muted); font-size: 11px; font-family: inherit; }
.series-line { fill: none; stroke-width: 2; stroke-linecap: round; stroke-linejoin: round; }
.endpoint-label { fill: var(--muted); font-size: 11px; font-family: inherit; }
.crosshair { stroke: var(--muted); stroke-width: 1; stroke-dasharray: 3 3; }
/* A 2px surface ring keeps the mark legible where lines cross. */
.crosshair-point { stroke: var(--bg-card); stroke-width: 2; }
.chart-hit { fill: transparent; }
.chart-hit:hover { fill: rgba(255,255,255,.03); }
.chart-tooltip { position: absolute; z-index: 2; top: .5rem; left: 0; display: grid; gap: .2rem; width: max-content; max-width: 15rem; padding: .5rem .6rem; color: var(--fg); background: #09090b; border: 1px solid var(--border); border-radius: .375rem; font-size: .75rem; line-height: 1.35; text-align: left; transform: translateX(.5rem); pointer-events: none; }
.chart-tooltip.flip { transform: translateX(calc(-100% - .5rem)); }
.tooltip-row { display: grid; grid-template-columns: auto 1fr auto; align-items: center; gap: .4rem; color: var(--muted); }
.tooltip-row b { color: var(--fg); font-weight: 650; font-variant-numeric: tabular-nums; }
.request-filters { gap: .75rem; flex-wrap: wrap; }.request-filters label { display: flex; align-items: center; gap: .4rem; color: var(--muted); font-size: .8125rem; }.request-filters select { padding: .35rem .5rem; }
.empty-state { padding: 3rem 1rem; text-align: center; color: var(--muted); }.requests-table { width: 100%; border-collapse: collapse; font-size: .875rem; }.requests-table th { padding: .75rem; border-bottom: 1px solid var(--border); color: var(--muted); font-weight: 550; text-align: left; }.requests-table td { padding: .75rem; border-bottom: 1px solid var(--border); vertical-align: top; }.requests-table tbody tr:hover { background: rgba(255,255,255,.02); }.time, .duration { color: var(--muted); white-space: nowrap; }.model { max-width: 16rem; overflow-wrap: anywhere; }.tokens { font-variant-numeric: tabular-nums; }.cached { color: var(--accent); font-size: .75rem; }.usage-flag { display: block; color: var(--warning); font-size: .75rem; }.muted { color: var(--muted); }
.endpoint-badge { position: relative; display: inline-block; padding: .125rem .4rem; color: #a1a1aa; background: rgba(161,161,170,.13); border-radius: .25rem; font-size: .75rem; cursor: help; }.endpoint-badge.streaming { color: #7dd3fc; background: rgba(14,165,233,.15); }.endpoint-badge::after { position: absolute; z-index: 3; bottom: calc(100% + .4rem); left: 50%; width: max-content; max-width: 12rem; padding: .35rem .45rem; color: var(--fg); background: #09090b; border: 1px solid var(--border); border-radius: .25rem; content: attr(data-tooltip); font-size: .75rem; transform: translateX(-50%); visibility: hidden; opacity: 0; pointer-events: none; }.endpoint-badge:hover::after, .endpoint-badge:focus-visible::after { visibility: visible; opacity: 1; }.endpoint-badge:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.status-cell { align-items: flex-start; gap: .5rem; flex-wrap: wrap; }.http-status { display: inline-flex; min-width: 2.5rem; justify-content: center; padding: .125rem .35rem; border: 1px solid currentColor; border-radius: .25rem; font-size: .75rem; font-variant-numeric: tabular-nums; }.http-status.success { color: var(--success); }.http-status.redirect { color: #7dd3fc; }.http-status.client-error, .http-status.unknown { color: var(--warning); }.http-status.server-error { color: var(--error); }
.filter-row { display: flex; justify-content: space-between; align-items: center; gap: 1rem; flex-wrap: wrap; margin-bottom: 1.5rem; }
.filter-side { display: flex; align-items: center; gap: .5rem; flex-wrap: wrap; }
.filter-muted { color: var(--muted); font-size: .8125rem; }
.filter-label { color: var(--muted); font-size: .75rem; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }
.whoami-name { font-size: .875rem; font-weight: 650; white-space: nowrap; }
.whoami-consumer { color: var(--muted); font-size: .8125rem; white-space: nowrap; }
.consumer-toggles { display: flex; gap: .4rem; flex-wrap: wrap; }
.consumer-chip { background: transparent; border: 1px solid var(--border); color: var(--muted); font-size: .8125rem; padding: .4rem .65rem; }
.consumer-chip[aria-pressed='true'] { color: var(--accent); border-color: rgba(59,130,246,.6); }
.consumer-chip:hover { color: var(--fg); border-color: var(--fg); background: transparent; }
.status-badge-anchor { position: relative; display: inline-flex; }
.status-badge-anchor .http-status:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.request-popover { position: absolute; z-index: 3; top: calc(100% + .4rem); left: 50%; width: max-content; max-width: 20rem; padding: .5rem .6rem; color: var(--fg); background: #09090b; border: 1px solid var(--border); border-radius: .25rem; font-size: .75rem; line-height: 1.35; text-align: left; transform: translateX(-50%); visibility: hidden; opacity: 0; pointer-events: none; transition: opacity .15s ease; }
.status-cell:hover .request-popover, .status-badge-anchor:focus-within .request-popover { visibility: visible; opacity: 1; }
.request-popover .popover-label { display: block; margin-bottom: .3rem; font-weight: 650; }
.request-popover pre { max-height: 12rem; overflow: auto; padding: .5rem; white-space: pre-wrap; color: var(--fg); background: var(--bg); border: 1px solid var(--border); border-radius: .25rem; font: inherit; overflow-wrap: anywhere; }
.requests-cards { display: grid; gap: .75rem; padding: 0; margin: 0; list-style: none; }.request-card { padding: .75rem; background: var(--bg); border: 1px solid var(--border); border-radius: .5rem; }.request-card-row { display: flex; justify-content: space-between; gap: 1rem; padding: .3rem 0; }.request-card-label { min-width: 5rem; color: var(--muted); font-size: .75rem; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }.request-card-value { text-align: right; overflow-wrap: anywhere; }.request-card .status-cell { justify-content: flex-end; }.request-card .status-badge-anchor { transform: translateX(0); }
.pagination { justify-content: center; gap: .5rem; flex-wrap: wrap; margin-top: 1.25rem; }.pagination span { min-width: 4.5rem; color: var(--muted); font-size: .8125rem; text-align: center; }.pagination button { padding: .45rem .7rem; }
.sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }
@media (max-width: 1050px) { .summary { grid-template-columns: repeat(2, minmax(0, 1fr)); } .filter-row { flex-direction: column; align-items: stretch; } .filter-side { justify-content: space-between; } }
@media (max-width: 700px) { .dashboard { padding: 1rem; }.header { align-items: stretch; }.header-controls { align-items: center; justify-content: flex-end; }.metric-selector { width: 100%; }.metric-selector button { flex: 1; }.filter-row { flex-direction: column; align-items: stretch; gap: .75rem; }.filter-side { align-items: stretch; flex-direction: column; width: 100%; }.filter-side .header-filters { flex-direction: column; align-items: stretch; gap: .5rem; width: 100%; }.filter-side .header-filters select { width: 100%; }.consumer-toggles { justify-content: flex-start; }.consumer-chip { flex: 1 1 auto; min-width: 5rem; }.whoami-name, .whoami-consumer { white-space: normal; }.header-filters { align-items: stretch; flex-direction: column; }.summary { grid-template-columns: 1fr; gap: .75rem; }.summary-card { padding: 1.125rem; }.section-heading, .requests-heading { align-items: flex-start; flex-direction: column; }.request-filters { width: 100%; }.request-filters label { flex: 1; justify-content: space-between; }.request-card-row { align-items: flex-start; }.request-card-value { max-width: 68%; }.request-card .status-cell { flex-direction: column; align-items: flex-end; }.pagination { justify-content: stretch; }.pagination button { flex: 1; }.pagination span { width: 100%; order: -1; } }
</style>
