<script setup lang="ts">
import { onMounted, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { useDashboardStore } from '@/stores/dashboard'

const store = useDashboardStore()
const { summary, timeseries, requests, loading, error, range, model, sseConnected, hasMore } = storeToRefs(store)

const models = ['gpt-4', 'gpt-4o', 'gpt-4o-mini', 'gpt-4-turbo']

const rangeOptions = [
  { value: 'today', label: 'Today' },
  { value: '24h', label: 'Last 24h' },
  { value: '7d', label: 'Last 7 days' },
  { value: '14d', label: 'Last 14 days' },
  { value: '30d', label: 'Last 30 days' },
]

onMounted(async () => {
  await store.fetchMe()
  await store.refresh()
  store.connectSSE()
})

watch([range, model], () => {
  store.refresh()
})

function formatNumber(n: number | null): string {
  if (n === null) return '—'
  if (n >= 1_000_000) return `${(n / 1_000_000).toFixed(1)}M`
  if (n >= 1_000) return `${(n / 1_000).toFixed(1)}K`
  return n.toLocaleString()
}

function formatDuration(ms: number): string {
  if (ms < 1000) return `${ms}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

function formatDate(iso: string): string {
  const d = new Date(iso)
  return d.toLocaleString()
}

function statusColor(status: string): string {
  switch (status) {
    case 'completed':
      return 'var(--success)'
    case 'failed':
      return 'var(--error)'
    case 'interrupted':
      return 'var(--warning)'
    default:
      return 'var(--muted)'
  }
}

async function loadMore() {
  await store.fetchRequests(true)
}
</script>

<template>
  <div class="dashboard">
    <header class="header">
      <div class="header-left">
        <h1>Partner Portal</h1>
        <span v-if="sseConnected" class="sse-badge connected">● Live</span>
        <span v-else class="sse-badge disconnected">○ Disconnected</span>
      </div>
      <div class="header-filters">
        <select v-model="range">
          <option v-for="opt in rangeOptions" :key="opt.value" :value="opt.value">
            {{ opt.label }}
          </option>
        </select>
        <select v-model="model">
          <option value="all">All models</option>
          <option v-for="m in models" :key="m" :value="m">
            {{ m }}
          </option>
        </select>
      </div>
    </header>

    <div v-if="error" class="error-banner">
      {{ error }}
    </div>

    <!-- Summary Cards -->
    <section class="summary">
      <div class="card summary-card">
        <div class="summary-label">Requests</div>
        <div class="summary-value">{{ formatNumber(summary?.total_requests ?? 0) }}</div>
        <div class="summary-sub">
          <span :style="{ color: 'var(--success)' }">{{ ((summary?.success_rate ?? 0) * 100).toFixed(1) }}%</span> success
        </div>
      </div>
      <div class="card summary-card">
        <div class="summary-label">Input Tokens</div>
        <div class="summary-value">{{ formatNumber(summary?.total_input_tokens ?? 0) }}</div>
        <div v-if="summary?.total_cached_tokens" class="summary-sub">
          <span style="color: var(--accent)">{{ formatNumber(summary.total_cached_tokens) }}</span> cached
        </div>
      </div>
      <div class="card summary-card">
        <div class="summary-label">Output Tokens</div>
        <div class="summary-value">{{ formatNumber(summary?.total_output_tokens ?? 0) }}</div>
      </div>
      <div class="card summary-card">
        <div class="summary-label">Avg Latency</div>
        <div class="summary-value">{{ formatDuration(summary?.avg_latency_ms ?? 0) }}</div>
        <div v-if="summary?.avg_ttft_ms" class="summary-sub">
          TTFT: {{ formatDuration(summary.avg_ttft_ms) }}
        </div>
      </div>
    </section>

    <!-- Timeseries Chart (simplified - would use chart library in production) -->
    <section class="card timeseries-section">
      <h2>Requests Over Time</h2>
      <div class="timeseries-chart">
        <div
          v-for="point in timeseries"
          :key="point.hour"
          class="timeseries-bar"
          :style="{ height: `${(point.requests / Math.max(...timeseries.map(t => t.requests), 1)) * 100}%` }"
          :title="`${point.hour}: ${point.requests} requests`"
        ></div>
      </div>
    </section>

    <!-- Requests Table -->
    <section class="card requests-section">
      <h2>Recent Requests</h2>
      <div v-if="requests.length === 0 && !loading" class="empty-state">
        No requests in the selected time range
      </div>
      <table v-else class="requests-table">
        <thead>
          <tr>
            <th>Time</th>
            <th>Model</th>
            <th>Endpoint</th>
            <th>Status</th>
            <th>Tokens</th>
            <th>Duration</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="req in requests" :key="req.request_id">
            <td class="time">{{ formatDate(req.created_at) }}</td>
            <td class="model">{{ req.model }}</td>
            <td class="endpoint">
              <span :class="['badge', req.endpoint]">{{ req.endpoint }}</span>
              <span v-if="req.streaming" class="badge streaming">streaming</span>
            </td>
            <td class="status">
              <span :style="{ color: statusColor(req.request_status) }">
                {{ req.http_status ?? '—' }} {{ req.request_status }}
              </span>
            </td>
            <td class="tokens">
              <span v-if="req.input_tokens || req.output_tokens">
                {{ formatNumber(req.input_tokens) }} / {{ formatNumber(req.output_tokens) }}
                <span v-if="req.cached_tokens" class="cached">(+{{ formatNumber(req.cached_tokens) }} cached)</span>
              </span>
              <span v-else class="muted">—</span>
            </td>
            <td class="duration">{{ formatDuration(req.duration_ms) }}</td>
          </tr>
        </tbody>
      </table>
      <div v-if="hasMore" class="load-more">
        <button @click="loadMore" :disabled="loading">
          {{ loading ? 'Loading...' : 'Load More' }}
        </button>
      </div>
    </section>
  </div>
</template>

<style scoped>
.dashboard {
  max-width: 1400px;
  margin: 0 auto;
  padding: 2rem;
}

.header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 2rem;
}

.header-left {
  display: flex;
  align-items: center;
  gap: 1rem;
}

.header h1 {
  font-size: 1.5rem;
  font-weight: 600;
}

.sse-badge {
  font-size: 0.75rem;
  padding: 0.25rem 0.5rem;
  border-radius: 0.25rem;
}

.sse-badge.connected {
  background: rgba(34, 197, 94, 0.1);
  color: var(--success);
}

.sse-badge.disconnected {
  background: rgba(239, 68, 68, 0.1);
  color: var(--error);
}

.header-filters {
  display: flex;
  gap: 0.75rem;
}

.error-banner {
  background: rgba(239, 68, 68, 0.1);
  border: 1px solid var(--error);
  color: var(--error);
  padding: 0.75rem 1rem;
  border-radius: 0.375rem;
  margin-bottom: 1.5rem;
}

.summary {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
  margin-bottom: 1.5rem;
}

.summary-card {
  text-align: center;
}

.summary-label {
  font-size: 0.75rem;
  text-transform: uppercase;
  color: var(--muted);
  margin-bottom: 0.5rem;
}

.summary-value {
  font-size: 2rem;
  font-weight: 600;
}

.summary-sub {
  font-size: 0.875rem;
  color: var(--muted);
  margin-top: 0.25rem;
}

.timeseries-section {
  margin-bottom: 1.5rem;
}

.timeseries-section h2 {
  font-size: 1rem;
  font-weight: 500;
  margin-bottom: 1rem;
}

.timeseries-chart {
  display: flex;
  align-items: flex-end;
  gap: 2px;
  height: 120px;
  border-bottom: 1px solid var(--border);
  padding-bottom: 0.5rem;
}

.timeseries-bar {
  flex: 1;
  background: var(--accent);
  min-height: 2px;
  border-radius: 2px 2px 0 0;
  transition: height 0.2s;
}

.timeseries-bar:hover {
  background: var(--accent-hover);
}

.requests-section h2 {
  font-size: 1rem;
  font-weight: 500;
  margin-bottom: 1rem;
}

.empty-state {
  text-align: center;
  color: var(--muted);
  padding: 3rem 1rem;
}

.requests-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.875rem;
}

.requests-table th {
  text-align: left;
  padding: 0.75rem;
  border-bottom: 1px solid var(--border);
  color: var(--muted);
  font-weight: 500;
}

.requests-table td {
  padding: 0.75rem;
  border-bottom: 1px solid var(--border);
}

.requests-table tbody tr:hover {
  background: rgba(255, 255, 255, 0.02);
}

.badge {
  display: inline-block;
  padding: 0.125rem 0.375rem;
  border-radius: 0.25rem;
  font-size: 0.75rem;
  background: var(--bg);
  margin-right: 0.25rem;
}

.badge.streaming {
  background: rgba(59, 130, 246, 0.2);
  color: var(--accent);
}

.cached {
  color: var(--accent);
  font-size: 0.75rem;
}

.muted {
  color: var(--muted);
}

.load-more {
  text-align: center;
  margin-top: 1rem;
}
</style>
