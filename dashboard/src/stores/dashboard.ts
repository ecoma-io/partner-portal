import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

export interface Summary {
  total_requests: number
  total_input_tokens: number
  total_output_tokens: number
  total_cached_tokens: number
  avg_latency_ms: number
  avg_ttft_ms: number | null
  success_rate: number
}

export interface TimeseriesPoint {
  hour: string
  requests: number
  input_tokens: number
  output_tokens: number
  cached_tokens: number
}

export interface RequestItem {
  request_id: string
  created_at: string
  model: string
  endpoint: string
  streaming: boolean
  http_status: number | null
  request_status: string
  input_tokens: number | null
  output_tokens: number | null
  cached_tokens: number | null
  duration_ms: number
  ttft_ms: number | null
}

export interface Me {
  consumer_id: string
  key_name: string
}

export const useDashboardStore = defineStore('dashboard', () => {
  const me = ref<Me | null>(null)
  const summary = ref<Summary | null>(null)
  const timeseries = ref<TimeseriesPoint[]>([])
  const requests = ref<RequestItem[]>([])
  const nextCursor = ref<string | null>(null)
  const loading = ref(false)
  const error = ref<string | null>(null)
  const range = ref('24h')
  const model = ref('all')
  const sseConnected = ref(false)

  const hasMore = computed(() => nextCursor.value !== null)

  const baseUrl = '/api'

  async function fetchMe() {
    try {
      const res = await fetch(`${baseUrl}/me`, {
        headers: authHeaders(),
      })
      if (!res.ok) throw new Error('Failed to fetch user info')
      me.value = await res.json()
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Unknown error'
    }
  }

  async function fetchSummary() {
    try {
      const params = new URLSearchParams({ range: range.value })
      if (model.value !== 'all') params.set('model', model.value)

      const res = await fetch(`${baseUrl}/dashboard/summary?${params}`, {
        headers: authHeaders(),
      })
      if (!res.ok) throw new Error('Failed to fetch summary')
      summary.value = await res.json()
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Unknown error'
    }
  }

  async function fetchTimeseries() {
    try {
      const params = new URLSearchParams({ range: range.value })
      if (model.value !== 'all') params.set('model', model.value)

      const res = await fetch(`${baseUrl}/dashboard/timeseries?${params}`, {
        headers: authHeaders(),
      })
      if (!res.ok) throw new Error('Failed to fetch timeseries')
      const data = await res.json()
      timeseries.value = data.data
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Unknown error'
    }
  }

  async function fetchRequests(append = false) {
    if (loading.value) return
    loading.value = true

    try {
      const params = new URLSearchParams({ range: range.value, limit: '50' })
      if (model.value !== 'all') params.set('model', model.value)
      if (append && nextCursor.value) params.set('cursor', nextCursor.value)

      const res = await fetch(`${baseUrl}/dashboard/requests?${params}`, {
        headers: authHeaders(),
      })
      if (!res.ok) throw new Error('Failed to fetch requests')
      const data = await res.json()

      if (append) {
        requests.value = [...requests.value, ...data.data]
      } else {
        requests.value = data.data
      }
      nextCursor.value = data.next_cursor
    } catch (e) {
      error.value = e instanceof Error ? e.message : 'Unknown error'
    } finally {
      loading.value = false
    }
  }

  function authHeaders(): HeadersInit {
    // Auth token should be provided via Authorization header by the embedding context
    // For now, assume the browser has a cookie or the embedding app passes it
    const token = localStorage.getItem('api_key')
    if (token) {
      return { Authorization: `Bearer ${token}` }
    }
    return {}
  }

  function connectSSE() {
    const token = localStorage.getItem('api_key')
    const url = new URL(`${baseUrl}/dashboard/events`, window.location.origin)
    if (token) {
      // SSE with auth via query param is insecure; prefer fetch + eventsource
      // For now, we'll use a simple fetch-based SSE with auth header
    }

    // Use EventSource API (no custom headers support, so rely on cookie)
    // For production, consider using fetch() with SSE parsing for auth header support
    const es = new EventSource(url.toString())

    es.onopen = () => {
      sseConnected.value = true
    }

    es.onerror = () => {
      sseConnected.value = false
      // Reconnect after 3s
      setTimeout(() => connectSSE(), 3000)
    }

    es.onmessage = (event) => {
      try {
        const data = JSON.parse(event.data)
        if (data.type === 'data_changed') {
          // Refetch all data
          refresh()
        }
      } catch {
        // Ignore parse errors
      }
    }
  }

  async function refresh() {
    await Promise.all([fetchSummary(), fetchTimeseries(), fetchRequests()])
  }

  return {
    me,
    summary,
    timeseries,
    requests,
    nextCursor,
    loading,
    error,
    range,
    model,
    sseConnected,
    hasMore,
    fetchMe,
    fetchSummary,
    fetchTimeseries,
    fetchRequests,
    connectSSE,
    refresh,
  }
})
