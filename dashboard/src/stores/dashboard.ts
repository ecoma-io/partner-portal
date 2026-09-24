import { defineStore } from 'pinia'
import { ref, computed } from 'vue'

/**
 * The summary the ledger reports for the selected window.
 *
 * Token totals are counts of tokens the provider actually reported. They are
 * never a stand-in for "unknown": `unavailable_usage_count` counts the requests
 * whose usage the provider did not report at all, and those requests' tokens are
 * absent from the totals rather than counted as zero.
 */
export interface Summary {
  total_requests: number
  success_count: number
  failure_count: number
  total_input_tokens: number
  total_output_tokens: number
  total_cached_tokens: number
  /** Null when no request in the window reported a duration. */
  avg_latency_ms: number | null
  /** Null when no request in the window reported a first token. */
  avg_ttft_ms: number | null
  success_rate: number
  /**
   * Requests in the window with `usage_status = 'unavailable'`: the provider
   * reported nothing for them. Counts requests, not tokens — there are no
   * tokens to count. Requests with `partial` usage are not included here: the
   * part the provider did report is in the totals above.
   */
  unavailable_usage_count: number
}

export interface TimeseriesPoint {
  hour: string
  requests: number
  success_count: number
  failure_count: number
  input_tokens: number
  output_tokens: number
  cached_tokens: number
  /** Sum of request durations (ms). Interval latency = this / `requests`. */
  total_duration_ms: number
  /** Sum of time to first token (ms) among requests that reported one. */
  total_ttft_ms: number
  /** Requests in this hour that reported a TTFT. 0 means TTFT is unavailable here. */
  ttft_count: number
}

export interface RequestItem {
  request_id: string
  created_at: string
  /** The consumer this request belongs to. Present on every row; only surfaced in the UI for managers. */
  consumer_id: string
  model: string
  endpoint: string
  streaming: boolean
  http_status: number | null
  request_status: string
  /**
   * `available`, `partial` or `unavailable`. Token fields are null exactly when
   * the provider did not report them; they are never 0 as a placeholder.
   */
  usage_status: string
  input_tokens: number | null
  output_tokens: number | null
  cached_tokens: number | null
  duration_ms: number
  ttft_ms: number | null
  /** Present for failed/interrupted requests; may repeat upstream text. */
  error_message: string | null
  /** Optional upstream error payload, supplied only when the backend recorded it. */
  error_body?: string | null
}

export interface Me {
  consumer_id: string
  key_name: string
  /** `consumer` for a regular key scoped to one consumer; `manager` for a reader that may view several. */
  role: 'consumer' | 'manager'
  /**
   * For managers only: the consumer_ids actually present in the ledger, offered
   * by the selector. Not a permission list — a manager sees every consumer
   * whether or not it appears here (docs/adr/0013).
   */
  consumers?: string[]
}

/**
 * State of the invalidation stream.
 *
 * `unauthenticated` is terminal on purpose: the stream needs the same bearer
 * key as the REST API, and retrying a rejected credential only produces a storm
 * of 401s.
 */
export type StreamState = 'idle' | 'connecting' | 'live' | 'retrying' | 'unauthenticated'

const BASE_URL = '/api'

/** SSE endpoint. GET, because that is the route the server exposes. */
const STREAM_PATH = `${BASE_URL}/dashboard/events`
/** First reconnect delay. Doubles per consecutive failure, up to the cap. */
const BACKOFF_INITIAL_MS = 1_000
/** Ceiling for the reconnect delay. */
const BACKOFF_MAX_MS = 30_000
/**
 * Largest unterminated frame we will hold. The server caps an event at 256 KiB;
 * a stream that has produced no frame separator within a megabyte is not SSE we
 * can parse, and buffering it forever is how a reconnect loop eats the tab.
 */
const MAX_FRAME_BYTES = 1 << 20

function messageOf(e: unknown): string {
  return e instanceof Error ? e.message : 'Unknown error'
}

/** A failure that names what went wrong instead of a generic "failed to fetch". */
function httpMessage(res: Response, what: string): string {
  if (res.status === 401 || res.status === 403) {
    return `Not authenticated — a valid API key is required to read ${what}`
  }
  return `Could not load ${what} (HTTP ${res.status})`
}

export const useDashboardStore = defineStore('dashboard', () => {
  const me = ref<Me | null>(null)
  /**
   * Whether the API key is known to be valid. `false` while discovering (or in
   * the login screen); `true` once `/api/me` has answered with this key.
   *
   * This is the distinction that gives the login screen its teeth: a rejected
   * key must NOT make the store silently carry on as if it were valid. Every
   * authenticated request fails. We only surface the view after the backend
   * accepts the key.
   */
  const authenticated = ref(false)
  const summary = ref<Summary | null>(null)
  const timeseries = ref<TimeseriesPoint[]>([])
  const requests = ref<RequestItem[]>([])
  const nextCursor = ref<string | null>(null)
  const cursorStack = ref<string[]>([])
  const status = ref('all')
  const pageSize = ref(10)
  const loading = ref(false)
  const error = ref<string | null>(null)
  const range = ref('24h')
  const model = ref('all')
  const streamState = ref<StreamState>('idle')

  /**
   * Whether the view follows the server: SSE invalidation connected, toasts
   * shown. One preference for both, persisted across reloads — pausing is the
   * user's choice, not a connection failure, so it survives a reload and a
   * sign-out rather than silently reconnecting on the next visit.
   *
   * The legacy `sse_alerts_enabled` key is honoured as a fallback so a view
   * paused under the old alerts-only toggle does not resume unasked.
   */
  const automaticUpdatesEnabled = ref(readAutomaticUpdatesPreference())

  function readAutomaticUpdatesPreference(): boolean {
    const stored = localStorage.getItem('automatic_updates_enabled')
    if (stored !== null) return stored !== 'false'
    const legacy = localStorage.getItem('sse_alerts_enabled')
    return legacy !== 'false'
  }

  const hasMore = computed(() => nextCursor.value !== null)
  const hasPrevious = computed(() => cursorStack.value.length > 0)
  const currentPage = computed(() => cursorStack.value.length + 1)
  const currentCursor = computed(() => cursorStack.value.at(-1) ?? null)

  /** Managers only: consumers to restrict the view to. Empty = the manager's full allowed set. */
  const selectedConsumers = ref<string[]>([])

  /** The newest request row seen. Feeds the live data-change toast. */
  const latestRequest = ref<RequestItem | null>(null)

  const isManager = computed(() => me.value?.role === 'manager')

  function requestSignature(): string {
    return `${range.value}+${model.value}+${status.value}+${pageSize.value}+${selectedConsumers.value.join(',')}`
  }

  function resetPagination() {
    cursorStack.value = []
    nextCursor.value = null
  }

  /**
   * Per-endpoint sequence numbers. A response is only applied when it belongs
   * to the newest request for its endpoint, so a slow earlier response can
   * never overwrite a newer one.
   */
  const seq = { me: 0, summary: 0, timeseries: 0, requests: 0 }

  /**
   * Tear down any live stream run so a new key starts clean. The stream is
   * read with `fetch` + an AbortController; aborting it is the disconnect.
   */
  function closeStream() {
    cancelReconnect()
    if (streamRun !== null) {
      streamRun.abort()
      streamRun = null
    }
  }

  function authHeaders(): HeadersInit {
    // The key is supplied by the embedding context. It is only ever sent in the
    // Authorization header — never a URL, a query parameter or a cookie.
    const token = localStorage.getItem('api_key')
    if (token) {
      return { Authorization: `Bearer ${token}` }
    }
    return {}
  }

  async function fetchMe(withKey?: string) {
    const ticket = ++seq.me
    try {
      const res = await fetch(`${BASE_URL}/me`, {
        headers: withKey ? { Authorization: `Bearer ${withKey}` } : authHeaders(),
      })
      if (!res.ok) throw new Error(httpMessage(res, 'the account'))
      const data: Me = await res.json()
      if (ticket !== seq.me) return
      me.value = data
      authenticated.value = true
      error.value = null
    } catch (e) {
      if (ticket === seq.me) error.value = messageOf(e)
    }
  }

  /**
   * Scope a dashboard query to the manager's selected consumers. Empty means
   * "everything the manager may see" — the backend already answers that, so the
   * parameter is only sent when the manager actually narrows the view.
   */
  function applyConsumersScope(params: URLSearchParams) {
    if (isManager.value && selectedConsumers.value.length > 0) {
      params.set('consumers', selectedConsumers.value.join(','))
    }
  }

  async function fetchSummary() {
    const ticket = ++seq.summary
    try {
      const params = new URLSearchParams({ range: range.value })
      if (model.value !== 'all') params.set('model', model.value)
      applyConsumersScope(params)

      const res = await fetch(`${BASE_URL}/dashboard/summary?${params}`, {
        headers: authHeaders(),
      })
      if (!res.ok) throw new Error(httpMessage(res, 'the summary'))
      const data: Summary = await res.json()
      if (ticket !== seq.summary) return
      summary.value = data
      error.value = null
    } catch (e) {
      if (ticket === seq.summary) error.value = messageOf(e)
    }
  }

  const models = ref<string[]>([])

  /**
   * The models this key has actually used, from the backend. The filter must
   * only ever show what the authenticated consumer really touched — a
   * hardcoded list would claim models that were never used. The endpoint is
   * consumer-scoped server-side (`consumer_id = ?1`), so a key sees only its
   * own model activity.
   */
  async function fetchModels() {
    try {
      const params = new URLSearchParams({ range: range.value })
      if (model.value !== 'all') params.set('model', model.value)
      applyConsumersScope(params)
      const res = await fetch(`${BASE_URL}/dashboard/models?${params}`, {
        headers: authHeaders(),
      })
      if (!res.ok) throw new Error(httpMessage(res, 'the model list'))
      const data: { models: string[] } = await res.json()
      models.value = data.models
    } catch {
      // The model filter is a convenience; the summary and table still load.
      models.value = []
    }
  }

  async function fetchTimeseries() {
    const ticket = ++seq.timeseries
    try {
      const params = new URLSearchParams({ range: range.value })
      if (model.value !== 'all') params.set('model', model.value)
      applyConsumersScope(params)

      const res = await fetch(`${BASE_URL}/dashboard/timeseries?${params}`, {
        headers: authHeaders(),
      })
      if (!res.ok) throw new Error(httpMessage(res, 'the timeseries'))
      const data: { data: TimeseriesPoint[] } = await res.json()
      if (ticket !== seq.timeseries) return
      timeseries.value = data.data
      error.value = null
    } catch (e) {
      if (ticket === seq.timeseries) error.value = messageOf(e)
    }
  }

  interface RequestPage {
    cursor: string | null
    stack: string[]
    /** An invalidation on page one preserves still-visible rows after fresh rows. */
    mergeFresh?: boolean
  }

  /** A request page queued behind the one currently in flight. */
  let pendingPage: RequestPage | null = null

  async function fetchRequests(page: RequestPage = {
    cursor: currentCursor.value,
    stack: [...cursorStack.value],
  }) {
    if (loading.value) {
      // The latest user action or invalidation is the only page that matters.
      pendingPage = page
      return
    }
    loading.value = true
    const signature = requestSignature()

    try {
      const ticket = ++seq.requests
      const params = new URLSearchParams({ range: range.value, limit: String(pageSize.value) })
      if (model.value !== 'all') params.set('model', model.value)
      if (status.value !== 'all') params.set('status', status.value)
      applyConsumersScope(params)
      if (page.cursor) params.set('cursor', page.cursor)

      const res = await fetch(`${BASE_URL}/dashboard/requests?${params}`, {
        headers: authHeaders(),
      })
      if (!res.ok) throw new Error(httpMessage(res, 'the request list'))
      const data: { data: RequestItem[]; next_cursor: string | null } = await res.json()
      if (ticket !== seq.requests || signature !== requestSignature()) return

      requests.value = page.mergeFresh
        ? [...data.data, ...requests.value]
            .filter((request, index, all) => all.findIndex(({ request_id }) => request_id === request.request_id) === index)
            .slice(0, pageSize.value)
        : data.data
      cursorStack.value = page.stack
      nextCursor.value = data.next_cursor
      // The first row is the newest request the backend has. Keep the most
      // recent row ever seen so the data-change toast names the latest request
      // even after pagination or an emptied filter moved it off-screen.
      latestRequest.value = data.data[0] ?? latestRequest.value
      error.value = null
    } catch (e) {
      error.value = messageOf(e)
    } finally {
      loading.value = false
      const queued = pendingPage
      pendingPage = null
      if (queued !== null) void fetchRequests(queued)
    }
  }

  async function firstPage() {
    await fetchRequests({ cursor: null, stack: [] })
  }

  async function previousPage() {
    if (!hasPrevious.value) return
    const stack = cursorStack.value.slice(0, -1)
    await fetchRequests({ cursor: stack.at(-1) ?? null, stack })
  }

  async function nextPage() {
    if (!nextCursor.value) return
    await fetchRequests({
      cursor: nextCursor.value,
      stack: [...cursorStack.value, nextCursor.value],
    })
  }

  /** Bursts of full reloads coalesce into at most one extra pass. */
  let refreshing = false
  let refreshQueued = false
  /** Each SSE event also advances this counter so the view can show an alert. */
  const dataChangeVersion = ref(0)

  /**
   * Sign in with a key the user typed. The key is validated against the backend
   * before the view is handed anything: a 401 here is the whole point of the
   * login screen, and it must show a message rather than pretend the key worked.
   */
  async function signIn(key: string) {
    const trimmed = key.trim()
    if (!trimmed) {
      error.value = 'Enter an API key to continue'
      return false
    }
    error.value = null

    // Store the typed key *before* validating it. Flipping `authenticated` is
    // what mounts the dashboard, and that view's first refresh reads the key
    // from localStorage — validating first would mount it with no credential
    // and fire every dashboard query unauthenticated. A rejected key never
    // survives: the previous value is restored below, so a wrong guess cannot
    // evict a key that was already working.
    const previous = localStorage.getItem('api_key')
    localStorage.setItem('api_key', trimmed)

    await fetchMe()
    if (!authenticated.value) {
      if (previous === null) localStorage.removeItem('api_key')
      else localStorage.setItem('api_key', previous)
      return false
    }
    closeStream()
    // The dashboard view mounts on the `authenticated` flip above and performs
    // the first load itself, so refreshing here as well would double every
    // query on every sign-in. `connect()` is left to the view for the same
    // reason; sign-out does not depend on it.
    return true
  }

  /** Sign out: drop the stored key, tear down the stream, return to login. */
  function signOut() {
    disconnect()
    localStorage.removeItem('api_key')
    me.value = null
    authenticated.value = false
    summary.value = null
    timeseries.value = []
    requests.value = []
    selectedConsumers.value = []
    latestRequest.value = null
    resetPagination()
    error.value = null
  }

  async function refresh() {
    if (refreshing) {
      refreshQueued = true
      return
    }
    refreshing = true
    try {
      do {
        refreshQueued = false
        await Promise.all([
          fetchSummary(),
          fetchTimeseries(),
          fetchRequests({ cursor: currentCursor.value, stack: [...cursorStack.value] }),
          fetchModels(),
        ])
      } while (refreshQueued)
    } finally {
      refreshing = false
    }
  }

  /**
   * An invalidation always updates summary and chart. On page one it makes one
   * list query only, placing newly fetched records before any still-visible
   * records and de-duplicating by request ID. Later pages retain their cursor
   * position and use their ordinary single-page reload.
   */
  async function refreshFromDataChange() {
    dataChangeVersion.value += 1
    const first = !hasPrevious.value
    await Promise.all([
      fetchSummary(),
      fetchTimeseries(),
      fetchModels(),
      fetchRequests({
        cursor: first ? null : currentCursor.value,
        stack: first ? [] : [...cursorStack.value],
        mergeFresh: first,
      }),
    ])
  }

  function resetAndFetchRequests() {
    resetPagination()
    void fetchRequests({ cursor: null, stack: [] })
  }

  function setStatus(nextStatus: string) {
    if (status.value === nextStatus) return
    status.value = nextStatus
    resetAndFetchRequests()
  }

  function setPageSize(nextPageSize: number) {
    if (pageSize.value === nextPageSize) return
    pageSize.value = nextPageSize
    resetAndFetchRequests()
  }

  // --- Invalidation stream --------------------------------------------------
  //
  // The stream carries no data, only "something changed, refetch", and the
  // server requires the same Authorization header as the REST API. EventSource
  // cannot send headers and would 401 forever, so the stream is read with
  // `fetch` and the SSE frames are parsed here.

  /** The one live connection run, or null when disconnected. */
  let streamRun: AbortController | null = null
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null

  function cancelReconnect() {
    if (reconnectTimer !== null) {
      clearTimeout(reconnectTimer)
      reconnectTimer = null
    }
  }

  /**
   * Open the invalidation stream. Idempotent: while a run exists — connecting,
   * live, or waiting out a backoff — this does nothing, so repeated calls
   * cannot accumulate sockets or timers. Does nothing while automatic updates
   * are disabled: the preference, not a retry, is what closes the stream.
   */
  function connect() {
    if (streamRun !== null) return
    if (!automaticUpdatesEnabled.value) {
      streamState.value = 'idle'
      return
    }
    cancelReconnect()

    // Nothing to authenticate with: the route would answer 401, and retrying a
    // missing credential can never succeed. Report it instead of a retry loop.
    if (!localStorage.getItem('api_key')) {
      streamState.value = 'unauthenticated'
      return
    }

    const controller = new AbortController()
    streamRun = controller
    void streamLoop(controller).finally(() => {
      // The run ended (a rejected credential, or a teardown). Release it so a
      // later `connect()` starts a fresh run instead of finding a dead one.
      if (streamRun === controller) {
        streamRun = null
      }
    })
  }

  /** Tear the stream down: abort the in-flight fetch and cancel any retry. */
  function disconnect() {
    cancelReconnect()
    const controller = streamRun
    streamRun = null
    controller?.abort()
    streamState.value = 'idle'
  }

  /**
   * Set the automatic-update preference. Disabling closes the SSE stream (and
   * the view dismisses its toasts); enabling refreshes once — the data that
   * arrived while paused must be fetched, not assumed from a stream that was
   * closed, and a missed invalidation is not one SSE event — then reconnects.
   */
  async function setAutomaticUpdates(enabled: boolean) {
    if (automaticUpdatesEnabled.value === enabled) return
    automaticUpdatesEnabled.value = enabled
    localStorage.setItem(
      'automatic_updates_enabled',
      enabled ? 'true' : 'false',
    )
    if (enabled) {
      await refresh()
      connect()
    } else {
      disconnect()
    }
  }

  /** Wait `ms`, resolving early — and clearing the timer — when aborted. */
  function wait(ms: number, signal: AbortSignal): Promise<void> {
    return new Promise((resolve) => {
      const finish = () => {
        cancelReconnect()
        signal.removeEventListener('abort', finish)
        resolve()
      }
      reconnectTimer = setTimeout(finish, ms)
      signal.addEventListener('abort', finish, { once: true })
    })
  }

  /**
   * Exponential backoff with equal jitter: the delay is drawn from
   * `[ceiling / 2, ceiling]` where the ceiling doubles per consecutive failure
   * (1 s, 2 s, 4 s, …) and stops at 30 s. The jitter keeps many tabs from
   * reconnecting in lockstep after a restart.
   */
  function backoffDelay(attempt: number): number {
    const ceiling = Math.min(BACKOFF_MAX_MS, BACKOFF_INITIAL_MS * 2 ** attempt)
    return Math.round(ceiling / 2 + Math.random() * (ceiling / 2))
  }

  async function streamLoop(controller: AbortController) {
    const signal = controller.signal
    let failures = 0

    while (!signal.aborted) {
      let sawFrame = false
      streamState.value = failures === 0 ? 'connecting' : 'retrying'

      try {
        const res = await fetch(STREAM_PATH, {
          method: 'GET',
          headers: { ...authHeaders(), Accept: 'text/event-stream' },
          cache: 'no-store',
          signal,
        })

        // A rejected credential is not a transient failure: retrying it is a
        // needless storm of 401s that can never succeed.
        if (res.status === 401 || res.status === 403) {
          streamState.value = 'unauthenticated'
          return
        }
        if (!res.ok || res.body === null) {
          throw new Error(`stream failed (HTTP ${res.status})`)
        }

        streamState.value = 'live'
        sawFrame = await readStream(res.body, signal)
      } catch {
        if (signal.aborted) return
        // Anything else — a refused connection, a dropped socket — is retried
        // below with a backoff.
      }

      if (signal.aborted) return

      // A stream that did deliver frames worked; the server or a proxy closed
      // it. Restart from the shortest delay rather than escalating.
      failures = sawFrame ? 0 : failures
      failures += 1
      streamState.value = 'retrying'
      await wait(backoffDelay(failures - 1), signal)
    }
  }

  /**
   * Parse SSE frames from the response body until the stream ends.
   *
   * Frames are separated by a blank line; comment lines (`:`, which is how the
   * server's 15 s keep-alive arrives) and the `event:` / `id:` / `retry:`
   * fields carry nothing this client acts on. Returns whether any frame
   * arrived, which is what tells a working-but-closed stream from a failed one.
   */
  async function readStream(
    body: ReadableStream<Uint8Array>,
    signal: AbortSignal,
  ): Promise<boolean> {
    const reader = body.getReader()
    const decoder = new TextDecoder()
    let buffer = ''
    let sawFrame = false

    try {
      while (!signal.aborted) {
        const { value, done } = await reader.read()
        if (done) return sawFrame
        buffer = (buffer + decoder.decode(value, { stream: true })).replace(/\r\n/g, '\n')

        let separator = buffer.indexOf('\n\n')
        while (separator !== -1) {
          const frame = buffer.slice(0, separator)
          buffer = buffer.slice(separator + 2)
          sawFrame = true
          handleFrame(frame)
          separator = buffer.indexOf('\n\n')
        }

        if (buffer.length > MAX_FRAME_BYTES) {
          throw new Error('oversized SSE frame')
        }
      }
      return sawFrame
    } finally {
      // Release the reader; the run's abort signal stops the transfer itself.
      void reader.cancel().catch(() => {
        // The body may already be closed or aborted; nothing to do about it.
      })
    }
  }

  /** Act on one frame. Only the `data:` payload is used. */
  function handleFrame(frame: string) {
    let data = ''
    for (const line of frame.split('\n')) {
      if (line === '' || line.startsWith(':')) continue // comment / keep-alive
      const colon = line.indexOf(':')
      const field = colon === -1 ? line : line.slice(0, colon)
      let value = colon === -1 ? '' : line.slice(colon + 1)
      if (value.startsWith(' ')) value = value.slice(1)
      if (field === 'data') data = data === '' ? value : `${data}\n${value}`
    }
    if (data === '') return

    try {
      const event: unknown = JSON.parse(data)
      if (
        typeof event === 'object' &&
        event !== null &&
        (event as { type?: unknown }).type === 'data_changed'
      ) {
        void refreshFromDataChange()
      }
    } catch {
      // A frame we cannot parse is not a reason to drop the stream.
    }
  }

  return {
    me,
    authenticated,
    summary,
    timeseries,
    requests,
    nextCursor,
    status,
    pageSize,
    loading,
    error,
    range,
    model,
    streamState,
    automaticUpdatesEnabled,
    dataChangeVersion,
    selectedConsumers,
    latestRequest,
    isManager,
    hasMore,
    hasPrevious,
    currentPage,
    models,
    fetchMe,
    signIn,
    signOut,
    fetchSummary,
    fetchTimeseries,
    fetchRequests,
    fetchModels,
    firstPage,
    setAutomaticUpdates,
    previousPage,
    nextPage,
    setStatus,
    setPageSize,
    resetAndFetchRequests,
    connect,
    disconnect,
    refresh,
  }
})
