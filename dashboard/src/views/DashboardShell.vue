<script setup lang="ts">
/**
 * The authenticated shell: the one place that owns the header, the identity,
 * the manager's consumer scope, the live-data alerts, and the invalidation
 * stream.
 *
 * # One connection, one owner
 *
 * The SSE stream is a single socket for the whole session, not one per page.
 * It is opened here when an authenticated route is first mounted and closed
 * when the shell itself unmounts (sign-out). Views come and go underneath it
 * and never touch `connect`/`disconnect`, so navigating between usage, billing
 * and partner administration cannot leave a second stream running or force a
 * reconnect cycle on every navigation.
 *
 * # Scope lives here too
 *
 * `selectedConsumers` is the manager's narrowing, and it outlives any one page:
 * a consumer chosen on the usage view is still the scope the billing view
 * reads. Keeping it in the store (and its chips here) means a view only has to
 * ask for "my current scope", never re-derive it.
 */
import { computed, onBeforeUnmount, onMounted, ref, watch } from 'vue'
import { storeToRefs } from 'pinia'
import { useRouter } from 'vue-router'
import { useDashboardStore } from '@/stores/dashboard'

const store = useDashboardStore()
const router = useRouter()
const { me, automaticUpdatesEnabled, isManager, selectedConsumers, latestRequest, dataChangeVersion } = storeToRefs(store)

const consumerOptions = computed(() => me.value?.consumers ?? [])

interface ChangeAlert {
  id: number
}

const changeAlerts = ref<ChangeAlert[]>([])
const alertTimers = new Map<number, ReturnType<typeof setTimeout>>()
let nextAlertId = 0

/**
 * The newest request seen, shaped for the toast. Read live rather than snapped
 * at announce time: an SSE event bumps the version *before* the refetch lands,
 * so a snapshot would show the previous request.
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

function formatDuration(ms: number | null | undefined): string {
  if (ms === null || ms === undefined) return '—'
  if (ms < 1000) return `${Math.round(ms)}ms`
  return `${(ms / 1000).toFixed(2)}s`
}

function statusClass(httpStatus: number | null): string {
  if (httpStatus === null) return 'unknown'
  if (httpStatus >= 200 && httpStatus < 300) return 'success'
  if (httpStatus >= 300 && httpStatus < 400) return 'redirect'
  if (httpStatus >= 400 && httpStatus < 500) return 'client-error'
  if (httpStatus >= 500 && httpStatus < 600) return 'server-error'
  return 'unknown'
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
  if (!automaticUpdatesEnabled.value) return

  const alert = { id: ++nextAlertId }
  if (changeAlerts.value.length === 2) dismissAlert(changeAlerts.value[0].id)
  changeAlerts.value.push(alert)
  scheduleAlertDismiss(alert.id)
}

function toggleAlerts() {
  const next = !automaticUpdatesEnabled.value
  void store.setAutomaticUpdates(next)
  if (!next) {
    for (const alert of changeAlerts.value) dismissAlert(alert.id)
  }
}

/**
 * The stream is the only source of the change signal: the store bumps
 * `dataChangeVersion` for every `data_changed` frame, and the views mounted
 * underneath watch that same counter to refetch what they own. The shell
 * watches it too, for the toast.
 */
watch(dataChangeVersion, (version) => {
  if (version > 0) announceDataChange()
})

/**
 * Toggle one consumer on the manager's multi-select. The store owns the
 * re-scoping: its request signature includes the selection, so a change
 * returns the usage table to page one and re-reads the summary and chart.
 */
function toggleConsumer(consumer: string) {
  const next = selectedConsumers.value.includes(consumer)
    ? selectedConsumers.value.filter((entry) => entry !== consumer)
    : [...selectedConsumers.value, consumer]
  selectedConsumers.value = next
  store.onConsumersScopeChanged()
}

/**
 * The "All" chip: clear the narrowing entirely. An empty selection is how the
 * store says "every consumer" — the manager's default view.
 */
function selectAllConsumers() {
  if (selectedConsumers.value.length === 0) return
  selectedConsumers.value = []
  store.onConsumersScopeChanged()
}

onMounted(() => {
  // The credential is validated by `App.vue` before this mounts, so the store
  // is already authenticated; opening the stream here is safe. It is idempotent
  // and stays a no-op while automatic updates are paused.
  store.connect()
})

onBeforeUnmount(() => {
  for (const timer of alertTimers.values()) clearTimeout(timer)
  alertTimers.clear()
  store.disconnect()
})

function signOut() {
  store.signOut()
  void router.replace({ name: 'login' })
}
</script>

<template>
  <div class="shell">
    <header class="header">
      <div class="header-left">
        <h1>Partner Portal</h1>
        <nav class="nav" aria-label="Sections">
          <RouterLink to="/" exact-active-class="nav-link-active">Usage</RouterLink>
          <RouterLink to="/billing" active-class="nav-link-active">Billing</RouterLink>
          <!-- Manager surfaces are hidden, not secured, here: the admin routes
               answer `ManagerOnly` regardless of what the SPA renders. -->
          <RouterLink v-if="isManager" to="/partners" active-class="nav-link-active">Partners</RouterLink>
          <RouterLink v-if="isManager" to="/manager/billing" active-class="nav-link-active">Invoices</RouterLink>
        </nav>
      </div>
      <div class="header-controls">
        <button
          class="icon-button live-toggle"
          type="button"
          :aria-pressed="automaticUpdatesEnabled"
          :aria-label="automaticUpdatesEnabled ? 'Pause automatic updates and data-change alerts' : 'Resume automatic updates and data-change alerts'"
          :title="automaticUpdatesEnabled ? 'Pause automatic updates and data-change alerts' : 'Resume automatic updates and data-change alerts'"
          @click="toggleAlerts"
        >
          <svg v-if="automaticUpdatesEnabled" class="icon" viewBox="0 0 16 16" aria-hidden="true">
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
          @click="signOut"
        >
          <svg class="icon" viewBox="0 0 16 16" aria-hidden="true">
            <path d="M2 2h6v1.5H3.5v9H8V14H2z" fill="currentColor"></path>
            <path d="M10.5 3.5l4 4.5-4 4.5-.9-.8L12.7 8.5H6v-1h6.7l-3.1-3.2z" fill="currentColor"></path>
          </svg>
        </button>
      </div>
    </header>

    <!-- Identity, and the manager's scope. Both sit in the shell because both
         apply to every page underneath, not only to the usage view. -->
    <div class="identity-row">
      <div v-if="isManager" class="identity-side consumer-filter">
        <span class="identity-label" aria-hidden="true">Consumers</span>
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
            v-for="consumer in consumerOptions"
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
             nothing to offer but All (docs/adr/0013). It is not the partner
             list — a commercial partner with no usage does not appear here. -->
        <p v-if="!consumerOptions.length" class="identity-muted">No usage recorded yet</p>
      </div>
      <div v-else-if="me" class="identity-side whoami" :title="`key: ${me.key_name}`">
        <span class="identity-label">Key</span>
        <span class="whoami-name">{{ me.key_name }}</span>
        <span class="whoami-consumer">{{ me.consumer_id }}</span>
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
          <button type="button" class="alert-dismiss" aria-label="Dismiss update alert" @click="dismissAlert(alert.id)">×</button>
        </div>
      </TransitionGroup>
    </div>

    <main class="shell-main">
      <RouterView />
    </main>
  </div>
</template>

<style scoped>
.shell { max-width: 1760px; margin: 0 auto; padding: 2rem; }
.header, .header-left, .header-controls, .nav, .identity-row, .identity-side, .consumer-toggles { display: flex; align-items: center; }
.header { justify-content: space-between; gap: 1rem; flex-wrap: wrap; margin-bottom: 1.5rem; }
.header-left { gap: 1.25rem; flex-wrap: wrap; }
.header-controls { gap: .75rem; }
.header h1 { font-size: 1.5rem; font-weight: 650; }
.nav { gap: .25rem; }
.nav a { padding: .4rem .7rem; border-radius: .375rem; color: var(--muted); font-size: .875rem; }
.nav a:hover { color: var(--fg); background: rgba(255,255,255,.04); }
.nav .nav-link-active { color: var(--fg); background: rgba(255,255,255,.08); }
.nav a:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.whoami { color: var(--muted); font-size: .8125rem; white-space: nowrap; }
/* Icon-only controls: 36px keeps them above the 24px minimum touch target. */
.icon-button { display: inline-flex; align-items: center; justify-content: center; width: 2.25rem; height: 2.25rem; padding: 0; background: transparent; border: 1px solid var(--border); border-radius: .375rem; color: var(--muted); }
.icon-button .icon { width: 1rem; height: 1rem; }
.icon-button:hover { color: var(--fg); border-color: var(--fg); background: rgba(255,255,255,.04); }
.live-toggle[aria-pressed='true'] { color: var(--accent); border-color: rgba(59,130,246,.6); }
.icon-button:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
/* Toasts sit in the bottom-right corner, small: at most two compact rows. */
.alert-stack { position: fixed; z-index: 10; right: .75rem; bottom: .75rem; display: grid; gap: .375rem; max-width: min(20rem, calc(100vw - 1.5rem)); }
.data-alert { display: flex; align-items: center; justify-content: space-between; gap: .625rem; padding: .3rem .55rem; color: var(--fg); background: #172554; border: 1px solid rgba(96,165,250,.65); border-radius: .375rem; box-shadow: 0 8px 20px rgba(0,0,0,.35); font-size: .75rem; line-height: 1.3; }
.alert-model { font-weight: 650; }
.alert-sep { margin: 0 .3rem; color: var(--muted); }
.alert-status.success { color: var(--success); }
.alert-status.redirect { color: #7dd3fc; }
.alert-status.client-error, .alert-status.unknown { color: var(--warning); }
.alert-status.server-error { color: var(--error); }
.alert-dismiss { display: inline-flex; align-items: center; justify-content: center; padding: 0; min-width: 1.125rem; height: 1.125rem; border-radius: .25rem; background: transparent; color: var(--muted); font-size: .9rem; line-height: 1; }
.alert-dismiss:hover { background: rgba(255,255,255,.12); color: var(--fg); }
.data-alert-enter-active, .data-alert-leave-active { transition: opacity .18s ease, transform .18s ease; }
.data-alert-enter-from, .data-alert-leave-to { opacity: 0; transform: translateY(.375rem); }
.data-alert-move { transition: transform .18s ease; }
.identity-row { justify-content: flex-start; gap: 1rem; flex-wrap: wrap; margin-bottom: 1.5rem; }
.identity-side { gap: .5rem; flex-wrap: wrap; }
.identity-muted { color: var(--muted); font-size: .8125rem; }
.identity-label { color: var(--muted); font-size: .75rem; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }
.whoami-name { font-size: .875rem; font-weight: 650; white-space: nowrap; }
.whoami-consumer { color: var(--muted); font-size: .8125rem; white-space: nowrap; }
.consumer-toggles { display: flex; gap: .4rem; flex-wrap: wrap; }
.consumer-chip { background: transparent; border: 1px solid var(--border); color: var(--muted); font-size: .8125rem; padding: .4rem .65rem; }
.consumer-chip[aria-pressed='true'] { color: var(--accent); border-color: rgba(59,130,246,.6); }
.consumer-chip:hover { color: var(--fg); border-color: var(--fg); background: transparent; }
.consumer-chip:focus-visible { outline: 2px solid var(--accent); outline-offset: 2px; }
.shell-main { display: block; }
@media (max-width: 700px) { .shell { padding: 1rem; } .header { align-items: stretch; } .header-controls { align-items: center; justify-content: flex-end; } .nav { width: 100%; overflow-x: auto; } .identity-row { flex-direction: column; align-items: stretch; gap: .75rem; } .identity-side { align-items: stretch; flex-direction: column; width: 100%; } .consumer-chip { flex: 1 1 auto; min-width: 5rem; } .whoami-name, .whoami-consumer { white-space: normal; } }
</style>
