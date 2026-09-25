<script setup lang="ts">
import { ref } from 'vue'
import { useDashboardStore } from '@/stores/dashboard'

const store = useDashboardStore()
const key = ref('')
const busy = ref(false)
const localError = ref('')

async function submit() {
  if (busy.value) return
  busy.value = true
  localError.value = ''
  const ok = await store.signIn(key.value)
  busy.value = false
  if (!ok) {
    // The store records the reason (missing key, 401, network). Show it here so
    // the login screen explains itself instead of silently returning.
    localError.value = store.error ?? 'Could not sign in'
  }
}
</script>

<template>
  <div class="login">
    <div class="login-card">
      <div class="login-lock" aria-hidden="true">🔑</div>
      <h1>Partner Portal</h1>
      <p class="login-sub">
        Sign in with the local API key assigned to your consumer. The dashboard
        shows the usage of the consumer this key belongs to — never another
        consumer's traffic.
      </p>

      <form class="login-form" @submit.prevent="submit">
        <label for="api-key">API key</label>
        <input
          id="api-key"
          v-model="key"
          type="password"
          autocomplete="off"
          autocapitalize="off"
          spellcheck="false"
          placeholder="pp-…"
          :disabled="busy"
          @keyup.enter="submit"
        />
        <p v-if="localError" class="login-error" role="alert">{{ localError }}</p>
        <button type="submit" :disabled="busy || !key.trim()">
          {{ busy ? 'Verifying…' : 'Sign in' }}
        </button>
      </form>

      <p class="login-hint">
        The key is sent only in the <code>Authorization</code> header and never
        appears in a URL. It is stored in this browser only, and can be cleared
        with <em>Sign out</em>.
      </p>
    </div>
  </div>
</template>

<style scoped>
.login {
  min-height: 100vh;
  display: flex;
  align-items: center;
  justify-content: center;
  padding: 1.5rem;
}

.login-card {
  background: var(--bg-card);
  border: 1px solid var(--border);
  border-radius: 0.75rem;
  padding: 2.5rem;
  width: 100%;
  max-width: 24rem;
  text-align: center;
}

.login-lock {
  font-size: 2rem;
  margin-bottom: 0.75rem;
}

.login-card h1 {
  font-size: 1.25rem;
  font-weight: 600;
  margin-bottom: 0.5rem;
}

.login-sub {
  color: var(--muted);
  font-size: 0.875rem;
  margin-bottom: 1.5rem;
}

.login-form {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  text-align: left;
}

.login-form label {
  font-size: 0.75rem;
  text-transform: uppercase;
  color: var(--muted);
}

.login-form input {
  padding: 0.625rem 0.75rem;
  font-size: 0.875rem;
}

.login-error {
  color: var(--error);
  font-size: 0.8125rem;
}

.login-form button {
  margin-top: 0.5rem;
  font-weight: 600;
}

.login-hint {
  margin-top: 1.25rem;
  font-size: 0.75rem;
  color: var(--muted);
}
</style>