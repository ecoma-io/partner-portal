<script setup lang="ts">
import { onMounted } from 'vue'
import { RouterView } from 'vue-router'
import { useDashboardStore } from '@/stores/dashboard'
import Login from '@/views/Login.vue'

const store = useDashboardStore()

onMounted(async () => {
  // A stored key may be stale or revoked; presence is not authentication.
  // Validate it against the backend before showing anything. When no key is
  // stored, fetchMe resolves to a 401 and the login screen appears.
  //
  // The login card stays up until this answers — the router deliberately does
  // not redirect on an unresolved identity, so rendering the shell early would
  // flash authenticated chrome at a credential that may be about to be refused.
  await store.fetchMe()
})
</script>

<template>
  <RouterView v-if="store.authenticated" />
  <Login v-else />
</template>
