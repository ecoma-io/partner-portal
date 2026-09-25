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
  await store.fetchMe()
})
</script>

<template>
  <RouterView v-if="store.authenticated" />
  <Login v-else />
</template>

<style scoped></style>