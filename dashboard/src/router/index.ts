import { createRouter, createWebHistory } from 'vue-router'
import { useDashboardStore } from '@/stores/dashboard'
import DashboardShell from '@/views/DashboardShell.vue'
import Login from '@/views/Login.vue'

/**
 * Routes.
 *
 * # The shell is a layout, not a page
 *
 * `DashboardShell` is the parent of every authenticated route, so it mounts
 * once for the whole session and owns the header, the identity, the manager's
 * consumer scope, the alerts, and the single invalidation stream. Its children
 * are the pages; navigating between them never re-opens the stream.
 *
 * # Manager routes are gated for navigation, not for security
 *
 * `requireManager` keeps a partner credential from *reaching* a page it has no
 * use for, and returns it to its own usage view. It is a convenience: the
 * authorization boundary is `ManagerOnly` on the server, which refuses these
 * routes with 403 whatever the SPA renders. Nothing here is trusted.
 *
 * # `/login` is a way in, not a page you can stay on
 *
 * `App.vue` decides what is rendered — the shell or the login card — and the
 * login route exists so a deep link has somewhere to land. Once a credential is
 * valid, the login route redirects to the dashboard instead of showing a
 * second, redundant way in.
 */
const router = createRouter({
  history: createWebHistory(),
  routes: [
    {
      path: '/login',
      name: 'login',
      component: Login,
      meta: { public: true },
    },
    {
      path: '/',
      component: DashboardShell,
      children: [
        {
          path: '',
          name: 'dashboard',
          component: () => import('@/views/UsageDashboard.vue'),
        },
        {
          path: 'billing',
          name: 'billing',
          component: () => import('@/views/Billing.vue'),
        },
        {
          path: 'partners',
          name: 'partners',
          component: () => import('@/views/Partners.vue'),
          meta: { managerOnly: true },
        },
        {
          path: 'manager/billing',
          name: 'manager-billing',
          component: () => import('@/views/ManagerBilling.vue'),
          meta: { managerOnly: true },
        },
      ],
    },
    // Anything else is a stale or mistyped link; the usage view is the default.
    { path: '/:pathMatch(.*)*', redirect: { name: 'dashboard' } },
  ],
})

router.beforeEach((to) => {
  const store = useDashboardStore()

  // Before `/api/me` has answered, the role is unknown rather than absent, so
  // any redirect would be a guess. Wait: `App.vue` keeps the login card up
  // until the identity is resolved.
  if (!store.identityResolved) return true

  if (to.meta.public) {
    // An authenticated visitor has no business on the login screen.
    return store.authenticated ? { name: 'dashboard' } : true
  }
  if (!store.authenticated) {
    return { name: 'login' }
  }
  if (to.meta.managerOnly && !store.isManager) {
    return { name: 'dashboard' }
  }
  return true
})

export default router
