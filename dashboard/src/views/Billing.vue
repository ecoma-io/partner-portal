<script setup lang="ts">
/**
 * A partner's own billing page.
 *
 * # What a partner may see and do here
 *
 * Their derived service status, their statements, and a statement's lines. That
 * is all. There is no payment action on this page and none is reachable from it:
 * money moves outside the product, and a partner who could mark their own
 * statement paid would be able to end their own suspension with one click. The
 * operator's surface is a separate route with its own gate.
 *
 * # Nothing about suspension is computed here
 *
 * `suspended`, the reason and the overdue totals come from `/api/billing/status`,
 * which derives them through the same `status_for` the request path uses. A
 * second comparison in the browser — "due date has passed, so call it overdue" —
 * is how a dashboard ends up telling a partner their service is suspended while
 * the proxy is still serving them, or the reverse. The page renders the server's
 * answer and says so.
 *
 * # No money is recomputed
 *
 * Every figure is the string the server formatted. An incomplete statement's
 * cost is not `$0.00` — it is an incomplete statement, and the count beside the
 * total is what says that.
 */
import { computed, onMounted, watch } from 'vue'
import { storeToRefs } from 'pinia'

import StatementDetail from '@/components/StatementDetail.vue'
import { useBillingStore } from '@/stores/billing'
import { useDashboardStore } from '@/stores/dashboard'

const billing = useBillingStore()
const dashboard = useDashboardStore()
const { dataChangeVersion, selectedConsumers, isManager } = storeToRefs(dashboard)
const { partnerStatements, serviceStatuses, partnerDetail, partnerLoading, partnerDetailLoading, partnerError } =
  storeToRefs(billing)

const statements = computed(() => partnerStatements.value.statements)
const hasStatements = computed(() => statements.value.length > 0)

/**
 * A manager may narrow the whole portal to one consumer, and that scope applies
 * here too. A partner has exactly one status — its own — so the list is either
 * that one or empty.
 */
const statuses = computed(() => serviceStatuses.value)

function selectStatement(id: number) {
  void billing.loadPartnerStatement(id)
}

function closeStatement() {
  billing.partnerDetail = null
}

onMounted(() => {
  void billing.loadPartner()
})

// The invalidation stream is the only live-update mechanism. A `data_changed`
// frame means "something was written"; the page re-reads its own scoped
// endpoints and the server decides what actually applies to this credential.
watch(dataChangeVersion, (version) => {
  if (version > 0) void billing.loadPartner()
})

// The manager's scope is the shell's, and changing it changes what this page is
// about — so it reloads rather than showing another partner's statements under
// the previous scope's selection.
watch(selectedConsumers, () => {
  if (isManager.value) void billing.loadPartner()
})
</script>

<template>
  <div class="billing-view">
    <h2 class="view-title">Billing</h2>

    <p v-if="partnerError" class="error-banner" role="alert">{{ partnerError }}</p>

    <section class="status-section" aria-label="Service status">
      <article
        v-for="status in statuses"
        :key="status.consumer_id"
        class="card status-card"
        :class="status.suspended ? 'is-suspended' : 'is-active'"
      >
        <div class="status-heading">
          <h3>
            <span v-if="isManager">{{ status.consumer_id }} — </span>Service status
          </h3>
          <!-- The word carries the state, not only the colour. -->
          <span class="status-badge" :class="status.suspended ? 'suspended' : 'active'">
            {{ status.suspended ? 'Suspended' : 'Active' }}
          </span>
        </div>

        <!-- The server's own message, so this page and the 403 a suspended
             request receives cannot say different things. -->
        <p v-if="status.suspended && status.message" class="status-message">{{ status.message }}</p>
        <p v-else-if="!status.suspended" class="status-message muted">
          Requests are being served normally.
        </p>

        <dl v-if="status.reason" class="reason-facts">
          <div>
            <dt>Reason</dt>
            <dd>Invoice overdue</dd>
          </div>
          <div>
            <dt>Statement</dt>
            <dd>
              <a href="#" @click.prevent="selectStatement(status.reason.statement_id)">
                {{ status.reason.billing_date }} — {{ status.reason.amount }}
              </a>
            </dd>
          </div>
          <div>
            <dt>Was due</dt>
            <dd>{{ new Date(status.reason.due_at).toLocaleString() }}</dd>
          </div>
        </dl>

        <!-- The count of overdue statements and the money they total are
             deliberately separate: an incomplete statement is overdue by date,
             contributes no money, and suspends nobody. -->
        <dl class="overdue-facts">
          <div>
            <dt>Overdue invoices</dt>
            <dd>{{ status.overdue_statements }}</dd>
          </div>
          <div>
            <dt>Overdue amount</dt>
            <dd>{{ status.overdue_amount }}</dd>
          </div>
        </dl>
      </article>

      <p v-if="statuses.length === 0 && !partnerLoading" class="empty-state">
        No service status is available for this account yet.
      </p>
    </section>

    <section class="statements-section" aria-label="Statements">
      <div class="section-heading">
        <div>
          <h3>Statements</h3>
          <p>
            {{ partnerStatements.total }} statement{{ partnerStatements.total === 1 ? '' : 's' }}
            <template v-if="partnerStatements.unpaid_count > 0">
              · {{ partnerStatements.unpaid_count }} unpaid totalling
              {{ partnerStatements.unpaid_total }}
            </template>
          </p>
        </div>
      </div>

      <p v-if="partnerLoading" class="loading-state">Loading statements…</p>
      <p v-else-if="!hasStatements" class="empty-state">
        No statements have been issued yet. A statement is written once per billing
        day, after the usage for that day is final.
      </p>

      <table v-else class="statements-table">
        <caption class="sr-only">Issued statements</caption>
        <thead>
          <tr>
            <th scope="col">Date</th>
            <th scope="col">Type</th>
            <th scope="col">Amount</th>
            <th scope="col">Status</th>
            <th scope="col"><span class="sr-only">Open</span></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="statement in statements" :key="statement.id">
            <td>{{ statement.billing_date }}</td>
            <td>
              <span
                v-if="statement.billing_mode === 'invoice'"
                class="mode-badge invoice"
              >Invoice</span>
              <span v-else class="mode-badge reconciliation">Reconciliation</span>
            </td>
            <td class="amount">{{ statement.total_amount }} {{ statement.currency }}</td>
            <td>
              <!-- A settlement record has no payment state to report, so it is
                   not given one. Labelling it "paid" would imply an obligation
                   that never existed. -->
              <span v-if="statement.billing_mode === 'reconciliation'" class="muted">
                Settlement record
              </span>
              <span v-else-if="statement.paid_at !== null" class="paid-text">Paid</span>
              <span v-else-if="statement.due_at === null" class="muted">No deadline</span>
              <span v-else class="outstanding-text">
                Due {{ new Date(statement.due_at).toLocaleDateString() }}
              </span>
              <span v-if="statement.has_incomplete_usage" class="incomplete-flag">
                {{ statement.incomplete_usage_count }} unpriced
              </span>
            </td>
            <td>
              <button type="button" class="link-button" @click="selectStatement(statement.id)">
                Details
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </section>

    <section v-if="partnerDetail || partnerDetailLoading" class="detail-section" aria-label="Statement detail">
      <div class="section-heading">
        <h3>Statement detail</h3>
        <button type="button" class="link-button" @click="closeStatement">Close</button>
      </div>
      <p v-if="partnerDetailLoading" class="loading-state">Loading statement…</p>
      <!-- `canRecordPayment` is never passed on this surface. That is the whole
           point: a partner's page has no payment control to omit later. -->
      <StatementDetail
        v-else-if="partnerDetail"
        :statement="partnerDetail"
      />
    </section>
  </div>
</template>

<style scoped>
.billing-view { max-width: 1200px; }
.view-title { margin-bottom: 1.5rem; font-size: 1.25rem; font-weight: 650; }
.status-section { display: grid; grid-template-columns: repeat(auto-fit, minmax(20rem, 1fr)); gap: 1rem; margin-bottom: 2rem; }
.status-card { border-left: 3px solid var(--border); }
.status-card.is-active { border-left-color: var(--success); }
.status-card.is-suspended { border-left-color: var(--error); }
.status-heading { display: flex; align-items: center; justify-content: space-between; gap: 1rem; margin-bottom: .75rem; }
.status-heading h3 { font-size: 1rem; font-weight: 650; }
.status-badge { padding: .15rem .5rem; border: 1px solid currentColor; border-radius: .25rem; font-size: .75rem; font-weight: 650; }
.status-badge.active { color: var(--success); }
.status-badge.suspended { color: var(--error); }
.status-message { margin-bottom: .75rem; color: #fca5a5; font-size: .875rem; }
.status-message.muted { color: var(--muted); }
.reason-facts, .overdue-facts { display: grid; grid-template-columns: repeat(auto-fit, minmax(9rem, 1fr)); gap: .75rem; }
.reason-facts { margin-bottom: .75rem; padding-bottom: .75rem; border-bottom: 1px solid var(--border); }
.reason-facts dt, .overdue-facts dt { color: var(--muted); font-size: .75rem; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }
.reason-facts dd, .overdue-facts dd { margin-top: .2rem; font-size: .875rem; font-variant-numeric: tabular-nums; }
.section-heading { display: flex; align-items: center; justify-content: space-between; gap: 1rem; flex-wrap: wrap; margin-bottom: 1rem; }
.section-heading h3 { font-size: 1rem; font-weight: 650; }
.section-heading p { margin-top: .2rem; color: var(--muted); font-size: .8125rem; }
.statements-section { margin-bottom: 2rem; }
.statements-table { width: 100%; border-collapse: collapse; font-size: .875rem; }
.statements-table th { padding: .75rem; border-bottom: 1px solid var(--border); color: var(--muted); font-weight: 550; text-align: left; }
.statements-table td { padding: .75rem; border-bottom: 1px solid var(--border); vertical-align: top; }
.statements-table tbody tr:hover { background: rgba(255,255,255,.02); }
.amount { font-variant-numeric: tabular-nums; font-weight: 600; }
.mode-badge { padding: .1rem .45rem; border: 1px solid currentColor; border-radius: .25rem; font-size: .75rem; font-weight: 600; }
.mode-badge.invoice { color: var(--warning); }
.mode-badge.reconciliation { color: #7dd3fc; }
.paid-text { color: var(--success); }
.outstanding-text { color: var(--warning); }
.muted { color: var(--muted); }
.incomplete-flag { display: block; color: var(--warning); font-size: .75rem; }
.link-button { padding: 0; background: transparent; color: var(--accent); font-size: .8125rem; text-decoration: underline; }
.link-button:hover { background: transparent; color: var(--accent-hover); }
.loading-state, .empty-state { padding: 2rem 1rem; color: var(--muted); text-align: center; }
.error-banner { margin-bottom: 1.5rem; padding: .75rem 1rem; color: #fca5a5; background: rgba(239,68,68,.1); border: 1px solid var(--error); border-radius: .375rem; }
.sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }
</style>
