<script setup lang="ts">
/**
 * The operator's billing page: what every partner has been billed, what is still
 * owed, and recording a payment.
 *
 * # Marking a payment is the one write on this page, and it is gated twice
 *
 * The button appears only for an invoice statement that is outstanding — the
 * same rule the server enforces with 409 `statement_not_payable`. The UI gate is
 * a convenience that keeps an operator from typing into a form the backend will
 * refuse; the server's guard is the one that is trusted, because a stale page is
 * exactly the case where the two disagree. A reconciliation statement has no
 * payment control at all, no due date, and no unpaid wording anywhere.
 *
 * # The suspension count is derived, not stored
 *
 * `suspended_partners` in the summary comes from the same `status_for` the
 * request path uses, so it moves on its own when a payment lands. This page
 * therefore refetches the whole summary after a payment rather than decrementing
 * a number locally: a stale count of suspended partners is a count someone acts
 * on.
 */
import { computed, onMounted, ref, watch } from 'vue'
import { storeToRefs } from 'pinia'

import StatementDetail from '@/components/StatementDetail.vue'
import { useBillingStore, type ManagerStatement, type PaymentInput, type Statement } from '@/stores/billing'
import { useDashboardStore } from '@/stores/dashboard'

const billing = useBillingStore()
const dashboard = useDashboardStore()
const { dataChangeVersion, selectedConsumers, isManager } = storeToRefs(dashboard)
const {
  managerStatements,
  managerDetail,
  summary,
  managerLoading,
  managerDetailLoading,
  managerError,
  onlyUnpaid,
  paying,
  paymentError,
} = storeToRefs(billing)

const statements = computed(() => managerStatements.value.statements)

// The form opens for one statement at a time, and only for an invoice that is
// still owed. `outstanding` is the server's own answer to "is this owed", so the
// page never re-derives it from the due date.
const paymentFor = ref<Statement | null>(null)
const form = ref<PaymentInput>({ paid_by: '', reference: '', note: '' })

const unpaidOnly = computed({
  get: () => onlyUnpaid.value,
  set: (next: boolean) => {
    billing.setOnlyUnpaid(next)
    void billing.loadManager()
  },
})

function openPayment(statement: Statement) {
  paymentFor.value = statement
  form.value = { paid_by: '', reference: '', note: '' }
  billing.clearPaymentError()
}

function cancelPayment() {
  paymentFor.value = null
  billing.clearPaymentError()
}

async function submitPayment() {
  const target = paymentFor.value
  if (target === null) return
  // Blank fields are sent as absent rather than as empty strings: the server
  // trims and drops them, and an empty payment reference in the audit trail
  // says nothing while looking like it was recorded.
  const payload: PaymentInput = {}
  if (form.value.paid_by?.trim()) payload.paid_by = form.value.paid_by.trim()
  if (form.value.reference?.trim()) payload.reference = form.value.reference.trim()
  if (form.value.note?.trim()) payload.note = form.value.note.trim()

  const recorded = await billing.markPaid(target.id, payload)
  if (recorded) {
    paymentFor.value = null
    // The statement just changed; the detail pane is now stale.
    if (managerDetail.value?.id === target.id) void billing.loadManagerStatement(target.id)
  }
}

function selectStatement(statement: ManagerStatement) {
  void billing.loadManagerStatement(statement.id)
}

function closeStatement() {
  billing.managerDetail = null
  cancelPayment()
}

onMounted(() => {
  // The route is manager-only, but a store that somehow holds a partner
  // credential would get 403s rather than data. Say so instead of rendering
  // an empty page that looks like "nobody owes anything".
  if (!isManager.value) {
    managerError.value = 'This page is for the manager credential.'
    return
  }
  void billing.loadManager()
})

watch(dataChangeVersion, (version) => {
  if (version > 0 && isManager.value) void billing.loadManager()
})

watch(selectedConsumers, () => {
  if (isManager.value) void billing.loadManager()
})
</script>

<template>
  <div class="manager-billing">
    <h2 class="view-title">Invoices</h2>

    <p v-if="managerError" class="error-banner" role="alert">{{ managerError }}</p>

    <section v-if="summary" class="summary" aria-label="Billing summary">
      <article class="card summary-card">
        <div class="summary-label">Partners</div>
        <div class="summary-value">{{ summary.partners }}</div>
        <div class="summary-sub">
          <span v-if="summary.suspended_partners > 0" class="suspended-text">
            {{ summary.suspended_partners }} suspended
          </span>
          <span v-else class="muted">none suspended</span>
        </div>
      </article>
      <article class="card summary-card">
        <div class="summary-label">Statements</div>
        <div class="summary-value">{{ summary.statements }}</div>
      </article>
      <article class="card summary-card">
        <div class="summary-label">Unpaid</div>
        <div class="summary-value">{{ summary.unpaid_statements }}</div>
        <div class="summary-sub">totalling {{ summary.unpaid_total }}</div>
      </article>
    </section>

    <section class="statements-section" aria-label="Statements">
      <div class="section-heading">
        <div>
          <h3>Statements</h3>
          <p>{{ managerStatements.total }} in this scope</p>
        </div>
        <!-- The filter is "unpaid", so it returns unpaid *invoices* only. A
             reconciliation statement is never in the result, which is why the
             control's label does not say "not paid". -->
        <label class="unpaid-filter">
          <input v-model="unpaidOnly" type="checkbox" />
          Only unpaid invoices
        </label>
      </div>

      <p v-if="managerLoading" class="loading-state">Loading statements…</p>
      <p v-else-if="statements.length === 0" class="empty-state">
        No statements{{ onlyUnpaid ? ' are unpaid in this scope' : ' yet in this scope' }}.
      </p>

      <table v-else class="statements-table">
        <caption class="sr-only">Issued statements</caption>
        <thead>
          <tr>
            <th scope="col">Date</th>
            <th scope="col">Consumer</th>
            <th scope="col">Type</th>
            <th scope="col">Amount</th>
            <th scope="col">Status</th>
            <th scope="col"><span class="sr-only">Actions</span></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="statement in statements" :key="statement.id">
            <td>{{ statement.billing_date }}</td>
            <td class="consumer">{{ statement.consumer_id }}</td>
            <td>
              <span v-if="statement.billing_mode === 'invoice'" class="mode-badge invoice">Invoice</span>
              <span v-else class="mode-badge reconciliation">Reconciliation</span>
            </td>
            <td class="amount">{{ statement.total_amount }} {{ statement.currency }}</td>
            <td>
              <!-- A settlement record gets a settlement label. It has no payment
                   state, so it is never shown as unpaid, due, or paid. -->
              <span v-if="statement.billing_mode === 'reconciliation'" class="reconciliation-text">
                Settlement record
              </span>
              <span v-else-if="statement.paid_at !== null" class="paid-text">
                Paid{{ statement.paid_by ? ` by ${statement.paid_by}` : '' }}
              </span>
              <span v-else class="outstanding-text">
                Due {{ statement.due_at ? new Date(statement.due_at).toLocaleDateString() : '—' }}
                <span v-if="statement.can_suspend" class="suspend-flag" title="Past due and complete: this statement is why service is suspended">
                  can suspend
                </span>
              </span>
              <span v-if="statement.has_incomplete_usage" class="incomplete-flag">
                {{ statement.incomplete_usage_count }} unpriced
              </span>
            </td>
            <td class="row-actions">
              <button type="button" class="link-button" @click="selectStatement(statement)">Details</button>
              <button
                v-if="statement.billing_mode === 'invoice' && statement.outstanding"
                type="button"
                class="link-button"
                @click="openPayment(statement)"
              >
                Mark paid
              </button>
            </td>
          </tr>
        </tbody>
      </table>
    </section>

    <!-- The payment form exists only while an outstanding invoice is selected,
         and it says what it records: that money moved outside this product. -->
    <section v-if="paymentFor" class="card payment-form" aria-label="Record a payment">
      <h3>Record payment for {{ paymentFor.billing_date }}</h3>
      <p class="payment-note">
        This records a payment of {{ paymentFor.total_amount }} {{ paymentFor.currency }} that
        has already been made. It does not charge anything.
      </p>
      <form @submit.prevent="submitPayment">
        <div class="form-grid">
          <label>
            Recorded by
            <input
              v-model="form.paid_by"
              type="text"
              placeholder="manager"
              autocomplete="off"
            />
          </label>
          <label>
            Payment reference
            <input
              v-model="form.reference"
              type="text"
              placeholder="bank reference, transfer id, cheque number"
              autocomplete="off"
            />
          </label>
          <label class="wide">
            Note
            <input v-model="form.note" type="text" autocomplete="off" />
          </label>
        </div>
        <p v-if="paymentError" class="error-banner" role="alert">{{ paymentError }}</p>
        <div class="form-actions">
          <button type="submit" :disabled="paying">
            {{ paying ? 'Recording…' : 'Record payment' }}
          </button>
          <button type="button" class="secondary-action" :disabled="paying" @click="cancelPayment">
            Cancel
          </button>
        </div>
      </form>
    </section>

    <section v-if="managerDetail || managerDetailLoading" class="detail-section" aria-label="Statement detail">
      <div class="section-heading">
        <h3>Statement detail</h3>
        <button type="button" class="link-button" @click="closeStatement">Close</button>
      </div>
      <p v-if="managerDetailLoading" class="loading-state">Loading statement…</p>
      <StatementDetail
        v-else-if="managerDetail"
        :statement="managerDetail"
        :manager="managerDetail"
        can-record-payment
        :paying="paying"
        :payment-error="paymentError"
        @record-payment="openPayment"
      />
    </section>
  </div>
</template>

<style scoped>
.manager-billing { max-width: 1400px; }
.view-title { margin-bottom: 1.5rem; font-size: 1.25rem; font-weight: 650; }
.summary { display: grid; grid-template-columns: repeat(3, minmax(0, 1fr)); gap: 1rem; margin-bottom: 2rem; }
.summary-label { color: var(--muted); font-size: .75rem; font-weight: 600; letter-spacing: .05em; text-transform: uppercase; }
.summary-value { margin: .45rem 0 .35rem; font-size: clamp(1.6rem, 2.5vw, 2.25rem); font-weight: 650; line-height: 1; font-variant-numeric: tabular-nums; }
.summary-sub { color: var(--muted); font-size: .8125rem; }
.suspended-text { color: var(--error); }
.statements-section { margin-bottom: 2rem; }
.section-heading { display: flex; align-items: center; justify-content: space-between; gap: 1rem; flex-wrap: wrap; margin-bottom: 1rem; }
.section-heading h3 { font-size: 1rem; font-weight: 650; }
.section-heading p { margin-top: .2rem; color: var(--muted); font-size: .8125rem; }
.unpaid-filter { display: flex; align-items: center; gap: .4rem; color: var(--muted); font-size: .8125rem; }
.statements-table { width: 100%; border-collapse: collapse; font-size: .875rem; }
.statements-table th { padding: .75rem; border-bottom: 1px solid var(--border); color: var(--muted); font-weight: 550; text-align: left; }
.statements-table td { padding: .75rem; border-bottom: 1px solid var(--border); vertical-align: top; }
.statements-table tbody tr:hover { background: rgba(255,255,255,.02); }
.consumer { max-width: 14rem; overflow-wrap: anywhere; }
.amount { font-variant-numeric: tabular-nums; font-weight: 600; }
.mode-badge { padding: .1rem .45rem; border: 1px solid currentColor; border-radius: .25rem; font-size: .75rem; font-weight: 600; }
.mode-badge.invoice { color: var(--warning); }
.mode-badge.reconciliation { color: #7dd3fc; }
.reconciliation-text { color: #7dd3fc; }
.paid-text { color: var(--success); }
.outstanding-text { color: var(--warning); }
.suspend-flag { display: block; color: var(--error); font-size: .75rem; }
.incomplete-flag { display: block; color: var(--warning); font-size: .75rem; }
.muted { color: var(--muted); }
.row-actions { display: flex; gap: .75rem; }
.link-button { padding: 0; background: transparent; color: var(--accent); font-size: .8125rem; text-decoration: underline; }
.link-button:hover { background: transparent; color: var(--accent-hover); }
.payment-form { margin-bottom: 2rem; }
.payment-form h3 { margin-bottom: .35rem; font-size: 1rem; font-weight: 650; }
.payment-note { margin-bottom: 1rem; color: var(--muted); font-size: .8125rem; }
.form-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(14rem, 1fr)); gap: .75rem; margin-bottom: 1rem; }
.form-grid label { display: grid; gap: .3rem; color: var(--muted); font-size: .75rem; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }
.form-grid input { text-transform: none; letter-spacing: normal; font-weight: 400; }
.form-grid .wide { grid-column: 1 / -1; }
.form-actions { display: flex; gap: .75rem; }
.secondary-action { background: transparent; border: 1px solid var(--border); color: var(--muted); }
.secondary-action:hover { background: rgba(255,255,255,.04); color: var(--fg); }
.loading-state, .empty-state { padding: 2rem 1rem; color: var(--muted); text-align: center; }
.error-banner { margin-bottom: 1rem; padding: .75rem 1rem; color: #fca5a5; background: rgba(239,68,68,.1); border: 1px solid var(--error); border-radius: .375rem; }
.sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }
@media (max-width: 700px) { .summary { grid-template-columns: 1fr; } .statements-table { display: block; overflow-x: auto; } }
</style>
