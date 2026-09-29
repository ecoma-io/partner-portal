// One statement, rendered the same way for a partner and for an operator.
//
// The two callers differ in *what they may do* with a statement, not in what a
// statement is: `src/billing/api.rs` builds both views from one `common()` so
// the same row cannot be rendered two ways. This component takes the same
// stance, and takes the operational fields as a separate optional group so the
// partner's copy is a type that simply has nowhere to put a payment reference.
//
// # The two modes are not interchangeable
//
// `invoice` carries a payment obligation and a deadline. `reconciliation` is a
// settlement record: it carries no obligation, has no due date, is never unpaid
// and never suspends anyone. It is labelled and described as such everywhere it
// appears — including in the manager's unpaid filter, which simply does not
// contain it — because a settlement record shown as "unpaid" is a bill that was
// never owed.

<script setup lang="ts">
import { computed } from 'vue'
import type { ManagerStatement, Statement } from '@/stores/billing'

const props = defineProps<{
  statement: Statement
  /** Supplied only by the manager surface; absent on a partner's own statement. */
  manager?: ManagerStatement | null
  /** A manager may record a payment. The gate is this component's; the server's is the real one. */
  canRecordPayment?: boolean
  paying?: boolean
  paymentError?: string | null
}>()

const emit = defineEmits<{
  (event: 'record-payment', statement: Statement): void
}>()

/** The only mode that can be paid, and the only one with a deadline. */
const isInvoice = computed(() => props.statement.billing_mode === 'invoice')

const isOutstanding = computed(() => isInvoice.value && props.statement.paid_at === null)

/** Paid, or never payable. Both are states where the button must not exist. */
const payable = computed(() => Boolean(props.canRecordPayment) && isOutstanding.value)

function formatTimestamp(value: string | null): string {
  if (value === null) return '—'
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleString()
}

function formatDateOnly(value: string): string {
  const parsed = new Date(value)
  return Number.isNaN(parsed.getTime()) ? value : parsed.toLocaleDateString()
}
</script>

<template>
  <article class="card statement-detail">
    <header class="statement-heading">
      <div>
        <h3>{{ formatDateOnly(statement.billing_date) }}</h3>
        <p>
          {{ statement.consumer_id }} ·
          <span v-if="isInvoice" class="mode-badge invoice">Invoice</span>
          <span v-else class="mode-badge reconciliation">Reconciliation</span>
        </p>
      </div>
      <div class="statement-amount">
        <span class="amount-label">Total</span>
        <span class="amount-value">{{ statement.total_amount }} {{ statement.currency }}</span>
      </div>
    </header>

    <!-- A reconciliation statement is a record of what was consumed and what it
         cost. Saying anything about payment, due dates or suspension would be a
         second, wrong answer to a question it does not apply to. -->
    <p v-if="!isInvoice" class="mode-explanation reconciliation">
      This is a settlement record. Nothing is owed on it, there is no payment
      deadline, and it cannot suspend service.
    </p>

    <dl class="statement-facts">
      <div>
        <dt>Period</dt>
        <dd>{{ formatTimestamp(statement.period_start) }} → {{ formatTimestamp(statement.period_end) }}</dd>
      </div>
      <div>
        <dt>Final at</dt>
        <dd>{{ formatTimestamp(statement.billing_cutoff_at) }}</dd>
      </div>
      <template v-if="isInvoice">
        <div>
          <dt>Due</dt>
          <dd>{{ formatTimestamp(statement.due_at) }}</dd>
        </div>
        <div>
          <dt>Status</dt>
          <dd>
            <span v-if="statement.paid_at !== null" class="paid-text">
              Paid {{ formatTimestamp(statement.paid_at) }}
            </span>
            <span v-else class="outstanding-text">Outstanding</span>
          </dd>
        </div>
      </template>
    </dl>

    <!-- Incomplete usage is a count of requests whose cost this statement cannot
         know. It is shown as its own fact rather than folded into the total,
         because a total that quietly excludes unpriceable requests is a total
         that reads as complete. -->
    <p v-if="statement.has_incomplete_usage" class="incomplete-note" role="note">
      <strong>{{ statement.incomplete_usage_count }}</strong>
      {{ statement.incomplete_usage_count === 1 ? 'request' : 'requests' }} in this
      period could not be priced — the provider did not report their usage in
      full, or no price was in force when they were accepted. They contribute
      nothing to the total above, and nothing is estimated in their place.
    </p>

    <table v-if="statement.lines?.length" class="lines-table">
      <caption>Usage priced as it stood when each request was accepted</caption>
      <thead>
        <tr>
          <th scope="col">Model</th>
          <th scope="col">Requests</th>
          <th scope="col">Uncached in</th>
          <th scope="col">Cached in</th>
          <th scope="col">Output</th>
          <th scope="col">Line total</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="line in statement.lines" :key="line.model">
          <td class="model">{{ line.model }}</td>
          <td>{{ line.request_count.toLocaleString() }}</td>
          <td>
            {{ line.uncached_input_tokens.toLocaleString() }}
            <span class="price-note">@ {{ line.input_per_million }}/M</span>
          </td>
          <td>
            {{ line.cached_input_tokens.toLocaleString() }}
            <span class="price-note">@ {{ line.cached_input_per_million }}/M</span>
          </td>
          <td>
            {{ line.output_tokens.toLocaleString() }}
            <span class="price-note">@ {{ line.output_per_million }}/M</span>
          </td>
          <td class="line-total">{{ line.total_cost }}</td>
        </tr>
      </tbody>
    </table>
    <p v-else class="no-lines">No per-model breakdown was returned for this statement.</p>

    <!-- Manager-only bookkeeping. Each field is optional because a
         reconciliation statement has none of them, and "we did not record that"
         must not render as a blank that reads like an oversight. -->
    <dl v-if="manager" class="manager-facts">
      <div v-if="manager.paid_by">
        <dt>Recorded by</dt>
        <dd>{{ manager.paid_by }}</dd>
      </div>
      <div v-if="manager.payment_reference">
        <dt>Payment reference</dt>
        <dd>{{ manager.payment_reference }}</dd>
      </div>
      <div v-if="manager.payment_note">
        <dt>Note</dt>
        <dd>{{ manager.payment_note }}</dd>
      </div>
      <div v-if="isInvoice && manager.email_sent_at">
        <dt>Emailed</dt>
        <dd>{{ formatTimestamp(manager.email_sent_at) }}</dd>
      </div>
      <div v-if="manager.email_attempts !== undefined">
        <dt>Delivery attempts</dt>
        <dd>{{ manager.email_attempts }}</dd>
      </div>
      <div v-if="manager.email_last_error">
        <dt>Last delivery error</dt>
        <dd class="error-text">{{ manager.email_last_error }}</dd>
      </div>
    </dl>

    <p v-if="paymentError" class="error-banner" role="alert">{{ paymentError }}</p>

    <button
      v-if="payable"
      type="button"
      class="primary-action"
      :disabled="paying"
      @click="emit('record-payment', statement)"
    >
      {{ paying ? 'Recording…' : 'Record payment…' }}
    </button>
  </article>
</template>

<style scoped>
.statement-detail { margin-bottom: 1.5rem; }
.statement-heading { display: flex; align-items: flex-start; justify-content: space-between; gap: 1rem; flex-wrap: wrap; margin-bottom: 1rem; }
.statement-heading h3 { font-size: 1.125rem; font-weight: 650; }
.statement-heading p { display: flex; align-items: center; gap: .5rem; margin-top: .3rem; color: var(--muted); font-size: .8125rem; }
.statement-amount { text-align: right; }
.amount-label { display: block; color: var(--muted); font-size: .75rem; font-weight: 600; letter-spacing: .05em; text-transform: uppercase; }
.amount-value { font-size: 1.375rem; font-weight: 650; font-variant-numeric: tabular-nums; }
.mode-badge { padding: .1rem .45rem; border: 1px solid currentColor; border-radius: .25rem; font-size: .75rem; font-weight: 600; }
.mode-badge.invoice { color: var(--warning); }
.mode-badge.reconciliation { color: #7dd3fc; }
.mode-explanation { margin-bottom: 1rem; padding: .6rem .75rem; border-radius: .375rem; font-size: .8125rem; }
.mode-explanation.reconciliation { color: #bae6fd; background: rgba(14,165,233,.08); border: 1px solid rgba(14,165,233,.35); }
.statement-facts { display: grid; grid-template-columns: repeat(auto-fit, minmax(11rem, 1fr)); gap: .75rem 1.25rem; margin-bottom: 1rem; }
.statement-facts dt, .manager-facts dt { color: var(--muted); font-size: .75rem; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }
.statement-facts dd, .manager-facts dd { margin-top: .2rem; font-size: .875rem; overflow-wrap: anywhere; }
.paid-text { color: var(--success); }
.outstanding-text { color: var(--warning); }
.incomplete-note { margin-bottom: 1rem; padding: .6rem .75rem; color: #fcd34d; background: rgba(245,158,11,.08); border: 1px solid rgba(245,158,11,.4); border-radius: .375rem; font-size: .8125rem; }
.lines-table { width: 100%; margin-bottom: 1rem; border-collapse: collapse; font-size: .8125rem; }
.lines-table caption { margin-bottom: .5rem; color: var(--muted); font-size: .75rem; text-align: left; }
.lines-table th { padding: .5rem; border-bottom: 1px solid var(--border); color: var(--muted); font-weight: 550; text-align: left; }
.lines-table td { padding: .5rem; border-bottom: 1px solid var(--border); font-variant-numeric: tabular-nums; vertical-align: top; }
.lines-table .model { max-width: 12rem; overflow-wrap: anywhere; }
.price-note { display: block; color: var(--muted); font-size: .6875rem; }
.line-total { font-weight: 650; }
.no-lines { margin-bottom: 1rem; color: var(--muted); font-size: .8125rem; }
.manager-facts { display: grid; grid-template-columns: repeat(auto-fit, minmax(11rem, 1fr)); gap: .75rem 1.25rem; margin-bottom: 1rem; padding-top: 1rem; border-top: 1px solid var(--border); }
.error-text { color: var(--error); }
.primary-action { margin-top: .5rem; }
@media (max-width: 700px) { .statement-amount { text-align: left; } .lines-table { display: block; overflow-x: auto; } }
</style>
