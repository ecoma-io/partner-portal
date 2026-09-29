// Billing state: what a partner is owed, and what an operator has to do about it.
//
// # Two surfaces, one representation, different actions
//
// `src/billing/api.rs` is explicit that the two surfaces must not be able to
// render the same row two different ways, and that is why `Statement` below is
// the shared shape: it holds the commercial fields both roles may read, and the
// operational payment/email bookkeeping — which partner's `for_partner` view
// omits at the source — is a separate interface the manager surface adds.
//
// The split is not cosmetic. A partner's copy of a statement has no
// `payment_reference` and no `email_attempts` because the server did not send
// them; typing the partner view as `ManagerStatement` would invite a render that
// prints `undefined` and a reader who concludes we withheld a payment reference
// on purpose.
//
// # No money is computed here
//
// Every amount on screen is a string the server formatted. This store never adds
// up a statement, never converts a price, and never turns a missing figure into
// a zero: an incomplete statement's cost is not $0.00, it is an incomplete
// statement, and the count that says so comes from `incomplete_usage_count`.

import { defineStore } from 'pinia'
import { computed, ref } from 'vue'

import { ApiError, useDashboardStore } from '@/stores/dashboard'

/** One model's line: usage as measured, priced as it was at accept time. */
export interface StatementLine {
  model: string
  /** Decimal dollars per million tokens, snapshotted when the request was accepted. */
  input_per_million: string
  cached_input_per_million: string
  output_per_million: string
  request_count: number
  input_tokens: number
  cached_input_tokens: number
  /** `input - cached`: what the input price is charged on. The cached tokens are billed at the cached price, not twice. */
  uncached_input_tokens: number
  output_tokens: number
  input_cost_micro_usd: number
  cached_input_cost_micro_usd: number
  output_cost_micro_usd: number
  total_cost_micro_usd: number
  total_cost: string
}

/** The commercial fields of a statement — the ones both roles may read. */
export interface Statement {
  id: number
  consumer_id: string
  /** The billing day this statement settles, `YYYY-MM-DD`. */
  billing_date: string
  /** `invoice` carries an obligation and a deadline; `reconciliation` is a settlement record. */
  billing_mode: 'invoice' | 'reconciliation'
  currency: string
  period_start: string
  period_end: string
  /** When the worker judged the day final. Never `period_end`. */
  billing_cutoff_at: string
  total_amount_micro_usd: number
  total_amount: string
  /** Requests whose usage could not be priced. They contribute no money — and are never a zero. */
  incomplete_usage_count: number
  has_incomplete_usage: boolean
  /** Only ever set for an invoice. A reconciliation statement has no deadline. */
  due_at: string | null
  paid_at: string | null
  /** Still owed. False for a reconciliation statement whatever it says. */
  outstanding: boolean
  /** Whether this statement is a reason service is suspended. Not the same as "overdue". */
  can_suspend: boolean
  created_at: string
  updated_at: string
  /** Absent on a list, present on a single-statement read. */
  lines?: StatementLine[]
}

/**
 * The manager's wider view: what was recorded, and how delivery went.
 *
 * Every one of these is `#[serde(skip_serializing_if = "Option::is_none")]` on
 * the server, so a reconciliation statement legitimately has no `paid_by` and no
 * `email_attempts`. They are optional here for that reason — a `0` fallback would
 * assert an attempt count the row does not have.
 */
export interface ManagerStatement extends Statement {
  paid_by?: string
  payment_reference?: string
  payment_note?: string
  email_sent_at?: string
  email_attempts?: number
  email_last_error?: string
  email_next_retry_at?: string
}

export interface StatementList<T extends Statement = Statement> {
  statements: T[]
  /** Counted under the same scope as the page, so a pager knows when to stop. */
  total: number
  limit: number
  offset: number
  /** Unpaid invoices in the whole scope — never a reconciliation statement, because nothing is owed on one. */
  unpaid_count: number
  unpaid_total_micro_usd: number
  unpaid_total: string
}

/** Why a partner is suspended, in the terms a human needs. */
export interface SuspensionReason {
  code: 'invoice_overdue'
  statement_id: number
  billing_date: string
  due_at: string
  amount_micro_usd: number
  amount: string
}

/** Derived at read time from the same rows the request path reads. */
export interface ServiceStatus {
  consumer_id: string
  status: 'active' | 'suspended'
  suspended: boolean
  /** The message a suspended request is refused with, verbatim from the server. */
  message: string | null
  reason: SuspensionReason | null
  /** A count of statements, not of money: an incomplete one is in it and suspends nobody. */
  overdue_statements: number
  overdue_amount_micro_usd: number
  overdue_amount: string
}

export interface BillingSummary {
  partners: number
  suspended_partners: number
  statements: number
  unpaid_statements: number
  unpaid_total_micro_usd: number
  unpaid_total: string
}

/** What an operator types when recording a payment. All three are optional. */
export interface PaymentInput {
  paid_by?: string
  reference?: string
  note?: string
}

/** A list with nothing in it, so a view never renders `undefined`. */
function emptyList<T extends Statement>(): StatementList<T> {
  return {
    statements: [],
    total: 0,
    limit: 50,
    offset: 0,
    unpaid_count: 0,
    unpaid_total_micro_usd: 0,
    unpaid_total: '',
  }
}

/**
 * The manager's consumer narrowing, as a query string.
 *
 * Only ever sent for a manager: a partner key's scope is resolved server-side
 * from its credential and this parameter is not read for it, so sending it would
 * be sending something that looks like a request to widen scope and is not.
 */
function scopeQuery(dashboard: ReturnType<typeof useDashboardStore>, options: { unpaid?: boolean } = {}): string {
  const params = new URLSearchParams()
  if (options.unpaid === true) params.set('unpaid', 'true')
  if (dashboard.isManager && dashboard.selectedConsumers.length > 0) {
    params.set('consumers', dashboard.selectedConsumers.join(','))
  }
  const query = params.toString()
  return query === '' ? '' : `?${query}`
}

function messageOf(cause: unknown): string {
  return cause instanceof Error ? cause.message : 'Something went wrong'
}

export const useBillingStore = defineStore('billing', () => {
  const dashboard = useDashboardStore()

  /**
   * One round trip, through the store's own `apiFetch`, so the credential is
   * read in exactly one place. `what` names the resource in a sentence, because
   * that is the text a user sees when a proxy has replaced the JSON error body
   * with something unreadable.
   */
  function get<T>(path: string, what: string): Promise<T> {
    return dashboard.apiFetch<T>(`/api${path}`, {}, what)
  }

  function send<T>(method: 'POST' | 'PUT' | 'PATCH' | 'DELETE', path: string, body: unknown, what: string): Promise<T> {
    return dashboard.apiFetch<T>(
      `/api${path}`,
      { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) },
      what,
    )
  }

  // --- Partner surface ------------------------------------------------------
  const partnerStatements = ref<StatementList>(emptyList())
  const serviceStatuses = ref<ServiceStatus[]>([])
  const partnerDetail = ref<Statement | null>(null)
  const partnerLoading = ref(false)
  const partnerDetailLoading = ref(false)

  // --- Manager surface ------------------------------------------------------
  const managerStatements = ref<StatementList<ManagerStatement>>(emptyList())
  const managerDetail = ref<ManagerStatement | null>(null)
  const summary = ref<BillingSummary | null>(null)
  const managerLoading = ref(false)
  const managerDetailLoading = ref(false)
  /** The manager's "only what is still owed" filter. Reconciliation is never in it. */
  const onlyUnpaid = ref(false)
  const paying = ref(false)
  const paymentError = ref<string | null>(null)

  const partnerError = ref<string | null>(null)
  const managerError = ref<string | null>(null)

  /** A partner key resolves to exactly one status; a manager may have several in view. */
  const statuses = computed(() => serviceStatuses.value)

  const outstandingStatements = computed(() => managerStatements.value.statements.filter((s) => s.outstanding))

  async function loadPartner() {
    partnerLoading.value = true
    try {
      const scope = scopeQuery(dashboard)
      const [status, statements] = await Promise.all([
        get<{ statuses: ServiceStatus[] }>(`/billing/status${scope}`, 'your service status'),
        get<StatementList>(`/billing/statements${scope}`, 'your statements'),
      ])
      serviceStatuses.value = status.statuses
      partnerStatements.value = statements
      partnerError.value = null
    } catch (cause) {
      partnerError.value = messageOf(cause)
    } finally {
      partnerLoading.value = false
    }
  }

  async function loadPartnerStatement(id: number) {
    partnerDetailLoading.value = true
    try {
      // The scope parameter is read by the server for a manager and ignored for
      // a partner; a statement outside the caller's scope is a 404 that cannot be
      // told apart from an id that does not exist, so there is nothing to guess at.
      partnerDetail.value = await get<Statement>(
        `/billing/statements/${id}${scopeQuery(dashboard)}`,
        'this statement',
      )
      partnerError.value = null
    } catch (cause) {
      partnerDetail.value = null
      partnerError.value = messageOf(cause)
    } finally {
      partnerDetailLoading.value = false
    }
  }

  async function loadManager() {
    managerLoading.value = true
    try {
      const [nextSummary, statements] = await Promise.all([
        get<BillingSummary>(`/admin/billing/summary${scopeQuery(dashboard)}`, 'the billing summary'),
        get<StatementList<ManagerStatement>>(
          `/admin/billing/statements${scopeQuery(dashboard, { unpaid: onlyUnpaid.value })}`,
          'the statement list',
        ),
      ])
      summary.value = nextSummary
      managerStatements.value = statements
      managerError.value = null
    } catch (cause) {
      managerError.value = messageOf(cause)
    } finally {
      managerLoading.value = false
    }
  }

  async function loadManagerStatement(id: number) {
    managerDetailLoading.value = true
    try {
      managerDetail.value = await get<ManagerStatement>(
        `/admin/billing/statements/${id}`,
        'this statement',
      )
      managerError.value = null
    } catch (cause) {
      managerDetail.value = null
      managerError.value = messageOf(cause)
    } finally {
      managerDetailLoading.value = false
    }
  }

  /**
   * Record a payment against an invoice statement.
   *
   * The caller gates this on `billing_mode === 'invoice' && outstanding`, but
   * that gate is a convenience, not the protection: the server answers a
   * reconciliation statement with 409 `statement_not_payable`, and this surfaces
   * that message rather than reporting a success that did not happen.
   */
  async function markPaid(id: number, payment: PaymentInput): Promise<boolean> {
    paying.value = true
    paymentError.value = null
    try {
      managerDetail.value = await send<ManagerStatement>(
        'POST',
        `/admin/billing/statements/${id}/mark-paid`,
        payment,
        'this payment',
      )
      // The row changed, and so did every total derived from it — including the
      // suspended count, which is derived rather than stored. Refetch rather than
      // patch the local copy, so the screen shows what the server now believes.
      await loadManager()
      return true
    } catch (cause) {
      paymentError.value =
        cause instanceof ApiError && cause.status === 409
          ? `${cause.message} — a reconciliation statement is a settlement record and is never payable.`
          : messageOf(cause)
      return false
    } finally {
      paying.value = false
    }
  }

  function setOnlyUnpaid(value: boolean) {
    onlyUnpaid.value = value
  }

  function clearPaymentError() {
    paymentError.value = null
  }

  return {
    partnerStatements,
    serviceStatuses,
    statuses,
    partnerDetail,
    partnerLoading,
    partnerDetailLoading,
    partnerError,
    managerStatements,
    managerDetail,
    managerLoading,
    managerDetailLoading,
    managerError,
    summary,
    onlyUnpaid,
    outstandingStatements,
    paying,
    paymentError,
    loadPartner,
    loadPartnerStatement,
    loadManager,
    loadManagerStatement,
    markPaid,
    setOnlyUnpaid,
    clearPaymentError,
  }
})
