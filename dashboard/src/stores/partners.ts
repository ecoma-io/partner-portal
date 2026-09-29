// Commercial partners: the operator's view of who can call the proxy and what
// they pay for it.
//
// # The partner list is `/api/admin/partners`, not `me.consumers`
//
// `Me.consumers` is derived from `SELECT DISTINCT consumer_id FROM usage_hourly`,
// so a partner who has not yet made a request is not in it. Using it as the
// partner list would mean a partner created seconds ago does not exist until they
// call, which is exactly the partner an operator is most likely to be looking
// for. `/api/admin/partners` is the source of truth and the only one read here.
//
// # Models are replaced as one list, never edited one at a time
//
// `PUT /api/admin/partners/{id}/models` takes the complete list and applies it
// atomically, because "which models may this partner call" and "what do they
// pay" are one fact: a model a partner may call with no price is a request that
// cannot be metered, and a price for a model they may not call is a price nobody
// can reach. A UI that looped `PUT` per row would show a half-applied price list
// after the second call failed, and would hide the atomicity the API provides.
//
// # Prices are decimal strings, end to end
//
// They arrive as `"0.0475"` and go back as `"0.0475"`. They are never parsed into
// a JavaScript number here, because `0.0475` has no exact binary representation
// and a price that arrives one ulp low is a partner's bill that is wrong in the
// last digit that shows. The only number this store handles is
// `payment_terms_minutes`, which the server validates as a plain integer.

import { defineStore } from 'pinia'
import { computed, ref } from 'vue'

import { useDashboardStore } from '@/stores/dashboard'

/** One model's price list, as the API expresses it: dollars per million tokens. */
export interface ModelPrice {
  model: string
  input_per_million: string
  cached_input_per_million: string
  output_per_million: string
}

export interface Partner {
  /** The identity and isolation boundary. Fixed at creation. */
  consumer_id: string
  name: string
  /** May be empty: "no address on file" is a legitimate way to run a partner. */
  billing_email: string
  /** `invoice` carries a payment obligation; `reconciliation` is a settlement record. */
  billing_mode: 'invoice' | 'reconciliation'
  payment_terms_minutes: number
  /** Derived, not stored: the mode owes money *and* there is somewhere to send it. */
  emails_statements: boolean
  created_at: string
  updated_at: string
  total_billed_micro_usd: number
  /** A server-formatted string. Never summed, converted or rounded here. */
  total_billed: string
  statement_count: number
  models: ModelPrice[]
}

export interface CreatePartnerInput {
  consumer_id: string
  name: string
  billing_email: string
  billing_mode: 'invoice' | 'reconciliation'
  payment_terms_minutes?: number
  models: ModelPrice[]
}

/**
 * A partial partner edit. An absent key means "unchanged" on the server, which is
 * what makes a one-field patch safe; that is why the editor sends the fields the
 * operator actually changed rather than the whole form.
 */
export interface UpdatePartnerInput {
  name?: string
  billing_email?: string
  billing_mode?: 'invoice' | 'reconciliation'
  payment_terms_minutes?: number
}

/** A row being typed, before it has been accepted. Prices are still raw text. */
export interface ModelPriceDraft {
  model: string
  input_per_million: string
  cached_input_per_million: string
  output_per_million: string
}

export function emptyModelDraft(): ModelPriceDraft {
  return { model: '', input_per_million: '', cached_input_per_million: '', output_per_million: '' }
}

/** The payment terms the server falls back to when the field is absent. */
export const DEFAULT_PAYMENT_TERMS_MINUTES = 1440

/**
 * A price an operator typed, checked the way the server checks it.
 *
 * This mirrors `PricePerMillion::parse` — digits, at most one `.`, a leading `.`
 * allowed and a trailing one refused, no sign, no exponent, no whitespace — and
 * it is a *mirror*, not a substitute: the client check exists so the form can
 * point at the field while the operator is still typing, and the server's parse
 * is the one that decides. Both agree on the point that matters most: a negative
 * price is a refund, and a refund is not something a partner is configured into
 * by accident.
 */
export function priceError(value: string): string | null {
  const text = value.trim()
  if (text === '') return 'a price is required'
  const match = /^(\d+)(?:\.(\d+))?$|^\.(\d+)$/.exec(text)
  if (match === null) return 'a price is dollars per million tokens, e.g. 0.095'
  const fraction = match[2] ?? match[3] ?? ''
  // More than six decimals cannot change a micro-USD amount. The server rounds
  // it; saying so here is better than sending a number that looks like it will be
  // honoured to the digit.
  if (fraction.length > 6) return 'at most six decimal places — a price is micro-USD'
  return null
}

function messageOf(cause: unknown): string {
  return cause instanceof Error ? cause.message : 'Something went wrong'
}

export const usePartnersStore = defineStore('partners', () => {
  const dashboard = useDashboardStore()

  function get<T>(path: string, what: string): Promise<T> {
    return dashboard.apiFetch<T>(`/api${path}`, {}, what)
  }

  function send<T>(method: 'POST' | 'PATCH' | 'PUT' | 'DELETE', path: string, body: unknown, what: string): Promise<T> {
    return dashboard.apiFetch<T>(
      `/api${path}`,
      { method, headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(body) },
      what,
    )
  }

  const partners = ref<Partner[]>([])
  const selected = ref<Partner | null>(null)
  const loading = ref(false)
  const detailLoading = ref(false)
  /** Set by whichever mutation is in flight, so the view can disable its controls. */
  const saving = ref(false)
  const error = ref<string | null>(null)
  /** Validation the client can do; the server still decides. */
  const validationError = ref<string | null>(null)

  const hasPartners = computed(() => partners.value.length > 0)

  async function load() {
    loading.value = true
    try {
      partners.value = await get<Partner[]>('/admin/partners', 'the partner list')
      error.value = null
    } catch (cause) {
      error.value = messageOf(cause)
    } finally {
      loading.value = false
    }
  }

  async function loadOne(consumerId: string) {
    detailLoading.value = true
    try {
      selected.value = await get<Partner>(`/admin/partners/${encodeURIComponent(consumerId)}`, 'this partner')
      error.value = null
    } catch (cause) {
      selected.value = null
      error.value = messageOf(cause)
    } finally {
      detailLoading.value = false
    }
  }

  function clearSelection() {
    selected.value = null
    validationError.value = null
  }

  /** Refuse a create whose visible fields are empty before spending a round trip. */
  function validateNew(input: CreatePartnerInput): string | null {
    if (input.consumer_id.trim() === '') return 'a consumer id is required'
    if (input.name.trim() === '') return 'a name is required'
    const terms = input.payment_terms_minutes ?? DEFAULT_PAYMENT_TERMS_MINUTES
    if (!Number.isInteger(terms) || terms < 0) {
      return 'payment terms must be a whole number of minutes, zero or more'
    }
    return validateModels(input.models)
  }

  function validateModels(models: ModelPrice[]): string | null {
    const seen = new Set<string>()
    for (const [index, price] of models.entries()) {
      const model = price.model.trim()
      if (model === '') return `row ${index + 1} needs a model name`
      if (seen.has(model)) return `${model} is listed twice; a model has one price`
      seen.add(model)
      for (const field of ['input_per_million', 'cached_input_per_million', 'output_per_million'] as const) {
        const problem = priceError(price[field])
        if (problem !== null) return `${model}: ${field.replace(/_per_million$/, '')} — ${problem}`
      }
    }
    return null
  }

  /**
   * Open an account.
   *
   * The whole model list goes in the create, so there is no window in which the
   * partner exists but its price list is a second, separately-failed request.
   */
  async function create(input: CreatePartnerInput): Promise<Partner | null> {
    const problem = validateNew(input)
    if (problem !== null) {
      validationError.value = problem
      return null
    }
    saving.value = true
    validationError.value = null
    try {
      const created = await send<Partner>('POST', '/admin/partners', input, 'the partner')
      // A create changes the request-path snapshot, and `me.consumers` may
      // change with it, so both lists are re-read rather than patched.
      await load()
      selected.value = created
      return created
    } catch (cause) {
      error.value = messageOf(cause)
      return null
    } finally {
      saving.value = false
    }
  }

  /**
   * Change the commercial facts.
   *
   * `PATCH` with only the fields that changed: the server reads an absent field as
   * "unchanged", and sending a whole form back is how an untouched field gets
   * overwritten with a stale value from a page that has been open for an hour.
   */
  async function update(consumerId: string, patch: UpdatePartnerInput): Promise<Partner | null> {
    if (patch.payment_terms_minutes !== undefined) {
      const terms = patch.payment_terms_minutes
      if (!Number.isInteger(terms) || terms < 0) {
        validationError.value = 'payment terms must be a whole number of minutes, zero or more'
        return null
      }
    }
    saving.value = true
    validationError.value = null
    try {
      const updated = await send<Partner>('PATCH', `/admin/partners/${encodeURIComponent(consumerId)}`, patch, 'the partner')
      selected.value = updated
      await load()
      return updated
    } catch (cause) {
      error.value = messageOf(cause)
      return null
    } finally {
      saving.value = false
    }
  }

  /**
   * Replace the whole price list.
   *
   * One `PUT`, one list, one transaction — never a per-row sequence. The server
   * answers 400 naming the model and the field, which is why the rows are trimmed
   * here but stored exactly as typed: a whitespace-only name is a typo the client
   * refuses, while a name with an inner space is a model the operator meant.
   */
  async function replaceModels(consumerId: string, models: ModelPrice[]): Promise<boolean> {
    const problem = validateModels(models)
    if (problem !== null) {
      validationError.value = problem
      return false
    }
    saving.value = true
    validationError.value = null
    try {
      const replaced = await send<ModelPrice[]>(
        'PUT',
        `/admin/partners/${encodeURIComponent(consumerId)}/models`,
        { models },
        'the model prices',
      )
      if (selected.value?.consumer_id === consumerId) {
        selected.value = { ...selected.value, models: replaced }
      }
      await load()
      return true
    } catch (cause) {
      error.value = messageOf(cause)
      return false
    } finally {
      saving.value = false
    }
  }

  /**
   * Remove a partner that has never been billed.
   *
   * The caller confirms first. The server refuses a partner with any statement —
   * their billing history is a financial record — and that refusal is surfaced
   * here as text rather than as a success the operator would act on.
   */
  async function remove(consumerId: string): Promise<boolean> {
    saving.value = true
    error.value = null
    try {
      await send<{ deleted: boolean; consumer_id: string }>(
        'DELETE',
        `/admin/partners/${encodeURIComponent(consumerId)}`,
        {},
        'the deletion',
      )
      if (selected.value?.consumer_id === consumerId) clearSelection()
      await load()
      return true
    } catch (cause) {
      error.value = messageOf(cause)
      return false
    } finally {
      saving.value = false
    }
  }

  function clearError() {
    error.value = null
    validationError.value = null
  }

  return {
    partners,
    hasPartners,
    selected,
    loading,
    detailLoading,
    saving,
    error,
    validationError,
    load,
    loadOne,
    clearSelection,
    clearError,
    validateNew,
    validateModels,
    create,
    update,
    replaceModels,
    remove,
  }
})
