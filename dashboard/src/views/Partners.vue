<script setup lang="ts">
/**
 * The operator's partner administration: who can call the proxy, at what price,
 * on what terms, and who can be removed.
 *
 * # Two editors, because the two facts change for different reasons
 *
 * The commercial facts (name, address, mode, terms) are a `PATCH` in which only
 * changed fields are sent, because the server reads an absent field as
 * "unchanged". The price list is a `PUT` of the complete list, because the
 * server replaces it atomically and a half-applied price list is a partner whose
 * requests cannot be metered. Neither editor is a generic "edit partner" form
 * pretending those are the same operation.
 *
 * # The mode is a commercial decision, so it says what it changes
 *
 * Switching a partner from `reconciliation` to `invoice` is what starts an
 * obligation and a suspension clock; the reverse is what stops one. The control
 * names the difference rather than showing two bare options.
 *
 * # Deletion is confirmed, and the refusal is shown
 *
 * A partner with any statement cannot be deleted — the history is a financial
 * record — and the server says so in a sentence worth reading. The button
 * therefore asks first, and the 400 is displayed rather than swallowed.
 */
import { computed, onMounted, ref, watch } from 'vue'
import { storeToRefs } from 'pinia'

import {
  DEFAULT_PAYMENT_TERMS_MINUTES,
  emptyModelDraft,
  priceError,
  usePartnersStore,
  type ModelPrice,
  type ModelPriceDraft,
} from '@/stores/partners'
import { useDashboardStore } from '@/stores/dashboard'

const partnerStore = usePartnersStore()
const dashboard = useDashboardStore()
const { dataChangeVersion, isManager } = storeToRefs(dashboard)
const { partners: list, selected, loading, detailLoading, saving, error, validationError } =
  storeToRefs(partnerStore)

type EditorKind = 'create' | 'edit' | null

const editor = ref<EditorKind>(null)
const confirmingDelete = ref<string | null>(null)

const newPartner = ref({
  consumer_id: '',
  name: '',
  billing_email: '',
  billing_mode: 'invoice' as 'invoice' | 'reconciliation',
  payment_terms_minutes: DEFAULT_PAYMENT_TERMS_MINUTES,
})
const newModels = ref<ModelPriceDraft[]>([emptyModelDraft()])

const editForm = ref({
  name: '',
  billing_email: '',
  billing_mode: 'invoice' as 'invoice' | 'reconciliation',
  payment_terms_minutes: DEFAULT_PAYMENT_TERMS_MINUTES,
})

/**
 * The price rows being typed, seeded from the server's decimal strings.
 *
 * Seeded rather than reformatted: a price that came back as `0.0475` is sent
 * back as `0.0475`, so an editor that re-derived the string would eventually
 * write back a price the operator never typed.
 */
const modelDraft = ref<ModelPriceDraft[]>([])

const partner = computed(() => selected.value)
const name = computed(() => partner.value?.name ?? partner.value?.consumer_id ?? '')

function openCreate() {
  partnerStore.clearError()
  newPartner.value = {
    consumer_id: '',
    name: '',
    billing_email: '',
    billing_mode: 'invoice',
    payment_terms_minutes: DEFAULT_PAYMENT_TERMS_MINUTES,
  }
  newModels.value = [emptyModelDraft()]
  editor.value = 'create'
  confirmingDelete.value = null
}

function openEdit() {
  const current = selected.value
  if (current === null) return
  partnerStore.clearError()
  editForm.value = {
    name: current.name,
    billing_email: current.billing_email,
    billing_mode: current.billing_mode,
    payment_terms_minutes: current.payment_terms_minutes,
  }
  modelDraft.value = current.models.map((price) => ({ ...price }))
  if (modelDraft.value.length === 0) modelDraft.value = [emptyModelDraft()]
  editor.value = 'edit'
  confirmingDelete.value = null
}

function closeEditor() {
  editor.value = null
  partnerStore.clearError()
}

// Four one-line helpers rather than one parameterised pair: a template unwraps
// a top-level `ref`, so a `addDraft(list)` that took the ref would be handed the
// array instead and would push onto a temporary.
function addNewModel() {
  newModels.value.push(emptyModelDraft())
}

function removeNewModel(index: number) {
  newModels.value.splice(index, 1)
}

function addEditModel() {
  modelDraft.value.push(emptyModelDraft())
}

function removeEditModel(index: number) {
  modelDraft.value.splice(index, 1)
}

function toPayload(drafts: ModelPriceDraft[]): ModelPrice[] {
  return drafts
    .filter((draft) => draft.model.trim() !== '')
    .map((draft) => ({
      model: draft.model,
      input_per_million: draft.input_per_million.trim(),
      cached_input_per_million: draft.cached_input_per_million.trim(),
      output_per_million: draft.output_per_million.trim(),
    }))
}

async function submitCreate() {
  const created = await partnerStore.create({
    consumer_id: newPartner.value.consumer_id.trim(),
    name: newPartner.value.name.trim(),
    billing_email: newPartner.value.billing_email.trim(),
    billing_mode: newPartner.value.billing_mode,
    payment_terms_minutes: newPartner.value.payment_terms_minutes,
    models: toPayload(newModels.value),
  })
  if (created !== null) editor.value = null
}

async function submitEdit() {
  const current = selected.value
  if (current === null) return

  // Only what actually changed. An unchanged field is left out so it cannot be
  // written back from a form that has been open long enough to be stale.
  const patch: Parameters<typeof partnerStore.update>[1] = {}
  if (editForm.value.name.trim() !== current.name) patch.name = editForm.value.name.trim()
  if (editForm.value.billing_email.trim() !== current.billing_email) {
    patch.billing_email = editForm.value.billing_email.trim()
  }
  if (editForm.value.billing_mode !== current.billing_mode) patch.billing_mode = editForm.value.billing_mode
  if (editForm.value.payment_terms_minutes !== current.payment_terms_minutes) {
    patch.payment_terms_minutes = editForm.value.payment_terms_minutes
  }

  if (Object.keys(patch).length > 0) {
    const updated = await partnerStore.update(current.consumer_id, patch)
    if (updated === null) return
  }

  // Prices go separately, and only when they are actually different — an
  // unchanged list is not re-sent, so opening and closing the editor is not a
  // write to the request path's configuration.
  const changed =
    JSON.stringify(toPayload(modelDraft.value)) !== JSON.stringify(current.models)
  if (changed && !(await partnerStore.replaceModels(current.consumer_id, toPayload(modelDraft.value)))) return

  editor.value = null
}

async function confirmDelete() {
  const current = selected.value
  if (current === null) return
  const done = await partnerStore.remove(current.consumer_id)
  if (done) confirmingDelete.value = null
}

function selectPartner(consumerId: string) {
  if (editor.value !== null) closeEditor()
  void partnerStore.loadOne(consumerId)
}

onMounted(() => {
  if (!isManager.value) {
    // The route is manager-only; a partner credential that reached it would get
    // 403s, and an empty page that reads as "you have no partners" is worse than
    // saying the page is not theirs.
    partnerStore.error = 'This page is for the manager credential.'
    return
  }
  void partnerStore.load()
})

watch(dataChangeVersion, (version) => {
  if (version > 0 && isManager.value) void partnerStore.load()
})
</script>

<template>
  <div class="partners-view">
    <div class="view-heading">
      <h2 class="view-title">Partners</h2>
      <button v-if="isManager" type="button" @click="openCreate">New partner</button>
    </div>

    <p v-if="error" class="error-banner" role="alert">{{ error }}</p>
    <p v-if="validationError" class="error-banner" role="alert">{{ validationError }}</p>

    <section v-if="editor === 'create'" class="card editor" aria-label="New partner">
      <h3>New partner</h3>
      <form @submit.prevent="submitCreate">
        <div class="form-grid">
          <label>
            Consumer id
            <input
              v-model="newPartner.consumer_id"
              type="text"
              required
              autocomplete="off"
              placeholder="acme"
            />
          </label>
          <label>
            Name
            <input v-model="newPartner.name" type="text" required autocomplete="off" placeholder="Acme Ltd" />
          </label>
          <label>
            Billing email
            <input
              v-model="newPartner.billing_email"
              type="email"
              autocomplete="off"
              placeholder="billing@acme.example"
            />
          </label>
          <label>
            Billing mode
            <select v-model="newPartner.billing_mode">
              <option value="invoice">Invoice — statements are owed, and can suspend</option>
              <option value="reconciliation">Reconciliation — a settlement record, nothing owed</option>
            </select>
          </label>
          <label>
            Payment terms (minutes)
            <input
              v-model.number="newPartner.payment_terms_minutes"
              type="number"
              min="0"
              step="1"
              required
            />
          </label>
        </div>

        <fieldset class="models-fieldset">
          <legend>Models and prices</legend>
          <p class="fieldset-note">
            Prices are US dollars per million tokens, as decimal strings. A partner
            with no rows can call nothing; both the list and the prices can be set
            later.
          </p>
          <div v-for="(draft, index) in newModels" :key="index" class="model-row">
            <label>
              <span class="sr-only">Model name</span>
              <input v-model="draft.model" type="text" autocomplete="off" placeholder="gpt-4o" />
            </label>
            <label>
              <span class="sr-only">Input price</span>
              <input
                v-model="draft.input_per_million"
                type="text"
                inputmode="decimal"
                autocomplete="off"
                placeholder="input"
                :aria-invalid="priceError(draft.input_per_million) !== null"
              />
            </label>
            <label>
              <span class="sr-only">Cached input price</span>
              <input
                v-model="draft.cached_input_per_million"
                type="text"
                inputmode="decimal"
                autocomplete="off"
                placeholder="cached input"
                :aria-invalid="priceError(draft.cached_input_per_million) !== null"
              />
            </label>
            <label>
              <span class="sr-only">Output price</span>
              <input
                v-model="draft.output_per_million"
                type="text"
                inputmode="decimal"
                autocomplete="off"
                placeholder="output"
                :aria-invalid="priceError(draft.output_per_million) !== null"
              />
            </label>
            <button
              type="button"
              class="link-button"
              :disabled="newModels.length === 1"
              @click="removeNewModel(index)"
            >
              Remove<span class="sr-only"> {{ draft.model || `row ${index + 1}` }}</span>
            </button>
          </div>
          <button type="button" class="secondary-action" @click="addNewModel">Add model</button>
        </fieldset>

        <div class="form-actions">
          <button type="submit" :disabled="saving">{{ saving ? 'Creating…' : 'Create partner' }}</button>
          <button type="button" class="secondary-action" :disabled="saving" @click="closeEditor">Cancel</button>
        </div>
      </form>
    </section>

    <section class="partners-section" aria-label="Partners">
      <p v-if="loading" class="loading-state">Loading partners…</p>
      <p v-else-if="list.length === 0" class="empty-state">
        No partners are configured. Until one exists, no consumer has a price list
        and nothing is billed.
      </p>

      <table v-else class="partners-table">
        <caption class="sr-only">Commercial partners</caption>
        <thead>
          <tr>
            <th scope="col">Consumer</th>
            <th scope="col">Name</th>
            <th scope="col">Mode</th>
            <th scope="col">Terms</th>
            <th scope="col">Models</th>
            <th scope="col">Billed</th>
            <th scope="col"><span class="sr-only">Open</span></th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="row in list" :key="row.consumer_id">
            <td class="consumer">{{ row.consumer_id }}</td>
            <td>{{ row.name }}</td>
            <td>
              <span
                class="mode-badge"
                :class="row.billing_mode === 'invoice' ? 'invoice' : 'reconciliation'"
              >
                {{ row.billing_mode === 'invoice' ? 'Invoice' : 'Reconciliation' }}
              </span>
            </td>
            <td class="numeric">{{ row.payment_terms_minutes }} min</td>
            <td class="numeric">{{ row.models.length }}</td>
            <td class="numeric">
              {{ row.total_billed }} USD
              <span class="muted">· {{ row.statement_count }} statement{{ row.statement_count === 1 ? '' : 's' }}</span>
            </td>
            <td>
              <button type="button" class="link-button" @click="selectPartner(row.consumer_id)">Details</button>
            </td>
          </tr>
        </tbody>
      </table>
    </section>

    <section v-if="partner || detailLoading" class="card detail" aria-label="Partner detail">
      <div class="section-heading">
        <h3>{{ name }}</h3>
        <button type="button" class="link-button" @click="partnerStore.clearSelection()">Close</button>
      </div>
      <p v-if="detailLoading" class="loading-state">Loading partner…</p>

      <template v-else-if="partner">
        <dl class="facts">
          <div>
            <dt>Consumer id</dt>
            <dd>{{ partner.consumer_id }}</dd>
          </div>
          <div>
            <dt>Billing email</dt>
            <!-- An empty address is a legitimate configuration, not a missing
                 field, and the difference is visible rather than guessed. -->
            <dd>
              <template v-if="partner.billing_email">{{ partner.billing_email }}</template>
              <span v-else class="muted">none on file — statements are written, not sent</span>
            </dd>
          </div>
          <div>
            <dt>Payment terms</dt>
            <dd>{{ partner.payment_terms_minutes }} minutes</dd>
          </div>
          <div>
            <dt>Statement emails</dt>
            <dd>
              {{ partner.emails_statements ? 'sent' : 'not sent' }}
              <span v-if="!partner.emails_statements && partner.billing_mode === 'invoice'" class="muted">
                — no address on file
              </span>
              <span v-else-if="!partner.emails_statements" class="muted">— nothing is owed on a settlement record</span>
            </dd>
          </div>
          <div>
            <dt>Total billed</dt>
            <dd>{{ partner.total_billed }} USD</dd>
          </div>
          <div>
            <dt>Statements</dt>
            <dd>{{ partner.statement_count }}</dd>
          </div>
        </dl>

        <section v-if="editor === 'edit'" class="editor" aria-label="Edit partner">
          <form @submit.prevent="submitEdit">
            <div class="form-grid">
              <label>
                Name
                <input v-model="editForm.name" type="text" autocomplete="off" />
              </label>
              <label>
                Billing email
                <input v-model="editForm.billing_email" type="email" autocomplete="off" />
              </label>
              <label>
                Billing mode
                <select v-model="editForm.billing_mode">
                  <option value="invoice">Invoice — statements are owed, and can suspend</option>
                  <option value="reconciliation">Reconciliation — a settlement record, nothing owed</option>
                </select>
              </label>
              <label>
                Payment terms (minutes)
                <input
                  v-model.number="editForm.payment_terms_minutes"
                  type="number"
                  min="0"
                  step="1"
                  required
                />
              </label>
            </div>

            <fieldset class="models-fieldset">
              <legend>Models and prices</legend>
              <p class="fieldset-note">
                Saving replaces the whole list in one request. A model that is not
                listed here cannot be called, and a model listed with no price
                cannot be metered — so an empty list means this partner calls nothing.
              </p>
              <div v-for="(draft, index) in modelDraft" :key="index" class="model-row">
                <label>
                  <span class="sr-only">Model name</span>
                  <input v-model="draft.model" type="text" autocomplete="off" />
                </label>
                <label>
                  <span class="sr-only">Input price</span>
                  <input
                    v-model="draft.input_per_million"
                    type="text"
                    inputmode="decimal"
                    autocomplete="off"
                    :aria-invalid="priceError(draft.input_per_million) !== null"
                  />
                </label>
                <label>
                  <span class="sr-only">Cached input price</span>
                  <input
                    v-model="draft.cached_input_per_million"
                    type="text"
                    inputmode="decimal"
                    autocomplete="off"
                    :aria-invalid="priceError(draft.cached_input_per_million) !== null"
                  />
                </label>
                <label>
                  <span class="sr-only">Output price</span>
                  <input
                    v-model="draft.output_per_million"
                    type="text"
                    inputmode="decimal"
                    autocomplete="off"
                    :aria-invalid="priceError(draft.output_per_million) !== null"
                  />
                </label>
                <button
                  type="button"
                  class="link-button"
                  :disabled="modelDraft.length === 1"
                  @click="removeEditModel(index)"
                >
                  Remove<span class="sr-only"> {{ draft.model || `row ${index + 1}` }}</span>
                </button>
              </div>
              <button type="button" class="secondary-action" @click="addEditModel">Add model</button>
            </fieldset>

            <div class="form-actions">
              <button type="submit" :disabled="saving">{{ saving ? 'Saving…' : 'Save changes' }}</button>
              <button type="button" class="secondary-action" :disabled="saving" @click="closeEditor">Cancel</button>
            </div>
          </form>
        </section>

        <table v-else class="models-table" aria-label="Configured models">
          <caption class="sr-only">Models this partner may call, and their prices</caption>
          <thead>
            <tr>
              <th scope="col">Model</th>
              <th scope="col">Input</th>
              <th scope="col">Cached input</th>
              <th scope="col">Output</th>
            </tr>
          </thead>
          <tbody>
            <tr v-if="partner.models.length === 0">
              <td colspan="4" class="muted">
                No models are configured, so this partner can call nothing.
              </td>
            </tr>
            <tr v-for="price in partner.models" :key="price.model">
              <td>{{ price.model }}</td>
              <td class="numeric">{{ price.input_per_million }}</td>
              <td class="numeric">{{ price.cached_input_per_million }}</td>
              <td class="numeric">{{ price.output_per_million }}</td>
            </tr>
          </tbody>
        </table>
        <p class="price-note">Prices are US dollars per million tokens.</p>

        <div v-if="editor !== 'edit'" class="detail-actions">
          <button type="button" :disabled="saving" @click="openEdit">Edit partner</button>
          <button
            v-if="!confirmingDelete"
            type="button"
            class="danger-action"
            :disabled="saving"
            @click="confirmingDelete = partner.consumer_id"
          >
            Delete partner
          </button>
        </div>

        <!-- Deletion is irreversible and is refused outright once any statement
             exists, so it is asked for rather than assumed. -->
        <div v-else class="confirm-delete" role="alertdialog" aria-label="Confirm deletion">
          <p>
            Delete <strong>{{ partner.consumer_id }}</strong>? Its key stops working
            immediately, and this cannot be undone. A partner that has been billed
            cannot be deleted at all — the server refuses, because their billing
            history is a financial record.
          </p>
          <div class="form-actions">
            <button type="button" class="danger-action" :disabled="saving" @click="confirmDelete">
              {{ saving ? 'Deleting…' : 'Delete' }}
            </button>
            <button type="button" class="secondary-action" :disabled="saving" @click="confirmingDelete = null">
              Cancel
            </button>
          </div>
        </div>
      </template>
    </section>
  </div>
</template>

<style scoped>
.partners-view { max-width: 1200px; }
.view-heading { display: flex; align-items: center; justify-content: space-between; gap: 1rem; margin-bottom: 1.5rem; }
.view-title { font-size: 1.25rem; font-weight: 650; }
.partners-section { margin-bottom: 2rem; }
.section-heading { display: flex; align-items: center; justify-content: space-between; gap: 1rem; flex-wrap: wrap; margin-bottom: 1rem; }
.section-heading h3 { font-size: 1rem; font-weight: 650; }
.partners-table, .models-table { width: 100%; border-collapse: collapse; font-size: .875rem; }
.partners-table th, .models-table th { padding: .75rem; border-bottom: 1px solid var(--border); color: var(--muted); font-weight: 550; text-align: left; }
.partners-table td, .models-table td { padding: .75rem; border-bottom: 1px solid var(--border); vertical-align: top; }
.partners-table tbody tr:hover { background: rgba(255, 255, 255, .02); }
.consumer { max-width: 14rem; overflow-wrap: anywhere; }
.numeric { font-variant-numeric: tabular-nums; }
.mode-badge { padding: .1rem .45rem; border: 1px solid currentColor; border-radius: .25rem; font-size: .75rem; font-weight: 600; }
.mode-badge.invoice { color: var(--warning); }
.mode-badge.reconciliation { color: #7dd3fc; }
.muted { color: var(--muted); }
.detail { margin-top: 1rem; }
.facts { display: grid; grid-template-columns: repeat(auto-fit, minmax(11rem, 1fr)); gap: 1rem; margin-bottom: 1.5rem; }
.facts dt { color: var(--muted); font-size: .75rem; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }
.facts dd { margin-top: .2rem; font-size: .875rem; overflow-wrap: anywhere; }
.price-note { margin-top: .5rem; color: var(--muted); font-size: .75rem; }
.editor { margin-bottom: 2rem; }
.editor h3 { margin-bottom: 1rem; font-size: 1rem; font-weight: 650; }
.form-grid { display: grid; grid-template-columns: repeat(auto-fit, minmax(14rem, 1fr)); gap: .75rem; margin-bottom: 1rem; }
.form-grid label { display: grid; gap: .3rem; color: var(--muted); font-size: .75rem; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }
.form-grid input, .form-grid select { text-transform: none; letter-spacing: normal; font-weight: 400; }
.models-fieldset { margin-bottom: 1.25rem; padding: 1rem; border: 1px solid var(--border); border-radius: .375rem; }
.models-fieldset legend { padding: 0 .35rem; color: var(--muted); font-size: .75rem; font-weight: 600; letter-spacing: .04em; text-transform: uppercase; }
.fieldset-note { margin-bottom: .75rem; color: var(--muted); font-size: .8125rem; }
.model-row { display: grid; grid-template-columns: minmax(0, 2fr) repeat(3, minmax(0, 1fr)) auto; gap: .5rem; align-items: center; margin-bottom: .5rem; }
.model-row input { width: 100%; }
.model-row input[aria-invalid='true'] { border-color: var(--error); }
.form-actions { display: flex; gap: .75rem; margin-top: 1rem; }
.secondary-action { background: transparent; border: 1px solid var(--border); color: var(--muted); }
.secondary-action:hover { background: rgba(255, 255, 255, .04); color: var(--fg); }
.danger-action { background: transparent; border: 1px solid var(--error); color: var(--error); }
.danger-action:hover { background: rgba(239, 68, 68, .12); }
.detail-actions { display: flex; gap: .75rem; margin-top: 1.25rem; }
.confirm-delete { margin-top: 1.25rem; padding: 1rem; border: 1px solid var(--error); border-radius: .375rem; background: rgba(239, 68, 68, .08); }
.confirm-delete p { font-size: .875rem; }
.link-button { padding: 0; background: transparent; color: var(--accent); font-size: .8125rem; text-decoration: underline; }
.link-button:hover { background: transparent; color: var(--accent-hover); }
.link-button:disabled { color: var(--muted); }
.loading-state, .empty-state { padding: 2rem 1rem; color: var(--muted); text-align: center; }
.error-banner { margin-bottom: 1rem; padding: .75rem 1rem; color: #fca5a5; background: rgba(239, 68, 68, .1); border: 1px solid var(--error); border-radius: .375rem; }
.sr-only { position: absolute; width: 1px; height: 1px; padding: 0; margin: -1px; overflow: hidden; clip: rect(0, 0, 0, 0); white-space: nowrap; border: 0; }
@media (max-width: 700px) {
  .partners-table, .models-table { display: block; overflow-x: auto; }
  .model-row { grid-template-columns: 1fr 1fr; }
}
</style>
