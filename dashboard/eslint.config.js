// Flat ESLint config for the dashboard.
//
// Flat rather than the legacy `.eslintrc` because ESLint 9 is the only
// supported form going forward, and a config that has to be migrated later is a
// config that quietly stops running before it is migrated.
//
// Three rule sets, in this order:
//   * `@eslint/js` recommended — the language-level correctness rules.
//   * `typescript-eslint` recommended — the same, type-aware.
//   * `eslint-plugin-vue` flat/essential — template and SFC correctness, which is
//     the part a dashboard actually breaks on (unused components, wrong `v-for`
//     keys, mutating props).
//
// flat/essential rather than flat/recommended on purpose. The difference is
// almost entirely the HTML formatting rules (`max-attributes-per-line`,
// `singleline-html-element-content-newline`, `html-self-closing`): enabling them
// turns roughly forty deliberate one-line `<div>`s into warnings, and this repo
// has no HTML formatter that could ever fix them. A lint run that always prints
// forty warnings is a lint run nobody reads.
//
// Types, not lint, own type errors here: `vue-tsc` runs in `pnpm typecheck` and
// is the gate that fails CI. ESLint is for the mistakes the compiler accepts.

import js from '@eslint/js'
import vue from 'eslint-plugin-vue'
import tseslint from 'typescript-eslint'

export default tseslint.config(
  // Build output and dependencies are never linted: `dist` is generated and
  // `node_modules` is not ours to police.
  { ignores: ['dist/**', 'node_modules/**', 'coverage/**'] },

  js.configs.recommended,
  ...tseslint.configs.recommended,
  ...vue.configs['flat/essential'],

  {
    // `lang="ts"` blocks in an SFC are parsed by typescript-eslint, not by
    // espree — without this the `<script setup lang="ts">` in every component
    // fails to parse at all.
    files: ['**/*.vue'],
    languageOptions: {
      parserOptions: { parser: tseslint.parser },
    },
  },

  {
    // TypeScript resolves globals itself, and `@types/node` supplies the ones
    // `vite.config.ts` uses (`__dirname`). `no-undef` in a TypeScript project
    // only ever produces false positives on ambient declarations.
    files: ['**/*.{ts,vue}'],
    rules: { 'no-undef': 'off' },
  },

  {
    rules: {
      // The entry component and the shell are single-word files by convention
      // (`App.vue`, `views/Dashboard.vue`) and are referenced by the router and
      // the Vite entry, not by name lookup. Renaming them to satisfy this rule
      // would churn the two files that are hardest to review.
      'vue/multi-word-component-names': 'off',
    },
  },
)
