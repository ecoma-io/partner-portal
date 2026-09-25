/**
 * Conventional Commits with this repository's scopes.
 *
 * Enforced by the `commit-msg` hook (see `lefthook.yml`). The scope list is the
 * module map: `src/proxy`, `src/ledger`, `src/auth`, `src/dashboard`, `src/admin`,
 * `src/web`, `src/config`, `src/telemetry`, `dashboard/` (the Vue app), plus the
 * repository-level scopes. When a new module lands, its scope lands here in the
 * same commit — and in the `commit-msg` fallback in `lefthook.yml`, which is what
 * checks the same rules when commitlint is not installed.
 *
 * There is no `package.json` in this repository, so commitlint is not installed
 * by default. This file is the contract either way; `pnpm add -D @commitlint/cli
 * @commitlint/config-conventional` (or `npx --no-install`) activates the strict
 * path without changing the rules below.
 *
 * @type {import("@commitlint/types").UserConfig}
 */
export default {
  extends: ["@commitlint/config-conventional"],
  rules: {
    // `style` is deliberately absent: rustfmt owns formatting, so a `style:`
    // commit would only ever describe something the formatter should have done.
    "type-enum": [
      2,
      "always",
      [
        "feat",
        "fix",
        "docs",
        "chore",
        "refactor",
        "test",
        "perf",
        "build",
        "ci",
        "revert",
      ],
    ],
    "scope-enum": [
      2,
      "always",
      [
        // src/
        "proxy",
        "ledger",
        "auth",
        "dashboard",
        "admin",
        "web",
        "config",
        "telemetry",
        // dashboard/
        "dashboard-ui",
        // repository-level
        "deploy",
        "docs",
        "deps",
        "ci",
        "release",
        "repo",
      ],
    ],
    "subject-empty": [2, "never"],
    "header-max-length": [2, "always", 100],
    // Bodies and footers are prose; a wrapped paragraph is not a defect.
    "body-max-line-length": [0],
    "footer-max-line-length": [0],
  },
};
