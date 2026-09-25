## What this changes

<!-- One paragraph. If the change is larger than a paragraph, the pull request is
     larger than it should be — see CONTRIBUTING.md on keeping them small. -->

Closes #

## Why

<!-- The defect or the need behind it, and what the current behaviour costs. -->

## The quiet direction

<!--
  Required for anything touching metering, recovery, retention, usage extraction
  or a dashboard query. What happens when this change is wrong *without saying
  so* — a dropped record, a zero where the upstream said nothing, a query that
  returns somebody else's rows, a rollup applied twice — and which test goes red
  when it happens. "N/A" is an acceptable answer only outside those areas.
-->

## How it was verified

<!--
  What you actually ran, and what it printed. Not "tests pass" — the commands.
  For anything in the request path, say whether you exercised it against a real
  upstream, the mock, or neither.
-->

- [ ] `cargo fmt --all -- --check`
- [ ] `cargo clippy --all-targets --all-features -- -D warnings`
- [ ] `cargo test --all-features`
- [ ] Dashboard: `pnpm install --frozen-lockfile && pnpm lint && pnpm typecheck && pnpm build` (if it changed)
- [ ] `docker build -t partner-portal:test .` plus `scripts/docker-smoke-test.sh` (if the image, the Dockerfile or the config changed)
- [ ] `deploy/smoke-test.sh` (if the deployment, the edge or the ledger's use by two instances changed)

## Notes for the reviewer

<!--
  Design decisions you made and the alternatives you rejected; anything you are
  unsure about; anything deliberately left out. Signed commits and the merge
  queue are the process — the reviewer should not have to ask about them.
-->
