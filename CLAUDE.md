@AGENTS.md

<!--
This file carries almost no guidance of its own — AGENTS.md does, and the import
above is the whole of the shared content. It is a regular file rather than a
symlink to that file on purpose: Git only reproduces a symlink on Windows when
`core.symlinks` is on, which needs Developer Mode or an elevated clone. Where it
is off, Git writes a one-line text file containing the path instead, so a Windows
contributor would get the literal string `AGENTS.md` and no guidance at all —
silently, since nothing errors. The `@` import resolves the same way on every
platform.
-->

## Claude-specific notes

- **Guide here, not in this file.** Anything that should reach Codex and opencode
  too belongs in `AGENTS.md`; add it there. This file is for what only applies to
  Claude Code.
- **No `.claude/` directory exists in this repository** — no settings, no hooks,
  no skills, no subagents. Nothing about working here depends on them, and adding
  one is a repository change like any other (issue, branch, draft pull request),
  not a local convenience.
- **Verify by running, not by reading.** The claims in `README.md`,
  `docs/architecture/overview.md` and the ADRs are checkable: `cargo test --lib`
  for the invariants, and the real binary plus a mock upstream for anything about
  streaming, metering, readiness or shutdown. Do not describe behaviour you have
  not executed — if a claim and the code disagree, that is a finding, and reporting
  it beats editing the prose to match.
- **A long-running process needs stopping.** `cargo run`, `cargo bench` and
  `pnpm --dir dashboard dev` are servers or sustained load; start them only when a
  task needs runtime evidence, and stop them when the evidence is captured.
- **Never edit a document to make it agree with a defect.** The invariants and the
  ADRs describe intent; if the code does not honour them, say so.
