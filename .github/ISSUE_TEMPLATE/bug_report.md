---
name: Bug report
about: Something behaves wrongly — a proxy response, a ledger record, the dashboard, a deploy
title: "[BUG] "
labels: ["bug", "needs triage"]
assignees: ""
---

<!--
  This is a metering system: the failures that matter most are the quiet ones.
  A request that was served but never recorded, a usage figure that is zero
  where the upstream said nothing, a total that is slightly too large. Those do
  not raise anything, so the report has to say what you expected the ledger (or
  the response) to say and what it said instead.

  Behaviour first, interpretation second. "The ledger row is missing" is a
  report; "the writer dropped it" is a theory — both are useful, but keep them
  in their own sections.
-->

## What happened

<!-- The observable behaviour. Include the exact command, request or click. -->

## What you expected instead

<!-- If a documented invariant disagrees with the behaviour, link the document
     and quote the line. Do not edit the prose to match the code. -->

## How to reproduce

<!--
  Minimal and concrete. If you cannot reproduce it, say so — that is important
  information, and a report that admits it is worth more than one that guesses.
-->

1.
2.
3.

**Frequency:** <!-- every time / intermittent (roughly how often) / once -->
**Started:** <!-- when you first saw it; what changed around then, if anything -->

## Environment

| | |
|---|---|
| partner-portal | <!-- a tag, or the commit (`GET /version` reports the commit) --> |
| How it runs | <!-- `cargo run`, the image (digest), docker compose, the rolling update --> |
| Deployment shape | <!-- one instance / two instances sharing one ledger --> |
| Storage | <!-- local volume, path; anything unusual about the filesystem --> |
| Upstream | <!-- OpenAI, an OpenAI-compatible provider, the mock --> |

## Evidence

<!--
  Whatever you have: log lines (with the surrounding context, not just the
  error), the `/healthz`, `/readyz` and `/version` bodies, the ledger rows from
  `/api/dashboard/requests`, `PRAGMA integrity_check` on the database. Redact
  partner keys and the upstream credential.
-->

## Impact

- [ ] Served traffic affected (requests failing, or failing for some callers)
- [ ] Accounting affected (a record is missing, duplicated, or wrong)
- [ ] Both

<!-- If accounting is affected, say which direction: usage that was served but
     not recorded, or usage recorded that was not served. -->

## Anything else

<!-- Theories, related issues, things you already tried and ruled out. -->
