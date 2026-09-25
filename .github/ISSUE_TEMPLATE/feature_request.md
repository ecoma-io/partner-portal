---
name: Feature request
about: A change to what partner-portal does — proxy behaviour, metering, the dashboard, deployment
title: "[FEAT] "
labels: ["enhancement", "needs triage"]
assignees: ""
---

<!--
  The bar for a feature here is not "is it useful" but "does it keep the
  accounting honest". A feature that makes a request harder to account for — a
  retry, a cached response, a code path that answers without reaching the
  ledger — is a change to the product's only output, so say how the new
  behaviour is recorded.
-->

## What you want to do

<!-- The task or need, from the operator's or the caller's point of view. -->

## Why the current behaviour does not get you there

<!-- What you do today instead, and what it costs. -->

## What you would expect instead

<!-- API shape, field names, dashboard surface, config key — as concrete as you
     can make it, without writing the implementation. -->

## How it would be accounted for

<!--
  Only if the change touches the request path: what happens to the ledger row
  for a request that takes this path? If the answer is "no row", say why that is
  correct rather than a gap. This is the section a review will read first.
-->

## Alternatives considered

<!-- Including: is there a way to do this without changing the proxy? -->

## Scope

- [ ] Proxy / request path
- [ ] Metering / ledger / recovery / retention
- [ ] Dashboard (API or UI)
- [ ] Configuration
- [ ] Deployment, image or release
- [ ] Documentation only
