# Security policy

## Supported versions

This project is **pre-release**: no tag has been cut and no build is distributed.
There is no version matrix to publish.

**What is supported is the current `main` branch.** Security fixes land there and
ship in the next release. If you are running partner-portal at all, you are
running it from source or from an image you built, and updating means pulling
`main`.

## Reporting a vulnerability

**Do not open a public issue.** A public report tells an attacker where to look
before there is anything to upgrade to.

Report privately through GitHub's
[security advisory form](https://github.com/ecoma-io/partner-portal/security/advisories/new).
If that is unavailable to you, email **john.itvn@gmail.com** with `SECURITY` in
the subject line.

Please include:

- what an attacker can do, and what they need in order to do it;
- the affected commit or build;
- a reproduction — the smaller the better. A request, a config, and an observed
  response beats a description.

## What to expect

This project is maintained by one person, so these are honest targets rather than
a contractual guarantee:

| Stage | Target |
|---|---|
| Acknowledgement | within 3 days |
| Initial assessment | within 7 days |
| Fix or documented mitigation | within 30 days |

You will be told which of those applies as soon as the assessment is done,
including when the answer is that the reported behaviour is not a vulnerability.

Fixes are released before details are published. Credit goes to the reporter
unless you ask otherwise.

## In scope

This process listens on a network, holds an upstream credential, and answers
per-consumer questions about traffic. Those three facts define the attack
surface. Anything below is a vulnerability and belongs in a private report:

| Class | What would qualify |
|---|---|
| **Auth bypass** | A request reaching a proxied route, a dashboard route or the SSE stream without a valid key; a bearer token parsed more permissively than the config means (scheme casing, whitespace, non-ASCII, a key that is a prefix of another); a disabled or removed key still being accepted after a reload |
| **Consumer isolation** | Any path by which one key observes another consumer's rows — a query missing its `consumer_id` filter, a cursor or filter parameter that widens the scope, a rollup bucket aggregated across consumers |
| **Credential leakage** | A local key or the upstream `api_key` appearing anywhere it can be read: a log line, an error body, `/version`, a dashboard response, an upstream request that forwards the client's own `Authorization` alongside the configured one, or a `401` body that echoes what was presented |
| **SQL injection** | Any request-derived value interpolated into a statement instead of bound as a parameter — `consumer_id` aside, the request list takes `range`, `start`, `end`, `model`, `status`, `cursor` and `limit` from the query string |
| **Resource exhaustion** | A request that makes memory or disk grow without bound: a streaming event larger than the 256 KiB scan cap buffered anyway, a non-streaming body past the 32 MiB buffer cap retained instead of truncated, a queue that grows past its bound instead of applying backpressure, a body past `server.max_body_size` accepted, or a single authenticated consumer able to exhaust the process with connections |
| **SSE abuse** | A subscriber that can read another consumer's invalidation stream, a payload that carries usage data, or an unbounded number of streams from one credential that degrades the process for others |
| **Path handling** | A request path that escapes the embedded asset table (traversal, encoded separators, a symlink in `dashboard/dist` at build time), or that reaches the upstream with a different path than the router matched |

## Known, deliberate, and not a vulnerability

Reporting one of these is welcome as an ordinary bug report, but it is not a
security finding:

- **The listener is plain HTTP.** There is no TLS termination in this binary, by
  design — put it behind something that speaks TLS. A deployment exposing the
  listener directly is an operator decision, documented in
  [`deploy/`](deploy/).
- **There is no rate limiting, quota or billing enforcement.** The ledger records
  what happened; it does not stop anything.
- **There is no cross-consumer or administrative API view.** The dashboard is
  self-scoped by construction, and the SQLite file on disk is the operator's
  cross-consumer view. Querying it directly is the intended answer, not a gap.
- **`config.yaml` holds the upstream credential and every local key**, and the
  file is not ignored by Git. Protecting it — permissions, an untracked path,
  a secret mount — is the operator's responsibility.
- **The shipped dashboard cannot authenticate its own SSE stream** (its
  `EventSource` sends no `Authorization` header). It fails closed: the stream is
  rejected, the "Live" badge stays disconnected. It is a defect, recorded in
  [`AGENTS.md`](AGENTS.md#known-gaps), and not an exposure.
- **Metrics and traces are not exported.** There is no `/metrics` endpoint to
  scrape and none is planned for this tier.

## Out of scope

- **The upstream provider** — its behaviour, its terms, its data handling.
- **Vulnerabilities in third-party crates and system packages with no
  exploitable path through this code.** Report those upstream; send them here if
  you can show the path.
- **Findings that require an attacker to already control the host, the process,
  the configuration file, or the SQLite file.** At that point the ledger and the
  upstream credential are both already readable.
- **Volumetric denial of service** against an exposed listener. That is a network
  and deployment problem, not a code path this project can close.
- **Anything that needs the shipped dashboard to be rebuilt or modified** by the
  attacker, unless the modification is possible through a request.

## Disclosure

Please give us time to ship a fix before publishing. We will keep you informed at
each stage above, and we will not ask you to stay silent past the point where a
fix is available — if a fix is going to take longer than 30 days, a mitigation and
a disclosure plan will be agreed with you rather than assumed.
