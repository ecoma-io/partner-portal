# Documentation

This directory holds the two things prose is good at here: **how a request
travels through the process**, and **why a design decision was made the way it
was**. Anything that can be read off the code — field names, defaults, routes —
lives in [`config.example.yaml`](../config.example.yaml) and under `src/`, and is
linked rather than copied.

## Where to start

| If you want to… | Read |
|---|---|
| Run it | [`README.md`](../README.md) — quick start, endpoints, operations |
| Understand the whole process end to end | [`architecture/overview.md`](architecture/overview.md) |
| Know why a guarantee exists before changing it | the ADR that owns it, below |
| Change code in this repository | [`AGENTS.md`](../AGENTS.md) |
| Open a pull request | [`CONTRIBUTING.md`](../CONTRIBUTING.md) |
| Configure a deployment | [`config.example.yaml`](../config.example.yaml) |
| Report a vulnerability | [`SECURITY.md`](../SECURITY.md) — never a public issue |

## Architecture

[`architecture/overview.md`](architecture/overview.md) is the map: the module
boundaries, the startup sequence, the non-streaming and streaming request
lifecycles, the metering state machine, the shutdown sequence, the data model,
and the isolation model. It describes the process as built; the ADRs below
describe the choices that shaped it.

## Architecture Decision Records

Each ADR states the forces at play, the decision, what was rejected and why,
what follows from the choice, and the files that evidence it. They are the
answer to "why is it like this?" and the place to argue with before changing one
of the invariants listed in [`AGENTS.md`](../AGENTS.md).

| # | Decision | In one line |
|---|---|---|
| [0001](adr/0001-single-upstream-not-a-routing-gateway.md) | Single upstream, not a routing gateway | One upstream, three paths, no failover — routing is a different product |
| [0002](adr/0002-sqlite-wal-full-sync-single-writer.md) | SQLite WAL + `synchronous=FULL`, one write owner | Durability and a single writer, so two instances can share one file |
| [0003](adr/0003-accept-then-finalize-and-crash-recovery.md) | Accept-then-finalize, recover stale `in_flight` at startup | A request is durable before the upstream is contacted; a crash cannot lose it |
| [0004](adr/0004-bounded-queue-backpressure-not-drop.md) | Bounded queue applies backpressure; readiness degrades | Metering is never dropped, and the load balancer is told when it cannot keep up |
| [0005](adr/0005-raw-and-rollup-one-transaction.md) | Raw row and hourly rollup in one transaction | The rollup is derived exactly once, from the transition that makes it true |
| [0006](adr/0006-usage-extraction-and-never-fabricate.md) | Per-endpoint extraction; never fabricate usage | No reported usage stays `NULL` with a `usage_status`, never `0` |
| [0007](adr/0007-sse-invalidation-only-data-version.md) | SSE invalidates; `PRAGMA data_version` is the signal | The stream carries no data, so it can be neither stale nor leaky |
| [0008](adr/0008-server-side-consumer-identity.md) | Server-side consumer identity | Identity comes from the credential, never from the request |
| [0009](adr/0009-streaming-stays-incremental.md) | Streaming stays incremental | Frames are forwarded as they arrive; scanning is bounded at 256 KiB |
| [0010](adr/0010-bodies-are-never-stored.md) | Bodies are never stored | The ledger is metadata; prompts and completions are not persisted |
| [0011](adr/0011-manager-password-cross-consumer-view.md) | Manager password: one deliberate cross-consumer view | An operator password opens dashboard-only usage across consumers — superseded by [0013](adr/0013-manager-sees-all-consumers.md), which removed its allow-list |
| [0012](adr/0012-per-key-model-allow-list.md) | Per-key model allow-list | Each key names the models it may call (strict-by-default); a refused model never reaches the upstream or the ledger, and `/v1/models` is filtered to the same list |
| [0013](adr/0013-manager-sees-all-consumers.md) | Manager sees every consumer; config carries only what it must | The manager allow-list is gone — `consumers=` is a filter fed from the ledger, `keys[].metadata` is deleted, and the listen address is `PARTNER_PORTAL_LISTEN` |

### Adding an ADR

- **One decision per record**, numbered in sequence: `NNNN-kebab-case-title.md`.
- **Same five sections** — Context / Decision / Alternatives considered /
  Consequences / Evidence — so a reader can skip to the part they need.
- **Evidence cites real paths** (`src/...`), and the file must exist. An ADR whose
  evidence does not resolve is a defect in the ADR.
- **An accepted ADR is not edited.** A decision that changes gets a new ADR that
  supersedes it; the old one keeps its number and gains a `Superseded by NNNN`
  line. The history of a decision is the point of keeping them.
